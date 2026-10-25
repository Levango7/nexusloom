"""类型化输入的四条提交口（P1 #7）：每条口都要有"确实到达闸门"的判据。

为什么判据要按口分组：`tests/test_input_schema.py` 全绿只证明**校验实现**是对的，
不证明 CLI/HTTP/MCP/webhook 真去调用它——本仓栽过"函数正确但没人调用"的形状。
所以每条口至少三条：

1. 非法载荷 → 该口必须拒，且**引擎没被调用**（拒绝必须发生在执行之前，否则只是事后道歉）；
2. 合法载荷 → 闸门产出的那份文档（含 `default` 填充）要真的交到引擎手里；
3. **没声明 inputs 的流水线照旧**——反向对照，防止闸门误伤随包 20 条流水线。

另有 `TestOneImplementation`：闸门只有一处实现，四条口不得各写一套判据
（各写一套迟早漂，漂了就没有"同一载荷同一结果"这条承诺了）。
"""
from __future__ import annotations

import io
import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from loom_core.admin_api import AdminAPI, AdminHandler
from loom_core.mcp_server import MCPServer
from loom_core.webhooks import TOKEN_HEADER, Webhook, handle_inbound

PROJECT = Path(__file__).resolve().parent.parent

TYPED_YAML = """
name: typed_demo
pipeline:
  inputs:
    type: object
    required: [topic]
    additionalProperties: false
    properties:
      topic: {type: str, minLength: 2}
      depth: {type: int, minimum: 1, maximum: 5, default: 3}
agents:
  - name: tldr
    version: "0.1"
    dependencies: []
    config: {max_sentences: 2}
topology:
  levels: [["tldr"]]
  edges: []
"""

PLAIN_YAML = """
name: plain_one
agents:
  - name: tldr
    version: "0.1"
    dependencies: []
    config: {max_sentences: 2}
topology:
  levels: [["tldr"]]
  edges: []
"""


class FakeTask:
    def __init__(self, task_id: str, pipeline: str):
        self.id = task_id
        self.status = "running"          # 普通字符串：admin 侧走 str(status) 分支
        self.pipeline_name = pipeline
        self.result: dict = {}
        self.error = None
        self.output_path = None
        self.output_content = None


class RecordingOrch:
    """假引擎：在 run_plan **调用当下**就把输入文档读走。

    必须在当下读：临时输入文件随后会被 wait 分支或 reaper 删掉，等断言时再读
    会偶发拿到"文件不存在"，那种红会被误读成闸门没接上。
    """

    def __init__(self):
        self.calls: list[dict] = []

    def run_plan(self, plan, input_file=None, task_id=None, wait=True, owner=""):
        text = Path(input_file).read_text(encoding="utf-8") if input_file else None
        self.calls.append({"input_text": text, "owner": owner, "wait": wait,
                           "task_id": task_id, "pipeline": plan.pipeline_name})
        return FakeTask(task_id or "t-1", plan.pipeline_name)

    def get_task(self, task_id):
        return None


@pytest.fixture
def root(tmp_path, monkeypatch):
    """LOOM_ROOT 指向临时项目根：一条声明了 inputs，一条没声明。"""
    p = tmp_path / "pipelines"
    p.mkdir(parents=True, exist_ok=True)
    (p / "typed_demo.yaml").write_text(TYPED_YAML, encoding="utf-8")
    (p / "plain_one.yaml").write_text(PLAIN_YAML, encoding="utf-8")
    monkeypatch.setenv("LOOM_ROOT", str(tmp_path))
    return tmp_path


def _handler(path: str, orch, principal=None) -> AdminHandler:
    h = AdminHandler.__new__(AdminHandler)
    h.path = path
    h.headers = {}
    h.orch = orch
    h.api_key = None
    h.server_host = "127.0.0.1"
    h.dashboard_dir = None
    h.principal = principal
    h._json = MagicMock()
    return h


def _sent(handler) -> tuple[dict, int | None]:
    args, kwargs = handler._json.call_args
    return args[0] if args else kwargs.get("body", {}), (args[1] if len(args) > 1 else None)


def _post(handler, path: str, body: bytes = b""):
    handler.path = path
    handler._serve_static = MagicMock(return_value=False)
    handler._check_auth = MagicMock(return_value=True)
    handler.headers = {"Content-Length": str(len(body))}
    handler.rfile = io.BytesIO(body)
    handler.do_POST()


# ─── 1. POST /api/workflows/{name}/runs ──────────────────

class TestRunsEndpoint:
    def test_route_dispatch_reaches_handler(self):
        """路由判据用"方法被调用"而不是返回码：写错分支时返回码也可能是别的 200。"""
        h = _handler("/api/workflows/typed_demo/runs", RecordingOrch())
        with patch.object(AdminHandler, "_handle_create_run", autospec=True) as m:
            _post(h, "/api/workflows/typed_demo/runs", b'{"inputs":{"topic":"x"}}')
            m.assert_called_once()
            assert m.call_args.args[1] == "typed_demo"

    def test_bad_payload_is_400_with_locatable_field(self, root):
        orch = RecordingOrch()
        h = _handler("/api/workflows/typed_demo/runs", orch)
        h._handle_create_run("typed_demo", json.dumps({"inputs": {"depth": 9}}).encode())
        body, status = _sent(h)
        assert status == 400, body
        assert body["code"] == "invalid_inputs"
        # 两条错都要报出来，且各自带字段名——只报第一条会让人改一轮试一轮
        assert {f["field"] for f in body["fields"]} == {"topic", "depth"}, body
        assert any("maximum" in f["reason"] for f in body["fields"]), body
        assert orch.calls == [], "非法载荷不该已经交给引擎"

    def test_boundary_value_is_400_not_200(self, root):
        orch = RecordingOrch()
        h = _handler("/x", orch)
        h._handle_create_run("typed_demo", json.dumps({"inputs": {"topic": "a"}}).encode())
        assert _sent(h)[1] == 400, "minLength=2 却放行了 1 个字符"

    def test_valid_payload_renders_defaults_into_the_doc(self, root):
        orch = RecordingOrch()
        h = _handler("/x", orch)
        h._handle_create_run("typed_demo",
                             json.dumps({"inputs": {"topic": "云成本"}}).encode())
        body, status = _sent(h)
        assert status is None, body
        assert orch.calls and "## topic" in orch.calls[0]["input_text"]
        # 默认值由闸门填上并落到交给引擎的那份文档里
        assert "## depth" in orch.calls[0]["input_text"], orch.calls[0]["input_text"]

    def test_owner_is_the_principal_not_a_hardcoded_string(self, root):
        """被打印/入库的身份必须跨进程可解析：owner 只能来自鉴权后的 principal。"""
        orch = RecordingOrch()
        h = _handler("/x", orch, principal=MagicMock(id="alice"))
        h._handle_create_run("typed_demo",
                             json.dumps({"inputs": {"topic": "云成本"}}).encode())
        assert orch.calls[0]["owner"] == "alice", orch.calls[0]

    def test_unknown_pipeline_name_is_400(self, root):
        h = _handler("/x", RecordingOrch())
        h._handle_create_run("ghost_pipe", b'{"inputs": {"topic": "ab"}}')
        body, status = _sent(h)
        assert status == 400 and "ghost_pipe" in body["error"], body

    def test_path_segment_cannot_escape_the_whitelist(self, root):
        """`..%2f` 之类先被路由白名单形状拦掉，不会进到 Scheduler 的名字拼接。"""
        h = _handler("/api/workflows/x/runs", RecordingOrch())
        with patch.object(AdminHandler, "_handle_create_run", autospec=True) as m:
            _post(h, "/api/workflows/..%2f..%2fetc/runs", b'{"inputs":{"topic":"ab"}}')
            m.assert_not_called()

    def test_undeclared_pipeline_still_submits(self, root):
        """反向对照：没声明 inputs 的流水线走同一端点照样能提交（闸门不误伤）。"""
        orch = RecordingOrch()
        h = _handler("/x", orch)
        h._handle_create_run("plain_one",
                             json.dumps({"inputs": {"任意": "东西"}}).encode())
        assert orch.calls and orch.calls[0]["pipeline"] == "plain_one"


# ─── 2. GET /api/workflows/{name}/inputs ──────────────────

class TestDeclarationReadSurface:
    def test_route_dispatch_reaches_handler(self):
        h = _handler("/api/workflows/typed_demo/inputs", RecordingOrch())
        h._serve_static = MagicMock(return_value=False)
        h._check_auth = MagicMock(return_value=True)
        h.headers = {}
        with patch.object(AdminHandler, "_handle_workflow_inputs", autospec=True) as m:
            h.do_GET()
            m.assert_called_once()
            assert m.call_args.args[1] == "typed_demo"

    def test_returns_the_declaration_for_form_generation(self, root):
        h = _handler("/x", RecordingOrch())
        h._handle_workflow_inputs("typed_demo")
        body, status = _sent(h)
        assert status is None, body
        assert body["declared"] is True
        assert body["inputs"]["required"] == ["topic"]
        assert body["inputs"]["properties"]["depth"]["default"] == 3

    def test_undeclared_reports_declared_false_not_an_error(self, root):
        h = _handler("/x", RecordingOrch())
        h._handle_workflow_inputs("plain_one")
        body, status = _sent(h)
        assert status is None and body["declared"] is False, body


# ─── 3. POST /api/tasks（两条载荷形状） ────────────────────

class TestTasksEndpoint:
    def test_inputs_shape_gates_before_engine(self, root):
        orch = RecordingOrch()
        h = _handler("/api/tasks", orch)
        h._handle_submit_task(json.dumps(
            {"pipeline": "typed_demo", "inputs": {"depth": "many"}}).encode())
        body, status = _sent(h)
        assert status == 400 and body["code"] == "invalid_inputs", body
        assert orch.calls == []

    def test_query_shape_on_typed_pipeline_is_refused(self, root):
        """关门判据：声明了类型化输入的流水线，不能用自由文本绕过闸门。"""
        orch = RecordingOrch()
        h = _handler("/api/tasks", orch)
        h._handle_submit_task(json.dumps(
            {"pipeline": "typed_demo", "query": "云成本"}).encode())
        body, status = _sent(h)
        assert status == 400, body
        assert orch.calls == [], "自由文本绕过了声明——那声明就是装饰"
        assert "inputs" in body["fields"][0]["reason"], body

    def test_query_shape_on_undeclared_pipeline_is_unchanged(self, root):
        """反向对照：随包那些没有声明的流水线，query 提交的文档与昨天同一份。"""
        orch = RecordingOrch()
        h = _handler("/api/tasks", orch)
        h._handle_submit_task(json.dumps(
            {"pipeline": "plain_one", "query": "云成本", "title": "标题"}).encode())
        assert orch.calls[0]["input_text"] == "# 标题\n\n## 查询\n\n云成本\n", \
            orch.calls[0]["input_text"]

    def test_missing_both_payloads_still_400(self, root):
        h = _handler("/api/tasks", RecordingOrch())
        h._handle_submit_task(b'{"title": "test"}')
        body, status = _sent(h)
        assert status == 400 and "query" in body["error"], body


# ─── 4. MCP run_workflow / generate_document ──────────────

class TestMCPSurface:
    def _call(self, orch, tool, arguments, req_id=7):
        s = MCPServer(orch=orch)
        return s._handle_request({"jsonrpc": "2.0", "id": req_id,
                                  "method": "tools/call",
                                  "params": {"name": tool, "arguments": arguments}})

    def test_run_workflow_bad_inputs_is_tool_error_without_running(self, root):
        orch = RecordingOrch()
        resp = self._call(orch, "run_workflow",
                          {"name": "typed_demo", "inputs": {"depth": 9}})
        assert resp["result"]["isError"] is True
        text = resp["result"]["content"][0]["text"]
        assert "topic" in text, text
        assert orch.calls == [], "MCP 侧同样是先拒后跑"

    def test_run_workflow_fills_defaults(self, root):
        orch = RecordingOrch()
        resp = self._call(orch, "run_workflow",
                          {"name": "typed_demo", "inputs": {"topic": "云成本"}})
        assert resp["result"].get("isError") is not True, resp
        assert "## depth" in orch.calls[0]["input_text"], orch.calls[0]

    def test_generate_document_on_typed_pipeline_is_refused(self, root):
        orch = RecordingOrch()
        resp = self._call(orch, "generate_document",
                          {"query": "云成本", "pipeline": "typed_demo"})
        assert resp["result"]["isError"] is True
        assert orch.calls == [], resp

    def test_generate_document_on_undeclared_pipeline_runs(self, root):
        orch = RecordingOrch()
        resp = self._call(orch, "generate_document",
                          {"query": "云成本", "pipeline": "plain_one", "title": "标题"})
        assert resp["result"].get("isError") is not True, resp
        assert orch.calls[0]["input_text"] == "# 标题\n\n## 查询\n\n云成本\n"

    def test_get_pipeline_info_exposes_the_declaration(self, root):
        orch = RecordingOrch()
        resp = self._call(orch, "get_pipeline_info", {"name": "typed_demo"})
        info = json.loads(resp["result"]["content"][0]["text"])
        assert info["inputs"]["required"] == ["topic"], info
        assert info["node_count"] == 1

    def test_parse_failure_does_not_leave_temp_input(self, root):
        """闸门上移后输入文件在 parse 之后才创建：坏流水线名不能留下孤儿临时文件。"""
        tmp = Path(tempfile.gettempdir())
        before = set(tmp.glob("mcp_*"))
        orch = RecordingOrch()
        self._call(orch, "run_workflow", {"name": "ghost_pipe", "inputs": {}})
        after = set(tmp.glob("mcp_*"))
        assert after <= before, f"新增孤儿临时文件: {sorted(after - before)}"


# ─── 5. 入站 webhook ─────────────────────────────────────

class TestWebhookSurface:
    def _wh(self, pipeline):
        return {"hook": Webhook(name="hook", pipeline=pipeline,
                                secret_env="TEST_TI_SECRET")}

    def test_bad_json_object_is_400_and_not_submitted(self, root, state_dir, monkeypatch):
        monkeypatch.setenv("TEST_TI_SECRET", "s3cret")
        orch = RecordingOrch()
        status, payload = handle_inbound(
            "hook", self._wh("typed_demo"), json.dumps({"inputs": {"depth": 9}}).encode(),
            headers={TOKEN_HEADER: "s3cret"}, remote="r", orch=orch)
        assert status == 400, payload
        assert payload["code"] == "invalid_inputs" and orch.calls == []

    def test_valid_object_submits_rendered_doc(self, root, state_dir, monkeypatch):
        monkeypatch.setenv("TEST_TI_SECRET", "s3cret")
        orch = RecordingOrch()
        status, payload = handle_inbound(
            "hook", self._wh("typed_demo"), json.dumps({"topic": "云成本"}).encode(),
            headers={TOKEN_HEADER: "s3cret"}, remote="r", orch=orch)
        assert status == 202, payload
        assert "## topic" in orch.calls[0]["input_text"]
        assert orch.calls[0]["owner"] == "webhook:hook"

    def test_free_text_on_typed_pipeline_is_400(self, root, state_dir, monkeypatch):
        """webhook 的自由文本载荷同样不能绕过声明。"""
        monkeypatch.setenv("TEST_TI_SECRET", "s3cret")
        orch = RecordingOrch()
        status, payload = handle_inbound(
            "hook", self._wh("typed_demo"), "整篇自由文本".encode(),
            headers={TOKEN_HEADER: "s3cret"}, remote="r", orch=orch)
        assert status == 400, payload
        assert orch.calls == []

    def test_free_text_on_undeclared_pipeline_is_unchanged(
            self, root, state_dir, monkeypatch):
        """反向对照：没声明的流水线，文本载荷渲染出的文档与改动前逐字节相同。"""
        from loom_core.input_docs import render_inputs_doc
        monkeypatch.setenv("TEST_TI_SECRET", "s3cret")
        orch = RecordingOrch()
        status, _payload = handle_inbound(
            "hook", self._wh("plain_one"), "整篇自由文本".encode(),
            headers={TOKEN_HEADER: "s3cret"}, remote="r", orch=orch)
        assert status == 202, _payload
        assert orch.calls[0]["input_text"] == render_inputs_doc("hook", "整篇自由文本")

    def test_invalid_inputs_is_audited(self, root, state_dir, monkeypatch):
        monkeypatch.setenv("TEST_TI_SECRET", "s3cret")
        handle_inbound("hook", self._wh("typed_demo"), b'{"depth": 9}',
                       headers={TOKEN_HEADER: "s3cret"}, remote="r",
                       orch=RecordingOrch())
        line = json.loads((state_dir / "audit" / "webhooks.jsonl")
                          .read_text(encoding="utf-8").strip().splitlines()[-1])
        assert line["reason"] == "invalid_inputs" and line["http_status"] == 400, line


@pytest.fixture
def state_dir(tmp_path, monkeypatch):
    d = tmp_path / "state"
    monkeypatch.setenv("LOOM_STATE_DIR", str(d))
    return d


# ─── 6. CLI ─────────────────────────────────────────────

class TestCLISurface:
    @staticmethod
    def _plan(name):
        from loom_core.scheduler import Scheduler
        return Scheduler().parse(name, verify_lock=False)

    @staticmethod
    def _args(argv, monkeypatch):
        """argv → Namespace。`_PIPELINE_NAMES_CACHE` 是进程内缓存（`--pipeline` 的
        choices 来源），前面别的 CLI 用例已经把真仓流水线名灌进去了；这里按 root
        夹具重算一次，monkeypatch 在 teardown 把缓存还原回去。"""
        import importlib
        run_mod = importlib.import_module("run")
        monkeypatch.setattr(run_mod, "_PIPELINE_NAMES_CACHE", None, raising=False)
        return run_mod.build_arg_parser().parse_args(argv)

    def test_set_input_parses_json_values_and_reaches_gate(self, root, monkeypatch):
        """`--set-input depth=9` 要落成真整数并**在闸门处被拦**，不是当字符串放行。"""
        import importlib
        args = self._args(["-p", "typed_demo", "--set-input", "depth=9"], monkeypatch)
        with pytest.raises(SystemExit) as exc:
            importlib.import_module("run")._resolve_cli_input(
                args, self._plan("typed_demo"), "cli-1")
        assert exc.value.code == 2

    def test_cli_renders_defaults_for_valid_payload(self, root, monkeypatch):
        import importlib

        from loom_core.input_schema import prepare_inputs
        args = self._args(["-p", "typed_demo", "--inputs-json", '{"topic": "云成本"}'],
                          monkeypatch)
        plan = self._plan("typed_demo")
        path = importlib.import_module("run")._resolve_cli_input(args, plan, "cli-2")
        text = Path(path).read_text(encoding="utf-8")
        assert text == prepare_inputs(plan, {"topic": "云成本", "depth": 3})
        assert "## depth" in text

    def test_free_text_input_on_typed_pipeline_refuses(self, root, tmp_path, monkeypatch):
        import importlib
        doc = tmp_path / "in.md"
        doc.write_text("# 标题\n\n正文\n", encoding="utf-8")
        args = self._args(["-p", "typed_demo", str(doc)], monkeypatch)
        with pytest.raises(SystemExit) as exc:
            importlib.import_module("run")._resolve_cli_input(
                args, self._plan("typed_demo"), "cli-3")
        assert exc.value.code == 2

    def test_free_text_input_on_undeclared_pipeline_passes_through(
            self, root, tmp_path, monkeypatch):
        """反向对照：位置参数在没声明的流水线上仍是原路径、原内容。"""
        import importlib
        doc = tmp_path / "in.md"
        doc.write_text("# 标题\n\n正文\n", encoding="utf-8")
        args = self._args(["-p", "plain_one", str(doc)], monkeypatch)
        got = importlib.import_module("run")._resolve_cli_input(
            args, self._plan("plain_one"), "cli-4")
        assert got == str(doc)

    def test_typed_and_file_input_are_mutually_exclusive(self, root, tmp_path,
                                                         monkeypatch):
        """两条输入形状同时给 = 悄悄忽略其中一个的形状，必须明确拒。"""
        import importlib
        doc = tmp_path / "in.md"
        doc.write_text("# 标题\n\n正文\n", encoding="utf-8")
        args = self._args(["-p", "typed_demo", str(doc), "--set-input", "topic=云成本"],
                          monkeypatch)
        with pytest.raises(SystemExit) as exc:
            importlib.import_module("run")._resolve_cli_input(
                args, self._plan("typed_demo"), "cli-5")
        assert exc.value.code == 2

    def test_real_cli_bad_typed_input_exits_2_with_field(self, root):
        """端到端一条：argv → 闸门 → 退出码，中间没人替 CLI 兜错。"""
        proc = subprocess.run(
            [sys.executable, str(PROJECT / "run.py"), "-p", "typed_demo",
             "--set-input", "depth=9"],
            capture_output=True, text=True, timeout=180,
            env={**os.environ, "LOOM_ROOT": str(root)})
        assert proc.returncode == 2, (proc.stdout[-800:], proc.stderr[-800:])
        assert "topic" in proc.stdout + proc.stderr

    def test_help_lists_the_typed_flags(self):
        proc = subprocess.run([sys.executable, str(PROJECT / "run.py"), "--help"],
                              capture_output=True, text=True, timeout=120)
        visible = proc.stdout + proc.stderr
        assert "--inputs-json" in visible and "--set-input" in visible


# ─── 7. 单一实现：四条口不许各写一套判据 ──────────────────

class TestOneImplementation:
    GATED = {
        "loom_core/admin_api.py": ["_handle_create_run", "_handle_submit_task"],
        "loom_core/mcp_server.py": ["_submit_plan_run"],
        "loom_core/webhooks.py": ["handle_inbound"],
        "run.py": ["_resolve_cli_input"],
    }

    @pytest.mark.parametrize("rel,funcs", sorted(GATED.items()))
    def test_surface_file_calls_the_shared_gate(self, rel, funcs):
        src = (PROJECT / rel).read_text(encoding="utf-8")
        assert "from .input_schema import" in src or \
            "from loom_core.input_schema import" in src or rel == "run.py", src[:200]
        for fn in funcs:
            assert fn in src, f"{rel} 里找不到 {fn}——提交口被改名/删掉时这条要红"
        assert "prepare_inputs(" in src, f"{rel} 没调用共享闸门"

    def test_no_second_validator_exists(self):
        """闸门只有一处：别的模块不该自带第二套 inputs 校验实现。"""
        hits = []
        for p in PROJECT.glob("loom_core/*.py"):
            if p.name == "input_schema.py":
                continue
            text = p.read_text(encoding="utf-8")
            if "def validate_inputs(" in text:
                hits.append(p.name)
        assert hits == [], f"出现第二套校验实现: {hits}"

    def test_mcp_and_http_share_the_same_doc_and_error_table(self, root):
        """同一份载荷在两条口得到**同一份文档**与**同一张错误表**——只有两边都跑才成立。"""
        payload = {"topic": "云成本"}
        orch_h = RecordingOrch()
        h = _handler("/x", orch_h)
        h._handle_create_run("typed_demo", json.dumps({"inputs": payload}).encode())
        orch_m = RecordingOrch()
        MCPServer(orch=orch_m)._handle_request({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "run_workflow",
                       "arguments": {"name": "typed_demo", "inputs": payload}}})
        assert orch_h.calls[0]["input_text"] == orch_m.calls[0]["input_text"], \
            (orch_h.calls[0]["input_text"], orch_m.calls[0]["input_text"])

        bad = {"depth": 9}
        orch_h2, orch_m2 = RecordingOrch(), RecordingOrch()
        h2 = _handler("/x", orch_h2)
        h2._handle_create_run("typed_demo", json.dumps({"inputs": bad}).encode())
        resp = MCPServer(orch=orch_m2)._handle_request({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": "run_workflow",
                       "arguments": {"name": "typed_demo", "inputs": bad}}})
        http_fields = _sent(h2)[0]["fields"]
        mcp_fields = re.search(r"\n\s+- (\S+): (.+)", resp["result"]["content"][0]["text"])
        assert mcp_fields, resp
        assert orch_h2.calls == [] and orch_m2.calls == [], "两条口都得先拒后跑"
        assert {f["field"] for f in http_fields} == {"topic", "depth"}, http_fields
        # MCP 的返回是文本（JSON-RPC content），字段名必须照样可定位
        assert mcp_fields.group(1) in {f["field"] for f in http_fields}, mcp_fields.group(1)


# ─── 8. 真 HTTP 一条（路由不只是"方法写对"） ───────────────

class TestRealHTTP:
    def test_new_endpoint_answers_over_the_wire(self, root):
        """真请求一条：进程内 handler 判据分不出"路由没接"和"handler 没接"。"""
        orch = RecordingOrch()
        api = AdminAPI(host="127.0.0.1", port=0, api_key="k-typed")
        assert api.start(orch) is True
        port = api._server.server_address[1]
        try:
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}/api/workflows/typed_demo/runs",
                data=json.dumps({"inputs": {"topic": "云成本"}}).encode(),
                headers={"Authorization": "Bearer k-typed",
                         "Content-Type": "application/json"},
                method="POST")
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode())
            assert resp.status == 200 and body["pipeline"] == "typed_demo", body
            assert "## topic" in orch.calls[0]["input_text"]

            bad = urllib.request.Request(
                f"http://127.0.0.1:{port}/api/workflows/typed_demo/runs",
                data=json.dumps({"inputs": {"depth": 9}}).encode(),
                headers={"Authorization": "Bearer k-typed"}, method="POST")
            with pytest.raises(urllib.error.HTTPError) as e:
                urllib.request.urlopen(bad, timeout=30)
            assert e.value.code == 400
            err = json.loads(e.value.read().decode())
            assert err["code"] == "invalid_inputs" and \
                {f["field"] for f in err["fields"]} == {"topic", "depth"}, err
        finally:
            api.stop()
