"""RunStore Stage 2：读侧切换（内存退化为缓存）+ 身份贯穿。

Stage 1 让新运行有了一份"事件 + 投影"的权威副本，但读侧仍以内存为存在性判据：
`_trim_task_history` 淘汰（硬编码 100 条）之后 `/tasks`、`/tasks/<id>`、MCP 都
查不到自己的运行。本文件的判据只回答两件事：

1. **淘汰不改答案**：同一条运行在内存里被清掉之后，列表/详情/MCP 仍读得到
   （来源变成 `runstore`），且**给不出的字段不编**（节点 result 正文本就从不落盘）；
2. **身份跟着事实走**：`runs.principal` 由引擎从 `owner` 写入，作用域过滤在
   投影层同样生效——两个 token 互不可见，越权 404 与"不存在" 404 同形。

刻意不用"替身返回已过滤好的行"来测作用域：那层过滤就一次也不会被执行。
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from loom_core import state_paths
from loom_core.deliveries import DeliveryLedger
from loom_core.pipeline import PipelineOrchestrator
from loom_core.run_store import RunStore
from loom_core.scheduler import Scheduler

PROJECT = Path(__file__).resolve().parent.parent

PLAIN_YAML = """
name: rs_read_plain
agents:
  - name: tldr
    version: "0.1"
    dependencies: []
    config: {max_sentences: 2}
topology:
  levels: [["tldr"]]
  edges: []
"""

PIPELINE_NAME = "plain"  # parse_file 取文件名的 stem，不是 YAML 里的 name

INPUT = "# 标题\n\n这是读侧切换的测试输入，用于触发摘要链路。\n"


def _state_dir(monkeypatch, tmp_path: Path) -> Path:
    d = tmp_path / "state"
    monkeypatch.setenv("LOOM_STATE_DIR", str(d))
    return d


def _plan(tmp_path: Path):
    d = tmp_path / "pipe"
    d.mkdir(parents=True, exist_ok=True)
    f = d / "plain.yaml"
    f.write_text(PLAIN_YAML, encoding="utf-8")
    return Scheduler(agents_dir=str(PROJECT / "agents"),
                     pipeline_dir=str(d)).parse_file(str(f), verify_lock=False)


def _orch(tmp_path: Path) -> PipelineOrchestrator:
    orch = PipelineOrchestrator(agents_dir=str(PROJECT / "agents"),
                                checkpoint_dir=str(tmp_path / "ckpt"))
    orch.register_agents()
    return orch


def _run_once(tmp_path: Path, task_id: str, owner: str = "") -> PipelineOrchestrator:
    """跑一条真流水线（同步），返回跑它的编排器（内存里仍有这个运行）。"""
    inp = tmp_path / "in.md"
    inp.write_text(INPUT, encoding="utf-8")
    orch = _orch(tmp_path)
    orch.run_plan(_plan(tmp_path), input_file=str(inp), task_id=task_id,
                  wait=True, owner=owner)
    return orch


def _evict(orch: PipelineOrchestrator) -> None:
    """模拟 `_trim_task_history` 的淘汰：只清内存，不动任何持久面。"""
    orch._running_tasks.clear()


# ─── 1. 列表读口：内存缓存 ∪ 投影，作用域在引擎里过滤 ───────────


class TestListRunsMerge:
    def test_evicted_run_is_still_listed(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        orch = _run_once(tmp_path, "rs-evicted")
        try:
            before = {r["id"]: r for r in orch.list_runs()}
            assert before["rs-evicted"]["source"] == "memory"
            _evict(orch)
            after = {r["id"]: r for r in orch.list_runs()}
            assert "rs-evicted" in after, "内存淘汰后列表里就没这条运行了——读侧仍挂在缓存上"
            assert after["rs-evicted"]["source"] == "runstore"
            assert after["rs-evicted"]["status"] == "done"
            assert after["rs-evicted"]["pipeline"] == PIPELINE_NAME
        finally:
            orch.shutdown()

    def test_memory_row_wins_for_the_same_run(self, tmp_path, monkeypatch):
        """同一 run 两边都有时以内存为准（它比投影新），但不是"内存说了算才列"。"""
        _state_dir(monkeypatch, tmp_path)
        orch = _run_once(tmp_path, "rs-both")
        try:
            rows = [r for r in orch.list_runs() if r["id"] == "rs-both"]
            assert len(rows) == 1, f"同一条运行被列了两次：{rows}"
            assert rows[0]["source"] == "memory"
        finally:
            orch.shutdown()

    def test_evicted_failed_run_keeps_its_error(self, tmp_path, monkeypatch):
        """列表里的 error 不能在淘汰后消失：失败原因本来就是运维第一眼要看的东西。"""
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        try:
            store = RunStore()
            store.open_run("rs-failed", workflow="w")
            store.close_run("rs-failed", "failed", error="boom")
            row = {r["id"]: r for r in orch.list_runs()}["rs-failed"]
            assert row["status"] == "failed"
            assert row["error"] == "boom"
        finally:
            orch.shutdown()

    def test_principal_filter_applies_to_both_sources(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        orch = _run_once(tmp_path, "rs-alice", owner="alice")
        try:
            # 再造一条"投影里有、内存没有"的 bob 运行
            store = RunStore()
            store.open_run("rs-bob", workflow="w-bob", principal="bob")
            store.close_run("rs-bob", "done")
            store.open_run("rs-anon", workflow="w-anon", principal="")
            store.close_run("rs-anon", "done")

            alice = {r["id"] for r in orch.list_runs(principal="alice")}
            assert alice == {"rs-alice"}, alice
            bob = {r["id"] for r in orch.list_runs(principal="bob")}
            assert bob == {"rs-bob"}, bob
            everything = {r["id"] for r in orch.list_runs()}
            assert {"rs-alice", "rs-bob", "rs-anon"} <= everything
        finally:
            orch.shutdown()


# ─── 2. 句柄：内存缺了就从投影回答，给不出的不编 ─────────────


class TestRunHandleFromStore:
    def test_handle_reads_projection_after_eviction(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        orch = _run_once(tmp_path, "rs-handle", owner="alice")
        try:
            _evict(orch)
            handle = orch.run_handle("rs-handle")
            assert handle.status() == "done"
            assert handle.principal == "alice"
            assert handle.exists() is True

            payload = handle.to_dict()
            assert payload["source"] == "runstore"
            assert payload["pipeline"] == PIPELINE_NAME
            assert payload["terminal"] is True
            # 逐节点摘要来自 run_nodes；产物键来自同一行的 artifact_keys
            assert [n["node"] for n in payload["nodes"]] == ["tldr"]
            assert any(a["node"] == "tldr" and a["keys"] for a in payload["artifacts"]) \
                or payload["artifacts"], payload["artifacts"]
            # 给不出的不编：节点 result 正文本从不落盘，投影这层就不该出现这个键
            assert "result" not in payload, (
                "淘汰后的载荷里出现了 result——那是内存态的字段，投影里没有它")
        finally:
            orch.shutdown()

    def test_store_unavailable_falls_back_to_ledger(self, tmp_path, monkeypatch):
        """读口不可用时退回账本——这是**数据边界**，不是开关。

        开关已经删了（2026-10-21），但"投影读不出来"这个故障仍然真实存在：
        用替身把读口关掉，验证回落链没有跟着开关一起被删掉。
        """
        _state_dir(monkeypatch, tmp_path)
        orch = _run_once(tmp_path, "rs-off")
        try:
            _evict(orch)
            monkeypatch.setattr(orch, "_runstore", lambda: None)
            handle = orch.run_handle("rs-off")
            payload = handle.to_dict()
            assert payload["source"] == "deliveries"
            assert payload["pipeline"] == PIPELINE_NAME
            # 读口不可用时投影不参与存在性判定，只剩账本里那份终态载荷
            assert DeliveryLedger().get("rs-off") is not None
        finally:
            orch.shutdown()

    def test_unknown_run_is_still_unknown(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        try:
            handle = orch.run_handle("never-existed")
            assert handle.exists() is False
            assert handle.status() == "unknown"
            assert handle.to_dict()["source"] == "none"
        finally:
            orch.shutdown()


# ─── 3. 真 HTTP：两个 token + 淘汰仍可读 ─────────────────────

TOK_A = "tok-alice-secret"
TOK_B = "tok-bob-secret"
TOK_OPS = "tok-ops-secret"


def _tokens_yaml(tmp_path: Path) -> Path:
    import hashlib
    f = tmp_path / "tokens.yaml"
    f.write_text(f"""
tokens:
  - id: alice
    token_sha256: {hashlib.sha256(TOK_A.encode()).hexdigest()}
  - id: bob
    token_sha256: {hashlib.sha256(TOK_B.encode()).hexdigest()}
  - id: ops
    admin: true
    token_sha256: {hashlib.sha256(TOK_OPS.encode()).hexdigest()}
""", encoding="utf-8")
    return f


def _start(orch, tokens_file: str):
    from loom_core.admin_api import AdminAPI
    api = AdminAPI(host="127.0.0.1", port=0, api_key="", tokens_file=tokens_file)
    assert api.start(orch) is True
    port = api._server.server_address[1]
    return f"http://127.0.0.1:{port}", api


def _req(method, url, token=None):
    req = urllib.request.Request(url, method=method)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, {"raw": raw}


class TestReadSideHTTP:
    @pytest.fixture
    def served(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        orch = _run_once(tmp_path, "rs-http-alice", owner="alice")
        _evict(orch)  # 关键：内存里查不到，接口仍必须答得出来
        base, api = _start(orch, str(_tokens_yaml(tmp_path)))
        yield base, orch
        api.stop()
        orch.shutdown()
        from loom_core.admin_api import AdminHandler
        AdminHandler.tokens = None
        AdminHandler.orch = None
        AdminHandler.principal = None

    def test_list_after_eviction_is_scoped(self, served):
        base, _orch = served
        code, body = _req("GET", f"{base}/tasks", TOK_A)
        assert code == 200
        assert [t["id"] for t in body["tasks"]] == ["rs-http-alice"]
        code, body = _req("GET", f"{base}/tasks", TOK_B)
        assert code == 200 and body["tasks"] == [], "别人的运行不该出现在 bob 的列表里"

    def test_detail_after_eviction_is_readable(self, served):
        base, _orch = served
        code, body = _req("GET", f"{base}/tasks/rs-http-alice", TOK_A)
        assert code == 200, body
        assert body["status"] == "done"
        assert body["source"] == "runstore"
        assert body["pipeline"] == PIPELINE_NAME
        assert [n["name"] for n in body["dag"]["nodes"]] == ["tldr"]

    def test_cross_tenant_404_is_indistinguishable_after_eviction(self, served):
        base, _orch = served
        code_cross, body_cross = _req("GET", f"{base}/tasks/rs-http-alice", TOK_B)
        code_missing, body_missing = _req("GET", f"{base}/tasks/no-such-run", TOK_B)
        assert code_cross == code_missing == 404, (code_cross, code_missing)
        assert body_cross == body_missing, "越权与不存在必须同形（不泄露存在性）"

    def test_admin_sees_evicted_run(self, served):
        base, _orch = served
        code, body = _req("GET", f"{base}/tasks", TOK_OPS)
        assert code == 200
        assert "rs-http-alice" in [t["id"] for t in body["tasks"]]
        code, body = _req("GET", f"{base}/tasks/rs-http-alice", TOK_OPS)
        assert code == 200 and body["id"] == "rs-http-alice"

    def test_health_deep_reports_runstore(self, served):
        """没有开关之后，RunStore 的状态必须在健康检查里看得见。"""
        base, _orch = served
        code, body = _req("GET", f"{base}/api/health/deep", TOK_OPS)
        assert code == 200, body
        comp = body["components"]["runstore"]
        assert comp["status"] == "healthy", comp
        assert comp["details"]["schema_version"] >= 1, comp
        assert "runstore" not in body["unhealthy_components"], body["unhealthy_components"]

    def test_dashboard_after_eviction(self, served):
        base, _orch = served
        code, body = _req("GET", f"{base}/api/dashboard", TOK_A)
        assert code == 200
        assert [t["id"] for t in body["tasks"]] == ["rs-http-alice"], body
        code, body = _req("GET", f"{base}/api/dashboard", TOK_B)
        assert code == 200 and body["tasks"] == []

    def test_cost_scope_includes_store_only_runs(self, served):
        """成本归因不能随内存淘汰丢：作用域集合要由投影补上。"""
        from loom_core.cost_tracker import get_cost_tracker
        base, _orch = served
        get_cost_tracker().record(provider="p", prompt_tokens=1, completion_tokens=1,
                                  cost=0.5, task_id="rs-http-alice")
        code, body = _req("GET", f"{base}/api/cost", TOK_A)
        assert code == 200, body
        assert body.get("scope") == "principal"
        assert body["task_count"] == 1, body
        assert body["total_cost"] >= 0.5 - 1e-9, body
        # bob 没有这条运行 ⇒ 他的作用域里不该出现这份成本
        code, body = _req("GET", f"{base}/api/cost", TOK_B)
        assert code == 200 and body["task_count"] == 0, body


# ─── 4. 身份贯穿：owner → principal 三处调用点 + SSE ───────────


class TestIdentityPlumbing:
    def test_sync_run_records_principal_in_store_and_queue(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        orch = _run_once(tmp_path, "rs-id-sync", owner="alice")
        try:
            run = RunStore().get_run("rs-id-sync")
            assert run is not None and run["principal"] == "alice", run
            with sqlite3.connect(state_paths.store_path("tasks.db")) as conn:
                row = conn.execute(
                    "SELECT principal FROM task_queue WHERE task_id = ?",
                    ("rs-id-sync",)).fetchone()
            assert row is not None and row[0] == "alice", (
                "队列行的 principal 是空串——submit 没把身份带下去")
        finally:
            orch.shutdown()

    def test_async_run_records_principal(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        inp = tmp_path / "in.md"
        inp.write_text(INPUT, encoding="utf-8")
        orch = _orch(tmp_path)
        try:
            asyncio.run(orch.run_plan_async(_plan(tmp_path), input_file=str(inp),
                                            task_id="rs-id-async", owner="bob"))
            run = RunStore().get_run("rs-id-async")
            assert run is not None and run["principal"] == "bob", run
        finally:
            orch.shutdown()

    def test_stream_worker_forwards_owner(self, tmp_path):
        """`/stream` 与 `/run` 走同一条身份链：worker 必须把 Principal.id 交给引擎。

        这一层此前是空的（SSE 发起的运行 owner 恒为空串），用"记录 kwargs 的替身"
        钉住接线：引擎侧 `run_plan_async(owner=…)` 已由上面那条判据证明会落库。
        """
        from loom_core.admin_api import AdminHandler

        captured: dict = {}

        class _FakeOrch:
            class config:
                @staticmethod
                def get(key, default=None):
                    return "docgen" if key == "default_pipeline" else default

            def run_plan_async(self, plan, input_file="", task_id="", owner=""):
                captured["owner"] = owner

                async def _done():
                    return SimpleNamespace(output_content="",
                                           status=SimpleNamespace(value="done"))
                return _done()

        handler = AdminHandler.__new__(AdminHandler)
        handler.orch = _FakeOrch()
        writer = MagicMock()
        callback = MagicMock()
        inp = tmp_path / "in.md"
        inp.write_text("x", encoding="utf-8")

        thread = AdminHandler._start_stream_worker(
            handler, writer, "rs-sse-owner", inp, callback, owner="alice")
        thread.join(timeout=30)
        assert not thread.is_alive(), "worker 没在 30s 内结束"
        assert captured.get("owner") == "alice", (
            f"SSE worker 没把 owner 传给引擎：{captured}")

# ─── 5. MCP：同一份投影，不另起一条回落链 ─────────────────


class TestMCPReadsProjection:
    def test_get_task_and_list_after_eviction(self, tmp_path, monkeypatch):
        from loom_core.mcp_server import MCPServer
        _state_dir(monkeypatch, tmp_path)
        orch = _run_once(tmp_path, "rs-mcp")
        try:
            _evict(orch)
            server = MCPServer(orch=orch)
            resp = server._handle_request({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {"name": "get_task", "arguments": {"task_id": "rs-mcp"}},
            })
            assert "isError" not in resp["result"], resp
            payload = json.loads(resp["result"]["content"][0]["text"])
            assert payload["id"] == "rs-mcp"
            assert payload["status"] == "done"
            assert payload["pipeline"] == PIPELINE_NAME

            resp = server._handle_request({
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {"name": "list_tasks", "arguments": {}},
            })
            listed = json.loads(resp["result"]["content"][0]["text"])
            assert "rs-mcp" in [t["id"] for t in listed["tasks"]], listed
        finally:
            orch.shutdown()

    def test_unknown_task_is_still_an_error(self, tmp_path, monkeypatch):
        from loom_core.mcp_server import MCPServer
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        try:
            server = MCPServer(orch=orch)
            resp = server._handle_request({
                "jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": {"name": "get_task", "arguments": {"task_id": "nope"}},
            })
            assert resp["result"].get("isError") is True, resp
        finally:
            orch.shutdown()
