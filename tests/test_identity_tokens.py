"""Token 身份与作用域隔离判据 —— spec §5.4 验收原文：
「两个 token 互不可见对方任务（真 HTTP 断言，非 mock）」。

五层：
  1. TokenStore 单元（加载/结构错误/缺密钥不可用/常数时间 resolve）；
  2. 鉴权矩阵（真 HTTP）：tokens 启用后回环也要凭证；ADMIN_API_KEY 兼容为 admin；
     未启用 tokens 时旧行为零变化；
  3. 任务作用域（真 HTTP + 真 orchestrator）：两个 token 互不可见、越权 404
     且与"不存在"响应一致（不泄露存在性）；
  4. owner 透传链（in-process 真 orchestrator）：run_plan/submit/acquire/断点往返；
  5. 成本作用域：非 admin 的 /api/cost 只统计自己任务，admin 全量。
"""
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from types import MethodType, SimpleNamespace
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from loom_core.admin_api import AdminAPI, AdminHandler  # noqa: E402
from loom_core.tokens import (  # noqa: E402
    TokenConfigError,
    TokenStore,
    parse_bearer,
    token_from_query,
)

TOK_A = "tok-alice-secret"
TOK_B = "tok-bob-secret"
TOK_OPS = "tok-ops-secret"


@pytest.fixture(autouse=True)
def reset_handler_state():
    yield
    AdminHandler.api_key = None
    AdminHandler.server_host = "127.0.0.1"
    AdminHandler.orch = None
    AdminHandler.tokens = None
    AdminHandler.principal = None


# ─── 1. TokenStore 单元 ─────────────────────────

class TestTokenStore:
    def _write(self, tmp_path: Path, text: str) -> Path:
        f = tmp_path / "tokens.yaml"
        f.write_text(text, encoding="utf-8")
        return f

    def test_load_env_and_sha256(self, tmp_path, monkeypatch):
        import hashlib
        monkeypatch.setenv("MY_ALICE_TOKEN", TOK_A)
        s = TokenStore.load(self._write(tmp_path, f"""
tokens:
  - id: alice
    tenant: acme
    token_env: MY_ALICE_TOKEN
  - id: bob
    token_sha256: {hashlib.sha256(TOK_B.encode()).hexdigest()}
"""))
        assert s.enabled
        assert s.resolve(TOK_A).id == "alice"
        assert s.resolve(TOK_A).tenant == "acme"
        assert s.resolve(TOK_B).id == "bob"
        assert s.resolve("wrong") is None
        assert s.resolve("") is None

    def test_missing_file_is_disabled_world(self, tmp_path):
        s = TokenStore.load(tmp_path / "nope.yaml")
        assert not s.enabled
        assert s.resolve("anything") is None

    @pytest.mark.parametrize("bad", [
        "tokens: not-a-list\n",
        "tokens:\n  - tenant: acme\n",                       # 缺 id
        "tokens:\n  - id: a\n    token_env: X\n  - id: a\n    token_env: Y\n",  # 重复 id
        "tokens:\n  - id: a\n",                              # 双密钥源都没给
        "tokens:\n  - id: a\n    token_env: X\n    token_sha256: " + "0" * 64 + "\n",  # 双源
        "tokens:\n  - id: a\n    token_sha256: zz\n",        # 坏 hex
    ])
    def test_structural_errors_raise(self, tmp_path, bad):
        with pytest.raises(TokenConfigError):
            TokenStore.load(self._write(tmp_path, bad))

    def test_missing_env_key_makes_token_unusable(self, tmp_path, monkeypatch, caplog):
        monkeypatch.delenv("NOT_SET_TOKEN", raising=False)
        s = TokenStore.load(self._write(tmp_path, """
tokens:
  - id: alice
    token_env: NOT_SET_TOKEN
"""))
        assert s.enabled                      # 配置存在
        assert s.resolve("") is None
        assert s.resolve("any") is None       # 该 token 一律不可用
        assert any("不可用" in r.message for r in caplog.records)

    def test_helpers(self):
        assert parse_bearer("Bearer abc") == "abc"
        assert parse_bearer("") == ""
        assert token_from_query("/tasks?token=xyz&x=1") == "xyz"
        assert token_from_query("/tasks") == ""


# ─── 2/3. 真 HTTP：鉴权矩阵 + 任务作用域 ─────────

def _start(api_key="", tokens_file=None, orch=None):
    api = AdminAPI(host="127.0.0.1", port=0, api_key=api_key,
                   tokens_file=tokens_file)
    assert api.start(orch if orch is not None else MagicMock()) is True
    port = api._server.server_address[1]
    return f"http://127.0.0.1:{port}", api


def _req(method, url, token=None, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, {"raw": raw}


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


def _task(task_id, owner, status="done"):
    return SimpleNamespace(
        id=task_id, owner=owner, pipeline_name="docgen",
        status=SimpleNamespace(value=status),
        created_at=0, finished_at=0, started_at=0, error=None,
        result={}, output_path="", output_content="",
        progress=0, steps=[],
    )


def _with_real_listing(orch, tasks):
    """把真正的 `PipelineOrchestrator.list_runs` 挂到替身上。

    Stage 2 把"内存缓存 ∪ RunStore 投影"和 principal 过滤收进了引擎的列表读口。
    替身若直接返回"已经过滤好的行"，这层过滤就一次都没被执行——§5.4 的
    「两个 token 互不可见」会退化成自己证明自己。
    """
    from loom_core.pipeline import PipelineOrchestrator
    orch._running_tasks = dict(tasks)
    orch._runstore = lambda: None
    orch.list_runs = MethodType(PipelineOrchestrator.list_runs, orch)
    return orch


class TestAuthMatrixHTTP:
    def test_loopback_requires_token_when_tokens_enabled(self, tmp_path):
        base, api = _start(tokens_file=str(_tokens_yaml(tmp_path)))
        try:
            code, _ = _req("GET", f"{base}/tasks")
            assert code == 401, "tokens 启用后回环无凭证必须 401"
            code, _ = _req("GET", f"{base}/tasks", token="wrong")
            assert code == 401
            code, body = _req("GET", f"{base}/tasks", token=TOK_A)
            assert code == 200
        finally:
            api.stop()

    def test_admin_api_key_still_works_as_admin(self, tmp_path):
        base, api = _start(api_key="legacy-key",
                           tokens_file=str(_tokens_yaml(tmp_path)))
        try:
            code, _ = _req("GET", f"{base}/tasks", token="legacy-key")
            assert code == 200, "ADMIN_API_KEY 兼容承诺：仍可作 admin 使用"
            code, _ = _req("GET", f"{base}/tasks", token=TOK_A)
            assert code == 200
        finally:
            api.stop()

    def test_bad_token_config_refuses_start(self, tmp_path):
        bad = tmp_path / "tokens.yaml"
        bad.write_text("tokens: broken\n", encoding="utf-8")
        api = AdminAPI(host="127.0.0.1", port=0, tokens_file=str(bad))
        assert api.start(MagicMock()) is False, "身份配置非法必须拒绝启动"

    def test_legacy_world_unchanged(self):
        """未启用 tokens 时：回环免鉴权（旧行为）——真 HTTP 回归钉。"""
        base, api = _start()
        try:
            code, _ = _req("GET", f"{base}/tasks")
            assert code == 200
        finally:
            api.stop()


class TestTaskScopeHTTP:
    """spec §5.4 验收原文：两个 token 互不可见对方任务（真 HTTP，非 mock）。"""

    @pytest.fixture
    def scoped(self, tmp_path):
        orch = MagicMock()
        alice_task = _task("t-alice", "alice")
        bob_task = _task("t-bob", "bob")
        tasks = {"t-alice": alice_task, "t-bob": bob_task}
        orch.get_task.side_effect = lambda tid: tasks.get(tid)
        _with_real_listing(orch, tasks)
        orch.cancel.return_value = True
        orch.pause.return_value = True
        base, api = _start(tokens_file=str(_tokens_yaml(tmp_path)), orch=orch)
        yield base, api, orch
        api.stop()

    def test_list_is_scoped(self, scoped):
        base, _api, _orch = scoped
        code, body = _req("GET", f"{base}/tasks", token=TOK_A)
        assert code == 200
        assert [t["id"] for t in body["tasks"]] == ["t-alice"]
        code, body = _req("GET", f"{base}/tasks", token=TOK_B)
        assert [t["id"] for t in body["tasks"]] == ["t-bob"]

    def test_admin_sees_all(self, scoped):
        base, _api, _orch = scoped
        code, body = _req("GET", f"{base}/tasks", token=TOK_OPS)
        assert code == 200
        assert sorted(t["id"] for t in body["tasks"]) == ["t-alice", "t-bob"]

    def test_cross_tenant_get_is_404_and_indistinguishable(self, scoped):
        base, _api, _orch = scoped
        code_a, body_a = _req("GET", f"{base}/tasks/t-bob", token=TOK_A)
        code_n, body_n = _req("GET", f"{base}/tasks/no-such-task", token=TOK_A)
        assert code_a == code_n == 404, (code_a, code_n)
        assert body_a == body_n, "越权 404 与不存在 404 必须同形（不泄露存在性）"

    @pytest.mark.parametrize("action,method", [
        ("cancel", "POST"), ("pause", "POST"), ("resume", "POST"),
        ("approve", "POST"), ("reject", "POST"), ("rerun", "POST"),
    ])
    def test_cross_tenant_actions_are_404(self, scoped, action, method):
        base, _api, orch = scoped
        code, _ = _req(method, f"{base}/tasks/t-bob/{action}", token=TOK_A)
        assert code == 404, f"alice 对 bob 的任务执行 {action} 必须 404"
        # 且后端真的没被调（404 在作用域检查就拦下）
        for fn in (orch.cancel, orch.pause, orch.resume, orch.rerun,
                   orch.approve, orch.reject):
            assert not fn.called, f"{action} 穿透到了后端"

    def test_own_task_actions_pass(self, scoped):
        base, _api, orch = scoped
        code, _ = _req("POST", f"{base}/tasks/t-alice/cancel", token=TOK_A)
        assert code == 200
        orch.cancel.assert_called_once_with("t-alice")


class TestCostScopeHTTP:
    def test_principal_cost_scoped_admin_full(self, tmp_path):
        from loom_core.cost_tracker import get_cost_tracker
        tracker = get_cost_tracker()
        tracker.record(provider="p", prompt_tokens=1, completion_tokens=1,
                       cost=0.25, task_id="t-cost-alice")
        tracker.record(provider="p", prompt_tokens=1, completion_tokens=1,
                       cost=1.00, task_id="t-cost-bob")

        orch = MagicMock()
        alice_task = _task("t-cost-alice", "alice")
        # 引擎层给全量、作用域过滤在 list_runs 里——这里挂真实现，验证过滤本身
        bob_task = _task("t-cost-bob", "bob")
        _with_real_listing(orch, {"t-cost-alice": alice_task,
                                  "t-cost-bob": bob_task})
        base, api = _start(tokens_file=str(_tokens_yaml(tmp_path)), orch=orch)
        try:
            code, body = _req("GET", f"{base}/api/cost", token=TOK_A)
            assert code == 200
            assert body.get("scope") == "principal"
            assert body["task_count"] == 1
            # bob 那笔 1.00 绝不能进 alice 的总额（同进程其他用例可能已记过
            # 小额成本，所以判"上界"而不是精确等于）
            assert body["total_cost"] <= 0.25 + 1e-9
            # admin 走全量（无 scope 标记）
            code, body = _req("GET", f"{base}/api/cost", token=TOK_OPS)
            assert code == 200 and "scope" not in body
        finally:
            api.stop()


# ─── 4. owner 透传链（in-process 真 orchestrator）──

class TestOwnerPropagation:
    def test_run_plan_records_owner_and_checkpoint_roundtrip(self, tmp_path):
        from loom_core import PipelineOrchestrator
        from loom_core.scheduler import Scheduler

        project = Path(__file__).resolve().parent.parent
        sched = Scheduler(agents_dir=str(project / "agents"),
                          pipeline_dir=str(project / "pipelines"))
        plan = sched.parse_file(str(project / "pipelines" / "test_pipeline.yaml"),
                                verify_lock=False)
        orch = PipelineOrchestrator(agents_dir=str(project / "agents"),
                                    checkpoint_dir=str(tmp_path / "ckpt"))
        orch.register_agents()
        try:
            inp = tmp_path / "in.md"
            inp.write_text("# Token 身份链路测试输入（占位主题）\n", encoding="utf-8")
            task = orch.run_plan(plan, input_file=str(inp),
                                 task_id="owner-chain", wait=True, owner="alice")
            assert task.owner == "alice"
            assert task.to_dict()["owner"] == "alice"
            # 断点往返不丢归属（任务 done 后断点通常被清——手动 save/load 验）
            from loom_core.checkpoint_manager import CheckpointManager
            mgr = CheckpointManager(str(tmp_path / "ckpt2"))
            mgr.save(task)
            loaded, _ = mgr.load("owner-chain")
            assert loaded is not None and loaded.owner == "alice"
        finally:
            orch.shutdown()

    def test_queue_submit_acquire_carries_principal(self, tmp_path):
        from loom_core.task_queue import TaskQueue
        q = TaskQueue(db_path=str(tmp_path / "q.db"))
        assert q.submit("qt-1", "docgen", "x.md", principal="alice") is True
        item = q.acquire(worker_id="w1")
        assert item and item["task_id"] == "qt-1"
        assert item["principal"] == "alice"

    def test_worker_passes_owner_to_run_plan(self, tmp_path):
        """worker 消费链：claim 回执的 principal 必须落到 run_plan(owner=...)。"""
        from loom_core.worker import TaskWorker
        calls = {}

        class FakeOrch:
            def run_plan(self, plan, **kw):
                calls.update(kw)
                return SimpleNamespace(id=kw.get("task_id"), status=SimpleNamespace(value="done"),
                                       result={}, error="")

        sched = MagicMock()
        # _execute 在 parse 前做真实存在性检查——给一个真文件（内容无碍：parse 是 mock）
        plan_file = tmp_path / "docgen.yaml"
        plan_file.touch()
        sched._pipeline_path.return_value = plan_file
        sched.parse_file.return_value = SimpleNamespace(
            raw={"pipeline": {}}, plan_id="p", pipeline_name="docgen",
            node_count=0, levels=[])
        worker = TaskWorker(orchestrator=FakeOrch(), queue=MagicMock(),
                            scheduler=sched)
        worker._execute({"task_id": "w-1", "pipeline_name": "docgen",
                         "input_file": "in.md", "config": {},
                         "principal": "alice"})
        assert calls.get("owner") == "alice"
