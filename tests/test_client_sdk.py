"""LoomClient SDK 的真 HTTP 集成测试（产品评审 A7）。

测试对象是 **SDK ↔ Admin API 的 HTTP 契约**——鉴权、作用域、JSON 形状、
错误码、轮询语义。审批状态机本身由 `test_approvals.py` 全量覆盖，这里
不重复测引擎内部逻辑，只测"SDK 发出来的请求服务端认不认"。

设计原则：
  - 真 HTTP：AdminAPI 绑 127.0.0.1:0 + tokens.yaml admin token，不走 mock；
  - 构造审批任务时直接把 PipelineTask 放进 `orch._running_tasks` + 台账
    request 行——审批状态机的"正常到达闸门"路径由 test_approvals.py 覆盖，
    这里只需保证 HTTP 层看到"一个正在等批的任务"；
  - submit-cancel-wait 链用真 run_plan 后台线程：LLM 缺失时任务自己快速
    失败，cancel 返回 False 也算链通（race-proof）；
  - 路由同源门禁：SDK `_ROUTES` 每条 (method, path) 必须在
    generate_spec()["paths"] 里能找到——spec 与实现只改一边会红。
"""
from __future__ import annotations

import hashlib
import sys
import time
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT))

from loom_core import LoomClient, LoomError, LoomTimeoutError  # noqa: E402
from loom_core.admin_api import AdminAPI, AdminHandler  # noqa: E402
from loom_core.client import routes as sdk_routes  # noqa: E402
from loom_core.openapi_spec import generate_spec  # noqa: E402
from loom_core.pipeline import PipelineTask, TaskStatus  # noqa: E402

TOK_ADMIN = "tok-admin-sdk"


@pytest.fixture(autouse=True)
def reset_handler_state():
    yield
    AdminHandler.api_key = None
    AdminHandler.server_host = "127.0.0.1"
    AdminHandler.orch = None
    AdminHandler.tokens = None
    AdminHandler.principal = None
    AdminHandler.dashboard_dir = None


def _tokens_yaml(tmp_path: Path) -> Path:
    f = tmp_path / "tokens.yaml"
    f.write_text(f"""
tokens:
  - id: admin
    admin: true
    token_sha256: {hashlib.sha256(TOK_ADMIN.encode()).hexdigest()}
""", encoding="utf-8")
    return f


def _new_orch(tmp_path: Path):
    from loom_core import PipelineOrchestrator
    orch = PipelineOrchestrator(agents_dir=str(PROJECT / "agents"),
                                checkpoint_dir=str(tmp_path / "ckpt"))
    orch.register_agents()
    return orch


def _start(orch, tokens_file: Path):
    api = AdminAPI(host="127.0.0.1", port=0, api_key="",
                   tokens_file=str(tokens_file))
    assert api.start(orch) is True
    return f"http://127.0.0.1:{api._server.server_address[1]}", api


def _make_approval_task(orch, task_id: str, nodes: list[str] | None = None,
                        messages: dict | None = None) -> PipelineTask:
    """在 orch 里置一个"停在审批闸门上"的任务（内存 + 台账双写）。

    审批状态机的完整路径由 test_approvals.py 覆盖；这里只保证 HTTP 层
    看到的任务满足 approve/reject 的前置条件：status=PAUSED + awaiting_approval
    非空 + 台账有对应 pending 行。
    """
    nodes = nodes or ["gate"]
    messages = messages or {"gate": "确认"}
    # 台账先行：approve() 内部 fail-closed，先写台账再唤醒——没有 pending 行
    # 时 _decide_in_store 返回 None，approve 直接返回 (False, "台账不可用")
    orch.approvals.request(task_id, "gate_demo", nodes, messages, principal="")
    task = PipelineTask(
        id=task_id,
        pipeline_name="gate_demo",
        input_file="",
        config={},
        status=TaskStatus.PAUSED,
        awaiting_approval={"nodes": nodes, "messages": messages},
        owner="",
    )
    orch._running_tasks[task_id] = task
    return task


# ─── 1. 提交 → 取消 → 等待链 ─────────────────────────────────────────


class TestSubmitCancelWaitChain:
    def test_submit_cancel_wait_chain(self, tmp_path):
        """submit → status → cancel → wait 返回终态。

        race-proof：LLM 缺失时任务可能自己快速 failed，cancel 返回 False
        也算链通——断言的是 wait 拿到终态，不是取消成功。
        """
        orch = _new_orch(tmp_path)
        base, api = _start(orch, _tokens_yaml(tmp_path))
        try:
            client = LoomClient(base, token=TOK_ADMIN)

            resp = client.submit(query="写一篇 Git rebase 指南", pipeline="demo")
            assert "task_id" in resp, resp
            task_id = resp["task_id"]

            detail = client.status(task_id)
            assert detail["id"] == task_id
            assert "status" in detail

            # cancel：任务可能还在跑（返回 True）也可能已失败（返回 False）
            cancel_resp = client.cancel(task_id)
            assert "cancelled" in cancel_resp and cancel_resp["task_id"] == task_id

            # wait 必须拿到终态——不管任务是自己跑完还是被取消
            final = client.wait(task_id, timeout=60)
            assert final["status"] in ("done", "failed", "cancelled"), \
                f"wait 没拿到终态：{final['status']}"
        finally:
            api.stop()
            orch.shutdown()


# ─── 2. 审批放行 / 驳回 + 收件箱 ────────────────────────────────────


class TestApproveRejectInboxChain:
    def test_approve_reject_inbox_chain(self, tmp_path):
        """approvals 收件箱可见 → approve 放行 → 二次 approve 409 →
        新任务 reject 驳回 + reason 透传。"""
        orch = _new_orch(tmp_path)
        base, api = _start(orch, _tokens_yaml(tmp_path))
        try:
            client = LoomClient(base, token=TOK_ADMIN)

            # ── 审批放行 ──
            task1 = _make_approval_task(orch, "approve_task_1",
                                        nodes=["gate"], messages={"gate": "确认"})

            # 收件箱可见（admin 看全量）
            inbox = client.approvals()
            run_ids = [a["run_id"] for a in inbox]
            assert "approve_task_1" in run_ids, f"收件箱里没有刚置的任务：{run_ids}"

            # 放行
            resp = client.approve("approve_task_1", note="放行")
            assert resp["approved"] is True
            assert resp["task_id"] == "approve_task_1"
            assert resp["note"] == "放行"
            # 引擎侧状态已翻转
            assert task1.awaiting_approval is None
            assert task1.status == TaskStatus.RUNNING

            # 二次 approve → 409（fail-closed：不在审批等待中）
            with pytest.raises(LoomError, match="409") as exc_info:
                client.approve("approve_task_1", note="再批")
            assert exc_info.value.status == 409

            # ── 审批驳回 ──
            task2 = _make_approval_task(orch, "reject_task_1",
                                        nodes=["gate"], messages={"gate": "确认"})
            resp = client.reject("reject_task_1", reason="内容不合规")
            assert resp["rejected"] is True
            assert resp["task_id"] == "reject_task_1"
            assert resp["reason"] == "内容不合规"
            assert task2.status == TaskStatus.CANCELLED
        finally:
            api.stop()
            orch.shutdown()


# ─── 3. 生命周期 404（不存在 / 越权统一 404）────────────────────────


class TestLifecycle404s:
    def test_lifecycle_404s(self, tmp_path):
        """对不存在的任务做所有写操作和详情查询 → 404。"""
        orch = _new_orch(tmp_path)
        base, api = _start(orch, _tokens_yaml(tmp_path))
        try:
            client = LoomClient(base, token=TOK_ADMIN)

            with pytest.raises(LoomError) as exc_info:
                client.status("nonexistent_task")
            assert exc_info.value.status == 404

            for method, args in [
                ("cancel", ("nonexistent_task",)),
                ("pause", ("nonexistent_task",)),
                ("resume", ("nonexistent_task",)),
                ("rerun", ("nonexistent_task",)),
                ("approve", ("nonexistent_task",)),
                ("reject", ("nonexistent_task",)),
            ]:
                with pytest.raises(LoomError) as exc_info:
                    getattr(client, method)(*args)
                assert exc_info.value.status == 404, \
                    f"{method} 对不存在任务应 404，实际 {exc_info.value.status}"

            with pytest.raises(LoomError) as exc_info:
                client.delivery("nonexistent_run")
            assert exc_info.value.status == 404
        finally:
            api.stop()
            orch.shutdown()


# ─── 4. OpenAPI 路由同源门禁 ────────────────────────────────────────


class TestOpenAPIRouteParity:
    def test_openapi_route_parity(self):
        """SDK 的每条路由必须在 OpenAPI spec 里注册，且方法名对得上。

        这是"spec 与实现同源"的机器判据：只改一边会红。
        """
        spec_paths = generate_spec()["paths"]
        for method_name, (method, path) in sdk_routes().items():
            assert path in spec_paths, \
                f"路由 {method_name} → {path} 不在 OpenAPI spec 里"
            operations = spec_paths[path]
            # OpenAPI 用小写方法名（get/post），SDK 用大写（GET/POST）
            assert method.lower() in operations, \
                f"spec 里 {path} 没有 {method} 方法（SDK 的 {method_name} 用它）"


# ─── 5. submit 本地校验 ────────────────────────────────────────────


class TestSubmitValidation:
    def test_submit_validation(self, tmp_path):
        """submit() 缺 query 和 inputs → 本地 ValueError（不发 HTTP）。"""
        orch = _new_orch(tmp_path)
        base, api = _start(orch, _tokens_yaml(tmp_path))
        try:
            client = LoomClient(base, token=TOK_ADMIN)
            with pytest.raises(ValueError, match="query 或 inputs"):
                client.submit()
        finally:
            api.stop()
            orch.shutdown()


# ─── 6. wait 超时 ──────────────────────────────────────────────────


class TestWaitTimeout:
    def test_wait_timeout_on_paused_task(self, tmp_path):
        """paused 任务不在终态里 → wait 超时抛 LoomTimeoutError。"""
        orch = _new_orch(tmp_path)
        base, api = _start(orch, _tokens_yaml(tmp_path))
        try:
            client = LoomClient(base, token=TOK_ADMIN)

            task = _make_approval_task(orch, "paused_task_1")

            with pytest.raises(LoomTimeoutError) as exc_info:
                client.wait("paused_task_1", timeout=0.5, poll_interval=0.1)
            assert exc_info.value.status == 0
            assert exc_info.value.body["task_id"] == "paused_task_1"
        finally:
            api.stop()
            orch.shutdown()


# ─── 7. run_workflow 400 ───────────────────────────────────────────


class TestRunWorkflow400:
    def test_run_workflow_unknown_pipeline(self, tmp_path):
        """未知流水线名 → 400（resolve_pipeline_name 返回错误）。"""
        orch = _new_orch(tmp_path)
        base, api = _start(orch, _tokens_yaml(tmp_path))
        try:
            client = LoomClient(base, token=TOK_ADMIN)
            with pytest.raises(LoomError) as exc_info:
                client.run_workflow("nonexistent_pipeline", {"x": 1})
            assert exc_info.value.status == 400
        finally:
            api.stop()
            orch.shutdown()
