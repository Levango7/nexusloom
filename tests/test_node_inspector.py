"""S3 节点检查器（docs/product-spec.md §4.2）：点 DAG 里一个节点 → 四类明细。

被测契约（全部走真 HTTP、随机端口真 server）：
- `GET /tasks/<id>/nodes/<name>` 一次给出 artifact 结构预览 / 幂等键 /
  熔断态 / 限流态 / 质量门明细 / 断点视图；S2 的 dag 视图同时多带
  `checkpoint` 标记（两处同源出数，不会漂移）；
- **越权与不存在同形（404）**——非 admin 看别人的任务节点，与任务不存在
  不可区分（复用 `_scoped_task`）；
- **取不到就不编**：熔断器/限流器从未建过、质量门不是本节点产物 →
  对应字段 None，而不是"closed"/空对象/把普通结果当质量分（FP-1 同类错误）；
- **artifact 预览是形状+截断**，不是全量回传；
- 幂等键形如 `{task_id}:{node}:{attempts}`，recorded 只在真问到
  processed_keys 时为布尔。
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from loom_core.admin_api import AdminAPI, AdminHandler


@pytest.fixture
def orch_mock():
    o = MagicMock()
    o.list_tasks.return_value = []
    o.registry.list.return_value = []
    o.bus.health.return_value = {"status": "ok", "subscribers": 0}
    o.get_task.return_value = None
    # "什么都没建过"的初始态必须显式置空：MagicMock 会自动造属性，
    # 不置空的话 _cb_registry.get()/bus._store 全是真值 mock，
    # "取不到就 None"的分支根本测不到。
    o.bus._store = None
    o._cb_registry.get.return_value = None
    o._rate_limiters.get.return_value = None
    return o


@pytest.fixture(autouse=True)
def reset_handler_state():
    yield
    AdminHandler.api_key = None
    AdminHandler.server_host = "127.0.0.1"
    AdminHandler.dashboard_dir = None
    AdminHandler.orch = None
    AdminHandler.tokens = MagicMock(enabled=False)


def _start(orch):
    api = AdminAPI(host="127.0.0.1", port=0, api_key="")
    assert api.start(orch) is True
    return api._server.server_address[1], api


def _get(port, path):
    import urllib.error
    import urllib.request
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            return e.code, json.loads(body)
        except ValueError:
            return e.code, {"raw": body}


# ── 复用 S2 判据里的最小 TaskNode/Task 形状 ──────────────────


class _Node:
    def __init__(self, name, agent=None, status="success", attempts=1,
                 skip_reason="", error="", started_at=0.0, finished_at=0.0,
                 dependencies=None, result=None, max_retries=3):
        self.name = name
        self.agent_name = agent or name
        self.status = status
        self.attempts = attempts
        self.skip_reason = skip_reason
        self.error = error
        self.started_at = started_at
        self.finished_at = finished_at
        self.dependencies = dependencies or []
        self.result = result if result is not None else {}
        self.max_retries = max_retries


class _Task:
    def __init__(self, nodes, owner="", status_value="done"):
        self.id = "t1"
        self.pipeline_name = "demo"
        self.input_file = "in.md"
        self.config = {}
        self.result = {}
        st = MagicMock()
        st.value = status_value
        self.status = st
        self.owner = owner
        self.created_at = 1.0
        self.finished_at = 2.0
        self.error = ""
        self.dag_nodes = {n.name: n for n in nodes}
        self.execution_order = [[n.name] for n in nodes] or []
        self.steps = []
        self.awaiting_approval = None
        self.approvals = []
        self.checkpoint_file = ""


class TestInspectorBasics:
    def test_full_payload_shape(self, orch_mock):
        now = 1000.0
        task = _Task([
            _Node("writer", status="success", attempts=2,
                  started_at=now, finished_at=now + 1.5, dependencies=["researcher"],
                  result={"content": "x" * 500, "word_count": 812,
                          "sections": ["a", "b"], "meta": {"model": "kimi"}}),
        ])
        orch_mock.get_task.return_value = task
        port, api = _start(orch_mock)
        try:
            status, data = _get(port, "/tasks/t1/nodes/writer")
            assert status == 200
            assert data["task_id"] == "t1"
            node = data["node"]
            assert node["name"] == "writer" and node["agent"] == "writer"
            assert node["status"] == "success" and node["attempts"] == 2
            assert node["dependencies"] == ["researcher"]
            assert node["max_retries"] == 3
            # artifact 预览：形状 + 截断，不是整包
            art = data["artifact"]
            assert art["keys"] == ["content", "meta", "sections", "word_count"]
            assert art["size"] == 4
            assert art["preview"]["content"]["length"] == 500
            assert art["preview"]["content"]["value"].endswith("…")
            assert art["preview"]["word_count"] == {"type": "int", "value": 812}
            assert art["preview"]["sections"] == {"type": "list", "length": 2}
            assert art["preview"]["meta"] == {"type": "dict", "keys": ["model"]}
            assert art["truncated"] is True
        finally:
            api.stop()

    def test_idempotency_key_format(self, orch_mock):
        task = _Task([_Node("researcher", attempts=2)])
        orch_mock.get_task.return_value = task
        port, api = _start(orch_mock)
        try:
            _, data = _get(port, "/tasks/t1/nodes/researcher")
            idem = data["idempotency"]
            assert idem["key"] == "t1:researcher:2"
            assert idem["attempts"] == 2
            # 这里是 MagicMock orch：bus._store 取不到 → None（不猜 false）
            assert idem["recorded"] is None
        finally:
            api.stop()

    def test_idempotency_reads_real_processed_keys(self, orch_mock, tmp_path):
        """真 PersistentStore：键在表里 recorded=True，不在=False。"""
        from loom_core.message_store import PersistentStore
        store = PersistentStore(str(tmp_path / "bus.db"))
        store.save_idempotent("t1:writer:1")
        orch_mock.bus._store = store
        task = _Task([_Node("writer", attempts=1)])
        orch_mock.get_task.return_value = task
        port, api = _start(orch_mock)
        try:
            _, data = _get(port, "/tasks/t1/nodes/writer")
            assert data["idempotency"]["recorded"] is True
            # 换个 attempts → 另一个键，不在表里 = False（而非 None：真问到了）
            task.dag_nodes["writer"].attempts = 2
            _, data2 = _get(port, "/tasks/t1/nodes/writer")
            assert data2["idempotency"]["key"] == "t1:writer:2"
            assert data2["idempotency"]["recorded"] is False
        finally:
            api.stop()
            store.close_all()


    def test_missing_case_and_breaker_are_none(self, orch_mock):
        """没有质量分形状、没有建过熔断器/限流器 → None，不编。"""
        task = _Task([_Node("fetcher", result={"content": "plain text"})])
        orch_mock.get_task.return_value = task
        port, api = _start(orch_mock)
        try:
            _, data = _get(port, "/tasks/t1/nodes/fetcher")
            assert data["quality"] is None, "普通节点结果不该被当成质量分"
            assert data["circuit_breaker"] is None, "没建过熔断器不该显示 closed"
            assert data["rate_limit"] is None
        finally:
            api.stop()

    def test_circuit_breaker_and_rate_limit_when_present(self, orch_mock):
        cb = MagicMock()
        cb.state.name = "OPEN"
        cb.failure_count = 3
        orch_mock._cb_registry.get.return_value = cb
        limiter = MagicMock()
        limiter.stats.return_value = {"rate": 10.0, "burst": 20,
                                      "available_tokens": 12.5,
                                      "total_acquired": 40, "total_blocked": 2}
        orch_mock._rate_limiters.get.return_value = limiter
        task = _Task([_Node("researcher")])
        orch_mock.get_task.return_value = task
        port, api = _start(orch_mock)
        try:
            _, data = _get(port, "/tasks/t1/nodes/researcher")
            assert data["circuit_breaker"] == {"state": "OPEN", "failure_count": 3}
            rl = data["rate_limit"]
            assert rl["rate"] == 10.0 and rl["available_tokens"] == 12.5
            assert rl["total_blocked"] == 2
        finally:
            api.stop()

    def test_quality_detail_only_for_quality_shaped_result(self, orch_mock):
        task = _Task([_Node("quality_gate", result={
            "status": "fail", "overall_score": 42.5,
            "scores": {"completeness": 40.0, "structure": 45.0},
            "profile": "strict", "threshold": 60.0,
            "style_issues": ["过多被动句"], "needs_regenerate": True})])
        orch_mock.get_task.return_value = task
        port, api = _start(orch_mock)
        try:
            _, data = _get(port, "/tasks/t1/nodes/quality_gate")
            q = data["quality"]
            assert q["overall_score"] == 42.5
            assert q["scores"] == {"completeness": 40.0, "structure": 45.0}
            assert q["profile"] == "strict" and q["needs_regenerate"] is True
            assert q["style_issues"] == ["过多被动句"]
        finally:
            api.stop()

    def test_not_found_and_invalid_names(self, orch_mock):
        task = _Task([_Node("writer")])
        orch_mock.get_task.return_value = task
        port, api = _start(orch_mock)
        try:
            status, data = _get(port, "/tasks/t1/nodes/ghost")
            assert status == 404 and "node" in str(data).lower()
            # 任务本身不存在：同为 404（越权不可区分由身份判据覆盖）
            orch_mock.get_task.return_value = None
            status2, _ = _get(port, "/tasks/t1/nodes/writer")
            assert status2 == 404
        finally:
            api.stop()


class TestCheckpointView:
    def test_no_checkpoint_file(self, orch_mock):
        task = _Task([_Node("writer")])
        orch_mock.get_task.return_value = task
        port, api = _start(orch_mock)
        try:
            _, data = _get(port, "/tasks/t1/nodes/writer")
            cp = data["checkpoint"]
            assert cp["path"] == "" and cp["exists"] is False
            assert cp["resumed"] is False
        finally:
            api.stop()

    def test_resumed_flag_from_paused_with_done_nodes(self, orch_mock, tmp_path):
        cp_file = tmp_path / "t1.json"
        cp_file.write_text("{}", encoding="utf-8")
        task = _Task([_Node("writer", status="success")], status_value="paused")
        task.checkpoint_file = str(cp_file)
        orch_mock.get_task.return_value = task
        port, api = _start(orch_mock)
        try:
            _, data = _get(port, "/tasks/t1/nodes/writer")
            cp = data["checkpoint"]
            assert cp["exists"] is True
            assert cp["resumed"] is True, "PAUSED + 有成功节点 = 从断点恢复"
            assert cp["completed_nodes"] == 1
        finally:
            api.stop()

    def test_paused_without_done_nodes_is_not_resumed(self, orch_mock):
        task = _Task([_Node("writer", status="running")], status_value="paused")
        orch_mock.get_task.return_value = task
        port, api = _start(orch_mock)
        try:
            _, data = _get(port, "/tasks/t1/nodes/writer")
            assert data["checkpoint"]["resumed"] is False
        finally:
            api.stop()


class TestS2CarriesCheckpoint:
    def test_dag_view_includes_checkpoint_marker(self, orch_mock):
        task = _Task([_Node("writer")])
        orch_mock.get_task.return_value = task
        port, api = _start(orch_mock)
        try:
            _, data = _get(port, "/tasks/t1")
            assert "checkpoint" in data["dag"]
            assert data["dag"]["checkpoint"]["exists"] is False
        finally:
            api.stop()


class TestProjectionFallback:
    """内存淘汰 / 跨进程运行：节点明细从 RunStore 投影答（与 `/tasks/<id>` 同口径）。

    回归动机（2026-10-30 控制台实测评审）：列表能开运行详情、点节点却 404——
    `_handle_node_inspector` 只查内存，没有 `/tasks/<id>` 已有的投影回退；
    跨进程跑完的运行（本仓控制台的主用例）点任何节点必 404。
    """

    RUN = {
        "run_id": "t9", "workflow": "demo", "status": "done", "principal": "",
        "started_at": 1000.0, "finished_at": 1002.0, "error": "",
    }

    NODES = [{
        "node": "writer", "status": "success", "attempts": 2,
        "started_at": 1000.0, "finished_at": 1000.5,
        "error": "", "artifact_keys": ["content"],
    }]

    def _handle(self, orch_mock, *, record, nodes):
        handle = MagicMock()
        handle.record.return_value = record
        handle.nodes.return_value = nodes
        orch_mock.get_task.return_value = None  # 内存里没有
        orch_mock.run_handle.return_value = handle
        return handle

    def test_node_detail_from_projection_when_not_in_memory(self, orch_mock):
        self._handle(orch_mock, record=self.RUN, nodes=self.NODES)
        port, api = _start(orch_mock)
        try:
            status, data = _get(port, "/tasks/t9/nodes/writer")
            assert status == 200
            assert data["source"] == "runstore"
            assert data["task_status"] == "done"
            assert data["node"]["name"] == "writer"
            assert data["node"]["status"] == "success"
            assert data["node"]["attempts"] == 2
            assert data["node"]["duration_ms"] == 500.0
            assert data["node"]["artifact_keys"] == ["content"]
            # 重试上限不落投影：给 None（前端显示 "?"），不拿 0 冒充"不许重试"
            assert data["node"]["max_retries"] is None
            # 四类明细不落投影：取不到就不编（显式 None，前端据此换文案）
            for key in ("artifact", "idempotency", "circuit_breaker",
                        "rate_limit", "quality", "checkpoint"):
                assert data[key] is None, key
        finally:
            api.stop()

    def test_ghost_node_in_projection_is_404(self, orch_mock):
        self._handle(orch_mock, record=self.RUN, nodes=self.NODES)
        port, api = _start(orch_mock)
        try:
            status, data = _get(port, "/tasks/t9/nodes/ghost")
            assert status == 404
            assert "node" in str(data).lower()
        finally:
            api.stop()

    def test_unknown_task_still_404_from_projection(self, orch_mock):
        """内存和投影都没有 → 404（越权与不存在同形由 `_scoped_run` 保证）。"""
        self._handle(orch_mock, record=None, nodes=self.NODES)
        port, api = _start(orch_mock)
        try:
            status, _ = _get(port, "/tasks/t9/nodes/writer")
            assert status == 404
        finally:
            api.stop()

    def test_memory_path_wins_when_task_is_present(self, orch_mock):
        """内存里有任务时仍走内存路径（不回归：投影键 `source` 不出现）。"""
        task = _Task([_Node("writer")])
        orch_mock.get_task.return_value = task
        port, api = _start(orch_mock)
        try:
            status, data = _get(port, "/tasks/t1/nodes/writer")
            assert status == 200
            assert "source" not in data
        finally:
            api.stop()
