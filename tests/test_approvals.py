"""审批跨重启 / 收件箱 / 租户作用域判据（2026-10-20 P0）。

背景：审批此前只活在内存（`PipelineTask.awaiting_approval`），进程一重启
`approve()` 就找不到任务——"人审"变成一次性演示。本文件钉住修复后的契约：

  1. 台账本身：请求复用、只有 pending 能被决策、决定消费、按 principal 过滤；
  2. **真重启**：子进程跑到闸门后 `os._exit`（无收尾、无内存），父进程用新
     orchestrator 批准 → 队列行回到 pending+resume → worker 从 checkpoint 续跑完成；
  3. 驳回同理：重启后驳回 → 队列行 cancelled、worker 不再执行；
  4. 真 HTTP：收件箱按 token 隔离、admin 看全量；越权审批 404（不泄露存在性）；
     日志/质量/告警这些跨租户聚合面对非 admin 403；dashboard 任务列表按 principal 过滤。
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import textwrap
import time
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import MagicMock

import pytest

PROJECT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT))

from loom_core import PipelineOrchestrator  # noqa: E402
from loom_core.admin_api import AdminAPI, AdminHandler  # noqa: E402
from loom_core.approvals import (  # noqa: E402
    APPROVED,
    REJECTED,
    ApprovalStore,
)
from loom_core.scheduler import Scheduler  # noqa: E402
from loom_core.task_queue import TaskQueue  # noqa: E402
from loom_core.worker import TaskWorker  # noqa: E402

GATE_YAML = """
name: gate_demo
agents:
  - name: tldr
    version: "0.1"
    dependencies: []
    config: {max_sentences: 2}
  - name: release_gate
    approval:
      message: "确认对外发布"
    dependencies: ["tldr"]
topology:
  levels: [["tldr"], ["release_gate"]]
  edges: [["tldr", "release_gate"]]
"""


# ─── 1. 台账单元 ───────────────────────────────────

class TestApprovalStore:
    @pytest.fixture
    def store(self, tmp_path):
        return ApprovalStore(str(tmp_path / "approvals.db"))

    def test_request_is_reused_while_pending(self, store):
        first = store.request("r1", "p", ["g"], {"g": "ok"}, principal="alice")
        second = store.request("r1", "p", ["g"], {"g": "ok"}, principal="alice")
        assert first == second, "同一运行在决定前重复 arm 不能造出两条待办"
        assert len(store.list_pending()) == 1

    def test_only_pending_can_be_decided(self, store):
        rid = store.request("r1", "p", ["g"])
        assert store.decide(rid, APPROVED, by="alice") is True
        assert store.decide(rid, REJECTED, by="bob") is False, "已决行不能被二次覆盖"
        row = store.get(rid)
        assert row["status"] == APPROVED and row["decided_by"] == "alice"

    def test_open_decision_until_consumed(self, store):
        rid = store.request("r1", "p", ["g"])
        store.decide(rid, APPROVED, note="看过了")
        assert store.open_decision("r1")["request_id"] == rid
        store.consume(rid)
        assert store.open_decision("r1") is None, "已消费的决定不能再次越过闸门"

    def test_pending_filtered_by_principal(self, store):
        store.request("a", "p", ["g"], principal="alice")
        store.request("b", "p", ["g"], principal="bob")
        assert [r["run_id"] for r in store.list_pending("alice")] == ["a"]
        assert {r["run_id"] for r in store.list_pending(None)} == {"a", "b"}

    def test_unknown_decision_rejected(self, store):
        rid = store.request("r1", "p", ["g"])
        with pytest.raises(ValueError):
            store.decide(rid, "maybe")

    def test_open_decision_is_bound_to_the_gate_nodes(self, store):
        """多闸门共用 run_id：G2 的决定不得被 G1 借走（2026-10-20 复核确认）。"""
        first = store.request("r1", "p", ["g1"])
        store.decide(first, APPROVED, note="g1-note")
        store.consume(first)
        second = store.request("r1", "p", ["g2"])
        store.decide(second, APPROVED, note="g2-note")
        assert store.open_decision("r1", nodes=["g1"]) is None,             "G1 拿到了 G2 的决定"
        assert store.open_decision("r1", nodes=["g2"])["note"] == "g2-note"

    def test_history_is_newest_first(self, store):
        first = store.request("r1", "p", ["g1"])
        store.decide(first, APPROVED)
        store.consume(first)          # 第一道闸门已过，才轮到第二道
        store.request("r1", "p", ["g2"])
        rows = store.list_for_run("r1")
        assert len(rows) == 2 and rows[0]["nodes"] == ["g2"]


# ─── 2/3. 真重启：子进程死在闸门上 ─────────────────

CHILD_SCRIPT = textwrap.dedent("""
    import os, sys, time
    sys.path.insert(0, sys.argv[1])
    from loom_core import PipelineOrchestrator
    from loom_core.scheduler import Scheduler

    yaml_path, agents_dir, ckpt, inp, task_id = sys.argv[2:7]
    orch = PipelineOrchestrator(agents_dir=agents_dir, checkpoint_dir=ckpt)
    orch.register_agents()
    plan = Scheduler(agents_dir=agents_dir,
                     pipeline_dir=str(__import__("pathlib").Path(yaml_path).parent)
                     ).parse_file(yaml_path, verify_lock=False)
    task = orch.run_plan(plan, input_file=inp, task_id=task_id, wait=False)
    deadline = time.time() + 40
    while time.time() < deadline and not task.awaiting_approval \\
            and task.status.value not in ("done", "failed", "cancelled"):
        time.sleep(0.05)
    print("GATE" if task.awaiting_approval
          else "NOSTATE:%s:%s" % (task.status.value, task.error))
    sys.stdout.flush()
    os._exit(0)  # 硬退出：不跑收尾、不留内存态——等价于进程被杀死
""")


def _pipeline_dir(tmp_path: Path) -> Path:
    d = tmp_path / "pipes"
    d.mkdir(exist_ok=True)
    (d / "gate_demo.yaml").write_text(GATE_YAML, encoding="utf-8")
    return d


def _run_child_to_gate(tmp_path: Path, task_id: str) -> None:
    pipes = _pipeline_dir(tmp_path)
    inp = tmp_path / "in.md"
    inp.write_text("# 标题\n\n这是测试输入内容，用于触发摘要链路。\n", encoding="utf-8")
    script = tmp_path / "child_to_gate.py"
    script.write_text(CHILD_SCRIPT, encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(script), str(PROJECT), str(pipes / "gate_demo.yaml"),
         str(PROJECT / "agents"), str(tmp_path / "ckpt"), str(inp), task_id],
        capture_output=True, text=True, timeout=120, cwd=str(tmp_path),
    )
    assert "GATE" in proc.stdout, \
        f"子进程没停在闸门上：{proc.stdout!r} / {proc.stderr[-800:]!r}"


def _new_orch(tmp_path: Path) -> PipelineOrchestrator:
    orch = PipelineOrchestrator(agents_dir=str(PROJECT / "agents"),
                                checkpoint_dir=str(tmp_path / "ckpt"))
    orch.register_agents()
    return orch


def _worker(tmp_path: Path, orch: PipelineOrchestrator) -> TaskWorker:
    pipes = _pipeline_dir(tmp_path)
    return TaskWorker(orchestrator=orch, queue=TaskQueue(),
                      scheduler=Scheduler(agents_dir=str(PROJECT / "agents"),
                                          pipeline_dir=str(pipes)),
                      agents_dir=str(PROJECT / "agents"),
                      pipeline_dir=str(pipes))


class TestDurableApprovalAcrossRestart:
    def test_approve_after_restart_resumes_and_completes(self, tmp_path, monkeypatch):
        task_id = "restart_ok"
        _run_child_to_gate(tmp_path, task_id)

        orch = _new_orch(tmp_path)
        try:
            assert orch.get_task(task_id) is None, "前提：新进程内存里没有这个任务"
            pending = orch.approvals.pending_for_run(task_id)
            assert pending is not None and pending["nodes"] == ["release_gate"]

            ok, err = orch.approve(task_id, note="重启后批准", by="alice")
            assert ok and err == "", err
            assert orch.task_queue.get(task_id)["status"] == "pending", \
                "批准后队列行必须放回 pending 才会被 worker 续跑"

            monkeypatch.chdir(tmp_path)  # worker 的 output/ 落点别写进仓库
            item = _worker(tmp_path, orch).run_once()
            assert item and item["task_id"] == task_id
            assert item["resume"] is True, "worker 必须拿到续跑标记"

            task = orch.get_task(task_id)
            assert task is not None
            deadline = time.time() + 30
            while time.time() < deadline and task.status.value not in ("done", "failed", "cancelled"):
                time.sleep(0.05)
            assert task.status.value == "done", f"续跑未完成：{task.status.value} / {task.error}"
            gate = task.dag_nodes["release_gate"]
            assert gate.status == "success"
            assert gate.result["status"] == "approved"
            assert gate.result["note"] == "重启后批准"
            assert orch.approvals.open_decision(task_id) is None, "决定应已被消费"
            assert orch.approvals.list_for_run(task_id)[0]["status"] == APPROVED
            assert orch.task_queue.get(task_id)["status"] == "done"
        finally:
            orch.shutdown()

    def test_reject_after_restart_blocks_resume(self, tmp_path):
        task_id = "restart_no"
        _run_child_to_gate(tmp_path, task_id)

        orch = _new_orch(tmp_path)
        try:
            ok, err = orch.reject(task_id, reason="内容不合规", by="alice")
            assert ok and err == "", err
            assert orch.task_queue.get(task_id)["status"] == "cancelled"
            assert orch.approvals.list_for_run(task_id)[0]["status"] == REJECTED
            assert _worker(tmp_path, orch).run_once() is None, "驳回过的任务不能再被执行"
        finally:
            orch.shutdown()


# ─── 4. 真 HTTP：收件箱作用域与 admin 面 ───────────

TOK_A = "tok-alice-approvals"
TOK_B = "tok-bob-approvals"
TOK_OPS = "tok-ops-approvals"


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
  - id: alice
    token_sha256: {hashlib.sha256(TOK_A.encode()).hexdigest()}
  - id: bob
    token_sha256: {hashlib.sha256(TOK_B.encode()).hexdigest()}
  - id: ops
    admin: true
    token_sha256: {hashlib.sha256(TOK_OPS.encode()).hexdigest()}
""", encoding="utf-8")
    return f


def _start(orch, tokens_file: Path):
    api = AdminAPI(host="127.0.0.1", port=0, api_key="",
                   tokens_file=str(tokens_file))
    assert api.start(orch) is True
    return f"http://127.0.0.1:{api._server.server_address[1]}", api


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


class TestApprovalHTTP:
    def test_inbox_scoped_and_durable_approve(self, tmp_path):
        orch = _new_orch(tmp_path)
        orch.approvals.request("run_a", "gate_demo", ["g"], {"g": "确认"}, principal="alice")
        orch.approvals.request("run_b", "gate_demo", ["g"], {"g": "确认"}, principal="bob")
        base, api = _start(orch, _tokens_yaml(tmp_path))
        try:
            code, data = _req("GET", base + "/api/approvals", TOK_A)
            assert code == 200, data
            assert [a["run_id"] for a in data["approvals"]] == ["run_a"]

            code, data = _req("GET", base + "/api/approvals", TOK_OPS)
            assert {a["run_id"] for a in data["approvals"]} == {"run_a", "run_b"}
            assert all("principal" in a for a in data["approvals"])

            # 越权审批按不存在处理（404，不泄露存在性）
            code, _ = _req("POST", base + "/tasks/run_a/approve", TOK_B, {})
            assert code == 404

            # 日志/质量/告警是跨租户聚合面：非 admin 403
            assert _req("GET", base + "/api/logs?limit=1", TOK_A)[0] == 403
            assert _req("GET", base + "/api/quality/feedback", TOK_A)[0] == 403
            assert _req("GET", base + "/api/logs?limit=1", TOK_OPS)[0] == 200

            # 自己的待办可以批（持久路径：任务不在内存）
            code, data = _req("POST", base + "/tasks/run_a/approve", TOK_A, {"note": "ok"})
            assert code == 200 and data["approved"] is True, data
            assert orch.approvals.pending_for_run("run_a") is None
            assert orch.approvals.list_for_run("run_a")[0]["status"] == APPROVED
        finally:
            api.stop()
            orch.shutdown()

    def test_dashboard_hides_other_tenants_tasks(self, tmp_path):
        from types import SimpleNamespace

        def _task(tid, owner):
            return SimpleNamespace(
                id=tid, owner=owner, pipeline_name="gate_demo",
                status=SimpleNamespace(value="done"),
                created_at=0, finished_at=0, error=None,
                result={}, output_path="", progress=100, steps=[],
            )

        orch = MagicMock()
        orch.list_tasks.return_value = [_task("t_a", "alice"), _task("t_b", "bob")]
        orch.registry.list.return_value = []
        base, api = _start(orch, _tokens_yaml(tmp_path))
        try:
            code, data = _req("GET", base + "/api/dashboard", TOK_A)
            assert code == 200 and [t["id"] for t in data["tasks"]] == ["t_a"]
            code, data = _req("GET", base + "/api/dashboard", TOK_OPS)
            assert {t["id"] for t in data["tasks"]} == {"t_a", "t_b"}
        finally:
            api.stop()


class TestPauseCheckpointIsFullState:
    def test_pause_boundary_keeps_completed_nodes(self, tmp_path):
        """暂停边界必须是完整检查点：浅存档会丢掉已完成节点（复核确认）。"""
        task_id = "cp_full"
        _run_child_to_gate(tmp_path, task_id)
        ckpt = tmp_path / "ckpt" / f"{task_id}.json"
        data = json.loads(ckpt.read_text(encoding="utf-8"))
        nodes = data.get("_dag_nodes") or {}
        assert "tldr" in nodes, f"暂停检查点没有已完成节点：{sorted(data)}"
        assert nodes["tldr"]["status"] == "success"
        assert data.get("_result"), "暂停检查点没有结果快照"
