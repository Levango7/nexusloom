"""两条执行路径必须登记同一份运行事实（RunStore 设计 Stage 0）。

背景（`docs/runstore-eventlog-design-2026-10-21.md` §2）：`run_plan_async` 的**唯一生产调用者**
是控制台的 SSE（`admin_api._start_stream_worker`），而它的收尾代码长期自己写着
"不更新 task_queue、不发事件钩子"，暂停边界还存浅检查点、且不观察别的进程写下的审批决定。
后果是实测过的产品现象：**从 UI 流式发起的运行，在持久队列里查不到、事件面一条不发**，
而它挂在闸门前时永远等不到批准（异步循环不看台账）。

本文件钉"并轨后"的契约：同一份流水线，同步跑与异步跑必须留下**同一份事实**——
队列行、事件序列、完整检查点、外部决定可唤醒、终态事件至多一条。
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from loom_core.event_hook import get_hook_manager
from loom_core.pipeline import PipelineOrchestrator, TaskStatus
from loom_core.scheduler import Scheduler

PROJECT = Path(__file__).resolve().parent.parent

PLAIN_YAML = """
name: parity_plain
agents:
  - name: tldr
    version: "0.1"
    dependencies: []
    config: {max_sentences: 2}
topology:
  levels: [["tldr"]]
  edges: []
"""

GATE_YAML = """
name: parity_gate
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

INPUT = "# 标题\n\n这是测试输入内容，用于触发摘要链路。\n"
TERMINAL = (TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.CANCELLED)


def _plan(tmp_path: Path, subdir: str, name: str, yaml_text: str):
    d = tmp_path / subdir
    d.mkdir(parents=True, exist_ok=True)
    f = d / f"{name}.yaml"
    f.write_text(yaml_text, encoding="utf-8")
    return Scheduler(agents_dir=str(PROJECT / "agents"),
                     pipeline_dir=str(d)).parse_file(str(f), verify_lock=False)


def _orch(tmp_path: Path) -> PipelineOrchestrator:
    orch = PipelineOrchestrator(agents_dir=str(PROJECT / "agents"),
                                checkpoint_dir=str(tmp_path / "ckpt"))
    orch.register_agents()
    return orch


class _Collector:
    """真 hook 通道收事件（不 mock `emit_event`——要钉的就是它实际发了什么）。"""

    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []
        self._hook_id = ""

    def __enter__(self) -> _Collector:
        self._hook_id = get_hook_manager().register(
            "task.*", callback=lambda e, p: self.events.append((e, p)))
        return self

    def __exit__(self, *exc) -> bool:
        get_hook_manager().unregister(self._hook_id)
        return False

    def names_for(self, task_id: str) -> list[str]:
        return [e for e, p in self.events if p.get("task_id") == task_id]


def _wait_until(probe, timeout: float = 60.0, interval: float = 0.05):
    """按判据轮询（不是固定 sleep）：探到值即返回，超时给 None 让断言有内容。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = probe()
        if value:
            return value
        time.sleep(interval)
    return None


def _wait_rows(orch: PipelineOrchestrator, task_ids: list[str],
               timeout: float = 30.0) -> dict | None:
    """等这些队列行都进终态（判据轮询，不是固定 sleep）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = {tid: orch.task_queue.get(tid) for tid in task_ids}
        if all(r and r.get("status") in ("done", "failed", "cancelled")
               for r in rows.values()):
            return rows
        time.sleep(0.05)
    return None


def _drive_async(orch, approver, plan, input_file, task_id, decision) -> dict:
    """在同一个事件循环里：驱动异步运行 → 等它挂在闸门 → 下决定 → 收事实。

    `decision="approve"` 时决定由**另一个实例**下达（走持久台账，执行内存里没有这个任务），
    验证异步循环真的会观察外部决定；
    `decision="cancel"` 时取消走**执行实例本身**——`cancel()` 只认 `_running_tasks`，
    跨实例取消本来就无效（RunStore 设计文档 §1 记的边界之一），这里要钉的是
    "取消与收尾各发一条终态事件"这个重复问题，不是取消的作用域。
    """
    async def main():
        drive = asyncio.create_task(
            orch.run_plan_async(plan, input_file=input_file, task_id=task_id))

        pending = None
        deadline = time.time() + 60
        while time.time() < deadline and pending is None:
            pending = approver.approvals.pending_for_run(task_id)
            if drive.done():          # 没挂起就自己跑完/失败了，别死等
                break
            await asyncio.sleep(0.05)

        facts: dict = {
            "pending": bool(pending),
            "pipeline_name": plan.pipeline_name,
            # 暂停边界的快照写在挂起之后第一件事，挂起行落定不等于快照已落盘
            "checkpoint": _wait_checkpoint(orch, task_id) if pending else {"exists": False},
        }
        if pending:
            if decision == "approve":
                facts["decide"] = approver.approve(task_id, note="异步路径外部批准", by="ops")
            else:
                facts["decide"] = (orch.cancel(task_id), "")
        task = await asyncio.wait_for(drive, timeout=90)
        await asyncio.sleep(0.5)  # 等收尾链把终态事件/队列行写完
        gate = task.dag_nodes.get("release_gate")
        facts.update({
            "status": task.status.value,
            "gate_status": getattr(gate, "status", None),
            "gate_note": (gate.result or {}).get("note") if gate else None,
        })
        return facts

    return asyncio.run(main())


def _wait_checkpoint(orch: PipelineOrchestrator, task_id: str,
                     timeout: float = 10.0) -> dict:
    """等暂停边界那次快照落盘（挂起行与快照是两笔写，先后不保证）。"""
    deadline = time.time() + timeout
    facts = _checkpoint_facts(orch, task_id)
    while time.time() < deadline and not facts["exists"]:
        time.sleep(0.05)
        facts = _checkpoint_facts(orch, task_id)
    return facts


def _checkpoint_facts(orch: PipelineOrchestrator, task_id: str) -> dict:
    """暂停边界检查点的形状：有没有保住已完成节点与结果快照。

    读的是 **manager 的目录**（`orch._checkpoint.checkpoint_dir`），不是 `orch.checkpoint_dir`：
    后者由 `config["checkpoint.dir"]` 派生（默认 `checkpoints`），与构造参数给的目录
    **不是同一个地方**——`save()` 写前者并回写 `task.checkpoint_file`。第一版探针按
    `orch.checkpoint_dir` 找，得到"异步没写检查点"的假结论。
    """
    path = Path(orch._checkpoint.checkpoint_dir) / f"{task_id}.json"  # noqa: SLF001
    if not path.is_file():
        return {"exists": False}
    data = json.loads(path.read_text(encoding="utf-8"))
    nodes = data.get("_dag_nodes") or {}
    return {
        "exists": True,
        "tldr_success": nodes.get("tldr", {}).get("status") == "success",
        "has_result": bool(data.get("_result")),
    }


def _projection_nodes(orch, task_id: str) -> list[str]:
    """投影里这条运行的逐节点行（名字，已排序）。

    Stage 3b 起这是节点结果**唯一**的权威位置：队列的 `result_json` 是第三份拷贝
    且实测全仓无读者（`RunHandle`/`_queue_row` 只用 `status`），已停写。
    """
    store = orch._runstore()  # noqa: SLF001
    assert store is not None, "前提：RunStore 可用，否则 parity 没有可比对象"
    return sorted(n["node"] for n in store.nodes(task_id))


class TestAsyncRegistersSameFacts:
    def test_async_and_sync_leave_the_same_run_facts(self, tmp_path):
        """同一份流水线：同步跑与异步跑的队列行与事件序列必须逐字段对得上。"""
        inp = tmp_path / "in.md"
        inp.write_text(INPUT, encoding="utf-8")
        sync_orch = _orch(tmp_path)
        async_orch = _orch(tmp_path)
        plan_s = _plan(tmp_path, "s", "plain", PLAIN_YAML)
        plan_a = _plan(tmp_path, "a", "plain", PLAIN_YAML)
        try:
            with _Collector() as col:
                sync_orch.run_plan(plan_s, input_file=str(inp), task_id="parity_sync")
                asyncio.run(async_orch.run_plan_async(
                    plan_a, input_file=str(inp), task_id="parity_async"))
                rows = _wait_rows(sync_orch, ["parity_sync", "parity_async"])
                assert rows is not None, "两条路径的队列行没都写成终态"

                assert col.names_for("parity_sync") == col.names_for("parity_async"), (
                    f"事件序列不同形：sync={col.names_for('parity_sync')} "
                    f"async={col.names_for('parity_async')}")
                assert col.names_for("parity_async") == [
                    "task.created", "task.started", "task.completed"]
                for tid, row in rows.items():
                    assert row["status"] == "done", f"{tid} 队列行不是 done：{row}"
                    assert row["pipeline_name"] == plan_s.pipeline_name, row
                    # 节点结果正文从队列挪到投影来比（队列那一列已停写，见 helper）
                    assert row["projection"] == "ok", (
                        f"{tid} 投影终态盖了却没升格，读起来像缺投影：{row}")
                names_s = _projection_nodes(sync_orch, "parity_sync")
                names_a = _projection_nodes(async_orch, "parity_async")
                assert names_s == names_a, (
                    f"逐节点投影不同形：sync={names_s} async={names_a}")
                assert "tldr" in names_s, names_s
        finally:
            sync_orch.shutdown()
            async_orch.shutdown()

    def test_sync_cancel_emits_exactly_one_terminal_event(self, tmp_path):
        """同步路径单独钉"终态事件至多一条"——这里的重复与队列行无关，两件事分开证。

        `cancel()` 在把状态置 cancelled 的当下就发 `task.cancelled`，收尾链随后按状态
        再发一次 ⇒ 一次运行两条收尾事件（Stage 0 前实测如此）。
        """
        inp = tmp_path / "in.md"
        inp.write_text(INPUT, encoding="utf-8")
        orch = _orch(tmp_path)
        try:
            with _Collector() as col:
                plan = _plan(tmp_path, "s2", "gate", GATE_YAML)
                task = orch.run_plan(plan, input_file=str(inp), task_id="cancel_sync",
                                     wait=False)
                pending = _wait_until(lambda: orch.approvals.pending_for_run("cancel_sync"))
                assert pending is not None, "同步运行没停在闸门上"
                assert orch.cancel("cancel_sync") is True
                rows = _wait_rows(orch, ["cancel_sync"])
                assert rows and rows["cancel_sync"]["status"] == "cancelled", rows
                got = col.names_for("cancel_sync")
                assert got.count("task.cancelled") == 1, f"终态事件重复：{got}"
                assert task.status is TaskStatus.CANCELLED
        finally:
            orch.shutdown()

    def test_async_cancel_emits_exactly_one_terminal_event(self, tmp_path):
        """取消路径与收尾链都想发 `task.cancelled`：一个运行至多发一条。"""
        inp = tmp_path / "in.md"
        inp.write_text(INPUT, encoding="utf-8")
        orch = _orch(tmp_path)
        other = _orch(tmp_path)  # 只用它读台账 pending 行，证明挂起来自持久通道
        try:
            with _Collector() as col:
                facts = _drive_async(orch, other,
                                     _plan(tmp_path, "c", "gate", GATE_YAML),
                                     str(inp), "cancel_once", decision="cancel")
                assert facts["pending"] is True, "运行没停在闸门上"
                rows = _wait_rows(orch, ["cancel_once"])
                assert rows and rows["cancel_once"]["status"] == "cancelled", rows
            got = col.names_for("cancel_once")
            assert got.count("task.cancelled") == 1, f"终态事件重复：{got}"
        finally:
            orch.shutdown()
            other.shutdown()


class TestAsyncApprovalGateIsLive:
    def test_async_pause_keeps_full_state_and_wakes_on_external_decision(self, tmp_path):
        """异步暂停边界必须是完整检查点，且必须能被另一个 orchestrator 实例批准唤醒。"""
        inp = tmp_path / "in.md"
        inp.write_text(INPUT, encoding="utf-8")
        orch = _orch(tmp_path)
        approver = _orch(tmp_path)
        try:
            with _Collector() as col:
                facts = _drive_async(orch, approver,
                                     _plan(tmp_path, "g", "gate", GATE_YAML),
                                     str(inp), "ax_gate", decision="approve")
                assert facts["decide"][0] is True, facts["decide"]
                rows = _wait_rows(orch, ["ax_gate"])
                assert rows and rows["ax_gate"]["status"] == "done", rows
            assert facts["checkpoint"]["exists"], "暂停边界没写检查点"
            assert facts["checkpoint"]["tldr_success"] and facts["checkpoint"]["has_result"], (
                f"暂停边界是浅检查点，续跑会整条重跑：{facts['checkpoint']}")
            assert facts["status"] == "done", f"外部批准后异步运行没继续：{facts}"
            assert facts["gate_status"] == "success", facts
            assert facts["gate_note"] == "异步路径外部批准", facts
            assert col.names_for("ax_gate").count("task.completed") == 1, (
                col.names_for("ax_gate"))
            assert approver.approvals.open_decision("ax_gate") is None, "决定没被消费"
        finally:
            orch.shutdown()
            approver.shutdown()
