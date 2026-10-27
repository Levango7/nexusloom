"""投影健康位（Stage 3b，2026-10-24 拍板："标 incomplete 后继续"）。

要治的是这个形状：队列写终态成功、RunStore 投影写失败 ⇒ 这条运行在观测面里
**和正常完成长得一模一样**（`/tasks/<id>` 靠回落读得到状态，只是投影里查无此运行）。
原先只有一条 warning 日志，等于把缺口留给以后翻日志的人。

口径落成的样子：

- 队列终态**照写**、worker 继续——观测面故障不放大成可用性故障；
- 但队列行留下 `projection` 标记（`ok` / `incomplete` / `legacy`），顺序是
  **先悲观后升格**：先按 incomplete 写，投影确认落地才升 ok。反过来会开一个
  "队列说一切正常、投影没有"的窗口，而那正是这批要消灭的东西；
- `run_store_health()` 报 `incomplete_runs`（要补账）与 `legacy_runs`（按设计缺），
  两个数不能混成一个：把 deprecated 兜底路径报成数据丢失，比不报更糟；
- `RunHandle.projection()` 让这个维度在句柄上直接可读，`to_dict()` 带着走。

范围边界（照实说）：这批只做到**可见**，没有做补账工具（把一个 incomplete 运行
重新投影进 runs 表）——那需要"重放一次终态写入"的幂等入口，属后续批。
"""
from __future__ import annotations

import time
from pathlib import Path

from loom_core.deliveries import DeliveryLedger
from loom_core.pipeline import PipelineOrchestrator
from loom_core.run_handle import RunHandle
from loom_core.run_store import RunStore
from loom_core.scheduler import Scheduler

PROJECT = Path(__file__).resolve().parent.parent

PLAIN_YAML = """
name: ph_plain
agents:
  - name: tldr
    version: "0.1"
    dependencies: []
    config: {max_sentences: 2}
topology:
  levels: [["tldr"]]
  edges: []
"""

INPUT = "# 标题\n\n这是投影健康位判据的测试输入，用于触发摘要链路。\n"


def _state_dir(monkeypatch, tmp_path: Path) -> Path:
    d = tmp_path / "state"
    monkeypatch.setenv("LOOM_STATE_DIR", str(d))
    return d


def _plan(tmp_path: Path, name: str = "plain"):
    d = tmp_path / f"pipe_{name}"
    d.mkdir(parents=True, exist_ok=True)
    f = d / f"{name}.yaml"
    f.write_text(PLAIN_YAML, encoding="utf-8")
    return Scheduler(agents_dir=str(PROJECT / "agents"),
                     pipeline_dir=str(d)).parse_file(str(f), verify_lock=False)


def _orch(tmp_path: Path) -> PipelineOrchestrator:
    orch = PipelineOrchestrator(agents_dir=str(PROJECT / "agents"),
                                checkpoint_dir=str(tmp_path / "ckpt"))
    orch.register_agents()
    return orch


def _run(orch, tmp_path, task_id: str):
    inp = tmp_path / f"{task_id}.md"
    inp.write_text(INPUT, encoding="utf-8")
    return orch.run_plan(_plan(tmp_path), input_file=str(inp),
                         task_id=task_id, wait=True)


def _wait_projection(orch, task_id: str, want: str, timeout: float = 10.0) -> dict:
    """等队列行落到期望的健康位——队列是**异步升格**的，不能读完就走。"""
    deadline = time.monotonic() + timeout
    row = None
    while time.monotonic() < deadline:
        row = orch.task_queue.get(task_id)
        if isinstance(row, dict) and row.get("projection") == want:
            return row
        time.sleep(0.05)
    raise AssertionError(f"投影健康位没到 {want!r}：{row}")


class TestIncompleteMarker:
    def test_terminal_projection_failure_is_marked_not_swallowed(self, tmp_path,
                                                                 monkeypatch):
        """投影盖章炸掉：队列终态照写，但行上必须留下 incomplete。"""
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)

        def _boom(*args, **kwargs):
            raise RuntimeError("runs.db 写不进去")

        monkeypatch.setattr(RunStore, "close_run", _boom)
        task = _run(orch, tmp_path, "ph_boom")
        assert task.status.value == "done", "产品口径：投影失败不拖垮执行"

        row = _wait_projection(orch, "ph_boom", "incomplete")
        assert row["status"] == "done"
        health = orch.run_store_health()
        assert health["incomplete_runs"] == 1, health
        assert orch.run_handle("ph_boom").projection() == "incomplete"
        assert orch.run_handle("ph_boom").to_dict()["projection"] == "incomplete"

    def test_store_unavailable_also_marks_incomplete(self, tmp_path, monkeypatch):
        """写口整个建不起来（比 close_run 抛更狠的一种）同样要留痕。"""
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        monkeypatch.setattr(orch, "_runstore", lambda: None)
        _run(orch, tmp_path, "ph_none")

        row = _wait_projection(orch, "ph_none", "incomplete")
        assert row["status"] == "done"
        health = orch.run_store_health()
        assert health["status"] == "degraded" and health["available"] is False
        assert health["incomplete_runs"] == 1, health

    def test_successful_run_upgrades_to_ok(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        _run(orch, tmp_path, "ph_ok")

        row = _wait_projection(orch, "ph_ok", "ok")
        assert row["status"] == "done"
        health = orch.run_store_health()
        assert health["incomplete_runs"] == 0, health
        assert orch.run_handle("ph_ok").projection() == "complete"
        # 投影里确实在：这条断言把 "ok" 与"查无此运行"钉死成互斥，
        # 否则标记可以随便写而不反映任何事实。
        assert orch._runstore().get_run("ph_ok") is not None

    def test_legacy_path_now_writes_real_projection(self, tmp_path, monkeypatch):
        """B8 引擎收敛后：legacy run() 委托给 run_plan()，写真实 RunStore 投影。

        收敛前 legacy 路径从不调 open/nodes/close（按设计缺，标记为 "legacy"）；
        收敛后 legacy 经由 run_plan() 的统一引擎执行，投影标记升为 "ok"，
        run_handle().projection() 报 "complete"。这里钉住的是"legacy 不再
        是投影缺失的代名词"——incomplete 计数仍为 0，但 legacy_runs 也为 0。

        运行结果不钉死：legacy 走注册表依赖序（真 docgen 链路，可能因环境判 failed），
        终态写没写成都会经过 _finalize_plan_task。
        """
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        inp = tmp_path / "in.md"
        inp.write_text(INPUT, encoding="utf-8")
        task = orch.run(task_id="ph_legacy", pipeline_name="plain",
                        input_file=str(inp), wait=True)
        assert task.status.value in ("done", "failed", "cancelled"), task.status.value

        row = _wait_projection(orch, "ph_legacy", "ok")
        assert row["status"] in ("done", "failed", "cancelled"), row
        health = orch.run_store_health()
        assert health["incomplete_runs"] == 0, health
        assert health["legacy_runs"] == 0, health
        assert orch.run_handle("ph_legacy").projection() == "complete"
        # 投影里确实在：legacy 不再是"按设计缺投影"，而是有真实运行行
        assert orch._runstore().get_run("ph_legacy") is not None


class TestHandleReportsIt:
    def test_ledger_only_run_reads_as_unknown(self, tmp_path, monkeypatch):
        """Stage 1 之前的老运行只存在于账本：说不了投影健康位，就报 unknown。

        反向的那条更要紧：不能因为"回落读到了什么"就报 complete——那正是
        这批要消灭的"回落掩盖缺失"。老运行的交付事实走 `deliveries` 面
        （`to_dict()["source"] == "deliveries"`），而 `status()` 从来不看账本，
        所以这里同时钉住两件事：账本回落在，投影维度不越权声称。
        """
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        DeliveryLedger().record({
            "run_id": "ph_old", "pipeline": "plain", "status": "done",
            "payload": {"nodes": []},
        })
        handle = orch.run_handle("ph_old")
        assert handle.projection() == "unknown", handle.projection()
        payload = handle.to_dict()
        assert payload["source"] == "deliveries", payload
        assert payload["projection"] == "unknown"

    def test_unknown_run_is_not_complete_either(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        handle = orch.run_handle("ph_ghost")
        assert handle.status() == "unknown"
        assert handle.projection() == "unknown"
        assert RunHandle(orch, "ph_ghost").to_dict()["source"] == "none"
