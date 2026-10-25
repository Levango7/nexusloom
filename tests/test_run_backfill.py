"""补账工具（2026-10-25）：把 `projection='incomplete'` 的运行按交付账本重投影。

Stage 3b 把缺口变成了**可见**（队列 `projection` 列 + `incomplete_runs` 计数），
这批给它一个出口。判据要钉的不是"能补上"，而是**补得诚实**：

- 只补缺：已有的 `run_nodes` 行不被覆盖（原始数据比事后重建的更细）；
- 至多一条终态事件：二次补账、或"已终态只差健康位"的情形，都不许多发一条；
- 不编数据：账本没有节点的绝对时间 ⇒ 补写的行时间列留 0，事件里记 `times=unavailable`；
- 口径冲突不猜：队列 / 账本 / 投影三方状态不一致就拒绝，交给人判断；
- `dry-run` 一个字节都不写；
- `legacy` / `ok` / 账本里没有这条运行都拒绝——"顺手也修一下"会把这个工具变成第二个事实源。

真实形态的来路：Stage 3b 的 `test_projection_health` 用"注入 `close_run` 抛异常"造出
incomplete；这里沿用同一种造法，再在**恢复之后**补账。
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from loom_core.deliveries import DeliveryLedger
from loom_core.pipeline import PipelineOrchestrator
from loom_core.run_store import RunStore
from loom_core.scheduler import Scheduler

PROJECT = Path(__file__).resolve().parent.parent

PLAIN_YAML = """
name: bf_plain
agents:
  - name: tldr
    version: "0.1"
    dependencies: []
    config: {max_sentences: 2}
topology:
  levels: [["tldr"]]
  edges: []
"""

WIDE_YAML = """
name: bf_wide
agents:
  - name: tldr
    version: "0.1"
    dependencies: []
    config: {max_sentences: 2}
  - name: readability
    version: "0.1"
    dependencies: []
topology:
  levels: [["tldr", "readability"]]
  edges: []
"""

INPUT = "# 标题\n\n这是补账工具的测试输入，用于触发摘要链路。\n"

TERMINAL_EVENTS = ("run.completed", "run.failed", "run.cancelled")


def _state_dir(monkeypatch, tmp_path: Path) -> Path:
    d = tmp_path / "state"
    monkeypatch.setenv("LOOM_STATE_DIR", str(d))
    return d


def _plan(tmp_path: Path, name: str, yaml_text: str):
    d = tmp_path / f"pipe_{name}"
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


def _run(orch, tmp_path: Path, task_id: str, yaml_text: str = PLAIN_YAML,
         name: str = "plain"):
    inp = tmp_path / f"{task_id}.md"
    inp.write_text(INPUT, encoding="utf-8")
    return orch.run_plan(_plan(tmp_path, name, yaml_text), input_file=str(inp),
                         task_id=task_id, wait=True)


def _break_projection(monkeypatch, *, nodes: bool = False):
    """让投影写失败：返回还原函数。close 必然失败；nodes=True 时节点也失败。"""
    orig_close = RunStore.close_run
    orig_nodes = RunStore.record_nodes

    def _boom(*args, **kwargs):
        raise RuntimeError("runs.db 写不进去")

    monkeypatch.setattr(RunStore, "close_run", _boom)
    if nodes:
        monkeypatch.setattr(RunStore, "record_nodes", _boom)

    def _restore():
        monkeypatch.setattr(RunStore, "close_run", orig_close)
        monkeypatch.setattr(RunStore, "record_nodes", orig_nodes)

    return _restore


def _events(orch, run_id: str) -> list[dict]:
    return orch._runstore().events(run_id)


def _terminal_events(orch, run_id: str) -> list[dict]:
    return [e for e in _events(orch, run_id) if e["type"] in TERMINAL_EVENTS]


def _replays(orch, run_id: str) -> list[dict]:
    return [e for e in _events(orch, run_id) if e["type"] == "run.replayed"]


def _ledger_done(run_id: str, status: str = "done", nodes: list[dict] | None = None):
    DeliveryLedger().record({
        "run_id": run_id, "pipeline": "plain", "status": status,
        "started_at": 1000.0, "finished_at": 1005.0,
        "nodes": nodes if nodes is not None else [
            {"node": "tldr", "status": "done", "attempts": 1, "duration_sec": 0.1,
             "error": "", "artifact_keys": ["a.md"]}],
    })


def _queued(orch, run_id: str, status: str = "done", projection: str = "incomplete"):
    """造一条"跑了但投影没写成"的队列行（不需要真跑流水线）。"""
    q = orch.task_queue
    q.submit(run_id, "plain", "in.md")
    q.acquire()
    assert q.finish(run_id, status, projection=projection) is True
    return q.get(run_id)


class TestReplayRealFailure:
    def test_after_failed_close_restores_terminal_projection(self, tmp_path, monkeypatch):
        """真正跑一条、只让盖章失败：补账后投影补齐，且终态事件恰好一条。"""
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        restore = _break_projection(monkeypatch)
        task = _run(orch, tmp_path, "bf_close")
        restore()
        assert task.status.value == "done"
        store = orch._runstore()
        assert store.get_run("bf_close")["status"] == "running", "前提：盖章没落"
        assert orch.task_queue.get("bf_close")["projection"] == "incomplete"

        res = orch.replay_run("bf_close")
        assert res["action"] == "replayed", res
        assert res["created_run"] is False
        assert res["nodes_written"] == 0, "节点当时写成功了，补账不该重写"
        assert res["closed"] is True

        run = store.get_run("bf_close")
        assert run["status"] == "done"
        assert run["finished_at"] > 0
        assert [n["node"] for n in store.nodes("bf_close")] == ["tldr"]
        assert orch.task_queue.get("bf_close")["projection"] == "ok"
        assert orch.run_store_health()["incomplete_runs"] == 0
        assert len(_terminal_events(orch, "bf_close")) == 1
        assert len(_replays(orch, "bf_close")) == 1
        assert _replays(orch, "bf_close")[0]["payload"]["times"] == "unavailable"

    def test_fills_nodes_that_the_failed_write_lost(self, tmp_path, monkeypatch):
        """节点写与盖章同时失败：补账把节点补回来，但不给假时间。"""
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        restore = _break_projection(monkeypatch, nodes=True)
        _run(orch, tmp_path, "bf_nodes")
        restore()
        store = orch._runstore()
        assert store.nodes("bf_nodes") == []

        res = orch.replay_run("bf_nodes")
        assert res["action"] == "replayed", res
        assert res["nodes_written"] == 1
        nodes = store.nodes("bf_nodes")
        assert [n["node"] for n in nodes] == ["tldr"]
        assert nodes[0]["status"] == "success", "节点状态字面量来自账本（dag node 的写法）"
        assert nodes[0]["started_at"] == 0 and nodes[0]["finished_at"] == 0, (
            "账本没有节点绝对时间，补写不许编一个")
        # 节点事件补上了（原来那条写失败时丢了）
        assert [e["type"] for e in _events(orch, "bf_nodes")].count("node.finished") == 1
        assert len(_terminal_events(orch, "bf_nodes")) == 1

    def test_second_replay_is_refused_and_emits_nothing(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        restore = _break_projection(monkeypatch)
        _run(orch, tmp_path, "bf_twice")
        restore()
        assert orch.replay_run("bf_twice")["action"] == "replayed"
        before = len(_events(orch, "bf_twice"))

        res = orch.replay_run("bf_twice")
        assert res["action"] == "refused"
        assert "不需要补账" in res["reason"]
        assert len(_events(orch, "bf_twice")) == before, (
            "二次补账又写事件了：重放必须是幂等的")


class TestReplayHonesty:
    def test_existing_node_rows_are_not_overwritten(self, tmp_path, monkeypatch):
        """只补缺：已有节点的行（可能更细/更准）必须原样保留。"""
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        restore = _break_projection(monkeypatch, nodes=True)
        _run(orch, tmp_path, "bf_partial", yaml_text=WIDE_YAML, name="wide")
        restore()
        store = orch._runstore()
        store.record_node("bf_partial", "tldr", "success",
                          error="keep-me", artifact_keys=["mine.md"])

        res = orch.replay_run("bf_partial")
        assert res["action"] == "replayed", res
        assert res["nodes_written"] == 1, "只该补 readability 这一条"
        rows = {n["node"]: n for n in store.nodes("bf_partial")}
        assert rows["tldr"]["error"] == "keep-me"
        assert rows["tldr"]["artifact_keys"] == ["mine.md"]
        assert rows["readability"]["status"] == "success"

    def test_missing_runs_row_is_created_from_ledger(self, tmp_path, monkeypatch):
        """连 runs 行都没有（open 也失败过）：用账本把它建起来。"""
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        _queued(orch, "bf_norow")
        _ledger_done("bf_norow")

        res = orch.replay_run("bf_norow")
        assert res["action"] == "replayed", res
        assert res["created_run"] is True and res["closed"] is True
        run = orch._runstore().get_run("bf_norow")
        assert run["status"] == "done"
        assert run["workflow"] == "plain"
        assert run["started_at"] == 1000.0, "开始时间要来自账本，不是补账的时刻"
        assert run["artifacts"] == ["a.md"]
        assert len(_terminal_events(orch, "bf_norow")) == 1
        assert orch.task_queue.get("bf_norow")["projection"] == "ok"

    def test_already_terminal_only_upgrades_marker(self, tmp_path, monkeypatch):
        """投影已终态、只有健康位没升（升格那步失败）：不许再发一条终态事件。"""
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        store = orch._runstore()
        store.open_run("bf_term", workflow="plain", started_at=1000.0)
        store.close_run("bf_term", "done", finished_at=1005.0)
        _queued(orch, "bf_term")
        _ledger_done("bf_term")
        assert len(_terminal_events(orch, "bf_term")) == 1

        res = orch.replay_run("bf_term")
        assert res["action"] == "replayed", res
        assert res["closed"] is False
        assert len(_terminal_events(orch, "bf_term")) == 1, "第二条终态事件是审计里的假节点"
        assert orch.task_queue.get("bf_term")["projection"] == "ok"

    def test_status_conflict_is_refused(self, tmp_path, monkeypatch):
        """队列说 done、账本说 failed：不猜，拒绝，且什么都不写。"""
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        _queued(orch, "bf_clash", status="done")
        _ledger_done("bf_clash", status="failed")

        res = orch.replay_run("bf_clash")
        assert res["action"] == "refused"
        assert "口径冲突" in res["reason"]
        assert orch._runstore().get_run("bf_clash") is None
        assert _events(orch, "bf_clash") == []
        assert orch.task_queue.get("bf_clash")["projection"] == "incomplete"

    @pytest.mark.parametrize("projection", ["ok", "legacy", ""])
    def test_non_incomplete_markers_are_refused(self, tmp_path, monkeypatch,
                                                projection):
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        q = orch.task_queue
        q.submit("bf_marker", "plain", "in.md")
        q.acquire()
        q.finish("bf_marker", "done", projection=projection or "ok")
        _ledger_done("bf_marker")

        res = orch.replay_run("bf_marker")
        assert res["action"] == "refused", res
        assert orch._runstore().get_run("bf_marker") is None

    def test_running_row_is_refused(self, tmp_path, monkeypatch):
        """运行还没结束（标记却是 incomplete）不许补：那会与执行方抢写同一批行。"""
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        q = orch.task_queue
        q.submit("bf_live", "plain", "in.md")
        q.acquire()                       # → running
        q.set_projection("bf_live", "incomplete")
        _ledger_done("bf_live")           # 账本里有旧行也不行

        res = orch.replay_run("bf_live")
        assert res["action"] == "refused", res
        assert "尚未终态" in res["reason"]
        assert orch._runstore().get_run("bf_live") is None

    def test_missing_ledger_row_is_refused(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        _queued(orch, "bf_noledger")

        res = orch.replay_run("bf_noledger")
        assert res["action"] == "refused"
        assert "交付账本" in res["reason"]
        assert orch.task_queue.get("bf_noledger")["projection"] == "incomplete"

    def test_dry_run_writes_nothing(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        restore = _break_projection(monkeypatch, nodes=True)
        _run(orch, tmp_path, "bf_dry")
        restore()
        store = orch._runstore()

        res = orch.replay_run("bf_dry", dry_run=True)
        assert res["action"] == "dry-run", res
        assert res["closed"] is True and res["nodes_written"] == 1
        assert store.get_run("bf_dry")["status"] == "running"
        assert store.nodes("bf_dry") == []
        assert _replays(orch, "bf_dry") == []
        assert orch.task_queue.get("bf_dry")["projection"] == "incomplete"
        assert orch.run_store_health()["incomplete_runs"] == 1

    def test_batch_touches_only_incomplete_rows(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        restore = _break_projection(monkeypatch)
        _run(orch, tmp_path, "bf_b1")
        _run(orch, tmp_path, "bf_b2")
        restore()
        _run(orch, tmp_path, "bf_b_ok")  # 正常运行，健康位 ok

        res = orch.replay_incomplete_runs()
        assert res["checked"] == 2, res
        assert res["replayed"] == 2
        assert res["failed"] == [] and res["refused"] == []
        for rid in ("bf_b1", "bf_b2"):
            assert orch._runstore().get_run(rid)["status"] == "done"
            assert orch.task_queue.get(rid)["projection"] == "ok"
        # 正常的那条一个字节都没被碰
        assert _replays(orch, "bf_b_ok") == []
        assert orch.task_queue.get("bf_b_ok")["projection"] == "ok"
        assert orch.run_store_health()["incomplete_runs"] == 0


class TestStoreLevelTerminalClaim:
    """存储层自己顶住"至多一条终态事件"——补账与收尾是两个写入者，进程内去重帮不上。

    这条与 PG 腿上那条同判据、不同方言：`_claim_terminal_event` 只在单个
    `PipelineTask` 对象上有效，跨进程（两个补账客户端、或引擎收尾晚到）绕得过它，
    所以裁决必须在 `close_run` 的那条 UPDATE 里。
    """

    def test_second_close_run_writes_no_event(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        store = orch._runstore()
        store.open_run("bf_two", workflow="plain", started_at=1000.0)
        first = store.close_run("bf_two", "done", finished_at=1001.0)
        assert isinstance(first, int), first
        assert store.close_run("bf_two", "failed", finished_at=1002.0) is None
        run = store.get_run("bf_two")
        assert run["status"] == "done" and run["finished_at"] == 1001.0, "晚到的收尾覆写了终态"
        types = [e["type"] for e in _events(orch, "bf_two")]
        assert types.count("run.completed") == 1
        assert "run.failed" not in types

    def test_closing_an_unknown_run_writes_no_event(self, tmp_path, monkeypatch):
        """没有投影行 ⇒ 不该有终态事件（那正是补账要消灭的半条事实）。"""
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        store = orch._runstore()
        assert store.close_run("bf_ghost_close", "done") is None
        assert _events(orch, "bf_ghost_close") == []


class TestCli:
    def test_cli_refuses_unknown_run(self, tmp_path):
        """接线判据：CLI 真能跑到补账口，并且拒绝时退出码非零（别让脚本误以为补过了）。"""
        # 继承父进程环境：只给 LOOM_STATE_DIR + PATH 会让 Windows 的 asyncio 起不来
        # （`_overlapped` 初始化失败 WinError 10106），那与补账接线无关。
        env = dict(os.environ, LOOM_STATE_DIR=str(tmp_path / "state"))
        p = subprocess.run([sys.executable, str(PROJECT / "run.py"),
                            "--replay-run", "bf_ghost", "--replay-dry-run"],
                           cwd=str(PROJECT), capture_output=True, text=True,
                           timeout=120, env=env)
        assert p.returncode == 1, (p.returncode, p.stdout[-800:], p.stderr[-800:])
        assert "bf_ghost" in p.stdout and "refused" in p.stdout
