"""RunStore Stage 1 的三方对账门禁（设计 §4 Stage 1 的退出判据）。

Stage 1 只做一件事：**新增**一个写入口（事件 + `runs`/`run_nodes` 投影同事务），
旧写面一个都不删。于是"两套口径漂移"不能靠人盯——本文件每跑完一次真流水线，
就把四个来源逐字段对一遍：

| 来源 | 写入时机 | 谁写 |
|---|---|---|
| `runs` / `run_nodes` | 层跑完 + 收尾（同一事务） | `RunStore`（新） |
| `task_queue` 行 | 收尾 `finish` | Stage 0 的共用口 |
| `deliveries.payload_json.nodes` | 收尾 `_deliver` | 交付账本 |
| `task.dag_nodes` | 执行期原地更新 | 内存 |

对账必须**跨时机**才有力气：`run_nodes` 读的是层跑完那一刻的 `dag_node`，
`deliveries` 读的是收尾那一刻的同一批对象——如果中间有节点被池化合并改写、
有闸门节点没进执行集合、或某一层的写入被漏掉，两边就会分叉，本文件当场判红。
（反过来：若让 `run_nodes` 直接抄 `_build_delivery_record`，这两栏就成了自己比自己，
射程为零。所以写口刻意分开，见 `pipeline._runstore_nodes` 的注释。）

范围边界（照实说，不留"看起来全覆盖了"的错觉）：门禁是**显式用例**覆盖
两引擎 × done/cancelled，不是"每条用例跑完自动后置检查"——后者要等 Stage 2
读侧切完才有意义，现在挂上去只会把每个既有用例都拖一遍库盘。
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import pytest

from loom_core import run_store
from loom_core.deliveries import DeliveryLedger
from loom_core.pipeline import PipelineOrchestrator
from loom_core.run_store import RunStore
from loom_core.scheduler import Scheduler

PROJECT = Path(__file__).resolve().parent.parent

PLAIN_YAML = """
name: rs_plain
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
name: rs_gate
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


def _state_dir(monkeypatch, tmp_path: Path) -> Path:
    """把 LOOM_STATE_DIR 指到 tmp：runs.db / deliveries.db / task_queue 都跟它走。"""
    d = tmp_path / "state"
    monkeypatch.setenv("LOOM_STATE_DIR", str(d))
    return d


def _plan(tmp_path: Path, subdir: str, yaml_text: str):
    d = tmp_path / subdir
    d.mkdir(parents=True, exist_ok=True)
    f = d / f"{subdir}.yaml"
    f.write_text(yaml_text, encoding="utf-8")
    return Scheduler(agents_dir=str(PROJECT / "agents"),
                     pipeline_dir=str(d)).parse_file(str(f), verify_lock=False)


def _orch(tmp_path: Path) -> PipelineOrchestrator:
    orch = PipelineOrchestrator(agents_dir=str(PROJECT / "agents"),
                                checkpoint_dir=str(tmp_path / "ckpt"))
    orch.register_agents()
    return orch


def _wait_terminal_row(orch, task_id: str, timeout: float = 30.0) -> dict | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = orch.task_queue.get(task_id)
        if row and row.get("status") in ("done", "failed", "cancelled"):
            return row
        time.sleep(0.05)
    return None


def _reconcile(store: RunStore, orch: PipelineOrchestrator, task_id: str,
               expect_status: str) -> dict:
    """四个来源逐字段对账；任何一处不一致就 AssertionError 带两侧实际值。

    顺序有讲究：**先等到队列行落进终态**再读投影。反过来读会拿到 `running`——
    那是在断言一个由收尾链自己会抹掉的中间态（同一个形状在 #52 里刚修过一条，
    别在门禁里再造一条）。
    """
    queue_row = _wait_terminal_row(orch, task_id)
    assert queue_row is not None, "队列行没进终态"
    assert queue_row["status"] == expect_status, queue_row

    run = store.get_run(task_id)
    assert run is not None, (
        f"运行 {task_id} 在 runs 里没有投影行——但事件流里有东西吗："
        f"{[e['type'] for e in store.events(task_id)]}")
    # `DeliveryLedger.get()` 返回的就是那份结构化记录本身（payload_json 解开的样子），
    # 不是 {"payload": ...} 的套娃——读错键会让节点集合恒空，断言白给。
    ledger = DeliveryLedger().get(task_id)
    assert ledger is not None, "交付账本没有这一行（失败运行也该记）"

    # 1) 终态三处必须同值——"事件说完成、投影说运行中"就是这个断言抓的
    assert run["status"] == expect_status, (
        f"runs.status={run['status']} != {expect_status}")
    assert str(ledger.get("status")) == expect_status, ledger

    # 2) 节点集合三方一致（run_nodes / 账本 payload / 内存）
    task = orch.get_task(task_id)
    assert task is not None, "内存里找不到这个运行"
    mem_nodes = {name: dag for name, dag in task.dag_nodes.items()}
    store_nodes = {n["node"]: n for n in store.nodes(task_id)}
    ledger_nodes = {n["node"]: n for n in (ledger.get("nodes") or [])}
    assert ledger_nodes, f"账本这行的 nodes 是空的：{sorted(ledger)}——判据没东西可比"
    assert set(store_nodes) == set(ledger_nodes), (
        f"节点集合分叉：run_nodes={sorted(store_nodes)} 账本={sorted(ledger_nodes)}")
    assert set(store_nodes) == set(mem_nodes), (
        f"节点集合分叉：run_nodes={sorted(store_nodes)} 内存={sorted(mem_nodes)}")

    # 3) 逐节点状态与产物键：投影 vs 账本（两个不同时刻的独立写入）
    for name, sn in sorted(store_nodes.items()):
        ln = ledger_nodes[name]
        assert sn["status"] == ln["status"], (
            f"{name} 状态分叉：投影={sn['status']} 账本={ln['status']}")
        assert sn["artifact_keys"] == sorted(ln["artifact_keys"]), (
            f"{name} 产物键分叉：投影={sn['artifact_keys']} 账本={ln['artifact_keys']}")

    # 4) last_seq 必须等于事件流最大 seq（游标跨进程续传的前提）
    evs = store.events(task_id, limit=500)
    assert evs, "事件流是空的——双写根本没发生"
    assert run["last_seq"] == evs[-1]["seq"], (
        f"last_seq={run['last_seq']} 落后于事件流尾部 {evs[-1]['seq']}")
    assert [e["seq"] for e in evs] == list(range(1, len(evs) + 1)), (
        f"seq 不连续：{[e['seq'] for e in evs]}")
    return {"run": run, "events": evs, "nodes": store_nodes}


class TestThreeWayReconcile:
    def test_sync_run_reconciles(self, monkeypatch, tmp_path):
        _state_dir(monkeypatch, tmp_path)
        inp = tmp_path / "in.md"
        inp.write_text(INPUT, encoding="utf-8")
        orch = _orch(tmp_path)
        try:
            orch.run_plan(_plan(tmp_path, "s", PLAIN_YAML),
                          input_file=str(inp), task_id="rs_sync")
            facts = _reconcile(RunStore(), orch, "rs_sync", "done")
            assert [e["type"] for e in facts["events"]] == [
                run_store.RUN_OPENED, run_store.RUN_STARTED,
                run_store.NODE_FINISHED, run_store.RUN_COMPLETED], facts["events"]
        finally:
            orch.shutdown()

    def test_async_run_reconciles(self, monkeypatch, tmp_path):
        """异步（SSE 那条）必须与同步留下同样的三方形状——Stage 0 的并轨在这里成立。"""
        _state_dir(monkeypatch, tmp_path)
        inp = tmp_path / "in.md"
        inp.write_text(INPUT, encoding="utf-8")
        orch = _orch(tmp_path)
        try:
            asyncio.run(orch.run_plan_async(_plan(tmp_path, "a", PLAIN_YAML),
                                            input_file=str(inp), task_id="rs_async"))
            facts = _reconcile(RunStore(), orch, "rs_async", "done")
            assert [e["type"] for e in facts["events"]] == [
                run_store.RUN_OPENED, run_store.RUN_STARTED,
                run_store.NODE_FINISHED, run_store.RUN_COMPLETED]
        finally:
            orch.shutdown()

    def test_cancelled_run_reconciles(self, monkeypatch, tmp_path):
        """非 done 的终态也要对得上：门禁只认 done 就等于把失败运行放回盲区。"""
        _state_dir(monkeypatch, tmp_path)
        inp = tmp_path / "in.md"
        inp.write_text(INPUT, encoding="utf-8")
        orch = _orch(tmp_path)
        plan = _plan(tmp_path, "c", GATE_YAML)
        try:
            task = orch.run_plan(plan, input_file=str(inp), task_id="rs_cancel",
                                 wait=False)
            deadline = time.time() + 60
            while time.time() < deadline and task.status.value != "paused":
                time.sleep(0.05)
            assert task.status.value == "paused", f"没停在闸门上：{task.status}"
            assert orch.cancel("rs_cancel") is True
            _reconcile(RunStore(), orch, "rs_cancel", "cancelled")
        finally:
            orch.shutdown()


class TestSwitchAndInvariants:
    def test_switch_off_writes_nothing_new_but_leaves_old_surfaces_intact(
            self, monkeypatch, tmp_path):
        """`LOOM_RUNSTORE=0` 是 §5 要求的退路：关掉后旧三面照旧，新表一行没有。"""
        _state_dir(monkeypatch, tmp_path)
        monkeypatch.setenv("LOOM_RUNSTORE", "0")
        assert run_store.enabled() is False, "开关 env 没生效——退路是假的"
        inp = tmp_path / "in.md"
        inp.write_text(INPUT, encoding="utf-8")
        orch = _orch(tmp_path)
        try:
            orch.run_plan(_plan(tmp_path, "off", PLAIN_YAML),
                          input_file=str(inp), task_id="rs_off")
            store = RunStore()
            assert store.get_run("rs_off") is None, "开关关着还写了投影"
            assert store.events("rs_off") == []
            # 旧面必须照旧，否则"退路"其实是把观测面一起删了
            row = orch.task_queue.get("rs_off")
            assert row and row["status"] == "done", row
            assert DeliveryLedger().get("rs_off") is not None
        finally:
            orch.shutdown()

    def test_no_insert_or_replace_in_run_store(self):
        """`INSERT OR REPLACE` 不在 `db_backend` 的方言翻译表里。

        写进本模块 = SQLite 能跑、PG 直接报错的**假可用**（设计 §3 明令禁用）。
        这条判据盯的是源码本身——加一句就红，不靠人去读。
        """
        src = (PROJECT / "loom_core" / "run_store.py").read_text(encoding="utf-8")
        body = src.split('"""', 2)[2]  # 模块 docstring 里是在解释这条规矩
        assert "INSERT OR REPLACE" not in body.upper(), (
            "run_store 里出现了 INSERT OR REPLACE：db_backend 翻译不了它，PG 侧必炸")

    def test_every_sql_shape_translates_to_postgres(self):
        """禁 OR REPLACE 只是下限；真正要保证的是本模块的 SQL 过了翻译层不剩 SQLite 味。"""
        from loom_core import db_backend

        src = (PROJECT / "loom_core" / "run_store.py").read_text(encoding="utf-8")
        stmts = [s for s in _sql_literals(src) if s.lstrip().upper().startswith(
            ("INSERT", "SELECT", "UPDATE", "CREATE"))]
        assert len(stmts) >= 8, f"只抓到 {len(stmts)} 条 SQL，探针大概是瞎的"
        for sql in stmts:
            out = db_backend._translate(sql)
            assert "?" not in out, f"占位符没翻译过去：{sql[:60]}"
            assert "OR IGNORE" not in out.upper(), f"UPSERT 方言没翻：{sql[:60]}"
            assert " OR REPLACE" not in out.upper(), f"残留 OR REPLACE：{sql[:60]}"


def _sql_literals(src: str) -> list[str]:
    """抓模块里所有字符串字面量（SQL 都写在里面）。"""
    import ast

    found: list[str] = []
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            found.append(node.value)
    return found


@pytest.mark.parametrize("yaml_text,subdir", [(PLAIN_YAML, "p1"), (GATE_YAML, "p2")])
def test_projection_never_has_events_without_run_row(monkeypatch, tmp_path,
                                                     yaml_text, subdir):
    """不变式「有事件必有投影行」：任何写口都不许留下半条事实。"""
    _state_dir(monkeypatch, tmp_path)
    store = RunStore()
    store.record("stray_run", "approval.requested", {"nodes": ["gate"]})
    run = store.get_run("stray_run")
    assert run is not None, "事件写进去了，runs 里却没有这个运行——读侧会当成不存在"
    assert run["last_seq"] == 1
    assert json.dumps([e["type"] for e in store.events("stray_run")])
