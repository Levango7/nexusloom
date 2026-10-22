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

#: 同一层里两个互不依赖的节点——专门用来数"一层开几次事务"
WIDE_YAML = """
name: rs_wide
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


def _wait_ledger(task_id: str, timeout: float = 30.0) -> dict | None:
    """等账本行落地——它比队列终态**晚一步**，不能假定同时到。

    收尾链的顺序是 `task_queue.finish` → 终态事件 → 投影 → `_deliver`（写账本）。
    `_wait_terminal_row` 只保证第一笔已落；机器忙时第二笔可能晚几十毫秒，这时读账本
    拿到 None 不是"失败运行没记账"，而是**断言了一个还没发生的写**（2026-10-21 全量
    在 20 分钟负载轮里就红在这里，单跑必绿）。

    预算取 30s，与 `_wait_terminal_row` 同量级：10s 那版在同一次 20 分钟负载全量里
    仍不够（收尾线程被挤掉），而这两笔写是**同一条线程顺序执行**的——等它出现不是
    放宽判据，是在等一个被代码保证会发生的写。
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        rec = DeliveryLedger().get(task_id)
        if rec is not None:
            return rec
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
    t_ledger = time.time()
    ledger = _wait_ledger(task_id)
    assert ledger is not None, (
        f"等 {time.time() - t_ledger:.1f}s 账本这行仍没出现（收尾链在 _deliver 前中断了？）"
        f" 队列行={queue_row.get('status')}")

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


class TestStoreUnavailable:
    def test_store_unavailable_keeps_old_surfaces_and_is_loud(
            self, monkeypatch, tmp_path):
        """没有开关了：Store 建不起来时旧三面照旧，**且失败必须报出来**（不静默）。

        注入"建口就抛"的替身（而不是环境变量）：开关删掉之后，能造出这条降级路径的
        只剩真实的故障，判据也要按故障来造，否则测的是已经不存在的东西。
        """
        _state_dir(monkeypatch, tmp_path)

        def _boom(*_a, **_k):
            raise RuntimeError("runs.db 不可写（注入）")

        monkeypatch.setattr("loom_core.run_store.RunStore", _boom)
        inp = tmp_path / "in.md"
        inp.write_text(INPUT, encoding="utf-8")
        orch = _orch(tmp_path)
        try:
            orch.run_plan(_plan(tmp_path, "off", PLAIN_YAML),
                          input_file=str(inp), task_id="rs_nostore")
            # 旧面必须照旧：投影写不出来不该把运行本身拖下水
            row = orch.task_queue.get("rs_nostore")
            assert row and row["status"] == "done", row
            assert DeliveryLedger().get("rs_nostore") is not None
            # 失败可见：健康检查要能把"这次进程写不出投影"报出来
            health = orch.run_store_health()
            assert health["status"] == "degraded", health
            assert health["available"] is False
            assert "注入" in health["error"], health
        finally:
            orch.shutdown()

    def test_health_reports_schema_version(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        try:
            health = orch.run_store_health()
            assert health["status"] == "healthy", health
            assert health["available"] is True
            assert health["dialect"] == "sqlite"
            assert health["schema_version"] == run_store.SCHEMA_VERSION
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


class TestBatching:
    def test_one_transaction_per_level_not_per_node(self, monkeypatch, tmp_path):
        """§5 的写放大缓解是**有判据的**：同一层的多个节点必须共用一次事务。

        `BEGIN IMMEDIATE` 拿的是整库写锁，逐节点各开一次 = 一层 N 个节点排队 N 回，
        并发运行时纯付锁切换的钱。数 `_Txn.__enter__` 的调用次数才能抓这件事——
        数据形状两种写法一模一样，只有事务边界不同，对账判据对它完全无感。
        """
        _state_dir(monkeypatch, tmp_path)
        inp = tmp_path / "in.md"
        inp.write_text(INPUT, encoding="utf-8")
        orch = _orch(tmp_path)
        counts = {"n": 0}
        store = RunStore()          # 先建：建口自带一次 schema 事务，不该算进运行批次
        real_enter = run_store._Txn.__enter__

        def counting(self):
            counts["n"] += 1
            return real_enter(self)

        monkeypatch.setattr(run_store._Txn, "__enter__", counting)
        try:
            orch.run_plan(_plan(tmp_path, "w", WIDE_YAML),
                          input_file=str(inp), task_id="rs_wide")
            nodes = store.nodes("rs_wide")
            assert {n["node"] for n in nodes} == {"tldr", "readability"}, (
                f"两个节点没都落进投影：{[n['node'] for n in nodes]}")
            # 建口 1（orchestrator 惰性构造 RunStore 时的 schema 事务）+ 开局 1
            # + 每层 1 + 收尾 1 = 4；退化成逐节点各开一次就是 5
            assert counts["n"] == 4, (
                f"层内合批的批次数对不上：宽层 2 个节点开了 {counts['n']} 次事务，"
                f"预期 4＝建口/开局/一层/收尾（逐节点退化会是 5）")
        finally:
            orch.shutdown()
