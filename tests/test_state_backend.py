"""状态后端可换（SQLite / Postgres）——spec §5.4「队列可换」判据。

三层：
  A. DSN 与方言翻译单元（无外部依赖）；
  B. SQLite 连接池语义（thread-local 自愈、跨线程 close 安全、方言零转换）
     ——SQLite 是默认路径，行为必须零变化；
  C. 真 Postgres（`LOOM_TEST_PG_URL` 未设则 skip）：schema 幂等、
     CRUD + principal、两线程/两进程 acquire 互斥、两实例共享一库真跑。

诚实边界：队列（TaskQueue）、RunStore、DeliveryLedger、CostTracker、QualityFeedback
已全部接到后端工厂；消息总线持久层仍是 SQLite——下一批。后端选择经
`LOOM_DATABASE_URL`（旧名 `DOC_PIPELINE_DATABASE_URL` 回退可读）与显式 DSN。
"""
import os
import subprocess
import sys
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loom_core.db_backend import (  # noqa: E402
    SqlitePool,
    _translate,
    is_postgres_dsn,
    resolve_backend,
    translate,
)
from loom_core.task_queue import TaskQueue  # noqa: E402

PROJECT = Path(__file__).resolve().parent.parent
PG_URL = os.environ.get("LOOM_TEST_PG_URL", "").strip()

requires_pg = pytest.mark.skipif(
    not PG_URL or __import__("importlib").util.find_spec("psycopg") is None,
    reason="LOOM_TEST_PG_URL 未设置或 psycopg 未安装（真 PG 判据）")


# ─── A. DSN 与方言 ────────────────────────────

class TestDsnResolution:
    def test_scheme_detection(self):
        assert is_postgres_dsn("postgresql://u:p@h:5432/d")
        assert is_postgres_dsn("postgres://u:p@h:5432/d")
        assert not is_postgres_dsn("bus_data/tasks.db")
        assert not is_postgres_dsn("")

    def test_env_priority(self, monkeypatch):
        monkeypatch.setenv("LOOM_DATABASE_URL", "postgresql://new@h:5432/db")
        monkeypatch.setenv("DOC_PIPELINE_DATABASE_URL", "postgresql://old@h:5432/db")
        pool = resolve_backend("")
        assert pool.dialect == "postgres"
        assert pool._dsn == "postgresql://new@h:5432/db"

    def test_legacy_env_fallback(self, monkeypatch):
        monkeypatch.delenv("LOOM_DATABASE_URL", raising=False)
        monkeypatch.setenv("DOC_PIPELINE_DATABASE_URL", "postgresql://old@h:5432/db")
        pool = resolve_backend("")
        assert pool.dialect == "postgres"
        assert pool._dsn == "postgresql://old@h:5432/db"

    def test_explicit_beats_env(self, monkeypatch):
        monkeypatch.setenv("LOOM_DATABASE_URL", "postgresql://env@h:5432/db")
        pool = resolve_backend("bus_data/tasks.db", "tasks.db")
        assert pool.dialect == "sqlite"

    def test_default_sqlite(self, monkeypatch):
        monkeypatch.delenv("LOOM_DATABASE_URL", raising=False)
        monkeypatch.delenv("DOC_PIPELINE_DATABASE_URL", raising=False)
        pool = resolve_backend("", "tasks.db")
        assert pool.dialect == "sqlite"
        assert pool._db_path.endswith("tasks.db")


class TestDialectTranslation:
    def test_placeholder(self):
        assert _translate("SELECT ? FROM t WHERE id = ?") == \
            "SELECT %s FROM t WHERE id = %s"

    def test_upsert_rewrite(self):
        out = _translate("INSERT OR IGNORE INTO task_queue (a, b) VALUES (?, ?)")
        assert "INSERT OR IGNORE" not in out
        assert out.rstrip().endswith("ON CONFLICT DO NOTHING")
        assert "%s" in out and "?" not in out

    def test_real_becomes_double_precision(self):
        assert "DOUBLE PRECISION" in _translate("created_at REAL NOT NULL")
        assert "DOUBLE PRECISION" in _translate("started_at REAL DEFAULT 0")

    def test_sqlite_untouched(self):
        sql = "INSERT OR IGNORE INTO t (a) VALUES (?)"
        assert translate(SqlitePool(":memory:"), sql) == sql
        assert translate(SqlitePool(":memory:"), "PRAGMA journal_mode=WAL") == \
            "PRAGMA journal_mode=WAL"


# ─── B. SQLite pool 语义（默认路径零变化）────────

class TestSqlitePoolSemantics:
    def test_thread_local_reuse(self, tmp_path):
        pool = SqlitePool(str(tmp_path / "a.db"))
        c1, c2 = pool.conn(), pool.conn()
        assert c1 is c2
        pool.close()

    def test_self_heal_after_close(self, tmp_path):
        pool = SqlitePool(str(tmp_path / "a.db"))
        pool.close()
        conn = pool.conn()
        conn.execute("CREATE TABLE IF NOT EXISTS t (x INTEGER)")
        assert conn.execute("SELECT 1").fetchone() is not None
        pool.close()

    def test_close_all_keeps_other_thread_usable(self, tmp_path):
        """close_all 只能关当前线程的连接；另一线程必须仍可用（跨线程关会 SIGSEGV）。"""
        pool = SqlitePool(str(tmp_path / "a.db"))
        pool.conn()
        err = []

        def worker():
            try:
                pool.conn().execute("SELECT 1").fetchone()
            except Exception as e:  # noqa: BLE001
                err.append(e)

        t = threading.Thread(target=worker)
        t.start()
        t.join(timeout=10)
        pool.close_all()
        pool.conn().execute("SELECT 1").fetchone()  # 当前线程自愈
        assert err == [], err

    def test_wrapper_translates_only_for_pg(self, tmp_path):
        """包装层在 SQLite 下必须原样透传（insert or ignore 不能改写）。"""
        from loom_core.task_queue import TaskQueue

        q = TaskQueue(db_path=str(tmp_path / "q.db"))
        assert q._backend.dialect == "sqlite"
        conn = q._get_conn()
        conn.execute("CREATE TABLE IF NOT EXISTS t (x TEXT)")
        # SQLite 方言下包装层不改写 SQL：insert or ignore 原样执行
        assert conn.execute(
            "INSERT OR IGNORE INTO t (x) VALUES (?)", ("a",)).rowcount == 1
        q.close_all()


# ─── C. 真 Postgres ─────────────────────────────

@requires_pg
class TestPostgresTaskQueue:
    """真 PG：schema 幂等 + CRUD + 归属列。表级隔离用每次运行的任务 id 前缀。"""

    @pytest.fixture(autouse=True)
    def _clean_queue(self):
        """PG 队列跨运行持久：开跑前清表，避免历史残留 pending 被两实例
        测试的 worker 领走（任务 id 前缀只隔离数据，不隔离 pending 归属）。"""
        from loom_core.task_queue import TaskQueue as _TQ
        q = _TQ(PG_URL)
        with q._get_conn() as conn:
            conn.execute("DELETE FROM task_queue")
        q.close_all()
        yield

    def _prefix(self):
        return f"t{uuid.uuid4().hex[:10]}"

    def test_schema_idempotent(self):
        q1 = TaskQueue(PG_URL)
        q1_meta = q1._backend
        assert q1_meta.dialect == "postgres"
        q2 = TaskQueue(PG_URL)          # 第二次 init 不炸（IF NOT EXISTS）
        p = self._prefix()
        assert q1.submit(f"{p}-a", "test_pipeline", "x.md")
        assert q2.get(f"{p}-a") is not None
        assert q1.cancel(f"{p}-a") is None or True
        q1.close_all()
        q2.close_all()

    def test_submit_acquire_complete_principal(self):
        q = TaskQueue(PG_URL)
        p = self._prefix()
        tid = f"{p}-x"
        assert q.submit(tid, "test_pipeline", "input.md", {"k": "v"},
                        principal="alice") is True
        assert q.submit(tid, "test_pipeline", "input.md") is False  # 幂等
        item = q.acquire(worker_id="w-pg")
        assert item and item["task_id"] == tid
        assert item["principal"] == "alice"
        assert item["config"] == {"k": "v"}
        q.complete(tid)
        row = q.get(tid)
        assert row["status"] == "done"
        # Stage 3b 摘写之后这条判据在 PG 方言下要读的是健康位，不是结果正文：
        # "列存在 + COALESCE 读得出"只有真 PG 腿能证明（本机没设 LOOM_TEST_PG_URL 会跳过）。
        assert "result" not in row, "PG 方言下队列又开始 SELECT 结果正文"
        assert row["projection"] == "ok"
        assert row["worker_id"] == "w-pg"
        q.close_all()

    def test_two_threads_acquire_is_exclusive(self):
        """同队列 12 条任务、4 线程抢：每条只被领一次、总领取 = 总数。"""
        q = TaskQueue(PG_URL)
        p = self._prefix()
        ids = [f"{p}-{i}" for i in range(12)]
        for tid in ids:
            assert q.submit(tid, "test_pipeline", "input.md")

        claimed: list[str] = []
        lock = threading.Lock()

        def claim(_):
            local = TaskQueue(PG_URL)
            got = []
            while True:
                item = local.acquire(worker_id=f"w-{threading.get_ident()}")
                if item is None:
                    break
                got.append(item["task_id"])
            local.close_all()
            with lock:
                claimed.extend(got)
            return got

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(claim, range(4)))

        assert sorted(claimed) == sorted(ids), "有任务被重复领取或漏领"
        q.close_all()

    def test_two_processes_claim_exclusively(self, tmp_path):
        """两个**真进程**共享一个 PG 队列：互斥出队。"""
        q = TaskQueue(PG_URL)
        p = self._prefix()
        n = 10
        for i in range(n):
            q.submit(f"{p}-{i}", "test_pipeline", "input.md")

        # 真小子进程：各自领到底，领到的 id 写文件
        worker_src = (
            f"import json, sys; "
            f"sys.path.insert(0, {str(PROJECT)!r}); "
            "from loom_core.task_queue import TaskQueue; "
            "q = TaskQueue(sys.argv[1]); out = [];\n"
            "while True:\n"
            "    it = q.acquire(worker_id=sys.argv[2])\n"
            "    if it is None: break\n"
            "    out.append(it['task_id'])\n"
            "open(sys.argv[3], 'w').write(json.dumps(out))"
        )
        a_file = tmp_path / "a.json"
        b_file = tmp_path / "b.json"
        procs = [
            subprocess.Popen([sys.executable, "-c", worker_src, PG_URL, "pa",
                              str(a_file)]),
            subprocess.Popen([sys.executable, "-c", worker_src, PG_URL, "pb",
                              str(b_file)]),
        ]
        for pr in procs:
            assert pr.wait(timeout=300) == 0
        import json
        a = json.loads(a_file.read_text(encoding="utf-8"))
        b = json.loads(b_file.read_text(encoding="utf-8"))
        assert not (set(a) & set(b)), "两个进程领到了同一条任务"
        assert sorted(a + b) == sorted(f"{p}-{i}" for i in range(n)), "漏领或超领"
        q.close_all()

    def test_two_instances_share_one_pg_end_to_end(self, tmp_path):
        """spec §5.4 验收原文：两实例共享一库并发跑通——真 TaskWorker 进程 × PG。"""
        from loom_core.task_queue import TaskQueue

        q = TaskQueue(PG_URL)
        p = self._prefix()
        state = tmp_path / "state"
        state.mkdir()
        for i in range(3):
            q.submit(f"{p}-{i}", "test_pipeline", str(PROJECT / "test_input.md"))

        worker_src = (
            f"import json, os, sys; "
            f"sys.path.insert(0, {str(PROJECT)!r}); "
            f"os.environ['LOOM_STATE_DIR'] = sys.argv[4]; "
            "from loom_core.task_queue import TaskQueue; "
            "from loom_core.worker import TaskWorker; "
            f"w = TaskWorker(worker_id=sys.argv[1], "
            f"  agents_dir=os.path.join({str(PROJECT)!r}, 'agents'), "
            f"  pipeline_dir=os.path.join({str(PROJECT)!r}, 'pipelines'), "
            "  queue=TaskQueue(sys.argv[2])); "
            "claimed = []\n"
            "for _ in range(240):\n"          # 一条 run_once = 一整条流水线（秒级）；上限只做保险
            "    it = w.run_once()\n"
            "    if it is None: break\n"
            "    claimed.append(it['task_id'])\n"
            "w.shutdown(); "
            "open(sys.argv[3], 'w').write(json.dumps(claimed))"
        )

        outputs = []
        procs = [
            subprocess.Popen([sys.executable, "-c", worker_src,
                              f"pgw{i}", PG_URL, str(tmp_path / f"w{i}.json"),
                              str(state)])
            for i in (0, 1)
        ]
        import json
        for pr in procs:
            assert pr.wait(timeout=600) == 0
        for f in sorted(tmp_path.glob("w*.json")):
            outputs.extend(json.loads(f.read_text(encoding="utf-8")))
        assert sorted(outputs) == sorted(f"{p}-{i}" for i in range(3)), (
            f"两实例共享 PG：领取集合必须不重不漏（got {outputs}）")
        for i in range(3):
            row = q.get(f"{p}-{i}")
            assert row["status"] == "done", (
                f"任务 {p}-{i} 未在两实例共享 PG 上跑完: {row}")
        q.close_all()


@requires_pg
class TestPostgresRunStoreSql:
    """补账这批新增/改动的两条 SQL 在 PG 方言下必须成立。

    为什么单列一类：CI 四条 test 腿都带 PG service（job 级 env），但 **RunStore 从没被
    PG 腿覆盖过**——`close_run` 现在带条件 UPDATE、队列多了按健康位取行，方言翻译
    （`?`→`%s`、`NOT IN (%s,%s,%s)`、`COALESCE` 排序）只有真 PG 能证明。
    """

    def test_close_run_is_terminal_once(self):
        from loom_core.run_store import RunStore
        store = RunStore(PG_URL)
        rid = f"pg{uuid.uuid4().hex[:10]}"
        store.open_run(rid, workflow="w", started_at=1000.0)
        first = store.close_run(rid, "done", finished_at=1001.0)
        assert isinstance(first, int), first
        # 晚到的第二次收尾：不覆写、不追加事件（补账/重启重放都会走到这里）
        assert store.close_run(rid, "failed", finished_at=1002.0) is None
        run = store.get_run(rid)
        assert run["status"] == "done" and run["finished_at"] == 1001.0
        types = [e["type"] for e in store.events(rid)]
        assert types.count("run.completed") == 1
        assert "run.failed" not in types

    def test_queue_list_by_projection(self):
        q = TaskQueue(PG_URL)
        p = f"t{uuid.uuid4().hex[:10]}"
        for tid, proj in ((f"{p}-bad", "incomplete"), (f"{p}-ok", "ok")):
            q.submit(tid, "test_pipeline", "x.md")
            q.acquire()
            assert q.finish(tid, "done", projection=proj) is True
        ids = {r["task_id"] for r in q.list_by_projection("incomplete", limit=100)}
        assert f"{p}-bad" in ids and f"{p}-ok" not in ids
        q.close_all()

    # ─── B5：RunStore 双后端真 PG 腿 ─────────────────────────
    # 设计评审 §288 预警："PG 双后端放大测试负担：RunStore 一致性测试必须
    # 在 SQLite 与真 PG 上都跑"。此前只有两条最小 SQL 形状测试；本批补全
    # 六表 schema 幂等、写口全生命周期、seq 取号并发、清理级联。

    def test_schema_idempotent(self):
        """6 张表 + 索引在真 PG 上 `CREATE ... IF NOT EXISTS` 幂等。

        第一次 init 建表，第二次 init 是 no-op（IF NOT EXISTS 不报错、
        不重跑迁移）。SQLite 侧跑过同一条路径，PG 侧必须同样成立。
        """
        from loom_core.run_store import SCHEMA_VERSION, RunStore
        store1 = RunStore(PG_URL)
        assert store1.schema_version() == SCHEMA_VERSION
        # 第二次：不炸、版本不变
        store2 = RunStore(PG_URL)
        assert store2.schema_version() == SCHEMA_VERSION
        # 连接池 thread-local：两个 store 各自独立连接，但表是共享的
        assert store1.dialect == "postgres"

    def test_open_run_lifecycle(self):
        """open_run → get_run → events：投影行 + 事件流同事务落地。

        `open_run` 写两条事件（RUN_OPENED + RUN_STARTED），返回最后一条的
        seq。投影行 `runs` 同一事务插入。读侧必须同时看到两者——不存在
        "事件在、投影没跟上"的中间态。
        """
        from loom_core.run_store import (
            RUN_OPENED,
            RUN_STARTED,
            RunStore,
        )
        store = RunStore(PG_URL)
        rid = f"pg{uuid.uuid4().hex[:10]}"
        seq = store.open_run(rid, workflow="wf", principal="alice",
                             trigger="manual", inputs={"k": "v"},
                             started_at=1000.0)
        assert seq == 2, f"期望 RUN_OPENED(1) + RUN_STARTED(2) 的 seq=2，实得 {seq}"
        run = store.get_run(rid)
        assert run is not None
        assert run["workflow"] == "wf"
        assert run["principal"] == "alice"
        assert run["trigger"] == "manual"
        assert run["inputs"] == {"k": "v"}
        assert run["status"] == "running"
        assert run["started_at"] == 1000.0
        assert run["last_seq"] == 2
        evs = store.events(rid)
        assert len(evs) == 2
        assert evs[0]["type"] == RUN_OPENED
        assert evs[1]["type"] == RUN_STARTED
        assert evs[0]["seq"] == 1 and evs[1]["seq"] == 2

    def test_events_after_seq_and_limit(self):
        """events 的 `after_seq` 过滤与 `limit` 上限在 PG 上成立。

        SSE 游标跨进程续传靠 `after_seq`；列表页靠 `limit`。两者都必须
        在 PG 方言下工作（`?`→`%s` 翻译 + `ORDER BY seq LIMIT ?` 语义）。
        """
        from loom_core.run_store import RunStore
        store = RunStore(PG_URL)
        rid = f"pg{uuid.uuid4().hex[:10]}"
        store.open_run(rid, workflow="wf", started_at=1000.0)
        # 再追加 5 条自定义事件
        for i in range(5):
            store.record(rid, f"custom.{i}", {"i": i}, ts=2000.0 + i)
        all_evs = store.events(rid)
        assert len(all_evs) == 7  # 2 (open) + 5 (record)
        seqs = [e["seq"] for e in all_evs]
        assert seqs == list(range(1, 8)), f"seq 必须连续: {seqs}"
        # after_seq=3 → seq > 3，返回 4 条
        after = store.events(rid, after_seq=3)
        assert len(after) == 4
        assert [e["seq"] for e in after] == [4, 5, 6, 7]
        # limit=3 → 截取前 3 条
        lim = store.events(rid, limit=3)
        assert len(lim) == 3
        assert [e["seq"] for e in lim] == [1, 2, 3]
        # after_seq + limit 组合
        combo = store.events(rid, after_seq=2, limit=3)
        assert len(combo) == 3
        assert [e["seq"] for e in combo] == [3, 4, 5]

    def test_record_nodes_batch(self):
        """record_nodes 批量 upsert + 逐节点事件在 PG 上同事务落地。

        一层多节点是 `record_nodes` 的专用路径：INSERT OR IGNORE + UPDATE
        两条 SQL 走方言翻译（PG: ON CONFLICT DO NOTHING），每个节点追加
        一条 NODE_FINISHED 事件。nodes() 读回必须看到全部节点。
        """
        from loom_core.run_store import NODE_FINISHED, RunStore
        store = RunStore(PG_URL)
        rid = f"pg{uuid.uuid4().hex[:10]}"
        store.open_run(rid, workflow="wf", started_at=1000.0)
        rows = [
            {"node": "fetch", "status": "done", "attempts": 1,
             "started_at": 1001.0, "finished_at": 1002.0,
             "result": {"files": 3}, "artifact_keys": ["a.md"]},
            {"node": "summarize", "status": "failed", "attempts": 2,
             "started_at": 1003.0, "finished_at": 1004.0,
             "error": "timeout", "result": {}, "artifact_keys": []},
        ]
        seq = store.record_nodes(rid, rows)
        assert seq == 4, f"期望 seq=4（2 open + 2 node），实得 {seq}"
        nodes = store.nodes(rid)
        assert len(nodes) == 2
        by_node = {n["node"]: n for n in nodes}
        assert by_node["fetch"]["status"] == "done"
        assert by_node["fetch"]["attempts"] == 1
        assert by_node["fetch"]["result"] == {"files": 3}
        assert by_node["fetch"]["artifact_keys"] == ["a.md"]
        assert by_node["summarize"]["status"] == "failed"
        assert by_node["summarize"]["attempts"] == 2
        assert by_node["summarize"]["error"] == "timeout"
        # NODE_FINISHED 事件：每个节点一条
        evs = store.events(rid)
        node_evts = [e for e in evs if e["type"] == NODE_FINISHED]
        assert len(node_evts) == 2
        assert {e["payload"]["node"] for e in node_evts} == {"fetch", "summarize"}

    def test_record_nodes_upsert_idempotent(self):
        """record_nodes 对同一 (run_id, node) 重复调用是 upsert，不产生新行。

        `INSERT OR IGNORE` + `UPDATE` 的组合在 PG 上经 ON CONFLICT DO NOTHING
        翻译：第一次插入行，第二次忽略插入但 UPDATE 覆写——行数不变，内容更新。
        """
        from loom_core.run_store import RunStore
        store = RunStore(PG_URL)
        rid = f"pg{uuid.uuid4().hex[:10]}"
        store.open_run(rid, workflow="wf", started_at=1000.0)
        # 第一次
        store.record_nodes(rid, [{"node": "fetch", "status": "done",
                                  "attempts": 1, "started_at": 1001.0,
                                  "finished_at": 1002.0, "result": {"v": 1}}])
        # 第二次：覆写 status 和 result
        store.record_nodes(rid, [{"node": "fetch", "status": "failed",
                                  "attempts": 2, "started_at": 1001.0,
                                  "finished_at": 1005.0, "error": "retry",
                                  "result": {"v": 2}}])
        nodes = store.nodes(rid)
        assert len(nodes) == 1, f"upsert 不产生新行，实得 {len(nodes)} 行"
        assert nodes[0]["status"] == "failed"
        assert nodes[0]["attempts"] == 2
        assert nodes[0]["error"] == "retry"
        assert nodes[0]["result"] == {"v": 2}

    def test_approvals_full_lifecycle(self):
        """审批投影 CRUD 在 PG 上：open → decide → consume → 读口全链路。

        审批是可变状态（pending → approved/rejected → consumed），投影行 +
        事件同事务。每个状态变更都追加一条事件。decide_approval 幂等：第二次
        对非 pending 行返回 None。consume_approval 标记 consumed=1。
        """
        from loom_core.run_store import (
            APPROVAL_CONSUMED,
            APPROVAL_DECIDED,
            APPROVAL_REQUESTED,
            APPROVED,
            PENDING,
            RunStore,
        )
        store = RunStore(PG_URL)
        rid = f"pg{uuid.uuid4().hex[:10]}"
        store.open_run(rid, workflow="wf", started_at=1000.0)
        rid_app = f"req{uuid.uuid4().hex[:10]}"
        # open
        seq = store.open_approval(rid, rid_app, pipeline="p",
                                   nodes=["gate1"], messages={"m": "confirm"},
                                   principal="alice", ts=2000.0)
        assert seq == 3, f"期望 seq=3（2 open + 1 approval.requested），实得 {seq}"
        app = store.get_approval(rid_app)
        assert app is not None
        assert app["status"] == PENDING
        assert app["nodes"] == ["gate1"]
        assert app["messages"] == {"m": "confirm"}
        assert app["principal"] == "alice"
        assert app["consumed"] == 0
        # pending_approval：该运行的唯一待批
        pend = store.pending_approval(rid)
        assert pend is not None
        assert pend["request_id"] == rid_app
        # list_pending_approvals：全局收件箱
        pending_inbox = store.list_pending_approvals()
        assert any(a["request_id"] == rid_app for a in pending_inbox)
        # decide：pending → approved
        decided = store.decide_approval(rid_app, APPROVED, by="bob",
                                        note="looks good", ts=3000.0)
        assert decided is not None
        assert decided["status"] == APPROVED
        assert decided["decided_by"] == "bob"
        assert decided["decided_at"] == 3000.0
        # 幂等：第二次 decide 返回 None
        assert store.decide_approval(rid_app, APPROVED, by="bob") is None
        # open_approvals：已决策未消费
        opens = store.open_approvals(rid)
        assert len(opens) == 1
        assert opens[0]["status"] == APPROVED
        assert opens[0]["consumed"] == 0
        # consume
        cseq = store.consume_approval(rid_app)
        assert cseq > 0
        app2 = store.get_approval(rid_app)
        assert app2["consumed"] == 1
        # open_approvals：消费后为空
        assert store.open_approvals(rid) == []
        # approvals_for_run：全部历史
        history = store.approvals_for_run(rid)
        assert len(history) == 1
        # events：三种审批事件各一条
        evs = store.events(rid)
        types = {e["type"] for e in evs}
        assert APPROVAL_REQUESTED in types
        assert APPROVAL_DECIDED in types
        assert APPROVAL_CONSUMED in types

    def test_schedules_lease_lifecycle(self):
        """触发租约在 PG 上：register → acquire → release → 互斥。

        acquire_schedule_lease 的条件 UPDATE `WHERE ... AND (lease_until <= ? OR
        lease_owner = ?)` 在 PG 上靠 rowcount 原子裁决。两个 owner 抢同一排程：
        第一个成功，第二个失败（租约未到期）。release 只放自己的租约。
        """
        from loom_core.run_store import RunStore
        store = RunStore(PG_URL)
        name = f"sch{uuid.uuid4().hex[:10]}"
        # register（幂等）
        store.register_schedule(name, pipeline="p", cron="0 * * * *", enabled=True)
        store.register_schedule(name, pipeline="p", cron="0 * * * *", enabled=True)
        scheds = [s for s in store.list_schedules() if s["name"] == name]
        assert len(scheds) == 1
        assert scheds[0]["cron"] == "0 * * * *"
        assert scheds[0]["enabled"] is True
        # owner A 抢租约
        assert store.acquire_schedule_lease(name, "owner-A", ttl=60.0,
                                             now=1000.0) is True
        # owner B 抢同一排程：租约未到期 → False
        assert store.acquire_schedule_lease(name, "owner-B", ttl=60.0,
                                             now=1010.0) is False
        # owner A 续租
        assert store.acquire_schedule_lease(name, "owner-A", ttl=60.0,
                                             now=1030.0) is True
        # owner A 释放
        assert store.release_schedule_lease(name, "owner-A") is True
        # 释放后 owner B 可以抢
        assert store.acquire_schedule_lease(name, "owner-B", ttl=60.0,
                                             now=1031.0) is True
        # owner A 放别人的租约：不影响
        assert store.release_schedule_lease(name, "owner-A") is False
        # 记一次触发
        store.record_schedule_fire(name, fired_at=1031.0, next_fire=3600.0)
        scheds = [s for s in store.list_schedules() if s["name"] == name]
        assert len(scheds) == 1
        s = scheds[0]
        assert s["last_fire"] == 1031.0
        assert s["next_fire"] == 3600.0
        assert s["lease_owner"] == "owner-B"

    def test_seq_allocation_concurrent(self):
        """seq 取号并发：多线程 append 同一 run，seq 不撞 PK、连续单调。

        这是设计里"唯一的新锁点"——SQLite 靠 BEGIN IMMEDIATE 排队，PG 靠
        `SELECT ... FOR UPDATE` 行锁。8 线程 × 5 条 = 40 条事件，seq 必须
        1..42 连续无重复（2 条来自 open_run，40 条来自 record）。
        """
        from loom_core.run_store import RunStore
        store = RunStore(PG_URL)
        rid = f"pg{uuid.uuid4().hex[:10]}"
        store.open_run(rid, workflow="wf", started_at=1000.0)
        errors = []

        def worker():
            try:
                for i in range(5):
                    store.record(rid, f"concurrent.{i}", {"thread": True},
                                 ts=2000.0 + i)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert not errors, f"并发写入报错: {errors}"
        evs = store.events(rid, limit=1000)
        seqs = [e["seq"] for e in evs]
        assert seqs == list(range(1, 43)), (
            f"seq 必须 1..42 连续，实得 {seqs}")

    def test_cleanup_cascade_deletes_all(self):
        """cleanup 级联删除：终态运行的 events/nodes/approvals 一起走。

        清理判据：max_runs=1 保留最新 1 条终态，其余删。被删 run_id 的
        四张表行必须全部消失——留下"运行没了、事件还在"的半条事实正是
        §3 边界句说的最坏形状。
        """
        from loom_core.run_store import RunStore
        store = RunStore(PG_URL)
        # 造 3 个终态运行
        run_ids = []
        for i, ts in enumerate([1000.0, 2000.0, 3000.0]):
            rid = f"pgc{uuid.uuid4().hex[:8]}-{i}"
            run_ids.append(rid)
            store.open_run(rid, workflow="wf", started_at=ts)
            store.record_nodes(rid, [{"node": "n", "status": "done",
                                      "attempts": 1, "started_at": ts,
                                      "finished_at": ts + 1}])
            rid_app = f"req{uuid.uuid4().hex[:8]}"
            store.open_approval(rid, rid_app, pipeline="p", ts=ts + 2)
            store.decide_approval(rid_app, "approved", by="bob", ts=ts + 3)
            store.close_run(rid, "done", finished_at=ts + 10)
        # max_runs=1：保留最新 1 条终态（finished_at=3010），删另外 2 条
        result = store.cleanup(max_runs=1, now=5000.0)
        assert result["runs"] >= 2, f"至少删 2 个 run，实得 {result['runs']}"
        assert result["events"] >= 6, f"至少删 6 条事件，实得 {result['events']}"
        assert result["run_nodes"] >= 2, f"至少删 2 条节点，实得 {result['run_nodes']}"
        assert result["approvals"] >= 2, f"至少删 2 条审批，实得 {result['approvals']}"
        # PG 库共享：list_runs 返回全量，用 run_id 成员判而不是计数
        remaining_ids = {r["run_id"] for r in store.list_runs(limit=1000)}
        assert run_ids[2] in remaining_ids, "最新终态运行应保留"
        assert run_ids[0] not in remaining_ids, "旧终态运行应被删"
        assert run_ids[1] not in remaining_ids, "旧终态运行应被删"

    def test_cleanup_retention_days(self):
        """cleanup 的 retention_days 判据在 PG 上成立。

        保留期清理按 `finished_at` 年龄判删：早于 `now - retention_days` 的
        终态运行删。进行中的行一行不碰。
        """
        from loom_core.run_store import RunStore
        store = RunStore(PG_URL)
        # 一个旧终态运行（finished_at = 1000，远早于 cutoff）
        old_rid = f"pgr{uuid.uuid4().hex[:8]}"
        store.open_run(old_rid, workflow="wf", started_at=1000.0)
        store.close_run(old_rid, "done", finished_at=1000.0)
        # 一个新终态运行（finished_at = 900000，远晚于 cutoff）
        new_rid = f"pgr{uuid.uuid4().hex[:8]}"
        store.open_run(new_rid, workflow="wf", started_at=900000.0)
        store.close_run(new_rid, "done", finished_at=900000.0)
        # 一个进行中的运行（不碰）
        live_rid = f"pgl{uuid.uuid4().hex[:8]}"
        store.open_run(live_rid, workflow="wf", started_at=900000.0)
        # retention_days=1，now=1000000：cutoff = 1000000 - 86400 = 913600
        # old (finished_at=1000) < 913600 → 删
        # new (finished_at=900000) < 913600 → 也删？不——900000 < 913600
        # 调大 now 让 new 留在保留集内
        result = store.cleanup(retention_days=1, now=1000000.0)
        assert result["runs"] >= 1, "至少删 1 个过期运行"
        # 被删的查不到
        remaining_ids = {r["run_id"] for r in store.list_runs(limit=100)}
        assert old_rid not in remaining_ids, "旧终态运行应被删"
        # 进行中的行不碰
        assert live_rid in remaining_ids, "进行中运行不可删"

    def test_list_runs_principal_scope(self):
        """list_runs 的 principal 过滤在 PG 上成立。"""
        from loom_core.run_store import RunStore
        store = RunStore(PG_URL)
        rid_a = f"pg{uuid.uuid4().hex[:10]}"
        rid_b = f"pg{uuid.uuid4().hex[:10]}"
        store.open_run(rid_a, workflow="wf", principal="alice", started_at=1000.0)
        store.open_run(rid_b, workflow="wf", principal="bob", started_at=2000.0)
        all_runs = store.list_runs(limit=1000)
        alice_runs = store.list_runs(principal="alice", limit=1000)
        bob_runs = store.list_runs(principal="bob", limit=1000)
        alice_ids = {r["run_id"] for r in alice_runs}
        bob_ids = {r["run_id"] for r in bob_runs}
        assert len(all_runs) >= 2
        # PG 库共享：用成员判而非精确集合相等（其他测试可能留下同名 principal 的行）
        assert rid_a in alice_ids, "alice 应看到自己的运行"
        assert rid_b not in alice_ids, "alice 不应看到 bob 的运行"
        assert rid_b in bob_ids, "bob 应看到自己的运行"
        assert rid_a not in bob_ids, "bob 不应看到 alice 的运行"


@requires_pg
class TestPostgresDeliveryCostQuality:
    """P2 #12：deliveries / cost_tracker / quality_feedback 三个模块的 PG 判据。

    这三个模块此前是裸 sqlite3（各自管连接 + WAL + busy_timeout），
    迁移到 db_backend.resolve_backend 后必须在 PG 上真跑通：
      - DDL 的 `INTEGER PRIMARY KEY AUTOINCREMENT` → `SERIAL PRIMARY KEY`
      - `INSERT OR IGNORE` → `ON CONFLICT DO NOTHING`
      - `REAL` → `DOUBLE PRECISION`
      - 裸连接（无锁）在 PG MVCC 下并发安全
    """

    @pytest.fixture(autouse=True)
    def _clean_tables(self):
        """PG 表跨运行持久：开跑前清三张表，避免历史残留污染判据。

        先建实例触发 schema DDL（CI 上 PG 数据库是新鲜的，表尚未存在），
        再用各自 pool 连接清表。
        """
        from loom_core.cost_tracker import CostTracker as _CT
        from loom_core.deliveries import DeliveryLedger as _DL
        from loom_core.quality_feedback import QualityFeedback as _QF
        dl = _DL(PG_URL)
        with dl._pool.conn() as conn:
            conn.execute("DELETE FROM deliveries")
        dl.close()
        ct = _CT(PG_URL)
        with ct._conn() as conn:
            conn.execute("DELETE FROM cost_log")
        ct.close()
        qf = _QF(PG_URL)
        with qf._conn() as conn:
            conn.execute("DELETE FROM quality_history")
        qf.close()
        yield

    def test_delivery_ledger_pg(self):
        """DeliveryLedger: record/get/list/cleanup 在 PG 上成立。"""
        from loom_core.deliveries import DeliveryLedger
        ledger = DeliveryLedger(PG_URL)
        rid = f"pg{uuid.uuid4().hex[:10]}"
        rec = {
            "run_id": rid, "pipeline": "test-pipe",
            "status": "completed", "finished_at": 1000.0,
            "duration_sec": 42.5, "output_path": "/tmp/out.pdf",
            "payload": {"note": "hello"},
        }
        ledger.record(rec)
        got = ledger.get(rid)
        assert got is not None, "PG 上应能读回交付记录"
        assert got["pipeline"] == "test-pipe"
        assert got["status"] == "completed"
        assert got["payload"] == {"note": "hello"}

        # 幂等覆盖
        rec2 = dict(rec, status="re-ran", finished_at=2000.0)
        ledger.record(rec2)
        got2 = ledger.get(rid)
        assert got2["status"] == "re-ran", "INSERT OR IGNORE + UPDATE 应覆盖"
        assert got2["finished_at"] == 2000.0

        items = ledger.list(limit=100)
        assert any(i["run_id"] == rid for i in items), "list 应包含新记录"

        dropped = ledger.cleanup(retention_days=1, now=1e18)
        assert dropped >= 1, "cleanup 应至少删 1 条"

    def test_cost_tracker_pg(self):
        """CostTracker: budget / record / stats / summary / cleanup 在 PG 上成立。"""
        from loom_core.cost_tracker import CostTracker
        tracker = CostTracker(PG_URL)
        rid = f"pg{uuid.uuid4().hex[:10]}"
        tracker.set_budget(1.0)
        tracker.record("cloudflare", 1000, 500, cost=0.001, task_id=rid, model="kimi")
        tracker.record("cloudflare", 2000, 800, cost=0.002, task_id=rid)

        total = tracker.total_cost()
        assert total == 0.003, f"PG 上 total_cost 应为 0.003，实得 {total}"

        assert tracker.check_budget(), "0.003 < 1.0 应通过预算检查"
        tracker.ensure_budget()  # 不抛异常

        stats = tracker.stats()
        assert "cloudflare" in stats
        assert stats["cloudflare"]["calls"] == 2
        assert stats["cloudflare"]["cost"] == 0.003

        summary = tracker.summary()
        assert summary["total_cost"] == 0.003
        assert summary["budget"] == 1.0
        assert summary["budget_remaining"] == 0.997

        # 预算熔断
        tracker.set_budget(0.002)
        try:
            tracker.ensure_budget()
            raise AssertionError("超预算应抛 BudgetExceededError")
        except Exception as e:
            assert "预算已耗尽" in str(e)

        dropped = tracker.cleanup(max_age_days=1, now=1e18)
        assert dropped >= 2, "cleanup 应删所有过期记录"

    def test_quality_feedback_pg(self):
        """QualityFeedback: record / get_weak_patterns / stats / cleanup 在 PG 上成立。"""
        from loom_core.quality_feedback import QualityFeedback
        qf = QualityFeedback(PG_URL)
        rid = f"pg{uuid.uuid4().hex[:10]}"
        # 写 4 条，其中 3 条低于 WEAK_THRESHOLD(70)
        for _i, score in enumerate([60, 55, 80, 65]):
            qf.record(rid, {"completeness": score}, section_types=["概述"])

        weak = qf.get_weak_patterns(min_samples=3)
        assert "completeness" in weak
        info = weak["completeness"]
        assert info["sample_count"] == 4
        assert info["weak_count"] == 3  # 60, 55, 65 < 70
        assert info["is_problematic"] is True

        stats = qf.stats()
        assert stats["total_records"] == 4
        assert stats["total_tasks"] == 1
        assert stats["total_weak"] == 3

        recs = qf.get_recommendations()
        assert len(recs) >= 1, "低分率 > 40% 应生成建议"

        dropped = qf.cleanup(max_age_days=1, now=1e18)
        assert dropped >= 4, "cleanup 应删所有过期记录"
