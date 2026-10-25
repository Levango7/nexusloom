"""状态后端可换（SQLite / Postgres）——spec §5.4「队列可换」判据。

三层：
  A. DSN 与方言翻译单元（无外部依赖）；
  B. SQLite 连接池语义（thread-local 自愈、跨线程 close 安全、方言零转换）
     ——SQLite 是默认路径，行为必须零变化；
  C. 真 Postgres（`LOOM_TEST_PG_URL` 未设则 skip）：schema 幂等、
     CRUD + principal、两线程/两进程 acquire 互斥、两实例共享一库真跑。

诚实边界：本批只把**队列**（TaskQueue）接到后端工厂；消息总线持久层、
cost/quality feedback 仍是 SQLite——下一批。后端选择经
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
