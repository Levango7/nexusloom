"""SQLite 并发首次初始化判据：两进程同建同一批库不许抛 lock/duplicate。

背景（2026-10-09 实证）：main 侧 CI 的 `test (3.14)` 腿在全量负载下变红，
失败点是 `test_two_instances_one_queue` 的 `e2e-0 终态是 failed: database is locked`。
本机探针（两进程 × 80 轮同建四库）复现两个**首建期**竞态：

1. `PRAGMA journal_mode=WAL` 切模式需要短暂独占锁，SQLite 明示此时**不走
   busy handler**，竞争下直接返回 SQLITE_BUSY（Python 文案 "database is locked"），
   与连接的 `timeout` 无关；
2. `ALTER TABLE ... ADD COLUMN` 的 check-then-act：两进程都判"列不存在"，
   都执行 ALTER，后到者拿 "duplicate column name"。

两处都只在"两个进程同时第一次初始化同一批库"时现形，单进程测试与隔离跑
都看不见。本文件把这个场景钉成判据：真起 subprocess（独立解释器），
同一 state 目录并发创建，断言零异常、终态正确。
"""
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parent.parent

#: 子进程：在同一 state 目录里并发创建四个 SQLite store（真初始化路径）
_INIT_SCRIPT = """
import json, os, sys
sys.path.insert(0, {project!r})
os.environ["LOOM_STATE_DIR"] = sys.argv[1]
errors = []
from loom_core.task_queue import TaskQueue
from loom_core.cost_tracker import CostTracker
from loom_core.quality_feedback import QualityFeedback
from loom_core.message_store import PersistentStore
for name, factory in (("task_queue", TaskQueue), ("cost_tracker", CostTracker),
                      ("quality_feedback", QualityFeedback),
                      ("message_store", PersistentStore)):
    try:
        store = factory()
        for closer in ("close_all", "close"):
            fn = getattr(store, closer, None)
            if fn:
                fn()
                break
    except Exception as e:
        errors.append(name + ": " + type(e).__name__ + ": " + str(e))
print(json.dumps(errors))
"""


def _spawn_init_pair(state: Path) -> list:
    """同一 state 目录上并发起两个初始化子进程。"""
    return [
        subprocess.Popen(
            [sys.executable, "-c", _INIT_SCRIPT.format(project=str(PROJECT)),
             str(state)],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for _ in range(2)
    ]


class TestConcurrentFirstInit:
    """两进程同时首建同一批库：零异常 + 终态可用。"""

    #: 轮数：单轮竞争窗口很小，靠轮数累积命中率（修复前 80 轮里命中 4 次）
    ROUNDS = 12

    def test_two_processes_init_same_stores_without_lock_errors(self, tmp_path):
        for round_idx in range(self.ROUNDS):
            state = tmp_path / ("r" + str(round_idx))
            state.mkdir()
            for proc in _spawn_init_pair(state):
                out, _ = proc.communicate(timeout=120)
                assert proc.returncode == 0, out[-1500:]
                last = out.strip().splitlines()[-1] if out.strip() else "[]"
                errors = json.loads(last)
                assert errors == [], (
                    f"第 {round_idx} 轮并发首建报错: {errors}")

    def test_stores_are_usable_after_concurrent_init(self, tmp_path, monkeypatch):
        """并发建完之后，store 必须真能读写（不只是"没抛异常"）。

        状态目录用 monkeypatch.setenv 注入——它在用例结束自动还原。
        这里**不能**用 os.environ + pop：conftest 已在会话级把
        LOOM_STATE_DIR 指向 .test_state，pop 会把会话值一并删掉，
        污染后续所有依赖它的判据（test_state_isolation 实测被抓红过）。
        """
        state = tmp_path / "state"
        state.mkdir()
        for proc in _spawn_init_pair(state):
            out, _ = proc.communicate(timeout=120)
            assert proc.returncode == 0, out[-1500:]

        monkeypatch.setenv("LOOM_STATE_DIR", str(state))
        from loom_core.task_queue import TaskQueue

        queue = TaskQueue()
        try:
            assert queue.submit("t-after", "api-report", "in.md", {})
            item = queue.acquire(worker_id="post")
            assert item and item["task_id"] == "t-after"
            queue.complete("t-after")
            assert queue.get("t-after")["status"] == "done"
        finally:
            queue.close_all()


class TestRetryHelpers:
    """sqlite_util 两个助手的行为判据（含失败面）。"""

    def test_enable_wal_retries_on_locked(self):
        """锁类错误必须重试；第 N 次成功后正常返回。"""
        from loom_core import sqlite_util

        calls = []

        class FakeConn:
            def execute(self, sql):
                calls.append(sql)
                if len(calls) < 3:
                    raise sqlite_util.sqlite3.OperationalError("database is locked")

        sqlite_util.enable_wal(FakeConn(), attempts=5)
        assert len(calls) == 3

    def test_enable_wal_raises_after_exhausting_attempts(self):
        from loom_core import sqlite_util

        class AlwaysLocked:
            def execute(self, sql):
                raise sqlite_util.sqlite3.OperationalError("database is locked")

        with pytest.raises(sqlite_util.sqlite3.OperationalError):
            sqlite_util.enable_wal(AlwaysLocked(), attempts=2)

    def test_enable_wal_non_lock_error_raises_immediately(self):
        """非锁类错误（如库损坏）不许被重试掩盖。"""
        from loom_core import sqlite_util

        calls = []

        class Corrupt:
            def execute(self, sql):
                calls.append(sql)
                raise sqlite_util.sqlite3.OperationalError(
                    "database disk image is malformed")

        with pytest.raises(sqlite_util.sqlite3.OperationalError):
            sqlite_util.enable_wal(Corrupt(), attempts=5)
        assert len(calls) == 1

    def test_add_column_if_missing_tolerates_duplicate_race(self, tmp_path):
        """并发下 check-then-act 撞 duplicate column：视为成功且不重复加列。"""
        from loom_core import sqlite_util

        conn = sqlite3.connect(str(tmp_path / "t.db"))
        try:
            conn.execute("CREATE TABLE t (a INTEGER)")
            conn.commit()
            # 首加：真的执行了 ALTER
            assert sqlite_util.add_column_if_missing(conn, "t", "b", "INTEGER DEFAULT 0")
            # 再加：列已在，跳过
            assert not sqlite_util.add_column_if_missing(conn, "t", "b", "INTEGER DEFAULT 0")
        finally:
            conn.close()

        class _EmptyCursor:
            def fetchall(self):
                return []

        class RacingConn:
            """模拟竞态：预检说没有（table_info 返回空），ALTER 却撞 duplicate。"""

            def execute(self, sql):
                if sql.strip().upper().startswith("PRAGMA"):
                    return _EmptyCursor()
                raise sqlite3.OperationalError("duplicate column name: b")

        assert not sqlite_util.add_column_if_missing(RacingConn(), "t", "b", "INTEGER")

    def test_add_column_if_missing_other_errors_propagate(self, tmp_path):
        """非 duplicate 的 OperationalError 照抛。"""
        from loom_core import sqlite_util

        class Broken:
            def execute(self, sql):
                raise sqlite3.OperationalError("attempt to write a readonly database")

        with pytest.raises(sqlite3.OperationalError):
            sqlite_util.add_column_if_missing(Broken(), "t", "b", "INTEGER")


class TestWalSwitchUnderDeterministicContention:
    """确定性复现"另有一方持库锁"时切 WAL 的行为（不靠概率碰窗口）。

    机制（本机实测，2026-10-09）：`PRAGMA journal_mode=WAL` 需要短暂独占锁，
    握手失败时 SQLite **不走 busy handler**，而是自带一个约 0.45s 的内部重试
    窗口——持锁方 0.3s 就放锁时旧写法也能等到（所以那个时长区分不出新旧）；
    持锁 1.2s（> 0.45s 窗口）时旧写法**必然**在 0.45s 前后抛
    "database is locked"，而修复后的 `enable_wal` 靠自身退避重试
    （0.05/0.1/0.2/0.4/0.8…，累计窗口约 3.5s）跨过 1.2s 等到锁释放。
    本文件顶部的多进程判据（真实首建现场）是概率性的（本机实测冲突率约
    2%/进程），对本文件的两条确定性判据是补充而非替代。
    """

    #: 持锁时长：必须 > SQLite 内部窗口（约 0.45s），才能把新旧写法分开
    HOLD_SECONDS = 1.2

    def _exclusive_lock_holder(self, db_path):
        holder = sqlite3.connect(db_path, timeout=5, check_same_thread=False)
        holder.execute("PRAGMA journal_mode")  # 触发建库
        holder.execute("BEGIN EXCLUSIVE")
        return holder

    def test_old_style_pragma_fails_under_exclusive_lock(self, tmp_path):
        """护栏正例：证明这个场景确实能把旧写法打红（否则下面的绿是空转）。"""
        db = str(tmp_path / "fresh.db")
        holder = self._exclusive_lock_holder(db)
        try:
            other = sqlite3.connect(db, timeout=0.1)
            try:
                with pytest.raises(sqlite3.OperationalError, match="locked"):
                    other.execute("PRAGMA journal_mode=WAL")
            finally:
                other.close()
        finally:
            holder.rollback()
            holder.close()

    def test_enable_wal_succeeds_once_lock_released(self, tmp_path):
        """修复路径：长持锁（1.2s）下 enable_wal 必须重试成功并切到 wal。

        旧写法在 ~0.45s 即抛（区分点就在这里）；新写法靠自带退避重试
        跨过 1.2s 等到锁释放。
        """
        import threading
        import time

        from loom_core import sqlite_util

        db = str(tmp_path / "fresh.db")
        holder = self._exclusive_lock_holder(db)

        def _release_soon():
            time.sleep(self.HOLD_SECONDS)
            holder.rollback()

        thread = threading.Thread(target=_release_soon)
        thread.start()
        try:
            conn = sqlite3.connect(db, timeout=0.1, check_same_thread=False)
            try:
                sqlite_util.enable_wal(conn)
                mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
                assert mode == "wal"
            finally:
                conn.close()
        finally:
            thread.join()
            holder.close()
