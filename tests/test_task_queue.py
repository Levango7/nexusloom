"""TaskQueue — 持久化任务队列测试"""
import sqlite3
import threading

import pytest

from loom_core.task_queue import TaskQueue


@pytest.fixture
def queue(tmp_path):
    db = str(tmp_path / "test_tasks.db")
    q = TaskQueue(db_path=db)
    yield q
    q.close()


class TestTaskQueue:
    def test_submit_and_acquire(self, queue):
        """基本入队出队"""
        assert queue.submit("t1", "docgen", "input.md", {"key": "val"})
        task = queue.acquire(worker_id="w1")
        assert task is not None
        assert task["task_id"] == "t1"
        assert task["pipeline_name"] == "docgen"
        assert task["input_file"] == "input.md"
        assert task["config"] == {"key": "val"}

    def test_acquire_empty_queue(self, queue):
        """空队列出队返回 None"""
        assert queue.acquire() is None

    def test_acquire_order(self, queue):
        """FIFO 顺序"""
        queue.submit("t1", "p", "a.md")
        queue.submit("t2", "p", "b.md")
        queue.submit("t3", "p", "c.md")
        assert queue.acquire()["task_id"] == "t1"
        assert queue.acquire()["task_id"] == "t2"
        assert queue.acquire()["task_id"] == "t3"

    def test_complete_success(self, queue):
        """完成任务：终态与时间戳落定，**结果正文不在这张表里**（Stage 3b 摘写）。

        `projection` 是这次运行的投影健康位；`complete` 不经引擎，所以它写的是默认
        升格后的 `ok`——真正标 `incomplete` 的是 worker 兜底那条分支（见 test_worker）。
        """
        queue.submit("t1", "p", "a.md")
        queue.acquire()
        queue.complete("t1")
        task = queue.get("t1")
        assert task["status"] == "done"
        assert "result" not in task, (
            "队列又开始承载结果正文了：事实源是 run_nodes.result_json / deliveries")
        assert task["projection"] == "ok"
        assert task["finished_at"] > 0

    def test_complete_with_error(self, queue):
        """失败任务"""
        queue.submit("t1", "p", "a.md")
        queue.acquire()
        queue.complete("t1", error="something broke")
        task = queue.get("t1")
        assert task["status"] == "failed"
        assert task["error"] == "something broke"

    def test_cancel(self, queue):
        """取消任务"""
        queue.submit("t1", "p", "a.md")
        queue.cancel("t1")
        task = queue.get("t1")
        assert task["status"] == "cancelled"

    def test_recover(self, queue):
        """重启恢复：running → pending"""
        queue.submit("t1", "docgen", "a.md")
        queue.submit("t2", "docgen", "b.md")
        queue.acquire()  # t1 → running
        queue.acquire()  # t2 → running

        recovered = queue.recover()
        assert len(recovered) == 2
        assert all(t["status"] == "pending" for t in queue.list_pending())

    def test_recover_empty(self, queue):
        """无 running 任务时恢复返回空"""
        queue.submit("t1", "p", "a.md")
        assert queue.recover() == []

    def test_idempotent_submit(self, queue):
        """重复 submit 同一 task_id 被忽略"""
        assert queue.submit("t1", "p", "a.md") is True
        assert queue.submit("t1", "p", "a.md") is False  # 已存在

    def test_list_all(self, queue):
        """列出所有任务"""
        queue.submit("t1", "p", "a.md")
        queue.submit("t2", "p", "b.md")
        queue.acquire()
        tasks = queue.list_all()
        assert len(tasks) == 2

    def test_list_by_status(self, queue):
        """按状态过滤"""
        queue.submit("t1", "p", "a.md")
        queue.submit("t2", "p", "b.md")
        queue.acquire()
        pending = queue.list_all(status="pending")
        running = queue.list_all(status="running")
        assert len(pending) == 1
        assert len(running) == 1

    def test_stats(self, queue):
        """队列统计"""
        queue.submit("t1", "p", "a.md")
        queue.submit("t2", "p", "b.md")
        queue.acquire()
        stats = queue.stats()
        assert stats.get("pending") == 1
        assert stats.get("running") == 1

    def test_cleanup(self, queue):
        """清理过期任务"""
        queue.submit("t1", "p", "a.md")
        queue.complete("t1")
        queue.cleanup(max_age_days=0)
        assert queue.get("t1") is None

    def test_concurrent_acquire(self, queue):
        """多线程并发 acquire 不会拿到同一个任务"""
        queue.submit("t1", "p", "a.md")
        queue.submit("t2", "p", "b.md")
        results = []
        lock = threading.Lock()

        def worker(wid):
            task = queue.acquire(worker_id=wid)
            if task:
                with lock:
                    results.append(task["task_id"])

        threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 2
        assert set(results) == {"t1", "t2"}

    def test_get_nonexistent(self, queue):
        """查询不存在的任务"""
        assert queue.get("nonexistent") is None


class TestAcquireLostRace:
    """抢输竞争（UPDATE rowcount=0）≠ 队列空：必须重试下一条。

    实测来源：P7 的跨进程判据（8 进程抢 6 条、各 acquire 一次）在全量负载下
    偶发转红；12 进程同步齐射压力复现 11/15 轮有任务没人认领。根因是旧实现
    在 rowcount=0 时直接 `return None`，调用方把"被别人抢先"读成了"没活了"。

    这里的复现是**确定性的**：用连接包装器精确注入竞争窗口（SELECT 与
    UPDATE 之间让第二个连接把该任务抢走），不靠线程时序碰运气——
    反向验证过：把 acquire 改回 `return None`，本类两条用例都转红。
    """

    @staticmethod
    def _wrap_steal_first_claim(q, db, thief_id="thief"):
        """让 q 的连接在第一次领取 UPDATE 前，先把当前目标任务抢走。"""
        real = q._get_conn()

        class _StealOnFirstClaim:
            def __init__(self):
                self._fired = False

            def execute(self, sql, params=()):
                if (not self._fired and isinstance(sql, str)
                        and sql.strip().startswith(
                            "UPDATE task_queue SET status = 'running'")):
                    self._fired = True
                    # 模拟第二个 worker 在 SELECT 与 UPDATE 之间抢先领走
                    with sqlite3.connect(db, timeout=5) as thief:
                        thief.execute(
                            "UPDATE task_queue SET status='running', worker_id=? "
                            "WHERE task_id=? AND status='pending'",
                            (thief_id, params[-1]))
                return real.execute(sql, params)

            def __getattr__(self, name):
                return getattr(real, name)

            def __enter__(self):
                real.__enter__()
                return self

            def __exit__(self, *exc):
                return real.__exit__(*exc)

        q._get_conn = _StealOnFirstClaim  # type: ignore[method-assign]
        return real

    def test_retries_next_pending_after_losing_race(self, tmp_path):
        db = str(tmp_path / "tasks.db")
        q = TaskQueue(db_path=db)
        q.submit("t1", "p", "a.md")
        q.submit("t2", "p", "b.md")
        self._wrap_steal_first_claim(q, db)

        task = q.acquire(worker_id="w1")
        assert task is not None, "抢输竞争被误报成队列空——t2 还在 pending"
        assert task["task_id"] == "t2"
        assert q.get("t1")["worker_id"] == "thief"   # 被抢走的那条归属对手
        assert q.get("t2")["worker_id"] == "w1"

    def test_returns_none_only_when_truly_empty(self, tmp_path):
        """抢输且没有下一条时必须返回 None——重试不许把"真没活"变成死循环。"""
        db = str(tmp_path / "tasks.db")
        q = TaskQueue(db_path=db)
        q.submit("t1", "p", "a.md")
        self._wrap_steal_first_claim(q, db)

        assert q.acquire(worker_id="w1") is None
        assert q.get("t1")["worker_id"] == "thief"
