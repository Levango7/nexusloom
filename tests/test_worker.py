"""tests/test_worker.py — 常驻 worker：出队互斥、崩溃回收、取消不被洗回。

这些用例都围着实测缺陷写：`TaskQueue.acquire()` 此前没有任何调用方，
所以"提交任务"与"执行任务"之间缺一个环；补环之后，环上的三个危险点必须钉住：
并发双跑、崩溃后永久卡在 running、用户取消被收尾覆写。
"""
import os
import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_core.pipeline import TaskStatus
from loom_core.task_queue import TaskQueue
from loom_core.worker import TaskWorker

PROJECT = Path(__file__).resolve().parent.parent


class _FakeOrchestrator:
    """替身执行器：只证明 worker 的编排与落状态，不跑真流水线。"""

    def __init__(self, status=TaskStatus.DONE, error="", raises=None):
        self.calls = []
        self._status = status
        self._error = error
        self._raises = raises

    def run_plan(self, plan, input_file="", task_id=None, wait=True, owner="",
                 resume=False):
        self.calls.append({"plan": plan.pipeline_name, "input_file": input_file,
                           "task_id": task_id, "output": plan.raw.get("pipeline", {}).get("output"),
                           "owner": owner})
        if self._raises:
            raise self._raises
        return SimpleNamespace(status=self._status, result={"writer": {"content": "正文"}},
                               error=self._error)

    def shutdown(self):
        pass


def _queue(tmp_path) -> TaskQueue:
    return TaskQueue(str(tmp_path / "tasks.db"))


def _worker(tmp_path, orch=None, **kw) -> TaskWorker:
    return TaskWorker(orchestrator=orch or _FakeOrchestrator(), queue=_queue(tmp_path),
                      agents_dir=str(PROJECT / "agents"),
                      pipeline_dir=str(PROJECT / "pipelines"), worker_id="w-test", **kw)


class TestAcquireIsExclusive:
    def test_same_task_never_given_to_two_workers(self, tmp_path):
        """跨"两个独立队列句柄"的互斥——这才会打到 SQL 守卫。

        反向验证做过：把 acquire 里的 `AND status='pending'` 去掉，本用例转红。
        别用同一个 TaskQueue 实例开两个线程来测——实例内的 threading.Lock 先把两次
        acquire 串行化，SQL 条件根本没被行使到，测出来是假绿（我第一版就踩了）。
        """
        db = str(tmp_path / "tasks.db")
        TaskQueue(db).submit("t-exclusive", "test_pipeline", "input.md", {})

        got: list = []
        got_lock = threading.Lock()
        barrier = threading.Barrier(2)

        def claim(worker_id):
            # 每个 worker 各自持有句柄，等价于两个进程共用同一份 tasks.db
            w = TaskWorker(orchestrator=_FakeOrchestrator(), queue=TaskQueue(db),
                           agents_dir=str(PROJECT / "agents"),
                           pipeline_dir=str(PROJECT / "pipelines"), worker_id=worker_id)
            barrier.wait()
            item = w.queue.acquire(worker_id=worker_id)
            if item:
                with got_lock:
                    got.append((worker_id, item["task_id"]))

        threads = [threading.Thread(target=claim, args=(f"w{i}",)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)

        assert len(got) == 1, f"同一任务被 {len(got)} 个 worker 抢到: {got}"
        assert TaskQueue(db).get("t-exclusive")["status"] == "running"

    def test_worker_id_and_owner_pid_are_recorded(self, tmp_path):
        q = _queue(tmp_path)
        q.submit("t-owner", "test_pipeline", "input.md", {})
        item = q.acquire(worker_id="w7")
        assert item and item["task_id"] == "t-owner"
        row = sqlite3.connect(str(tmp_path / "tasks.db")).execute(
            "select status, worker_id, owner_pid from task_queue where task_id='t-owner'"
        ).fetchone()
        assert row == ("running", "w7", os.getpid())


class TestReclaimStaleLease:
    def test_dead_owner_lease_is_reclaimed(self, tmp_path):
        q = _queue(tmp_path)
        q.submit("t-crashed", "test_pipeline", "input.md", {})
        q.acquire(worker_id="w-dead")
        # 伪造"上一个 worker 已崩溃"：owner_pid 换成一个不存在的进程，租约也过期
        with sqlite3.connect(str(tmp_path / "tasks.db")) as conn:
            conn.execute("update task_queue set owner_pid = ?, started_at = ? "
                         "where task_id = 't-crashed'", (999_999, time.time() - 7200))

        worker = _worker(tmp_path, lease_stale_seconds=60)
        assert worker.reclaim_stale() == 1
        assert q.get("t-crashed")["status"] == "pending"

    def test_live_owner_is_left_alone(self, tmp_path):
        """活着的 worker 正在跑的任务不能被别的 worker 翻回 pending（否则双跑）。"""
        q = _queue(tmp_path)
        q.submit("t-live", "test_pipeline", "input.md", {})
        q.acquire(worker_id="w-live")

        worker = _worker(tmp_path, lease_stale_seconds=60)
        assert worker.reclaim_stale() == 0
        assert q.get("t-live")["status"] == "running"


class TestRunOnce:
    def test_queued_task_is_executed_without_manual_recover(self, tmp_path):
        """API 提交之后不需要人工 `--recover`：worker 一轮就能消费掉。"""
        q = _queue(tmp_path)
        q.submit("t-e2e", "test_pipeline", "input.md", {"output": str(tmp_path / "out.md")})
        orch = _FakeOrchestrator()
        worker = _worker(tmp_path, orch)

        assert worker.run_once() is not None
        assert orch.calls == [{"plan": "test_pipeline", "input_file": "input.md",
                               "task_id": "t-e2e", "output": str(tmp_path / "out.md"),
                               "owner": ""}]  # 队列条目无 principal（无身份世界）→ owner 空串
        row = q.get("t-e2e")
        assert row["status"] == "done"
        assert row["result"], "结果没写回队列，API 侧就查不到产出"

    def test_idle_worker_does_nothing(self, tmp_path):
        worker = _worker(tmp_path)
        assert worker.run_once() is None
        assert worker.processed == 0

    def test_unknown_pipeline_fails_honestly_and_lists_available(self, tmp_path):
        q = _queue(tmp_path)
        q.submit("t-nope", "no-such-pipeline", "input.md", {})
        worker = _worker(tmp_path)

        worker.run_once()
        row = q.get("t-nope")
        assert row["status"] == "failed"
        assert "可用" in row["error"], row["error"]

    def test_executor_crash_still_writes_terminal_state(self, tmp_path):
        """崩在收尾之前也要落地一个终态，不能永久停在 running。"""
        q = _queue(tmp_path)
        q.submit("t-crash", "test_pipeline", "input.md", {})
        worker = _worker(tmp_path, _FakeOrchestrator(raises=RuntimeError("段错误")))

        worker.run_once()
        assert q.get("t-crash")["status"] == "failed"
        assert "段错误" in q.get("t-crash")["error"]
        assert worker.failed == 1


class TestCancelIsRespected:
    def test_cancelled_task_is_not_resurrected_as_done(self, tmp_path):
        """`finish` 只写一次终态：API 取消过的行，不能被收尾覆写成 done。"""
        q = _queue(tmp_path)
        q.submit("t-cancel", "test_pipeline", "input.md", {})
        q.acquire(worker_id="w-x")
        q.cancel("t-cancel")

        assert q.finish("t-cancel", "done", result={"writer": {"content": "x"}}) is False
        assert q.get("t-cancel")["status"] == "cancelled"

    def test_finish_writes_when_still_running(self, tmp_path):
        q = _queue(tmp_path)
        q.submit("t-run", "test_pipeline", "input.md", {})
        q.acquire(worker_id="w-y")
        assert q.finish("t-run", "done", result={"a": 1}) is True
        assert q.get("t-run")["status"] == "done"


class TestRunForever:
    def test_stop_event_exits_the_loop(self, tmp_path):
        worker = _worker(tmp_path)
        stop = threading.Event()
        stop.set()
        assert worker.run_forever(poll_interval=0.01, stop_event=stop) == 0

    def test_idle_timeout_ends_the_worker(self, tmp_path):
        worker = _worker(tmp_path)
        done = worker.run_forever(poll_interval=0.01, idle_timeout=0.05)
        assert done == 0

    def test_processes_until_queue_empty(self, tmp_path):
        q = _queue(tmp_path)
        for i in range(3):
            q.submit(f"t-loop-{i}", "test_pipeline", "input.md", {})
        worker = _worker(tmp_path)
        # idle_timeout 到点退出：证明它把 3 条都消费完才闲下来
        assert worker.run_forever(poll_interval=0.01, idle_timeout=0.05) == 3
        assert q.list_all(status="done") and worker.processed == 3


@pytest.mark.parametrize("bad_id", ["", None])
def test_worker_falls_back_to_pid_identifier(bad_id):
    """worker_id 缺省时落到 pidN——队列行的 worker_id 是排查"谁领走了任务"的唯一线索。"""
    w = TaskWorker(orchestrator=_FakeOrchestrator(), worker_id=bad_id)
    assert w.worker_id == f"pid{os.getpid()}"
