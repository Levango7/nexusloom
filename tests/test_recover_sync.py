"""P0-3 recover_tasks 内存状态同步测试。

队列侧（SQLite）running → pending 的同时，_running_tasks 中同 id 的
RUNNING 任务必须同步置为 PENDING，避免 get_task/list_tasks 返回旧状态。
PAUSED/DONE 等其他状态不动。
"""
import threading

from loom_core.pipeline import PipelineOrchestrator, PipelineTask, TaskStatus
from loom_core.run_store import RunStore
from loom_core.task_queue import TaskQueue


class _FakeLogger:
    """记录 .log(level, msg, **kw) 调用，供断言告警形态。"""

    def __init__(self):
        self.calls = []

    def log(self, level, msg, **kw):
        self.calls.append((level, msg, kw))


def _make_orch(tmp_path) -> PipelineOrchestrator:
    """最小 orchestrator 骨架：recover_tasks 只依赖 task_queue/_running_tasks/_lock/logger/投影口。"""
    orch = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orch._running_tasks = {}
    orch._lock = threading.RLock()
    orch.task_queue = TaskQueue(str(tmp_path / "tasks.db"))
    orch._logger = _FakeLogger()
    orch._run_store = None
    orch._run_store_failed = True  # 默认无投影口：不干扰既有恢复语义用例
    return orch


def _attach_run_store(orch, tmp_path) -> RunStore:
    """挂真实 RunStore：对账告警用例需要队列×投影双源可造漂移。"""
    orch._run_store = RunStore(str(tmp_path / "runs.db"))
    orch._run_store_failed = False
    return orch._run_store


class TestRecoverSyncsMemory:
    def test_running_task_synced_to_pending(self, tmp_path):
        orch = _make_orch(tmp_path)
        q = orch.task_queue
        q.submit("t1", "docgen", "in.md", {"k": 1})
        assert q.acquire("w1")["task_id"] == "t1"
        assert q.get("t1")["status"] == "running"

        mem = PipelineTask(id="t1", pipeline_name="docgen", input_file="in.md", config={})
        mem.status = TaskStatus.RUNNING
        orch._running_tasks["t1"] = mem

        recovered = orch.recover_tasks()

        assert [r["task_id"] for r in recovered] == ["t1"]
        assert q.get("t1")["status"] == "pending"
        assert mem.status == TaskStatus.PENDING
        q.close()

    def test_paused_memory_state_untouched(self, tmp_path):
        orch = _make_orch(tmp_path)
        q = orch.task_queue
        q.submit("t2", "docgen", "in.md")
        q.acquire("w1")

        mem = PipelineTask(id="t2", pipeline_name="docgen", input_file="in.md", config={})
        mem.status = TaskStatus.PAUSED
        orch._running_tasks["t2"] = mem

        orch.recover_tasks()

        assert q.get("t2")["status"] == "pending"  # 队列侧照常恢复
        assert mem.status == TaskStatus.PAUSED  # 内存侧 PAUSED 不动
        q.close()

    def test_empty_recover_leaves_memory_alone(self, tmp_path):
        orch = _make_orch(tmp_path)
        mem = PipelineTask(id="t3", pipeline_name="docgen", input_file="in.md", config={})
        mem.status = TaskStatus.DONE
        orch._running_tasks["t3"] = mem

        assert orch.recover_tasks() == []
        assert mem.status == TaskStatus.DONE
        orch.task_queue.close()


class TestRecoverDriftAlert:
    """recover_tasks 收尾的只读 stale-running 对账告警（§四十四 B2）。"""

    def test_drift_logs_warning_with_task_ids(self, tmp_path):
        orch = _make_orch(tmp_path)
        store = _attach_run_store(orch, tmp_path)
        q = orch.task_queue
        q.submit("t1", "docgen", "in.md")
        q.acquire("w1")
        q.complete("t1")  # 队列终态 done
        store.open_run("t1", workflow="docgen")  # 投影仍 running＝漂移

        assert orch.recover_tasks() == []
        hits = [c for c in orch._logger.calls if "漂移待对账" in c[1]]
        assert len(hits) == 1
        assert hits[0][0] == "warning"
        assert hits[0][2]["task_ids"] == ["t1"]
        assert hits[0][2]["count"] == 1
        q.close()

    def test_no_drift_no_log(self, tmp_path):
        orch = _make_orch(tmp_path)
        store = _attach_run_store(orch, tmp_path)
        q = orch.task_queue
        q.submit("t1", "docgen", "in.md")
        q.acquire("w1")
        q.complete("t1")
        store.open_run("t1", workflow="docgen")
        store.close_run("t1", "done")  # 投影盖章终态＝一致行

        assert orch.recover_tasks() == []
        assert orch._logger.calls == []
        q.close()

    def test_report_exception_degrades(self, tmp_path, monkeypatch):
        orch = _make_orch(tmp_path)
        _attach_run_store(orch, tmp_path)

        def boom(*a, **k):
            raise RuntimeError("boom")

        monkeypatch.setattr("loom_core.pipeline.stale_running_report", boom)
        assert orch.recover_tasks() == []
        assert any("对账报告失败" in c[1] for c in orch._logger.calls)

        # 降级不影响恢复主链：坏报告用例下 running 仍照常回 pending
        q = orch.task_queue
        q.submit("t9", "docgen", "in.md")
        q.acquire("w1")
        assert [r["task_id"] for r in orch.recover_tasks()] == ["t9"]
        assert q.get("t9")["status"] == "pending"
        q.close()

    def test_drift_report_is_read_only(self, tmp_path):
        orch = _make_orch(tmp_path)
        store = _attach_run_store(orch, tmp_path)
        q = orch.task_queue
        q.submit("t1", "docgen", "in.md")
        q.acquire("w1")
        q.complete("t1")
        store.open_run("t1", workflow="docgen")

        before_q = q.list_all(limit=100_000)
        before_r = store.get_run("t1")
        orch.recover_tasks()
        assert q.list_all(limit=100_000) == before_q
        assert store.get_run("t1") == before_r
        q.close()
