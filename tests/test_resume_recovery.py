"""断点续传修复簇 + 执行判定 bug 回归测试。

覆盖：
  P0 resume: checkpoint 恢复 _dag_nodes → 已完成节点跳过（不重发 bus.request）→ 文档非空 DONE
  P0 兜底:   resume 模式 attempts==0 节点绕过遗留幂等键（一次性新键）
  checkpoint 原子写: json.dump 失败时旧文件完好可 load，无 .tmp 残留
  P1 audit B4: error 字典判业务失败走重试/失败通道
  P1 scheduler: 同层依赖负例报错
  附带: fail_fast 未启动节点置 cancelled；registry 状态不残留 RUNNING
"""
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import loom_core.checkpoint_manager as ckpt_module
from loom_core.checkpoint_manager import CheckpointManager
from loom_core.dag_executor import DAGExecutor
from loom_core.executor_factory import create_executor
from loom_core.pipeline import PipelineTask, TaskNode, TaskStatus
from loom_core.registry import AgentStatus

PROJECT = Path(__file__).parent.parent
INPUT_FILE = str(PROJECT / "test_input.md")


# ── 测试替身 ─────────────────────────────

class _FakeLogger:
    def __init__(self):
        self.records = []

    def log(self, level, msg, **kw):
        self.records.append((level, msg, kw))


class _FakeMetrics:
    def observe(self, *args, **kwargs):
        pass

    def counter(self, *args, **kwargs):
        pass

    def gauge(self, *args, **kwargs):
        pass


class _RecordingRegistry:
    def __init__(self):
        self.statuses = {}

    def get_instance(self, name):
        return object()

    def set_status(self, name, status):
        self.statuses[name] = status

    def get_meta(self, name):
        return None


class CountingBus:
    def __init__(self, payload=None, error=None):
        self.calls = []
        self.payload = payload if payload is not None else {"ok": True}
        self.error = error

    def request(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return dict(self.payload)


class ExplodingBus(CountingBus):
    def __init__(self):
        super().__init__()
        self.error = RuntimeError("boom")


def _make_node(**overrides) -> TaskNode:
    kwargs = dict(name="a", agent_name="a", dependencies=[],
                  max_retries=10, initial_delay=0.0, backoff="fixed")
    kwargs.update(overrides)
    return TaskNode(**kwargs)


def _make_task(nodes) -> PipelineTask:
    task = PipelineTask(id="t1", pipeline_name="docgen", input_file="in.md", config={})
    for n in nodes:
        task.dag_nodes[n.name] = n
    return task


def _make_plan(fail_fast=True):
    return SimpleNamespace(pipeline_name="docgen", checkpoint={},
                           fail_fast=fail_fast, raw={})


def _make_executor(bus=None, registry=None) -> DAGExecutor:
    return DAGExecutor(
        registry=registry or _RecordingRegistry(), bus=bus or CountingBus(),
        cb_registry=None, rate_limiters=None, metrics=_FakeMetrics(),
        logger=_FakeLogger(),
    )


# ── checkpoint：恢复 _dag_nodes + 原子写 ─────────────────────────────

class TestCheckpointRestoreAndAtomicity:
    def _ckpt(self, tmp_path) -> CheckpointManager:
        return CheckpointManager(str(tmp_path / "ckpts"), _FakeLogger())

    def _task_with_nodes(self, tmp_path) -> PipelineTask:
        task = PipelineTask(id="atomic1", pipeline_name="p",
                            input_file="in.md", config={})
        task.checkpoint_file = str(tmp_path / "ckpts" / "atomic1.json")
        node = TaskNode(name="researcher", agent_name="researcher")
        node.status = "success"
        node.attempts = 1
        node.result = {"content": "hello"}
        task.dag_nodes["researcher"] = node
        return task

    def test_save_writes_full_dag_node_state(self, tmp_path):
        cm = self._ckpt(tmp_path)
        cm.save(self._task_with_nodes(tmp_path), full_state=True)
        import json
        data = json.loads((tmp_path / "ckpts" / "atomic1.json").read_text(encoding="utf-8"))
        snap = data["_dag_nodes"]["researcher"]
        assert snap["status"] == "success"
        assert snap["attempts"] == 1
        assert snap["result"] == {"content": "hello"}

    def test_load_restores_dag_nodes_to_task_fields(self, tmp_path):
        cm = self._ckpt(tmp_path)
        cm.save(self._task_with_nodes(tmp_path), full_state=True)
        loaded, snaps = cm.load("atomic1")
        assert loaded is not None
        assert loaded.dag_nodes["researcher"].status == "success"
        assert loaded.dag_nodes["researcher"].attempts == 1
        assert loaded.dag_nodes["researcher"].result == {"content": "hello"}
        assert snaps == {} or snaps is not None
        assert loaded._resumed_node_snapshots["researcher"]["result"] == {"content": "hello"}

    def test_load_legacy_checkpoint_without_dag_nodes(self, tmp_path):
        cm = self._ckpt(tmp_path)
        legacy_task = PipelineTask(id="legacy1", pipeline_name="p",
                                   input_file="in.md", config={})
        legacy_task.checkpoint_file = str(tmp_path / "ckpts" / "legacy1.json")
        cm.save(legacy_task)
        loaded, _ = cm.load("legacy1")
        assert loaded is not None
        assert getattr(loaded, "_resumed_node_snapshots", "missing") == "missing"

    def test_failed_save_keeps_old_file_intact_and_loadable(self, tmp_path, monkeypatch):
        cm = self._ckpt(tmp_path)
        task = self._task_with_nodes(tmp_path)
        ckpt_file = tmp_path / "ckpts" / "atomic1.json"
        cm.save(task, full_state=True)
        before = ckpt_file.read_text(encoding="utf-8")

        def boom(*args, **kwargs):
            raise RuntimeError("disk full")

        monkeypatch.setattr(ckpt_module.json, "dump", boom)
        with pytest.warns(RuntimeWarning):
            cm.save(task, full_state=True)

        assert cm.save_failure_count == 1
        assert ckpt_file.read_text(encoding="utf-8") == before
        leftovers = [p.name for p in (tmp_path / "ckpts").iterdir() if p.suffix == ".tmp"]
        assert leftovers == []

        loaded, _ = cm.load("atomic1")
        assert loaded is not None and loaded.id == "atomic1"
        assert loaded.dag_nodes["researcher"].result == {"content": "hello"}


# ── DAG 执行器层：跳过已完成节点 + 幂等兜底 ─────────────────────────────

class TestResumeMergeAndSkip:
    def test_merge_restores_states_and_marks_bypass(self):
        ex = _make_executor()
        done = _make_node(name="w", agent_name="w")
        pend = _make_node(name="q", agent_name="q", dependencies=["w"])
        task = _make_task([done, pend])
        task._resumed_node_snapshots = {
            "w": {"status": "success", "attempts": 2,
                  "result": {"content": "x"}, "error": "", "finished_at": 123.0},
            "q": {"status": "failed", "attempts": 0, "result": {}, "error": "boom"},
        }
        ex._merge_resumed_nodes(task)
        assert task.dag_nodes["w"].status == "success"
        assert task.dag_nodes["w"].result == {"content": "x"}
        assert task.dag_nodes["q"].status == "pending"
        assert task.dag_nodes["q"].attempts == 0
        assert getattr(task.dag_nodes["q"], "_bypass_idempotency", False) is True
        assert getattr(task.dag_nodes["w"], "_bypass_idempotency", False) is False
        task._resumed_node_snapshots = {}
        ex._merge_resumed_nodes(task)
        assert task.dag_nodes["w"].status == "success"

    def test_completed_node_not_resubmitted_and_result_injected(self):
        bus = CountingBus()
        ex = _make_executor(bus=bus)
        done = _make_node(name="writer", agent_name="writer")
        done.status = "success"
        done.attempts = 1
        done.result = {"content": "# Resumed"}
        fresh = _make_node(name="layout", agent_name="layout", dependencies=["writer"])
        task = _make_task([done, fresh])
        with create_executor(max_workers=2) as executor:
            ret = ex.execute_level(task, [done, fresh], INPUT_FILE, _make_plan(), executor)
        assert ret is True
        assert [c["to_a"] for c in bus.calls] == ["layout"]
        assert task.result["writer"] == {"content": "# Resumed"}
        writer_steps = [s for s in task.steps if s.step_name == "writer"]
        assert writer_steps and writer_steps[0].status == "success"

    def test_completed_with_empty_result_reruns(self):
        bus = CountingBus()
        ex = _make_executor(bus=bus)
        empty_ok = _make_node(name="writer", agent_name="writer")
        empty_ok.status = "success"
        empty_ok.result = {}
        task = _make_task([empty_ok])
        with create_executor(max_workers=2) as executor:
            ret = ex.execute_level(task, [empty_ok], INPUT_FILE, _make_plan(), executor)
        assert ret is True
        assert [c["to_a"] for c in bus.calls] == ["writer"]

    def test_async_level_skips_completed_node(self):
        import asyncio
        bus = CountingBus()
        ex = _make_executor(bus=bus)
        done = _make_node(name="writer", agent_name="writer")
        done.status = "success"
        done.result = {"content": "# A"}
        fresh = _make_node(name="layout", agent_name="layout", dependencies=["writer"])
        task = _make_task([done, fresh])
        ret = asyncio.run(ex.execute_level_async(task, [done, fresh], INPUT_FILE, _make_plan()))
        assert ret is True
        assert [c["to_a"] for c in bus.calls] == ["layout"]

    def test_bypass_flag_mints_fresh_idempotency_key(self):
        bus = CountingBus()
        ex = _make_executor(bus=bus)
        node = _make_node()
        node.attempts = 0
        node._bypass_idempotency = True
        task = _make_task([node])
        result = ex.execute_node_from_scheduler(task, node, INPUT_FILE, _make_plan())
        assert result == {"ok": True}
        key1 = bus.calls[0]["idempotency_key"]
        assert key1.startswith("t1:a:0:r") and key1 != "t1:a:0"
        ex.execute_node_from_scheduler(task, node, INPUT_FILE, _make_plan())
        assert bus.calls[1]["idempotency_key"] != key1

    def test_normal_node_keeps_stable_idempotency_key(self):
        bus = CountingBus()
        ex = _make_executor(bus=bus)
        node = _make_node()
        node.attempts = 1
        task = _make_task([node])
        ex.execute_node_from_scheduler(task, node, INPUT_FILE, _make_plan())
        assert bus.calls[0]["idempotency_key"] == "t1:a:1"


# ── 端到端续传（orchestrator.run(resume=True)） ─────────────────────────────

class TestResumeEndToEnd:
    def test_resume_reuses_completed_levels_and_produces_doc(self, orch, tmp_path):
        task_id = "resume_e2e"
        out_file = tmp_path / "resumed_doc.md"
        content = ("# Resumed Doc\n\n## 背景\n\n这是从断点恢复的正文内容，用于验证续传后文档非空。\n\n"
                   "## 要点\n\n- 断点续传保留已完成结果\n- 幂等键不再吞掉重放请求\n\n引用 [1] 说明。\n")

        craft = PipelineTask(id=task_id, pipeline_name="test_pipeline",
                             input_file=INPUT_FILE, config={})
        craft.checkpoint_file = str(Path(orch._checkpoint.checkpoint_dir) / f"{task_id}.json")
        crafted_results = {
            "researcher": {"results": [{"title": "t", "url": "u", "snippet": "s"}]},
            "fetcher": {"articles": [{"title": "t", "text": "正文"}]},
            "quality_gate": {"scores": {"overall": 90}, "overall_score": 90},
            "checker": {"issues": [], "P0": 0},
            "layout": {"optimized": False},
            "writer": {"content": content},
        }
        for name, res in crafted_results.items():
            n = TaskNode(name=name, agent_name=name)
            n.status = "success"
            n.attempts = 1
            n.result = res
            craft.dag_nodes[name] = n
        orch._checkpoint.save(craft, full_state=True)

        orig_request = orch.bus.request
        calls = []

        def spy(*args, **kwargs):
            calls.append(kwargs.get("to_a"))
            return orig_request(*args, **kwargs)

        orch.bus.request = spy
        try:
            task = orch.run(pipeline_name="test_pipeline", input_file=INPUT_FILE,
                            config={"output": str(out_file)}, wait=True,
                            resume=True, task_id=task_id)
        finally:
            orch.bus.request = orig_request

        deadline = time.time() + 60
        while time.time() < deadline and task.status not in (
                TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.CANCELLED):
            time.sleep(0.2)

        assert task.status == TaskStatus.DONE, f"task ended {task.status}: {task.error}"
        reused = set(crafted_results)
        resent = [c for c in calls if c in reused]
        assert not resent, f"已完成节点被重发 bus.request: {resent} (all={calls})"
        assert "writer" in task.result and task.result["writer"].get("content")
        assert out_file.exists() and out_file.stat().st_size > 0
        text = out_file.read_text(encoding="utf-8")
        assert "Resumed Doc" in text


# ── 声明式路径续传（run_plan(resume=True)） ─────────────────────────────

class TestDeclarativeResumeEndToEnd:
    """#24：--resume 此前只在 legacy 分支生效，而 run.py 默认走声明式路径，
    于是 `--resume` 静默变成全新执行——每个已完成节点都重跑一遍。
    run_plan 补 resume 后：已完成节点复用产物且不重发 bus.request，
    未完成节点照常执行，交付物照写。"""

    def _craft_checkpoint(self, orch, task_id: str, nodes: dict,
                          pipeline_name: str = "test_pipeline") -> None:
        craft = PipelineTask(id=task_id, pipeline_name=pipeline_name,
                             input_file=INPUT_FILE, config={})
        craft.checkpoint_file = str(Path(orch._checkpoint.checkpoint_dir) / f"{task_id}.json")
        for name, res in nodes.items():
            n = TaskNode(name=name, agent_name=name)
            n.status = "success"
            n.attempts = 1
            n.result = res
            craft.dag_nodes[name] = n
        orch._checkpoint.save(craft, full_state=True)

    @pytest.mark.parametrize("async_mode", [False, True], ids=["sync", "async"])
    def test_resume_entry_preserves_delivery_fields(self, orch, docgen_plan, async_mode):
        """loader 读对还不够：run_plan 新建任务时也必须带入交付字段。"""
        import asyncio

        task_id = f"resume_delivery_{'async' if async_mode else 'sync'}"
        craft = PipelineTask(id=task_id, pipeline_name=docgen_plan.pipeline_name,
                             input_file=INPUT_FILE, config={})
        craft.output_path = f"output/{task_id}.md"
        craft.output_content = "# 审批前已生成"
        orch._checkpoint.save(craft, full_state=True)
        docgen_plan.levels = []

        if async_mode:
            task = asyncio.run(orch.run_plan_async(
                plan=docgen_plan, input_file=INPUT_FILE, task_id=task_id, resume=True))
        else:
            task = orch.run_plan(plan=docgen_plan, input_file=INPUT_FILE,
                                 task_id=task_id, wait=True, resume=True)

        assert task.output_path == craft.output_path
        assert task.output_content == craft.output_content

    def test_run_plan_resume_reuses_completed_levels(self, orch, docgen_plan, tmp_path):
        task_id = "resume_decl_e2e"
        out_file = tmp_path / "decl_resumed_doc.md"
        content = ("# Resumed Declarative\n\n## 背景\n\n这是从断点恢复的正文内容，用于验证声明式续传后文档非空。\n\n"
                   "## 要点\n\n- 已完成节点复用产物\n- 未完成节点照常执行\n\n引用 [1] 说明。\n")
        crafted = {
            "researcher": {"results": [{"title": "t", "url": "u", "snippet": "s"}]},
            "fetcher": {"articles": [{"title": "t", "text": "正文"}]},
            "writer": {"content": content},
            "quality_gate": {"scores": {"overall": 90}, "overall_score": 90},
        }
        self._craft_checkpoint(orch, task_id, crafted)
        docgen_plan.raw.setdefault("pipeline", {})["output"] = str(out_file)

        orig_request = orch.bus.request
        calls = []

        def spy(*args, **kwargs):
            calls.append(kwargs.get("to_a"))
            return orig_request(*args, **kwargs)

        orch.bus.request = spy
        try:
            task = orch.run_plan(plan=docgen_plan, input_file=INPUT_FILE,
                                 task_id=task_id, wait=True, resume=True)
        finally:
            orch.bus.request = orig_request

        assert task.status == TaskStatus.DONE, f"task ended {task.status}: {task.error}"
        resent = [c for c in calls if c in crafted]
        assert not resent, f"已完成节点被重发 bus.request: {resent} (all={calls})"
        assert task.result["writer"]["content"] == content
        for name in ("checker", "layout", "safe_writer"):
            assert name in calls, f"{name} 未执行 (calls={calls})"
        assert out_file.exists() and out_file.stat().st_size > 0
        assert "Resumed Declarative" in out_file.read_text(encoding="utf-8")

    def test_run_plan_async_resume_reuses_completed_levels(self, orch, docgen_plan, tmp_path):
        import asyncio
        task_id = "resume_decl_async"
        out_file = tmp_path / "decl_async_doc.md"
        content = ("# Resumed Async\n\n## 背景\n\n声明式 async 入口的续传验证正文，用于确认复用可跨入口生效。\n\n"
                   "## 要点\n\n- 已完成节点复用产物\n\n引用 [1] 说明。\n")
        crafted = {
            "researcher": {"results": [{"title": "t", "url": "u", "snippet": "s"}]},
            "fetcher": {"articles": [{"title": "t", "text": "正文"}]},
            "writer": {"content": content},
            "quality_gate": {"scores": {"overall": 90}, "overall_score": 90},
        }
        self._craft_checkpoint(orch, task_id, crafted)
        docgen_plan.raw.setdefault("pipeline", {})["output"] = str(out_file)

        orig_request = orch.bus.request
        calls = []

        def spy(*args, **kwargs):
            calls.append(kwargs.get("to_a"))
            return orig_request(*args, **kwargs)

        orch.bus.request = spy
        try:
            task = asyncio.run(orch.run_plan_async(
                plan=docgen_plan, input_file=INPUT_FILE,
                task_id=task_id, resume=True))
        finally:
            orch.bus.request = orig_request

        assert task.status == TaskStatus.DONE, f"task ended {task.status}: {task.error}"
        resent = [c for c in calls if c in crafted]
        assert not resent, f"已完成节点被重发 bus.request: {resent} (all={calls})"
        assert out_file.exists() and out_file.stat().st_size > 0

    def test_run_plan_async_requires_deliverable_for_done(self, orch, scheduler):
        """async 入口此前无条件盖 DONE，声明了落盘节点却没有产物时报 done——
        SSE 触发的那条路径上，报表与用户都会被骗过。与同步版对齐为交付契约。"""
        import asyncio
        plan = _build_dep_plan(scheduler, [("safe_writer", [])], [["safe_writer"]])

        async def noop_level(task, level, input_file, plan_):
            return True

        orig = orch._executor.execute_level_async
        orch._executor.execute_level_async = noop_level
        try:
            task = asyncio.run(orch.run_plan_async(
                plan=plan, input_file=INPUT_FILE, task_id="async_nodeliver"))
        finally:
            orch._executor.execute_level_async = orig

        assert task.status == TaskStatus.FAILED
        assert "没有交付物" in (task.error or "")

    def test_resume_rejects_foreign_pipeline_checkpoint(self, orch, docgen_plan):
        """task_id 由调用方拼，断点归属未必是当前流水线：归错会静默全量重跑
        （节点名对不上）或用错结果（对得上），所以必须按流水线名拒收。"""
        self._craft_checkpoint(orch, "foreign_ckpt", {"researcher": {"results": [1]}},
                               pipeline_name="other_pipeline")
        assert orch._resume_snapshots_for(docgen_plan, "foreign_ckpt") is None
        assert orch._resume_snapshots_for(docgen_plan, "missing_ckpt") is None

    def test_resume_looks_up_checkpoint_by_task_id(self, orch, docgen_plan):
        self._craft_checkpoint(orch, "own_ckpt", {"researcher": {"results": [1]}})
        snaps = orch._resume_snapshots_for(docgen_plan, "own_ckpt")
        assert snaps and snaps["researcher"]["status"] == "success"


# ── P1 audit B4：error 字典判业务失败 ─────────────────────────────

class TestErrorDictBusinessFailure:
    def test_error_dict_unit(self):
        ok, msg = DAGExecutor._business_failure({"error": "Agent x 未找到"})
        assert ok and "未找到" in msg
        ok, msg = DAGExecutor._business_failure({"error": None})
        assert ok
        ok, _ = DAGExecutor._business_failure({"status": "ok"})
        assert not ok
        ok, _ = DAGExecutor._business_failure(None)
        assert not ok

    def test_error_dict_goes_to_retry_channel_not_success(self):
        bus = CountingBus(payload={"error": "x"})
        ex = _make_executor(bus=bus)
        node = _make_node(max_retries=2)
        task = _make_task([node])
        with create_executor(max_workers=2) as executor:
            ret = ex.execute_level(task, [node], INPUT_FILE, _make_plan(), executor)
        assert ret is False
        assert task.status == TaskStatus.FAILED
        assert len(bus.calls) == 2
        assert node.status == "failed"
        assert any(s.status == "failed" for s in task.steps)


# ── P1 scheduler：同层依赖负例 ─────────────────────────────

def _build_dep_plan(scheduler, agents, levels):
    raw = {
        "name": "dep_check",
        "agents": [{"name": n, "dependencies": deps, "config": {}} for n, deps in agents],
        "topology": {"levels": levels},
    }
    return scheduler._build_plan(raw, "dep_check")


class TestSchedulerSameLayerDependency:
    def test_same_level_dependency_rejected(self, scheduler):
        with pytest.raises(ValueError, match="同层依赖禁止"):
            _build_dep_plan(scheduler, [("a", []), ("b", ["a"])], [["a", "b"]])

    def test_prior_level_dependency_allowed(self, scheduler):
        plan = _build_dep_plan(scheduler, [("a", []), ("b", ["a"])], [["a"], ["b"]])
        assert plan.levels[1][0].dependencies == ["a"]

    def test_forward_dependency_rejected(self, scheduler):
        with pytest.raises(ValueError):
            _build_dep_plan(scheduler, [("a", ["z"]), ("z", [])], [["a"], ["z"]])


# ── 附带：fail_fast 僵尸节点清理 + registry 状态复位 ─────────────────────────────

class TestUnstartedSiblingCleanup:
    def test_helper_marks_only_cancellable_futures(self):
        ex = _make_executor()
        rn, rd = _make_node(name="run1", agent_name="run1"), TaskNode(
            name="run1", agent_name="run1")
        pn, pd = _make_node(name="pend1", agent_name="pend1"), TaskNode(
            name="pend1", agent_name="pend1")
        rs = SimpleNamespace(status="running", error="", result={}, step_name="run1",
                             agent_name="run1", started_at=time.time(), finished_at=0)
        ps = SimpleNamespace(status="running", error="", result={}, step_name="pend1",
                             agent_name="pend1", started_at=time.time(), finished_at=0)
        plan = _make_plan()
        task = PipelineTask(id="t1", pipeline_name="docgen", input_file="in.md", config={})
        task.dag_nodes["run1"] = rd
        task.dag_nodes["pend1"] = pd
        with create_executor(max_workers=1) as executor:
            f_running = executor.submit(time.sleep, 0.6)
            f_pending = executor.submit(time.sleep, 0.0)
            ex._cancel_unstarted_siblings(task, plan,
                                          {f_running: (rn, rd, rs),
                                           f_pending: (pn, pd, ps)},
                                          processed=set())
        assert rd.status != "cancelled"
        assert pd.status == "cancelled"
        assert pd.error == "cancelled"
        cancelled_steps = [s for s in task.steps if s.step_name == "pend1"]
        assert cancelled_steps and cancelled_steps[0].status == "cancelled"
        assert [s for s in task.steps if s.step_name == "run1"] == []

    def test_started_sibling_not_force_cancelled(self):
        import threading

        release = threading.Event()
        sib_started = threading.Event()

        class GateBus:
            def __init__(self):
                self.calls = []

            def request(self, **kwargs):
                self.calls.append(kwargs.get("to_a"))
                if kwargs.get("to_a") == "sib":
                    sib_started.set()
                    release.wait(timeout=5)
                    return {"ok": True}
                # bad 等 sib 确认已进入 running 后再失败：保证 fail_fast
                # 清理执行时 sib 必为 running 态（消除 worker 调度竞态，
                # 原 max_workers=1 下 sib 可能仍在队列 pending 被误判 unstarted）
                sib_started.wait(timeout=5)
                raise RuntimeError("boom")

        bus = GateBus()
        ex = _make_executor(bus=bus)
        bad = _make_node(name="bad", agent_name="bad", max_retries=1)
        sib = _make_node(name="sib", agent_name="sib", max_retries=1)
        task = _make_task([bad, sib])
        try:
            with create_executor(max_workers=2) as executor:
                ret = ex.execute_level(task, [bad, sib], INPUT_FILE, _make_plan(), executor)
                assert ret is False
                assert task.status == TaskStatus.FAILED
                assert task.dag_nodes["bad"].status == "failed"
                assert task.dag_nodes["sib"].status == "running"
                assert not [s for s in task.steps if s.status == "cancelled"]
                release.set()
        finally:
            release.set()


class TestRegistryStatusNoLeak:
    def test_error_status_on_bus_exception(self):
        reg = _RecordingRegistry()
        ex = _make_executor(bus=ExplodingBus(), registry=reg)
        node = _make_node(max_retries=1)
        task = _make_task([node])
        with create_executor(max_workers=1) as executor:
            ex.execute_level(task, [node], INPUT_FILE, _make_plan(), executor)
        assert reg.statuses["a"] == AgentStatus.ERROR

    def test_stopped_status_after_success(self):
        reg = _RecordingRegistry()
        ex = _make_executor(bus=CountingBus(), registry=reg)
        node = _make_node()
        task = _make_task([node])
        with create_executor(max_workers=1) as executor:
            ret = ex.execute_level(task, [node], INPUT_FILE, _make_plan(), executor)
        assert ret is True
        assert reg.statuses["a"] == AgentStatus.STOPPED
