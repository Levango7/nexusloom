"""审批闸门判据 —— spec §9 决策 C（approval 机制先行，最小闭环）。

机制面：声明式 `approval` 节点（跑到此处挂起）→ `POST /tasks/<id>/approve|reject`
决策 → 决策进节点结果/任务台账/审计。五层判据：

  1. 解析与校验（scheduler）：形状、互斥、锁指纹含 message；
  2. 清册（pack_manifest）：闸门不是 agent，不得被算成"缺失的引用"；
  3. 执行（真 orchestrator）：挂起 → resume 被拒 → approve 放行 / reject 终止；
  4. 条件闸门：when 不成立不挂起，按条件跳过（与普通条件节点同语义）；
  5. HTTP handler 与审计：409 语义、决策落 approvals.jsonl。
"""
import json
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

PROJECT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT))

from loom_core import PipelineOrchestrator  # noqa: E402
from loom_core.scheduler import Scheduler  # noqa: E402

GATE_YAML = """
name: gate_demo
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


def _scheduler():
    return Scheduler(agents_dir=str(PROJECT / "agents"),
                     pipeline_dir=str(PROJECT / "pipelines"))


def _write_gate_yaml(tmp_path: Path, text: str = GATE_YAML, name: str = "gate_demo"):
    f = tmp_path / f"{name}.yaml"
    f.write_text(text, encoding="utf-8")
    return f


# ─── 1. 解析与校验 ────────────────────────────────

class TestApprovalParsing:
    def test_parse_bare_true_and_message(self, tmp_path):
        sched = _scheduler()
        plan = sched.parse_file(str(_write_gate_yaml(tmp_path)), verify_lock=False)
        gate = plan.levels[1][0]
        assert gate.agent_name == "release_gate"
        assert gate.approval == {"message": "确认对外发布"}
        # 普通节点不受影响
        assert plan.levels[0][0].approval is None

    def test_parse_bare_true(self, tmp_path):
        text = GATE_YAML.replace(
            '    approval:\n      message: "确认对外发布"\n', "    approval: true\n")
        plan = _scheduler().parse_file(
            str(_write_gate_yaml(tmp_path, text)), verify_lock=False)
        assert plan.levels[1][0].approval == {"message": ""}

    @pytest.mark.parametrize(("bad_key", "value"), [
        ("config", "{threshold: 70}"),
        ("foreach", "{over: 'artifacts.results', max_items: 5}"),
        ("pool_size", "2"),
    ])
    def test_gate_rejects_agentish_keys(self, tmp_path, bad_key, value):
        text = GATE_YAML.replace(
            '    dependencies: ["tldr"]',
            f'    dependencies: ["tldr"]\n    {bad_key}: {value}')
        with pytest.raises(ValueError, match="审批闸门"):
            _scheduler().parse_file(str(_write_gate_yaml(tmp_path, text)),
                                    verify_lock=False)

    def test_gate_rejects_bad_shape(self, tmp_path):
        with pytest.raises(ValueError, match="approval"):
            _scheduler().parse_file(
                str(_write_gate_yaml(tmp_path, GATE_YAML.replace(
                    '    approval:\n      message: "确认对外发布"\n',
                    "    approval: {message: [1, 2]}\n"))),
                verify_lock=False)

    def test_lock_hash_covers_message_and_roundtrips(self, tmp_path):
        """message 是给人读的契约：改了必须让锁漂移（loom lock 显式重写）。"""
        sched = _scheduler()
        f = _write_gate_yaml(tmp_path)
        plan = sched.parse_file(str(f), verify_lock=False)
        lock_path = sched.generate_lockfile(plan, output_dir=str(tmp_path))
        assert sched.verify_lockfile(plan, lockfile=lock_path) == []
        gate_cfg = plan.levels[1][0].agent_config
        h1 = sched._node_config_hash(gate_cfg)

        changed = sched.parse_file(
            str(_write_gate_yaml(tmp_path, GATE_YAML.replace("确认对外发布", "改了措辞"))),
            verify_lock=False)
        h2 = sched._node_config_hash(changed.levels[1][0].agent_config)
        assert h1 != h2, "审批 message 未进锁指纹"
        issues = sched.verify_lockfile(changed, lockfile=lock_path)
        assert any("配置漂移" in i for i in issues), issues


# ─── 2. 清册：闸门不是 agent ──────────────────────

class TestInventory:
    def test_gate_not_counted_as_agent(self, tmp_path):
        from loom_core.pack_manifest import _yaml_agent_names

        f = _write_gate_yaml(tmp_path)
        names = _yaml_agent_names(f)
        assert "tldr" in names
        assert "release_gate" not in names, "闸门被算成了 agent 引用"


# ─── 3/4. 执行（真 orchestrator）──────────────────

@pytest.fixture
def gate_orch(tmp_path):
    orch = PipelineOrchestrator(
        agents_dir=str(PROJECT / "agents"),
        checkpoint_dir=str(tmp_path / "ckpt"),
    )
    orch.register_agents()
    yield orch
    import contextlib
    with contextlib.suppress(Exception):
        orch.shutdown()


def _run_to_gate(orch, tmp_path, yaml_text=GATE_YAML, task_id="gate_task"):
    plan = _scheduler().parse_file(
        str(_write_gate_yaml(tmp_path, yaml_text)), verify_lock=False)
    inp = tmp_path / "in.md"
    inp.write_text("# 标题\n\n这是测试输入内容，用于触发摘要链路。\n", encoding="utf-8")
    task = orch.run_plan(plan, input_file=str(inp), task_id=task_id, wait=False)
    deadline = time.time() + 30
    while time.time() < deadline and not task.awaiting_approval \
            and task.status.value not in ("done", "failed", "cancelled"):
        time.sleep(0.1)
    return task


def _wait_final(task, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline and task.status.value not in ("done", "failed", "cancelled"):
        time.sleep(0.1)
    return task.status.value


class TestGateExecution:
    def test_gate_holds_task_with_metadata(self, gate_orch, tmp_path):
        task = _run_to_gate(gate_orch, tmp_path)
        assert task.awaiting_approval, f"未挂起（status={task.status.value}）"
        assert task.awaiting_approval["nodes"] == ["release_gate"]
        assert task.awaiting_approval["messages"]["release_gate"] == "确认对外发布"
        assert task.status.name == "PAUSED"
        # 边界 checkpoint 由等待循环首轮保存（arm 与保存之间有 0.5s 轮询间隔，
        # 所以这里轮询等它出现，而不是挂起一瞬就断言）
        deadline = time.time() + 10
        ckpt_files: list = []
        while time.time() < deadline and not ckpt_files:
            ckpt_files = list((tmp_path / "ckpt").glob("gate_task*.json"))
            time.sleep(0.1)
        assert ckpt_files, "闸门挂起时未保存 checkpoint"

    def test_resume_is_refused_while_awaiting(self, gate_orch, tmp_path):
        task = _run_to_gate(gate_orch, tmp_path)
        assert gate_orch.resume(task.id) is False, "resume 绕过了审批闸门"
        assert task.awaiting_approval, "被拒的 resume 不应清掉等待状态"

    def test_approve_releases_and_records(self, gate_orch, tmp_path):
        task = _run_to_gate(gate_orch, tmp_path)
        ok, err = gate_orch.approve(task.id, note="看过了，放行", by="tester")
        assert ok and err == ""
        status = _wait_final(task)
        gate = task.dag_nodes["release_gate"]
        assert gate.status == "success"
        assert gate.result["status"] == "approved"
        assert gate.result["note"] == "看过了，放行"
        assert task.approvals[-1]["decision"] == "approved"
        assert task.approvals[-1]["by"] == "tester"
        assert task.awaiting_approval is None
        # 放行后下游/收尾照常（本例整条链路就是 tldr → gate）
        assert status == "done", f"放行后未正常收尾: {status} / {task.error}"

    def test_reject_terminates_with_reason(self, gate_orch, tmp_path):
        task = _run_to_gate(gate_orch, tmp_path, task_id="gate_reject")
        ok, err = gate_orch.reject(task.id, reason="内容不合规", by="tester")
        assert ok and err == ""
        status = _wait_final(task)
        assert status == "cancelled"
        assert "内容不合规" in task.error
        assert task.approvals[-1]["decision"] == "rejected"

    def test_approve_on_idle_task_is_refused(self, gate_orch, tmp_path):
        """没等在审批上的任务不允许被"批准"——审计里不出现无中生有的决策。"""
        task = _run_to_gate(gate_orch, tmp_path, task_id="gate_done")
        gate_orch.approve(task.id)
        _wait_final(task)
        ok, err = gate_orch.approve(task.id)
        assert not ok and "未在审批等待中" in err
        ok2, err2 = gate_orch.reject(task.id)
        assert not ok2 and "未在审批等待中" in err2
        ok3, err3 = gate_orch.approve("no-such-task")
        assert not ok3 and "not found" in err3

    def test_condition_false_gate_is_skipped_without_hold(self, gate_orch, tmp_path):
        """when 不成立 = 作者声明这轮不需要该关卡：不挂起，按条件跳过。"""
        text = GATE_YAML.replace(
            '    dependencies: ["tldr"]',
            '    dependencies: ["tldr"]\n'
            '    when: {path: "task.id", op: "==", value: "never_matches"}')
        task = _run_to_gate(gate_orch, tmp_path, text, task_id="gate_skip")
        status = _wait_final(task)
        assert status == "done", f"{status} / {task.error}"
        assert task.awaiting_approval is None
        assert task.dag_nodes["release_gate"].status == "skipped"
        assert task.dag_nodes["release_gate"].skip_reason == "condition"


# ─── 5. HTTP handler 与审计 ───────────────────────

class TestApprovalHTTPAndAudit:
    @pytest.fixture
    def handler(self):
        from loom_core.admin_api import AdminHandler

        h = AdminHandler.__new__(AdminHandler)
        h.orch = MagicMock()
        h.orch.get_task.return_value = MagicMock()
        h.orch.approve.return_value = (True, "")
        h.orch.reject.return_value = (True, "")
        h._json = MagicMock()
        h.client_address = ("127.0.0.1", 12345)
        h.headers = {}
        return h

    def test_approve_ok_with_note(self, handler):
        handler._handle_approve_task("t1", json.dumps({"note": "ok"}).encode())
        handler.orch.approve.assert_called_once_with("t1", note="ok", by=handler._approval_actor())
        handler._json.assert_called_once_with(
            {"approved": True, "task_id": "t1", "note": "ok"})

    def test_reject_conflict_returns_409(self, handler):
        handler.orch.reject.return_value = (False, "task t1 未在审批等待中（status=done）")
        handler._handle_reject_task("t1", json.dumps({"reason": "x"}).encode())
        handler._json.assert_called_once_with(
            {"error": "task t1 未在审批等待中（status=done）", "task_id": "t1"}, 409)

    def test_task_not_found_404(self, handler):
        handler.orch.get_task.return_value = None
        handler._handle_approve_task("missing", b"")
        handler._json.assert_called_once_with({"error": "task not found"}, 404)

    def test_bad_json_body_400(self, handler):
        handler._handle_reject_task("t1", b"{not json")
        handler._json.assert_called_once_with({"error": "请求体不是合法 JSON"}, 400)

    def test_decision_lands_in_audit_jsonl(self, gate_orch, tmp_path):
        from loom_core.audit import read_audit

        task = _run_to_gate(gate_orch, tmp_path, task_id="gate_audit")
        gate_orch.approve(task.id, note="审计用例", by="auditor|fp")
        _wait_final(task)
        rows = [dict(r) for r in read_audit("approvals", limit=20)]
        mine = [r for r in rows if r.get("task_id") == "gate_audit"]
        assert mine, "审批决策未落审计"
        assert mine[-1]["decision"] == "approved"
        assert mine[-1]["note"] == "审计用例"
        assert mine[-1]["by"] == "auditor|fp"
        assert mine[-1]["nodes"] == ["release_gate"]


# ─── 6. 附带修复：checkpoint save/load 同源（本批勘察时发现）─────

class TestCheckpointSaveLoadSameSource:
    """save 落点必须 == load 读取点。

    此前 save 用 `task.checkpoint_file`（orchestrator 侧字段，受 config 的
    `checkpoint.dir` 覆盖、可以是相对路径），load/remove 用 manager 自己的
    目录——两处不同源时表现为"断点保存成功但 resume 读不到"，且随 cwd 漂移。
    实测来源：审批闸门的挂起判据要断言"闸门挂起会保存边界 checkpoint"，
    结果文件根本没出现在指定目录（写去了 config 覆盖的相对路径）。
    """

    def test_save_lands_in_manager_dir_and_field_follows(self, tmp_path):
        from loom_core import pipeline as P
        from loom_core.checkpoint_manager import CheckpointManager

        task = P.PipelineTask(id="same_source", pipeline_name="gate_demo",
                              input_file="in.md", config={})
        mgr = CheckpointManager(str(tmp_path / "ckpt"))
        mgr.save(task)
        assert (tmp_path / "ckpt" / "same_source.json").exists(), \
            "save 未落在 manager 目录"
        assert task.checkpoint_file == str(tmp_path / "ckpt" / "same_source.json"), \
            "task.checkpoint_file 未回写真实落点"

    def test_save_load_roundtrip(self, tmp_path):
        from loom_core import pipeline as P
        from loom_core.checkpoint_manager import CheckpointManager

        task = P.PipelineTask(id="roundtrip", pipeline_name="gate_demo",
                              input_file="in.md", config={})
        task.status = P.TaskStatus.PAUSED
        mgr = CheckpointManager(str(tmp_path / "ckpt"))
        mgr.save(task, full_state=True)
        loaded, _snaps = mgr.load("roundtrip")
        assert loaded is not None, "save 写的断点 load 读不到（双源漂移）"
        assert loaded.pipeline_name == "gate_demo"

    def test_roundtrip_preserves_delivery_fields(self, tmp_path):
        from loom_core import pipeline as P
        from loom_core.checkpoint_manager import CheckpointManager

        task = P.PipelineTask(id="delivery_fields", pipeline_name="docgen-review",
                              input_file="in.md", config={})
        task.status = P.TaskStatus.PAUSED
        task.output_path = "output/delivery_fields.md"
        task.output_content = "# 已生成，等待审批"
        mgr = CheckpointManager(str(tmp_path / "ckpt"))
        mgr.save(task, full_state=True)

        loaded, _snaps = mgr.load("delivery_fields")
        assert loaded is not None
        assert loaded.output_path == "output/delivery_fields.md"
        assert loaded.output_content == "# 已生成，等待审批"

    def test_remove_clears_the_same_file(self, tmp_path):
        from loom_core import pipeline as P
        from loom_core.checkpoint_manager import CheckpointManager

        task = P.PipelineTask(id="rm_case", pipeline_name="gate_demo",
                              input_file="in.md", config={})
        mgr = CheckpointManager(str(tmp_path / "ckpt"))
        mgr.save(task)
        mgr.remove("rm_case")
        assert not (tmp_path / "ckpt" / "rm_case.json").exists()
