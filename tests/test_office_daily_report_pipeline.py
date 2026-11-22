"""office-daily-report 流水线判据（板面 §五十四）。

钉三件事：
1. 拓扑只含 writer＋质量尾，且当前 lockfile 可校验（不是新增一个没人跑的 YAML）；
2. 该流水线没有 typed inputs 声明，POST /api/tasks 的自由文本闸门应放行；
3. 成稿路径不含 researcher/fetcher，内部业务上下文不会被公网检索外送。
"""
from loom_core.input_schema import declaration, gate_freeform
from loom_core.scheduler import Scheduler


def test_context_only_topology_and_lock():
    plan = Scheduler().parse("office-daily-report")
    ids = [n.agent_name for level in plan.levels for n in level]
    assert ids == [
        "writer",
        "quality_gate__quality_tail",
        "checker__quality_tail",
        "fact_checker__quality_tail",
        "layout__quality_tail",
        "safe_writer__quality_tail",
    ]


def test_freeform_task_gate_allows_pipeline():
    plan = Scheduler().parse("office-daily-report")
    assert declaration(plan) == {}
    gate_freeform(plan, surface="POST /api/tasks")


def test_no_research_or_fetch_in_context_pipeline():
    plan = Scheduler().parse("office-daily-report")
    ids = {n.agent_name for level in plan.levels for n in level}
    assert "researcher" not in ids
    assert "fetcher" not in ids
    assert ids == {"writer", "quality_gate__quality_tail", "checker__quality_tail",
                   "fact_checker__quality_tail", "layout__quality_tail",
                   "safe_writer__quality_tail"}
