"""docgen-review 产品流水线契约：标准 docgen 质量尾之后必须真的有人审闸门。"""
import sys
from pathlib import Path

import yaml

PROJECT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT))

from loom_core.pack_manifest import physical_packs  # noqa: E402
from loom_core.scheduler import Scheduler, installed_pipelines  # noqa: E402


def _scheduler() -> Scheduler:
    return Scheduler(agents_dir=str(PROJECT / "agents"),
                     pipeline_dir=str(PROJECT / "pipelines"))


def test_docgen_review_is_installed_and_ends_at_approval_gate():
    assert "docgen-review" in installed_pipelines()
    sched = _scheduler()
    plan = sched.parse("docgen-review")

    assert plan.levels[-1][-1].agent_name == "approval_gate"
    assert plan.levels[-1][-1].approval == {"message": ""}
    assert all(node.approval is None
               for level in plan.levels[:-1]
               for node in level)
    # 锁文件必须覆盖这次真实拓扑；否则改掉了闸门但 CI 仍可能假绿。
    assert sched.verify_lockfile(
        plan, lockfile=str(PROJECT / "packs" / "docgen" / "pipelines" /
                           "docgen-review.lock")) == []


def test_docgen_review_is_declared_by_docgen_pack():
    pack = next(p for p in physical_packs() if p.name == "docgen")
    assert "docgen-review" in pack.declared_pipelines


def test_docgen_review_keeps_standard_quality_tail():
    plan = _scheduler().parse("docgen-review")
    names = [node.agent_name for level in plan.levels for node in level]
    for expected in ("writer", "quality_gate__quality_tail", "checker__quality_tail",
                     "layout__quality_tail", "safe_writer__quality_tail"):
        assert expected in names
    assert names.index("safe_writer__quality_tail") < names.index("approval_gate")


def test_docgen_review_uses_compact_prompt_and_token_cap():
    plan = _scheduler().parse("docgen-review")
    writer = next(n for level in plan.levels for n in level if n.agent_name == "writer")
    assert writer.agent_config.config["prompt_profile"] == "delivery-brief"
    assert writer.agent_config.config["restructure_max_tokens"] == 768

    prompt_path = PROJECT / "packs" / "docgen" / "prompts" / "delivery-brief.yaml"
    prompt = yaml.safe_load(prompt_path.read_text(encoding="utf-8"))
    assert len(prompt["sections"]) == 1
    section_prompt = prompt["sections"][0]["prompt"]
    assert "250—500 字" in section_prompt
    assert "风险与下一步" in section_prompt
    assert "Mermaid" not in section_prompt
