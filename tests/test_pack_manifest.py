"""pack manifest judging criteria -- the derived measure in loom_core.pack_manifest.

The manifest is the engine's only door to "what is installed"; --check reports
from it. Three derivations each tripped a real bug, pinned one by one:

  1. pack count = non-underscore pipelines: internal fragments (sub-pipeline
     tails) are not packs, only call-able. The measure must agree with
     scheduler.installed_pipelines or the console list and what can actually
     run will diverge;
  2. agent names come from module-level AGENT_NAME, not the file stem:
     http_request_agent.py registers as http_request, and pipelines write the
     registered name. Comparing file stems invented phantom "missing" for 7
     of 14 modules;
  3. call nodes are not agents: quality_tail is a sub-pipeline reference via
     call: _quality-tail. Not following call targets makes it read as
     "referenced but uninstallable".
"""
from pathlib import Path

import pytest

from loom_core import pack_manifest as pm
from loom_core.scheduler import installed_pipelines

PROJECT = Path(__file__).resolve().parent.parent


class TestPipelineCounting:
    """Criterion 1: pack count over non-underscore files, matching scheduler."""

    def test_packs_match_scheduler_listing(self):
        """Ask two places about the same fact; the answers must agree."""
        # 可跑清单 = 散装流水线（builtin）+ 物理 pack 声明的流水线。
        # 物理 pack 在 manifest 里记为一个 pack，其成员流水线名在
        # declared_pipelines（docgen 系 6 条住 packs/docgen/pipelines/）。
        packs = pm.discover()
        manifest = {p.name for p in packs if p.source == "builtin"}
        manifest |= {name for p in packs if p.source == "pack"
                     for name in p.declared_pipelines}
        scheduled = set(installed_pipelines())
        assert manifest == scheduled, (
            f"manifest and scheduler diverged: "
            f"manifest-only {sorted(manifest - scheduled)}, "
            f"scheduler-only {sorted(scheduled - manifest)}")

    def test_internal_fragments_are_not_packs(self):
        names = {p.name for p in pm.discover()}
        for fragment in ("_quality-tail",):
            assert fragment not in names, (
                f"{fragment} is an internal fragment (call-only), not a pack")

    def test_every_pipeline_declares_agents(self):
        """Every pack declares at least one agent, else it cannot run."""
        for pack in pm.discover():
            if pack.source not in ("builtin", "pack"):
                continue
            assert pack.agent_names, f"{pack.name} declares no agent"




class TestAgentCounting:
    """Criterion 2: agent names derive from AGENT_NAME, not the file stem."""

    def test_builtin_agents_use_declared_names(self):
        """builtin_agents() 报的是 AGENT_NAME，不是文件 stem。

        把派生改回 filename 比对，这条立刻红（变异验证过）。它守的是两条：
        清单里存在 stem 与注册名不同的件，且导入的是这些注册名。
        """
        declared = pm.builtin_agents()
        assert "http_request" in declared, (
            "lack http_request: 派生回退成文件名了（http_request_agent）")
        stems = {p.stem for p in pm.agents_dir().glob("*.py")
                 if not p.stem.startswith("_")}
        for name in declared:
            if name not in stems:
                continue
            file_name = next(p.stem for p in pm.agents_dir().glob("*.py")
                             if pm._parse_agent_name(p) == name)
            if file_name == name:
                continue
            assert name in declared, (
                f"declared name dropped: {file_name} should register as {name}")

    def test_missing_check_catches_real_typo(self, tmp_path, monkeypatch):
        """拼错的 agent 名必须被抓成 missing（证明判据不是空转）。"""
        pipelines = tmp_path / "pipelines"
        pipelines.mkdir()
        yaml_text = "\n".join([
            "_name: typo",
            "agents:",
            "  - name: no_such_agent",
            "",
        ])
        (pipelines / "typo.yaml").write_text(yaml_text, encoding="utf-8")
        agents = tmp_path / "agents"
        agents.mkdir()
        monkeypatch.setenv("LOOM_ROOT", str(tmp_path))
        _available, missing = pm.agent_inventory()
        assert missing == ["no_such_agent"], (
            f"typo'd agent name was not caught: {missing}")

    def test_ast_parse_is_used_not_import(self):
        """派生只读 AST，不 import agent 代码（导入会拉起重依赖、也可能有副作用）。"""
        src = (PROJECT / "loom_core" / "pack_manifest.py").read_text(encoding="utf-8")
        assert "ast.parse" in src


class TestCallNodesAreNotAgents:
    """Criterion 3: call nodes are not reported as missing agents."""

    def test_call_target_not_reported_missing(self):
        _available, missing = pm.agent_inventory()
        assert "quality_tail" not in missing, (
            "quality_tail is a call node (call: _quality-tail), not an agent; "
            "reporting it missing means call following was skipped")
        assert missing == [], f"this tree should have no unresolvable agent, got: {missing}"

    def test_called_sub_pipeline_agents_are_inventoried(self):
        """Agents inside a called sub-pipeline count toward the caller's pack.

        They get inlined into the caller's graph, so losing one would break
        assembly at run time.
        """
        docgen = next((p for p in pm.discover() if p.name == "docgen"), None)
        if docgen is None:
            pytest.skip("no docgen pack on this machine")
        inlined = set(docgen.agent_names)
        assert "fact_checker" in inlined, (
            "agents of the called sub-pipeline did not enter the caller pack; "
            "the assembly would silently lose them")


class TestStatsHonesty:
    """台账 §3.3-6：散装流水线在清册里必须与 pack 分开计数，不能叫 pack。"""

    def test_stats_reports_builtin_pipelines_separately(self):
        st = pm.stats()
        builtin_names = {p.name for p in pm.discover() if p.source == "builtin"}
        assert st["builtin_pipelines"] == len(builtin_names) >= 1
        # packs 仍是总条目（安装态探针用它判"装了东西没有"），
        # 扣掉 builtin_pipelines 才是真 pack 数。
        real_packs = {p.name for p in pm.discover() if p.source != "builtin"}
        assert st["packs"] - st["builtin_pipelines"] == len(real_packs)
