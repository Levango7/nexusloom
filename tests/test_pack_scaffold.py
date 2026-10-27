"""pack scaffold judging criteria -- `loom pack new` 的模板生成判据。

验收线：生成的 pack 必须能被 pack_manifest 发现、agent 能被 AgentLoader
加载、pipeline 能被 Scheduler 解析——三件事缺一件，`loom pack check` 就是红的，
这个 pack 就是死代码。

每条判据都绑定一个真实失败模式：
  1. pack.yaml 缺字段 → discover() 跳过整个 pack（静默消失）
  2. AGENT_NAME 与 pack.yaml 声明不一致 → agent_inventory 报 missing
  3. pipeline YAML 语法错误 → Scheduler.parse_file 抛异常
  4. 目录结构不对 → AgentLoader 找不到 agents/
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest
import yaml

from loom_core import pack_manifest as pm
from loom_core.pack_scaffold import (
    scaffold_pack,
    validate_pack_name,
)

PROJECT = Path(__file__).resolve().parent.parent


class TestValidatePackName:
    """pack 名合法性判据。"""

    @pytest.mark.parametrize("name", [
        "mypack", "my-pack", "pack1", "a1", "data-pipeline-v2",
        "x" * 64,
    ])
    def test_valid_names(self, name):
        assert validate_pack_name(name) is None

    @pytest.mark.parametrize("name", [
        "", "a", "-leading", "trailing-", "UPPER", "under_score",
        "sp ace", "x" * 65, "中文", "pack/name", "pack.name",
    ])
    def test_invalid_names(self, name):
        assert validate_pack_name(name) is not None

    def test_reserved_name_rejected(self):
        assert validate_pack_name("docgen") is not None


class TestScaffoldPack:
    """scaffold_pack 生成骨架的判据。"""

    def test_creates_all_expected_files(self, tmp_path):
        created = scaffold_pack("mypack", tmp_path)
        rel = {p.relative_to(tmp_path).as_posix() for p in created}
        expected = {
            "packs/mypack/pack.yaml",
            "packs/mypack/agents/__init__.py",
            "packs/mypack/agents/mypack_agent.py",
            "packs/mypack/pipelines/mypack.yaml",
            "packs/mypack/prompts/default.yaml",
            "packs/mypack/quality/default.yaml",
        }
        assert rel == expected, f"missing: {expected - rel}, extra: {rel - expected}"

    def test_pack_yaml_has_required_fields(self, tmp_path):
        scaffold_pack("mypack", tmp_path)
        manifest = yaml.safe_load(
            (tmp_path / "packs" / "mypack" / "pack.yaml").read_text(encoding="utf-8"))
        assert manifest["name"] == "mypack"
        assert "mypack_agent" in manifest["agents"]
        assert "mypack" in manifest["pipelines"]
        assert "default" in manifest["quality_profiles"]

    def test_agent_module_has_correct_agent_name(self, tmp_path):
        """AGENT_NAME 必须与 pack.yaml 声明一致——不一致会让 agent_inventory 报 missing。"""
        scaffold_pack("mypack", tmp_path)
        agent_file = tmp_path / "packs" / "mypack" / "agents" / "mypack_agent.py"
        src = agent_file.read_text(encoding="utf-8")
        # AST 解析 AGENT_NAME（与 pack_manifest._parse_agent_name 同一方法）
        tree = ast.parse(src)
        for node in tree.body:
            if isinstance(node, ast.Assign):
                targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
                if "AGENT_NAME" in targets and isinstance(node.value, ast.Constant):
                    assert node.value.value == "mypack_agent"
                    return
        pytest.fail("AGENT_NAME not found in scaffolded agent")

    def test_agent_module_has_sandbox_trusted(self, tmp_path):
        """SANDBOX_TRUSTED 让 loader 跳过 AST 沙箱检查——没有它，加载会失败。"""
        scaffold_pack("mypack", tmp_path)
        agent_file = tmp_path / "packs" / "mypack" / "agents" / "mypack_agent.py"
        src = agent_file.read_text(encoding="utf-8")
        assert "SANDBOX_TRUSTED = True" in src

    def test_pipeline_yaml_is_parseable(self, tmp_path):
        """pipeline YAML 语法错误 → Scheduler.parse_file 抛异常。"""
        scaffold_pack("mypack", tmp_path)
        pipeline_file = tmp_path / "packs" / "mypack" / "pipelines" / "mypack.yaml"
        raw = yaml.safe_load(pipeline_file.read_text(encoding="utf-8"))
        assert raw["_name"] == "mypack"
        assert "mypack_agent" in [a["name"] for a in raw["agents"]]
        assert raw["topology"]["levels"] == [["mypack_agent"]]

    def test_prompts_yaml_is_parseable(self, tmp_path):
        scaffold_pack("mypack", tmp_path)
        prompts_file = tmp_path / "packs" / "mypack" / "prompts" / "default.yaml"
        raw = yaml.safe_load(prompts_file.read_text(encoding="utf-8"))
        assert "system_prompt" in raw
        assert "sections" in raw

    def test_quality_yaml_is_parseable(self, tmp_path):
        scaffold_pack("mypack", tmp_path)
        quality_file = tmp_path / "packs" / "mypack" / "quality" / "default.yaml"
        raw = yaml.safe_load(quality_file.read_text(encoding="utf-8"))
        assert raw["name"] == "default"
        assert "threshold" in raw
        assert "weights" in raw

    def test_existing_dir_raises(self, tmp_path):
        scaffold_pack("mypack", tmp_path)
        with pytest.raises(FileExistsError):
            scaffold_pack("mypack", tmp_path)

    def test_invalid_name_raises(self, tmp_path):
        with pytest.raises(ValueError):
            scaffold_pack("Bad Name", tmp_path)

    def test_invalid_name_does_not_create_dir(self, tmp_path):
        with pytest.raises(ValueError):
            scaffold_pack("Bad Name", tmp_path)
        assert not (tmp_path / "packs" / "Bad Name").exists()


class TestHyphenatedPackName:
    """hyphen 处理：目录/文件/AGENT_NAME 用下划线，pack.yaml name 用原始名。"""

    def test_directory_uses_underscore(self, tmp_path):
        scaffold_pack("my-pack", tmp_path)
        assert (tmp_path / "packs" / "my_pack").is_dir()
        assert not (tmp_path / "packs" / "my-pack").exists()

    def test_agent_file_uses_underscore(self, tmp_path):
        scaffold_pack("my-pack", tmp_path)
        agent_file = tmp_path / "packs" / "my_pack" / "agents" / "my_pack_agent.py"
        assert agent_file.is_file()

    def test_agent_name_uses_underscore(self, tmp_path):
        """AGENT_NAME 必须用下划线——Python 模块名不能含连字符。"""
        scaffold_pack("my-pack", tmp_path)
        agent_file = tmp_path / "packs" / "my_pack" / "agents" / "my_pack_agent.py"
        src = agent_file.read_text(encoding="utf-8")
        tree = ast.parse(src)
        for node in tree.body:
            if isinstance(node, ast.Assign):
                targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
                if "AGENT_NAME" in targets and isinstance(node.value, ast.Constant):
                    assert node.value.value == "my_pack_agent"
                    return
        pytest.fail("AGENT_NAME not found in scaffolded agent")

    def test_pack_yaml_name_uses_original(self, tmp_path):
        """pack.yaml 的 name 字段保留原始名（含连字符）。"""
        scaffold_pack("my-pack", tmp_path)
        manifest = yaml.safe_load(
            (tmp_path / "packs" / "my_pack" / "pack.yaml").read_text(encoding="utf-8"))
        assert manifest["name"] == "my-pack"
        assert "my_pack_agent" in manifest["agents"]
        assert "my-pack" in manifest["pipelines"]

    def test_pipeline_file_uses_original_name(self, tmp_path):
        """pipeline 文件名用原始名——YAML 文件名不受 Python 标识符限制。"""
        scaffold_pack("my-pack", tmp_path)
        pipeline_file = tmp_path / "packs" / "my_pack" / "pipelines" / "my-pack.yaml"
        assert pipeline_file.is_file()
        raw = yaml.safe_load(pipeline_file.read_text(encoding="utf-8"))
        assert raw["_name"] == "my-pack"

    def test_class_name_is_pascal_case(self, tmp_path):
        """class 名用 PascalCase（my_pack → MyPack）。"""
        scaffold_pack("my-pack", tmp_path)
        agent_file = tmp_path / "packs" / "my_pack" / "agents" / "my_pack_agent.py"
        src = agent_file.read_text(encoding="utf-8")
        assert "class MyPackAgent(BaseAgent):" in src


class TestScaffoldedPackIsDiscoverable:
    """生成的 pack 必须能被 pack_manifest 发现——否则就是死代码。"""

    def test_discoverable_via_pack_manifest(self, tmp_path, monkeypatch):
        """用 LOOM_ROOT 隔离：scaffold 到 tmp_path，discover 从 tmp_path 读。"""
        scaffold_pack("mypack", tmp_path)
        monkeypatch.setenv("LOOM_ROOT", str(tmp_path))

        packs = pm.discover()
        mypack = next((p for p in packs if p.name == "mypack"), None)
        assert mypack is not None, "scaffolded pack not discovered"
        assert mypack.source == "pack"
        assert "mypack_agent" in mypack.agent_names
        assert "mypack" in mypack.declared_pipelines

    def test_agent_inventory_includes_scaffolded_agent(self, tmp_path, monkeypatch):
        scaffold_pack("mypack", tmp_path)
        monkeypatch.setenv("LOOM_ROOT", str(tmp_path))

        available, missing = pm.agent_inventory()
        assert "mypack_agent" in available
        assert "mypack_agent" not in missing

    def test_pipeline_dirs_include_scaffolded_pack(self, tmp_path, monkeypatch):
        scaffold_pack("mypack", tmp_path)
        monkeypatch.setenv("LOOM_ROOT", str(tmp_path))

        dirs = pm.pipeline_dirs()
        mypack_pipelines = tmp_path / "packs" / "mypack" / "pipelines"
        assert mypack_pipelines in dirs

    def test_agent_sources_include_scaffolded_pack(self, tmp_path, monkeypatch):
        scaffold_pack("mypack", tmp_path)
        monkeypatch.setenv("LOOM_ROOT", str(tmp_path))

        sources = pm.agent_sources(tmp_path / "agents")
        mypack_agents = tmp_path / "packs" / "mypack" / "agents"
        assert any(src_dir == mypack_agents for src_dir, _ in sources)

    def test_scaffolded_pack_passes_pack_check_logic(self, tmp_path, monkeypatch):
        """--check 的 pack 完整性判据：声明的 pipeline 文件必须存在。"""
        scaffold_pack("mypack", tmp_path)
        monkeypatch.setenv("LOOM_ROOT", str(tmp_path))

        packs = pm.discover()
        mypack = next((p for p in packs if p.name == "mypack"), None)
        assert mypack is not None
        # 每个声明的 pipeline 都有对应 YAML 文件
        for pipeline_name in mypack.declared_pipelines:
            yaml_file = mypack.pipelines_dir / f"{pipeline_name}.yaml"
            assert yaml_file.is_file(), f"declared pipeline {pipeline_name} has no YAML"

    def test_hyphenated_pack_is_discoverable(self, tmp_path, monkeypatch):
        """含连字符的 pack 名：manifest name 用原始名，目录名用 slug。"""
        scaffold_pack("my-pack", tmp_path)
        monkeypatch.setenv("LOOM_ROOT", str(tmp_path))

        packs = pm.discover()
        mypack = next((p for p in packs if p.name == "my-pack"), None)
        assert mypack is not None, "hyphenated pack not discovered"
        assert mypack.source == "pack"
        assert mypack.root.name == "my_pack", "directory should use slug"
        assert mypack.module_prefix == "packs.my_pack.agents"
        assert "my_pack_agent" in mypack.agent_names
        assert "my-pack" in mypack.declared_pipelines
        # module_prefix 必须可 import（不含连字符）
        assert "-" not in mypack.module_prefix
