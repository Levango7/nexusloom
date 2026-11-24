"""pack manifest -- the engine-side authoritative answer to "what is installed".

`--check` used to hardcode docgen's own files as required
(agents/writer.py, agents/researcher.py, pipelines/docgen.yaml, and tests/ in
required_dirs). Swapping in any other set of packs turned the check red for no
real fault. This module collapses it to one rule: **the engine may never name
individual pack files; it may only ask the manifest**.

Measured sources (all via loom_core.paths, which honours LOOM_ROOT
and still resolves after a wheel install):
  1. pipelines/*.yaml -- every non-underscore loose pipeline counts as one
     **built-in pipeline**: a bare YAML in the core dir, not a pack (no
     pack.yaml, no bundle dir). The inventory must not call these packs —
     `stats()` reports the subset separately as builtin_pipelines;
  2. agents/*.py      -- built-in Agent modules;
  3. entry_points group loom.packs -- packs from external pip packages.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from importlib.metadata import entry_points
from pathlib import Path

logger = logging.getLogger(__name__)

PACK_ENTRY_POINT_GROUP = "loom.packs"
#: 旧 group 名（命名收官前的外部契约）——过渡期仍被发现，并按名字记一次迁移提示
LEGACY_PACK_ENTRY_POINT_GROUPS = ("doc_pipeline.packs",)


def _call_depth_cap() -> int:
    """Follow `call` sub-pipelines as deep as the scheduler itself allows.

    Reuses the scheduler's own constant instead of keeping a second number
    here: beyond this depth, inlining is rejected at parse time, so the
    inventory must not (and need not) reach further.
    """
    from .scheduler import MAX_CALL_DEPTH

    return MAX_CALL_DEPTH


def pipelines_dir() -> Path:
    """pipelines 目录（经 paths 收口：env 可改，wheel 装后也找得到）。"""
    from .paths import pipelines_dir as _pd

    return _pd()


def agents_dir() -> Path:
    """agents 目录（同样经 paths 收口）。"""
    from .paths import agents_dir as _ad

    return _ad()


@dataclass
class PackInfo:
    name: str
    source: str
    agent_names: list[str] = field(default_factory=list)
    #: 物理 pack（packs/<name>/）的目录与模块前缀；散装流水线（builtin）为 None
    root: Path | None = None
    module_prefix: str | None = None
    declared_pipelines: list[str] = field(default_factory=list)

    @property
    def is_physical(self) -> bool:
        return self.root is not None

    @property
    def pipelines_dir(self) -> Path | None:
        return self.root / "pipelines" if self.root else None

    @property
    def agents_dir(self) -> Path | None:
        return self.root / "agents" if self.root else None

    @property
    def quality_dir(self) -> Path | None:
        return self.root / "quality" if self.root else None

    @property
    def prompts_dir(self) -> Path | None:
        return self.root / "prompts" if self.root else None


def packs_root() -> Path:
    """物理 pack 根目录（源码树 packs/，安装态 site-packages/packs 同名解析）。"""
    from .paths import agents_dir as _ad

    # packs/ 与 agents/ 同级（同一项目根），复用 agents 的解析链
    return _ad().parent / "packs"


def _load_pack_manifest(pack_dir: Path) -> dict | None:
    """读 packs/<name>/pack.yaml；缺失或解析失败返回 None（由调用方决定是否红）。"""
    import yaml

    manifest = pack_dir / "pack.yaml"
    if not manifest.is_file():
        return None
    try:
        raw = yaml.safe_load(manifest.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else None
    except (OSError, yaml.YAMLError) as e:
        logger.warning("pack manifest 解析失败 %s: %s", manifest, e)
        return None


def physical_packs() -> list[PackInfo]:
    """packs/*/ 下的物理 pack（带 manifest 与目录）。"""
    return _physical_packs_in(packs_root())


def _physical_packs_in(root: Path) -> list[PackInfo]:
    """给定 pack 根下的物理 pack。

    manifest 声明的 agent 名以文件里的 AGENT_NAME 为准（与 loader 注册名一致）；
    声明的 pipelines 必须真实存在于 pack 的 pipelines/ 目录（缺文件由
    --check 的 pack 完整性判据抓红）。
    """
    if not root.is_dir():
        return []
    packs: list[PackInfo] = []
    for pack_dir in sorted(root.iterdir()):
        if not pack_dir.is_dir():
            continue
        manifest = _load_pack_manifest(pack_dir)
        if manifest is None:
            continue
        name = str(manifest.get("name") or pack_dir.name)
        declared_pipelines = [str(x) for x in manifest.get("pipelines", []) or []]
        agents_dir = pack_dir / "agents"
        agent_names: list[str] = []
        if agents_dir.is_dir():
            for path in agents_dir.glob("*.py"):
                if path.stem.startswith("_"):
                    continue
                agent_names.append(_parse_agent_name(path) or path.stem)
        packs.append(PackInfo(
            name=name, source="pack", agent_names=sorted(set(agent_names)),
            root=pack_dir, module_prefix=f"packs.{pack_dir.name}.agents",
            declared_pipelines=declared_pipelines,
        ))
        # 声明与实存的对账留在 discover 之后的 validate；这里只记录
        missing = [p for p in declared_pipelines
                   if not (pack_dir / "pipelines" / f"{p}.yaml").is_file()]
        if missing:
            logger.warning("pack %s 声明的流水线缺失: %s", name, missing)
    return packs


def _yaml_agent_names(path: Path, _depth: int = 0) -> list[str]:
    """Agent names a pipeline really needs (unparseable YAML -> empty).

    Three subtleties that produced phantom results before:
      1. `call` nodes declare a `name` too (e.g. quality_tail) but they are
         *sub-pipeline* references, not agents;
      2. that sub-pipeline's own agents get inlined into the caller's graph,
         so they must be inventoried as well or a missing one goes unseen;
      3. `approval` nodes are human gates, not agents — counting them as
         "referenced but uninstallable" is the same phantom class as (1).
    """
    import yaml

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError):
        return []
    if not isinstance(raw, dict):
        return []
    agents = raw.get("agents")
    if not isinstance(agents, list):
        return []
    names: list[str] = []
    for item in agents:
        if not isinstance(item, dict):
            continue
        if "approval" in item:
            continue
        call = item.get("call")
        if isinstance(call, str) and call:
            if _depth < _call_depth_cap():
                target = path.parent / f"{call}.yaml"
                if target.is_file():
                    names.extend(_yaml_agent_names(target, _depth + 1))
            continue
        name = item.get("name")
        if isinstance(name, str) and name:
            names.append(name)
    return names


def discover() -> list[PackInfo]:
    """Inventory all packs: 物理 pack（packs/*/）+ 散装流水线 + entry_points 包。

    物理 pack 与散装流水线的分工：pack-docgen 这类"领域件集合"带目录与
    manifest；core pipelines/ 里的单文件任务型流水线仍按散装计。
    """
    packs: list[PackInfo] = physical_packs()
    root = pipelines_dir()
    if root.is_dir():
        for path in sorted(root.glob("*.yaml")):
            if path.stem.startswith("_"):
                continue
            packs.append(PackInfo(name=path.stem, source="builtin",
                                  agent_names=_yaml_agent_names(path)))
    names = (PACK_ENTRY_POINT_GROUP, *LEGACY_PACK_ENTRY_POINT_GROUPS)
    eps_all: list = []
    seen_names: set[str] = set()
    try:
        for group in names:
            for ep in entry_points(group=group):
                if ep.name in seen_names:
                    continue  # 新 group 优先；旧 group 同名不重复
                seen_names.add(ep.name)
                eps_all.append(ep)
                if group != PACK_ENTRY_POINT_GROUP:
                    logger.warning(
                        "pack %s 用的 entry_points group %r 已更名：请改用 %r"
                        "（旧名过渡期仍被发现）", ep.name, group, PACK_ENTRY_POINT_GROUP)
    except Exception as e:  # noqa: BLE001  metadata read failure must not kill self-check
        logger.warning("pack entry_points read failed: %s", e)
        eps_all = []
    for ep in sorted(eps_all, key=lambda e: e.name):
        packs.append(PackInfo(name=ep.name, source="entry_point"))
    return packs


def _parse_agent_name(path: Path) -> str | None:
    """Module-level AGENT_NAME via AST (no import/exec of agent code).

    Registered name is AGENT_NAME, not the file stem: http_request_agent.py
    registers as http_request and pipelines declare the registered name.
    Comparing file stems invented phantom "missing" for 7 of 14 modules.
    Unparseable file -> None, caller falls back to the stem (which is what
    the registry would end up using anyway).
    """
    import ast

    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError, ValueError):
        return None
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
            if "AGENT_NAME" in targets and isinstance(node.value, ast.Constant):
                value = node.value.value
                if isinstance(value, str) and value:
                    return value
    return None


def builtin_agents() -> list[str]:
    """Registered agent names shipped under agents/ (_ prefix = internals)."""
    root = agents_dir()
    if not root.is_dir():
        return []
    names = []
    for path in root.glob("*.py"):
        if path.stem.startswith("_"):
            continue
        names.append(_parse_agent_name(path) or path.stem)
    return sorted(set(names))


def agent_inventory() -> tuple[list[str], list[str]]:
    """(available agent names, referenced-but-unresolvable names).

    Available = agents/*.py plus entry_points-provided plugins (same two
    paths as AgentLoader). Missing = a pipeline wants it but neither path has
    it (includes names misspelled in YAML).
    """
    available = set(builtin_agents())
    # 物理 pack 的 agents/ 也是安装面的一部分（pack-docgen 拆出后 writer 等在此）
    for pack in physical_packs():
        available.update(pack.agent_names)
    try:
        from .agent_loader import ENTRY_POINT_GROUP
        plugin_names = [ep.name for ep in entry_points(group=ENTRY_POINT_GROUP)]
    except Exception as e:  # noqa: BLE001  import-time failures must degrade, not crash
        logger.warning("agent entry_points read failed: %s", e)
        plugin_names = []
    available.update(plugin_names)

    referenced: set[str] = set()
    for pack in discover():
        if pack.source == "builtin":
            referenced.update(pack.agent_names)
    return sorted(available), sorted(referenced - available)


def stats() -> dict:
    """One-line summary for --check: pack count, available and missing agents.

    `packs` counts every inventory entry (physical packs + entry-point packs
    + built-in loose pipelines) — it answers "did anything install at all"
    （安装态探针用它判空）。`builtin_pipelines` isolates the loose-pipeline
    subset so consumers neither re-derive it nor mistake those YAMLs for
    packs（台账 §3.3-6）。
    """
    packs = discover()
    available, missing = agent_inventory()
    builtins = [p for p in packs if p.source == "builtin"]
    return {
        "packs": len(packs),
        "pack_names": [p.name for p in packs],
        "pack_infos": packs,
        "builtin_pipelines": len(builtins),
        "agents": len(available),
        "agents_available": available,
        "agents_missing": missing,
    }


def agent_sources(agents_dir: str | Path) -> list[tuple[Path, str]]:
    """AgentLoader 的发现目录清单：核心 agents/ + 各物理 pack 的 agents/。

    返回 (目录, 模块前缀) 对——loader 用前缀合成 sys.modules 键
    （packs.docgen.agents.writer 这类，与直接 import 路径一致，
    保证测试的 import/patch 与 loader 加载命中同一个模块对象）。

    pack 根以传入的 agents_dir 为锚（兄弟目录 packs/）：真实调用链
    （loader/scheduler）传的是项目 agents/，于是发现的是项目 packs/；
    测试传临时 agents 目录时不会泄露真实仓库的 pack（隔离语义天然成立）。
    """
    core = Path(agents_dir)
    sources = [(core, "agents")]
    for pack in _physical_packs_in(core.parent / "packs"):
        agents = pack.agents_dir
        if agents and agents.is_dir() and pack.module_prefix:
            sources.append((agents, pack.module_prefix))
    return sources


def pipeline_dirs() -> list[Path]:
    """流水线解析顺序：核心 pipelines/ 优先，其次各物理 pack 的 pipelines/。"""
    dirs = [pipelines_dir()]
    for pack in physical_packs():
        pdir = pack.pipelines_dir
        if pdir and pdir.is_dir():
            dirs.append(pdir)
    return dirs


def quality_dirs() -> list[Path]:
    """质量档案解析顺序：核心 pipelines/quality + 各物理 pack 的 quality/。"""
    dirs = [pipelines_dir() / "quality"]
    for pack in physical_packs():
        qdir = pack.quality_dir
        if qdir and qdir.is_dir():
            dirs.append(qdir)
    return dirs


def prompts_dirs() -> list[Path]:
    """prompt 模板解析顺序：核心 pipelines/prompts + 各物理 pack 的 prompts/。"""
    dirs = [pipelines_dir() / "prompts"]
    for pack in physical_packs():
        pdir = pack.prompts_dir
        if pdir and pdir.is_dir():
            dirs.append(pdir)
    return dirs
