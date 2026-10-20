"""路径解析收口 —— 引擎找自己资源的唯一入口。

背景：`pipelines/`（流水线 YAML + lock）、`agents/`、`scripts/` 的位置此前
由五个调用方各自推导（`Path(__file__).parent.parent / "pipelines"`
之类的写法散落在 scheduler / mcp_server / admin_api / worker / run.py）。
两件事因此同时成立：
  1. 开发树里能跑（源码目录就是项目根）；
  2. `pip install .` 之后全白屏——wheel 把 Python 包装进 site-packages，
     而 `pipelines/*.yaml` 从来不在任何 package 里，装完即不存在。

product-spec P0 判据 1（干净 venv 装完 `--check` 不红）钉住这个方向。
现在统一从这里解析，并按顺序尝试：
  1. 环境变量 `LOOM_ROOT`（旧名 `DOC_PIPELINE_ROOT` 过渡期仍可读）——
     部署时显式指定（MAOP 侧按 spec §7.3 约定用的就是旧名，双读规则见
     `loom_core.envs`）；
  2. 安装位置：`pitlines` 作为包数据被打进 wheel 时，它在
     site-packages/loom_core/../pipelines（即 `loom_core` 的邻目录）。

开发树下两种都命中同一处（源码目录），行为不变。
"""
from __future__ import annotations

import sys
from pathlib import Path

from .envs import LEGACY_PREFIX, PREFIX, read

#: 部署期显式指定项目根（双读：LOOM_ROOT 优先，DOC_PIPELINE_ROOT 回退——
#: MAOP 的 doc_pipeline_adapter 用的正是旧名，双读规则见 loom_core.envs）
ROOT_ENV = f"{PREFIX}ROOT"
LEGACY_ROOT_ENV = f"{LEGACY_PREFIX}ROOT"

_PROJECT_ROOT = Path(__file__).parent.parent.absolute()


def project_root() -> Path:
    """项目根：env 优先，否则源码/安装树根。"""
    env = (read("ROOT") or "").strip()
    return Path(env) if env else _PROJECT_ROOT


def _sys_prefix() -> Path:
    """当前解释器的安装前缀（venv 下 = venv 根）。"""
    return Path(sys.prefix)


def _has_pipeline_yaml(cand: Path) -> bool:
    """候选目录里是否真有流水线 YAML。

    只看 `is_dir()` 会被**空目录**骗过：安装态下 site-packages/pipelines 会
    被 wheel 的 data-files 顺手创建一个空壳（pip 先建目录再放文件，装失败
    或部分安装时就剩个空目录）。实测踩过：判 is_dir 时命中的正是它，
    于是装完的引擎认为"没有流水线"。所以判据必须是"有 yaml"。
    """
    try:
        return any(cand.glob("*.yaml"))
    except OSError:
        return False


def pipelines_dir() -> Path:
    """流水线目录（YAML + .lock + quality/ + prompts/）。

    解析顺序（安装态落点是实测出来的，2026-10-10）：
      1. env `LOOM_ROOT`（旧名 `DOC_PIPELINE_ROOT` 回退可读）；
      2. 源码树：`loom_core` 的邻目录（开发时就是仓库根）；
      3. 安装态 `sys.prefix/pipelines`：`[tool.setuptools.data-files]` 的真
         落点——wheel 把 data-files 装到 venv 根，不是 site-packages。
    都不命中时退回源码树路径，让调用方自己决定怎么报（宁可报"没有"，
    也不要静默指到一个不存在的目录）。
    """
    env = (read("ROOT") or "").strip()
    if env:
        return Path(env) / "pipelines"
    for cand in (_PROJECT_ROOT / "pipelines", _sys_prefix() / "pipelines"):
        if _has_pipeline_yaml(cand):
            return cand
    return _PROJECT_ROOT / "pipelines"


def agents_dir() -> Path:
    """内置 Agent 目录。"""
    return project_root() / "agents"


def dashboard_dir() -> Path:
    """控制台静态资源目录（随包发布，`loom_core/dashboard/`）。

    放在包内而不是项目根：项目根在安装态是 site-packages 的上一级，
    源码树里能跑、`pip install` 之后却是一片空白（2026-10-20 复核实测：
    3.12.0 wheel 里 0 个 dashboard 资源）。
    """
    return Path(__file__).resolve().parent / "dashboard"


def scripts_dir() -> Path:
    """脚本/工具目录。"""
    return project_root() / "scripts"


def versions_root_env() -> str:
    """版本库根（沿用 state_paths 的同名语义，未设时返回相对路径）。"""
    return read("VERSIONS_DIR", "versions") or "versions"
