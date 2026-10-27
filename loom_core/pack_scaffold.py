"""pack scaffold -- `loom pack new <name>` 的模板生成。

物理 pack 的目录结构与 manifest 格式来自 packs/docgen 的既有实现。本模块
只负责**生成骨架**——新建目录、写模板文件，不做注册、不做校验（--check
会在下一次运行时自然抓红缺失的 agent/pipeline）。

模板选择的判据：agent 用 tldr 式的纯离线确定性模板（无网络、无 Key、
无第三方依赖），pipeline 用最小可用的单节点 DAG。这样新 pack 生成后
`loom pack check` 能直接过（agent 可注册、pipeline 可解析）。

hyphen 处理：pack 名允许连字符（如 my-pack），但目录名/文件名/AGENT_NAME
必须用下划线（my_pack）——Python 模块名不能含连字符，AgentLoader 按
`packs.<dir_name>.agents.<file_stem>` 合成 import 路径。pack.yaml 的
`name:` 字段保留原始名，目录名用 slug。
"""
from __future__ import annotations

import re
from pathlib import Path

#: pack 名合法性：小写字母/数字/连字符，2-64 字符，不以连字符开头/结尾
_PACK_NAME_RE = re.compile(r"^[a-z][a-z0-9-]{0,62}[a-z0-9]$")

#: 禁止使用的 pack 名（已有 pack / Python 关键字）
_RESERVED_NAMES = frozenset({"docgen"})


def validate_pack_name(name: str) -> str | None:
    """校验 pack 名；合法返回 None，非法返回错误消息。"""
    if not _PACK_NAME_RE.match(name):
        return f"pack 名 {name!r} 不合法：只允许小写字母/数字/连字符，2-64 字符，不以连字符开头/结尾"
    if name in _RESERVED_NAMES:
        return f"pack 名 {name!r} 已被占用"
    return None


def _slug(name: str) -> str:
    """pack 名转 slug：连字符 → 下划线（my-pack → my_pack）。"""
    return name.replace("-", "_")


def _safe_class_name(slug: str) -> str:
    """slug 转 PascalCase（my_pack → MyPack）。"""
    return "".join(part.capitalize() for part in slug.split("_"))


# ─── 模板常量 ───────────────────────────────────────────────────

_PACK_YAML_TEMPLATE = """\
# {name} pack（loom pack new 生成）
#
# 引擎不 import 本包；本包经 loom_core.pack_manifest 被发现与装配：
#   - agents/     由 AgentLoader 多目录发现
#   - pipelines/  由 installed_pipelines / 流水线解析 union 进候选
#   - quality/    由 quality_gate.load_profile 按名解析
#   - prompts/    由 writer 按 profile 名解析
name: {name}
version: "0.1.0"
description: {name} 领域件包（loom pack new 生成）
agents:
  - {slug}_agent
pipelines:
  - {name}
quality_profiles:
  - default
"""

_AGENTS_INIT = '"""{slug} pack agents."""\n'

_AGENT_TEMPLATE = '''\
"""
{slug}_agent -- {name} pack 的示例 Agent
==========================================
离线确定性模板（无网络、无 Key、无第三方依赖）。
自定义时替换 handle() 的逻辑；AGENT_NAME 必须与 pack.yaml 声明一致。
"""
from loom_core.base_agent import AgentStatus, BaseAgent, Message

SANDBOX_TRUSTED = True
AGENT_NAME = "{slug}_agent"
AGENT_VERSION = "0.1.0"
AGENT_DESC = "{name} pack: 示例 agent（离线确定性）"
AGENT_AUTHOR = "loom pack new"
AGENT_PRIORITY = 50
INPUT_TOPICS = ["{slug}_agent.input"]
OUTPUT_TOPICS = ["{slug}_agent.done"]


class {class_name}Agent(BaseAgent):
    """示例 Agent：将输入内容原样回传（占位逻辑，替换为你的业务）。"""

    def handle(self, msg: Message) -> dict | None:
        content = msg.payload.get("content") or ""
        self.report(AgentStatus.RUNNING, "processing")
        self.publish("{slug}_agent.done", {
            "task_id": msg.payload.get("task_id", ""),
            "content": content,
            "status": "ok",
        })
        return {"status": "ok", "content": content}
'''

_PIPELINE_TEMPLATE = '''\
# {name} pack 流水线（loom pack new 生成）
_name: {name}
_version: "0.1"
description: {name} pack 示例流水线

defaults:
  parallelism:
    mode: single
    max_workers: 1

agents:
  - name: {slug}_agent
    version: "0.1.0"
    timeout: 60
    dependencies: []
    pool_size: 1
    config: {}

topology:
  type: dag
  levels:
    - [{slug}_agent]
  edges: []

pipeline:
  timeout: 120
  fail_fast: false
'''

_PROMPTS_DEFAULT = '''\
# {name} pack prompt 模板
system_prompt: |
  你是一个专业的内容生成助手。请根据要求生成高质量内容。
  直接输出 markdown，无需额外说明。

sections:
  - name: "正文"
    prompt: |
      # {{title}}
      请根据以下要求生成内容。
'''

_QUALITY_DEFAULT = '''\
# {name} pack 质量档案
name: default
description: "{name} pack 默认质量标准"
threshold: 70
max_regenerations: 3

weights:
  completeness: 0.30
  structure: 0.20
  readability: 0.20
  depth: 0.30

style_rules: []
'''


def scaffold_pack(name: str, root: Path) -> list[Path]:
    """在 ``root/packs/<slug>/`` 下生成 pack 骨架。

    目录名用 slug（连字符 → 下划线），pack.yaml 的 name 字段用原始名。
    返回创建的文件路径列表。已存在的目录不覆盖（幂等：已存在的目录会报错）。

    Raises:
        ValueError: pack 名非法
        FileExistsError: pack 目录已存在
    """
    err = validate_pack_name(name)
    if err:
        raise ValueError(err)

    slug = _slug(name)
    class_name = _safe_class_name(slug)
    packs_dir = Path(root) / "packs" / slug
    if packs_dir.exists():
        raise FileExistsError(f"pack 目录已存在: {packs_dir}")

    created: list[Path] = []

    # 目录结构
    for sub in ("", "agents", "pipelines", "prompts", "quality"):
        (packs_dir / sub if sub else packs_dir).mkdir(parents=True, exist_ok=True)

    templates = [
        ("pack.yaml", _PACK_YAML_TEMPLATE),
        ("agents/__init__.py", _AGENTS_INIT),
        (f"agents/{slug}_agent.py", _AGENT_TEMPLATE),
        (f"pipelines/{name}.yaml", _PIPELINE_TEMPLATE),
        ("prompts/default.yaml", _PROMPTS_DEFAULT),
        ("quality/default.yaml", _QUALITY_DEFAULT),
    ]

    for rel, template in templates:
        path = packs_dir / rel
        text = (template
                .replace("{name}", name)
                .replace("{slug}", slug)
                .replace("{class_name}", class_name))
        path.write_text(text, encoding="utf-8")
        created.append(path)

    return created
