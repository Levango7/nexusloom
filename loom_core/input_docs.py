"""输入文档渲染：任意载荷 → 引擎的"运行输入文档"。

引擎以输入文档作为运行接口（首层 Agent 从它提取查询词），MCP 的
`run_workflow(inputs=...)` 与入站 webhook 的 JSON 载荷都经这里渲染，
保证"同一个载荷在两条入口得到同一份文档"。

渲染规则：
- 对象 → 按 `## 键` 分节（标量直书、列表成 `- ` 行、嵌套对象走 JSON）；
- 字符串 → 原样写入（前面加标题行）；
- 空输入 → 只给标题行（首层查询词提取会跳过 `#` 行，标题不会被误当主题）。
"""
from __future__ import annotations

import re
from typing import Any

from artesian.fast_json import dumps as _fast_dumps

_LOOM_TITLE_PREFIX = re.compile(r"^\[loom:[0-9a-f-]{36}\]\s+")


def display_title(title: str) -> str:
    """返回给用户内容使用的标题：剥离 `[loom:<wid>]` 归属传输前缀。

    Corps 用该前缀做多租户归属校验，任务元数据/响应里的原 title 不动；
    但它不能进入输入文档，否则 writer 会把内部租户标记写成成品 H1。
    """
    stripped = _LOOM_TITLE_PREFIX.sub("", title, count=1).strip()
    return stripped or title


def render_inputs_doc(title: str, inputs: Any) -> str:
    """把载荷渲染成输入文档（title 为 `# 标题` 行，用于人工辨识来源）。"""
    if inputs is None or (isinstance(inputs, (dict, list, str)) and not inputs):
        return f"# {title}\n"
    if isinstance(inputs, str):
        return f"# {title}\n\n{inputs}\n"
    if isinstance(inputs, dict):
        lines = [f"# {title}", ""]
        for key, value in inputs.items():
            lines.append(f"## {key}")
            lines.append("")
            if isinstance(value, (list, tuple)):
                lines.extend(f"- {item}" for item in value)
            elif isinstance(value, dict):
                lines.append(_fast_dumps(value, ensure_ascii=False))
            else:
                lines.append(str(value))
            lines.append("")
        return "\n".join(lines)
    return f"# {title}\n\n{_fast_dumps(inputs, ensure_ascii=False)}\n"
