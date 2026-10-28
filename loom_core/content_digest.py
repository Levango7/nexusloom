"""产物内容的稳定摘要（A2 追溯面的内容级补充）。

账本里已经有整篇交付物的 `output_sha256`（v3.15.0 起），但"这一跑各个节点分别
产出了什么、是不是与当时一致"仍然只能看到键名。本模块把**每个产物键的内容**
摘要算成一份可核对的事实：

- 字符串按 UTF-8 **原文**摘要——用户拿到的就是这段文本，摘要要能对着文件核；
- 非字符串走 `sort_keys` 的规范 JSON：不排序的话同一个 dict 换个键序就换一个
  sha，"追溯"当场失效；
- 编不动的对象退到 `repr`，**绝不在这里抛**——调用点在交付收尾（`_deliver`
  只把失败记 warning），为一条摘要丢掉整行账本代价不对称。

摘要只在**写入时**算一次：产物值活到进程结束，事后没有第二处能算出同一个值。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any


def digest_of(value: Any) -> dict[str, Any]:
    """一段产物内容的摘要：sha256 + 参与摘要的字节数 + 编码形态。"""
    if isinstance(value, str):
        raw = value.encode("utf-8")
        kind = "text"
    else:
        try:
            text = json.dumps(value, ensure_ascii=False, sort_keys=True,
                              default=str)
        except (TypeError, ValueError):
            text = repr(value)
        raw = text.encode("utf-8")
        kind = "json"
    return {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw),
            "kind": kind}


def artifact_digests(result: Any) -> dict[str, dict[str, Any]]:
    """节点结果里每个产物键的摘要，按键名排序。

    只对 dict 结果生效：非 dict（None/字符串）没有"逐键产物"可言，给空表——
    与 `artifact_keys` 那里"没产出的节点给空表"的口径一致。
    """
    if not isinstance(result, dict):
        return {}
    return {str(k): digest_of(result[k]) for k in sorted(result, key=str)}
