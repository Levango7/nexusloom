"""运行输入的类型化声明与校验（P1 #7）——三条提交口共用这一处实现。

先说清射程：这是**声明式子集**，不是完整 JSON Schema 校验器。支持的 keyword：

- 对象级：`type: "object"`、`properties`、`required`、`additionalProperties: false`
- 值级：`type`（`str`/`int`/`float`/`bool`/`list`/`dict`，或这几种的名字列表）、`enum`、
  `pattern`（仅 str）、`minimum`/`maximum`（仅数值）、`minLength`/`maxLength`（仅 str）、
  `items`（数组元素的子 schema）、`properties`（嵌套对象，递归）、`default`（缺键时填充）

`type` 用的是**引擎既有的类型名**（`config_schema.TYPES`），不是 JSON Schema 的
`string`/`integer`：同一个值在本仓只该有一种叫法，否则 `int` 与 `integer` 会在错误消息、
YAML、文档三处各自漂移。要真·完整 JSON Schema 语义得引 `jsonschema`——那是独立的
依赖决策，不在这里替你决定。

为什么"未知 keyword 判成 schema 写错"而不是忽略：把 `maxLength` 误写成 `max_length`
时，忽略等于那条约束**从没存在过**，而运行照常提交——静默失效比报错难发现得多（本仓
栽过同类形状）。同理 `required` 里指了一个没声明的字段也是写错，不是"宽松通过"。

为什么校验放在"载荷 → 文档"之前而不是之后：`input_docs.render_inputs_doc` 把 dict 拍成
`## 键` 分节的文本，而 `dag_executor._extract_queries` 读文档时把所有 `#` 行丢掉——结构在
渲染那一刻就没了。所以渲染之后再也问不出"哪个字段错了"，只能整篇重解析当第二套判据，
那一定会漂。恢复/worker 那两条路传的是**已渲染的 `input_file`**，它们不过这道门，也不该过。

入口分两种形状，各自只有一个闸门口子：带键→值对象的走 `prepare_inputs`（真校验），
只带自由文本的走 `gate_freeform`（流水线一旦声明了 inputs 就拒绝，防止留一条绕过门的路）。
"""
from __future__ import annotations

import re
from typing import Any

from .config_schema import resolve_types, type_names, type_ok
from .input_docs import render_inputs_doc

#: 值级 keyword：出现在任何 schema 节点上都合法
_VALUE_KEYWORDS = frozenset({
    "type", "enum", "pattern", "minimum", "maximum",
    "minLength", "maxLength", "default", "items", "properties",
    "required", "additionalProperties",
})

#: 顶层额外允许的 keyword（`$schema`/`title`/`description` 这类只做标注，不判语义）
_META_KEYWORDS = frozenset({"$schema", "title", "description"})


class SchemaDeclarationError(ValueError):
    """schema 自己写错了。

    这个必须在**解析流水线时**就抛，而不是等到有人提交——声明写错却照常放行，等于
    那条流水线从来没有 schema。
    """


class InputValidationError(ValueError):
    """提交载荷不符合 schema。`errors` 是结构化的，字段可定位。"""

    def __init__(self, errors: list[dict[str, str]]):
        self.errors = list(errors)
        detail = "\n".join(f"  - {e['field']}: {e['reason']}" for e in self.errors)
        super().__init__(f"输入校验未通过（{len(self.errors)} 项）:\n{detail}")

    def to_dict(self) -> dict[str, Any]:
        """HTTP/MCP 侧的统一形状：一句话 + 可定位的字段表。"""
        return {"error": str(self).splitlines()[0],
                "code": "invalid_inputs",
                "fields": [dict(e) for e in self.errors]}


def load_declaration(raw: Any, *, who: str = "pipeline.inputs") -> dict[str, Any]:
    """校验并返回一份运行输入声明（顶层必须是 object）。"""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise SchemaDeclarationError(
            f"{who} 必须是映射，实际 {type(raw).__name__}={raw!r}")
    _reject_unknown_keywords(raw, who, _VALUE_KEYWORDS | _META_KEYWORDS)
    top_type = raw.get("type", "object")
    if top_type != "object":
        raise SchemaDeclarationError(
            f"{who}.type 只能是 \"object\"（运行输入就是一个对象），实际 {top_type!r}")
    props = raw.get("properties")
    if props is not None:
        if not isinstance(props, dict):
            raise SchemaDeclarationError(
                f"{who}.properties 必须是映射，实际 {type(props).__name__}")
        for name, sub in props.items():
            _load_node(sub, f"{who}.properties.{name}")
    req = raw.get("required")
    if req is not None:
        _check_required(req, props or {}, who)
    _check_additional(raw.get("additionalProperties"), who)
    return dict(raw)


def _load_node(spec: Any, who: str) -> None:
    """递归校验一个值级 schema 节点（声明期，不碰实际值）。"""
    if not isinstance(spec, dict):
        raise SchemaDeclarationError(
            f"{who} 必须是映射（类型/约束的声明），实际 {type(spec).__name__}={spec!r}")
    _reject_unknown_keywords(spec, who, _VALUE_KEYWORDS | _META_KEYWORDS)
    if "type" in spec:
        try:
            resolve_types(spec["type"], where=who)
        except ValueError as e:
            raise SchemaDeclarationError(str(e)) from e
    if "enum" in spec:
        enum = spec["enum"]
        if not isinstance(enum, list) or not enum:
            raise SchemaDeclarationError(f"{who}.enum 必须是非空列表，实际 {enum!r}")
    if "pattern" in spec:
        try:
            re.compile(str(spec["pattern"]))
        except re.error as e:
            raise SchemaDeclarationError(f"{who}.pattern 不是合法正则: {e}") from e
    for key in ("minimum", "maximum"):
        if key in spec and (isinstance(spec[key], bool)
                            or not isinstance(spec[key], (int, float))):
            raise SchemaDeclarationError(f"{who}.{key} 必须是数值，实际 {spec[key]!r}")
    for key in ("minLength", "maxLength"):
        if key in spec and (isinstance(spec[key], bool)
                            or not isinstance(spec[key], int) or spec[key] < 0):
            raise SchemaDeclarationError(f"{who}.{key} 必须是非负整数，实际 {spec[key]!r}")
    if "items" in spec:
        _load_node(spec["items"], f"{who}.items")
    if "properties" in spec:
        props = spec["properties"]
        if not isinstance(props, dict):
            raise SchemaDeclarationError(f"{who}.properties 必须是映射")
        for name, sub in props.items():
            _load_node(sub, f"{who}.properties.{name}")
        if "required" in spec:
            _check_required(spec["required"], props, who)
    if "required" in spec and "properties" not in spec:
        raise SchemaDeclarationError(f"{who}.required 需要和 properties 同时出现")
    if "additionalProperties" in spec:
        _check_additional(spec["additionalProperties"], who)


def _check_required(req: Any, props: dict, who: str) -> None:
    if not isinstance(req, list) or any(not isinstance(x, str) for x in req):
        raise SchemaDeclarationError(f"{who}.required 必须是字符串列表，实际 {req!r}")
    dangling = sorted(k for k in req if k not in props)
    if dangling:
        raise SchemaDeclarationError(
            f"{who}.required 里这些名字没有对应的 properties 声明: {dangling}")


def _check_additional(value: Any, who: str) -> None:
    if value is not None and not isinstance(value, bool):
        raise SchemaDeclarationError(
            f"{who}.additionalProperties 本实现只支持 true/false，实际 {value!r}")


def _reject_unknown_keywords(node: dict, who: str, allowed: frozenset) -> None:
    unknown = sorted(set(node) - allowed)
    if unknown:
        raise SchemaDeclarationError(
            f"{who} 有本实现不支持的 keyword: {unknown}"
            f"（支持: {sorted(allowed - _META_KEYWORDS)}）")


def _brief(value: Any, limit: int = 80) -> str:
    """错误消息里带上实际值才有诊断价值，但一个几 MB 的列表不能整段打进响应。"""
    text = repr(value)
    return text if len(text) <= limit else text[:limit] + "…"


def validate_inputs(schema: dict[str, Any], inputs: Any) -> list[dict[str, str]]:
    """按声明校验载荷，返回结构化错误（空表 = 通过）。

    错误一定带 `field`：CLI 打给人看，HTTP/MCP 要能定位到具体字段，
    三者读同一份表，不各写一套话术。
    """
    if not schema:
        return []
    if not isinstance(inputs, dict):
        return [{"field": "", "reason":
                 f"输入必须是对象（键→值），实际 {type(inputs).__name__}"}]
    errors: list[dict[str, str]] = []
    props = schema.get("properties") or {}
    for name in schema.get("required") or []:
        if name not in inputs:
            errors.append({"field": name, "reason": "缺少必填输入"})
    if schema.get("additionalProperties") is False:
        for key in sorted(set(inputs) - set(props)):
            errors.append({"field": key, "reason": "未声明的输入键"})
    for key, spec in props.items():
        if key in inputs:
            errors.extend(_check_value(spec, inputs[key], key))
    return errors


def _check_value(spec: dict[str, Any], value: Any, path: str) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    if "type" in spec and not type_ok(value, spec["type"]):
        errors.append({"field": path, "reason":
                       f"期望 {type_names(spec['type'])}，"
                       f"实际 {type(value).__name__}={_brief(value)}"})
        # 类型都不对，再查 enum/长度只会报一串无关的话
        return errors
    if "enum" in spec and value not in spec["enum"]:
        errors.append({"field": path, "reason":
                       f"必须是其中之一: {spec['enum']}，实际 {_brief(value)}"})
    if isinstance(value, str):
        if "pattern" in spec and not re.search(str(spec["pattern"]), value):
            errors.append({"field": path, "reason":
                           f"不匹配 pattern {spec['pattern']!r}"})
        if "minLength" in spec and len(value) < int(spec["minLength"]):
            errors.append({"field": path, "reason":
                           f"长度 {len(value)} 小于 minLength {spec['minLength']}"})
        if "maxLength" in spec and len(value) > int(spec["maxLength"]):
            errors.append({"field": path, "reason":
                           f"长度 {len(value)} 大于 maxLength {spec['maxLength']}"})
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in spec and value < float(spec["minimum"]):
            errors.append({"field": path, "reason":
                           f"{value} 小于 minimum {spec['minimum']}"})
        if "maximum" in spec and value > float(spec["maximum"]):
            errors.append({"field": path, "reason":
                           f"{value} 大于 maximum {spec['maximum']}"})
    if isinstance(value, list) and "items" in spec:
        for i, item in enumerate(value):
            errors.extend(_check_value(spec["items"], item, f"{path}[{i}]"))
    if isinstance(value, dict):
        if "properties" in spec:
            inner = spec["properties"]
            for name in spec.get("required") or []:
                if name not in value:
                    errors.append({"field": f"{path}.{name}", "reason": "缺少必填输入"})
            if spec.get("additionalProperties") is False:
                for key in sorted(set(value) - set(inner)):
                    errors.append({"field": f"{path}.{key}", "reason": "未声明的输入键"})
            for name, sub in inner.items():
                if name in value:
                    errors.extend(_check_value(sub, value[name], f"{path}.{name}"))
        elif "minLength" in spec and len(value) < int(spec["minLength"]):
            # 本实现里 `minLength`/`maxLength` 只管字符串；dict 的键数约束不做，
            # 别让它悄悄当成"约束通过了"。
            errors.append({"field": path, "reason":
                           "dict 值不支持 minLength/maxLength（只约束字符串）"})
    return errors


def apply_defaults(schema: dict[str, Any], inputs: dict[str, Any]) -> dict[str, Any]:
    """填上声明里的 `default`（只填**没给**的键，不覆盖调用方显式传的值）。"""
    if not schema:
        return dict(inputs)
    out = dict(inputs)
    props = schema.get("properties") or {}
    for name, spec in props.items():
        if name not in out and "default" in spec:
            out[name] = spec["default"]
        elif name in out and isinstance(out[name], dict) and "properties" in spec:
            out[name] = apply_defaults(spec, out[name])
    return out


def declaration(plan: Any) -> dict[str, Any]:
    """取 plan 上的输入声明；**不是 dict 就当没声明**。

    为什么要有这条：`inputs_schema` 挂在 ExecutionPlan 上，但 plan 也会被替身
    （测试里的 MagicMock）或第三方 pack 自行构造的对象顶上来。对这类对象
    `getattr(plan, "inputs_schema")` 返回的是一个**真值但非 dict** 的东西，
    于是"没声明"会被读成"声明了但内容为空"——自由文本入口就会凭空拒绝，
    假阳性比没门禁更难查。
    """
    schema = getattr(plan, "inputs_schema", None)
    return schema if isinstance(schema, dict) else {}


def gate_freeform(plan: Any, *, surface: str) -> None:
    """文档形入口（CLI 的 `--input`、`POST /api/tasks` 的 `query`、MCP 的
    `generate_document`、webhook 的非 JSON 文本载荷）的唯一退路。

    为什么需要这一条：这些入口传的是**自由文本**，没有可供校验的键→值结构。如果让它们
    照常放行，"声明了类型化输入的流水线"仍然有一条不经过闸门的路——那这道门就是装饰，
    比没门更糟（看起来有）。所以：
      - 流水线**没声明** inputs → 原样通过（随包 20 条流水线行为不变）；
      - 声明了 → 明确拒绝，并指出该用哪个 typed 入口，而不是丢一句"缺少必填 topic"。

    抛的是同一个 `InputValidationError`，因此四个入口的错误形状仍然只有一套。
    """
    schema = declaration(plan)
    if not schema:
        return
    props = schema.get("properties")
    names = sorted(props) if isinstance(props, dict) else []
    raise InputValidationError([{
        "field": "inputs",
        "reason": (f"{surface} 传的是自由文本，无法按声明校验；"
                   f"流水线 {getattr(plan, 'pipeline_name', '?')} 声明了类型化输入 "
                   f"{names}，请改用带 inputs 对象的入口"
                   "（HTTP: POST /api/workflows/{name}/runs，"
                   "CLI: --inputs-json / --set-input，MCP: run_workflow）")
    }])


def prepare_inputs(plan: Any, inputs: dict[str, Any], *, title: str = "") -> str:
    """三条提交口共用的那一步：校验 → 填默认值 → 渲染成输入文档文本。

    放在这里而不是各入口各写一遍，是因为"同一个载荷在 MCP 与 HTTP 得到同一份文档"
    本身就是既有承诺（见 `input_docs` 模块注释）——校验加进来之后这条更要成立。
    返回**文本**不是文件路径：临时文件归调用方管（MCP/webhook 各自有清理逻辑），
    把文件生命周期也吸进来会让恢复/worker 那条路没法复用同一份校验。
    """
    schema = declaration(plan)
    errors = validate_inputs(schema, inputs)
    if errors:
        raise InputValidationError(errors)
    # 没声明时载荷可以是任意形状（webhook 可能送来整篇文本或 JSON 数组）：
    # 照旧原样渲染，`default` 填充只对键→值对象有意义
    effective = apply_defaults(schema, inputs) if isinstance(inputs, dict) else inputs
    return render_inputs_doc(title or getattr(plan, "pipeline_name", "") or "run",
                             effective)
