"""类型化运行输入（P1 #7）：声明式子集 + 三条提交口共用的那一步校验。

判据分成四组，缺一组都不算这条线做完：

1. **声明期就拦**——schema 自己写错（拼错的 keyword、`required` 指向没声明的字段、坏正则、
   类型名不在词表里）必须在**解析流水线时**抛。留到提交期，等于那条流水线从来没有 schema。
2. **载荷校验**——每类约束都要红，且错误必须**可定位**（`tags[1]`、`spec.min` 这种点号/下标路径）。
   不可定位的错误在 HTTP 侧就是一句"你参数错了"，用户只能猜。
3. **默认值只填没给的**——覆盖调用方显式传的值，等于引擎替用户改了输入。
4. **边界**——没声明的流水线一律不受影响（随包 20 条的行为不变），以及 schema **不进锁指纹**
   这条刻意选择：它是选择不是遗漏，所以钉一条测试；将来谁要把它挪进哈希，必须先面对
   "20 条流水线全部重锁"。
"""
from __future__ import annotations

import pytest

from loom_core import input_schema as ins
from loom_core.scheduler import Scheduler

DEMO_YAML = """
name: typed_demo
pipeline:
  inputs:
    type: object
    required: [topic]
    additionalProperties: false
    properties:
      topic: {type: str, minLength: 2, maxLength: 40}
      depth: {type: int, minimum: 1, maximum: 5, default: 3}
      tags: {type: list, items: {type: str}}
      mode: {enum: [fast, full]}
      stage: {type: str, enum: [draft, final]}
      spec:
        type: dict
        required: [region]
        additionalProperties: false
        properties:
          region: {type: str}
          reps: {type: int, minimum: 0}
agents:
  - name: tldr
    version: "0.1"
    dependencies: []
    config: {max_sentences: 2}
topology:
  levels: [["tldr"]]
  edges: []
"""

BARE_YAML = DEMO_YAML.replace("""  inputs:
    type: object
    required: [topic]
    additionalProperties: false
    properties:
      topic: {type: str, minLength: 2, maxLength: 40}
      depth: {type: int, minimum: 1, maximum: 5, default: 3}
      tags: {type: list, items: {type: str}}
      mode: {enum: [fast, full]}
      stage: {type: str, enum: [draft, final]}
      spec:
        type: dict
        required: [region]
        additionalProperties: false
        properties:
          region: {type: str}
          reps: {type: int, minimum: 0}
""", "  fail_fast: false\n")


def _plan(tmp_path, yaml_text: str, name: str = "typed_demo"):
    d = tmp_path / f"pipe_{name}"
    d.mkdir(parents=True, exist_ok=True)
    f = d / f"{name}.yaml"
    f.write_text(yaml_text, encoding="utf-8")
    return Scheduler(pipeline_dir=str(d)).parse_file(str(f), verify_lock=False)


@pytest.fixture
def plan(tmp_path):
    return _plan(tmp_path, DEMO_YAML)


# ─── 1. 声明期拦截 ──────────────────────────────────

class TestDeclarationPhase:
    def test_plan_carries_the_declaration(self, plan):
        assert plan.inputs_schema, "解析没把 pipeline.inputs 带进 plan——提交口就无源可校验"
        props = plan.inputs_schema["properties"]
        assert sorted(props) == ["depth", "mode", "spec", "stage", "tags", "topic"]
        assert plan.inputs_schema["additionalProperties"] is False

    def test_missing_declaration_is_not_an_error(self, tmp_path):
        p = _plan(tmp_path, BARE_YAML, "bare")
        assert p.inputs_schema == {}
        # 没声明 ⇒ 什么载荷都照旧通过（随包 20 条流水线行为不变）
        assert ins.validate_inputs(p.inputs_schema, {"任意": "东西"}) == []

    @pytest.mark.parametrize("broken,frag", [
        ("maxLength: 40", "max_length: 40"),          # 拼错的 keyword：忽略=约束从没存在
        ("required: [topic]", "required: [topc]"),     # required 指向没声明的名字
        ("minimum: 1, maximum: 5", "minimum: many"),   # 边界不是数值
        ("type: str, minLength: 2", "type: string"),   # 类型名不在词表里
        ("additionalProperties: false", "additionalProperties: strict"),
    ])
    def test_bad_declaration_raises_at_parse_time(self, tmp_path, broken, frag):
        yaml_text = DEMO_YAML.replace(broken, frag)
        assert yaml_text != DEMO_YAML, "锚点没命中，这条判据在验空气"
        with pytest.raises(ins.SchemaDeclarationError):
            # 文件名只用序号：把片段文本当流水线名会写出带 `:` 的文件，Windows 上先炸在 I/O 上
            _plan(tmp_path, yaml_text, f"bad_{abs(hash(frag)) % 997}")

    def test_bad_declaration_names_the_pipeline_and_field(self, tmp_path):
        with pytest.raises(ins.SchemaDeclarationError) as e:
            _plan(tmp_path, DEMO_YAML.replace("maxLength: 40", "max_length: 40"), "typed_bad")
        text = str(e.value)
        assert "typed_bad.pipeline.inputs.properties.topic" in text, text

    def test_top_level_must_be_object(self, tmp_path):
        with pytest.raises(ins.SchemaDeclarationError):
            _plan(tmp_path, DEMO_YAML.replace("    type: object", "    type: str"), "typed_t")

    def test_bad_pattern_regex_raises(self, tmp_path):
        with pytest.raises(ins.SchemaDeclarationError):
            _plan(tmp_path,
                  DEMO_YAML.replace("required: [topic]", "required: [topic]")
                  .replace("type: str, minLength: 2", "type: str, pattern: \"([\""),
                  "typed_re")


# ─── 2. 载荷校验：可定位 ────────────────────────────

class TestValueValidation:
    def test_valid_payload_passes(self, plan):
        assert ins.validate_inputs(
            plan.inputs_schema,
            {"topic": "云成本治理", "depth": 2, "tags": ["a", "b"],
             "mode": "fast", "spec": {"region": "cn", "reps": 1}}) == []

    @pytest.mark.parametrize("payload,field,frag", [
        ({"depth": 3}, "topic", "缺少必填"),
        ({"topic": 7}, "topic", "期望 str"),
        ({"topic": "a"}, "topic", "小于 minLength"),
        ({"topic": "问" * 41}, "topic", "大于 maxLength"),
        ({"topic": "ok", "depth": 9}, "depth", "大于 maximum"),
        ({"topic": "ok", "depth": 0}, "depth", "小于 minimum"),
        ({"topic": "ok", "mode": "slow"}, "mode", "必须是其中之一"),
        ({"topic": "ok", "extra": 1}, "extra", "未声明的输入键"),
        ({"topic": "ok", "tags": ["a", 2]}, "tags[1]", "期望 str"),
        ({"topic": "ok", "spec": {"reps": 1}}, "spec.region", "缺少必填"),
        ({"topic": "ok", "spec": {"region": "cn", "nope": 1}}, "spec.nope", "未声明的输入键"),
        ({"topic": "ok", "spec": "不是对象"}, "spec", "期望 dict"),
        ("整篇不是对象", "", "输入必须是对象"),
    ])
    def test_each_constraint_reports_its_field(self, plan, payload, field, frag):
        errors = ins.validate_inputs(plan.inputs_schema, payload)
        assert errors, f"{payload} 本该被拦"
        hit = next((e for e in errors if e["field"] == field), None)
        assert hit is not None, f"没有一条错误落在 {field!r}：{errors}"
        assert frag in hit["reason"], hit

    def test_bool_is_not_an_int(self, plan):
        """仓里的既有陷阱：isinstance(True, int) 为真，声明 int 却收 bool 就是漏。"""
        errors = ins.validate_inputs(plan.inputs_schema, {"topic": "ok", "depth": True})
        assert [e["field"] for e in errors] == ["depth"], errors

    def test_wrong_type_short_circuits_that_field(self, plan):
        """类型都不对，别再为同一字段叠一串无关话术。"""
        errors = ins.validate_inputs(plan.inputs_schema, {"topic": 12345})
        assert len(errors) == 1, errors

    def test_wrong_type_does_not_stack_the_enum_error(self, plan):
        """短路的**射程**在这里：`stage` 同时声明了 type 与 enum。

        上一轮变异电池抓到的零射程判据——原短路用例只带 `type` 的字段，把"类型不符后
        继续查 enum"的变异换成 `pass` 也一样只报一条，等于没在验那件事。整数撞
        `enum: [draft, final]` 必然落不进枚举，所以不短路就会在同一字段上多冒一条。
        """
        errors = ins.validate_inputs(plan.inputs_schema, {"topic": "ok", "stage": 7})
        assert [e["field"] for e in errors] == ["stage"], errors
        assert "期望 str" in errors[0]["reason"], errors

    def test_multiple_failures_are_all_listed(self, plan):
        errors = ins.validate_inputs(plan.inputs_schema, {"depth": 9, "mode": "slow"})
        assert {e["field"] for e in errors} == {"topic", "depth", "mode"}, errors

    def test_error_payload_shape_for_http(self, plan):
        err = ins.InputValidationError(ins.validate_inputs(
            plan.inputs_schema, {"topic": "ok", "extra": 1}))
        body = err.to_dict()
        assert body["code"] == "invalid_inputs"
        assert body["fields"] == [{"field": "extra", "reason": "未声明的输入键"}], body
        assert "输入校验未通过" in body["error"]

    def test_huge_value_is_not_dumped_into_the_message(self, plan):
        err = ins.InputValidationError(ins.validate_inputs(
            plan.inputs_schema, {"topic": "x" * 5000}))
        assert "x" * 200 not in str(err), "错误消息把整段载荷复述出来了"


# ─── 3. 默认值 ──────────────────────────────────────

class TestDefaults:
    def test_default_fills_only_absent_keys(self, plan):
        assert ins.apply_defaults(plan.inputs_schema, {"topic": "a"})["depth"] == 3

    def test_explicit_value_is_never_overwritten(self, plan):
        """引擎替用户改输入=静默改变执行内容，这条必须钉死。"""
        out = ins.apply_defaults(plan.inputs_schema, {"topic": "a", "depth": 5})
        assert out["depth"] == 5, out

    def test_no_default_no_injection(self, plan):
        out = ins.apply_defaults(plan.inputs_schema, {"topic": "a", "tags": ["x"]})
        assert "mode" not in out and "spec" not in out


# ─── 4. 共用那一步 & 边界 ───────────────────────────

class TestPrepareAndBoundaries:
    def test_prepare_renders_the_same_doc_as_input_docs(self, plan):
        """"同一个载荷在两条入口得到同一份文档"是既有承诺，校验加进来后更要成立。"""
        from loom_core.input_docs import render_inputs_doc
        typed = ins.prepare_inputs(plan, {"topic": "云成本"})
        assert typed == render_inputs_doc("typed_demo", {"topic": "云成本", "depth": 3})

    def test_prepare_rejects_before_rendering(self, plan):
        with pytest.raises(ins.InputValidationError):
            ins.prepare_inputs(plan, {"nope": 1})

    def test_prepare_without_declaration_is_passthrough(self, tmp_path):
        p = _plan(tmp_path, BARE_YAML, "bare2")
        doc = ins.prepare_inputs(p, {"查询": "任意文本"})
        assert "## 查询" in doc and "任意文本" in doc

    def test_duck_typed_plan_is_treated_as_undeclared(self, plan):
        """替身/第三方 plan 顶上来时，`inputs_schema` 可能是"真值但不是 dict"。

        那必须读成**没声明**：读成"声明了但为空"会让自由文本入口凭空被拒，
        而门禁假阳性比没门禁更难查（本仓的形状）。
        """
        from types import SimpleNamespace
        from unittest.mock import MagicMock

        for bogus in (SimpleNamespace(pipeline_name="x", inputs_schema=MagicMock()),
                      SimpleNamespace(pipeline_name="x", inputs_schema="不是 dict"),
                      SimpleNamespace(pipeline_name="x")):
            assert ins.declaration(bogus) == {}, bogus
            ins.gate_freeform(bogus, surface="judge")        # 不抛 = 照旧放行
            assert ins.prepare_inputs(bogus, {"任意": "东西"})

    def test_declaration_does_not_change_lock_hashes(self, tmp_path, plan):
        """schema 不进锁指纹是**选择**不是遗漏：它一改，随包 20 条流水线全要重锁。

        谁将来要把它挪进哈希，这条判据会红——那时候要先面对重锁的代价，
        而不是在某个 PR 里顺手改掉。
        """
        bare = _plan(tmp_path, BARE_YAML, "bare3")
        sched = Scheduler(pipeline_dir=str(tmp_path))
        assert sched._topology_hash(plan) == sched._topology_hash(bare)
        node = plan.levels[0][0]
        bare_node = bare.levels[0][0]
        assert sched._node_config_hash(node.agent_config) == \
            sched._node_config_hash(bare_node.agent_config)
