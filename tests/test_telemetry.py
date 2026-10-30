"""telemetry 的双路径判据：OTel 缺席时的 no-op 降级 + 启用路径的完整逻辑。

这个模块是可选依赖包装层——价值全在「两种环境下都必须正确」。启用路径用
**假 OTel 模块注入 sys.modules** 的方式驱动（不要求真包、不 mock 模块内部
函数，CI 无 OTel 也照跑）；no-op 路径直接置 ``_TRACER = None``。
reload 类判据用 try/finally 显式还原 ``_TRACER = None``，避免污染其他判据。
"""
import contextlib
import importlib
import sys
import types

from loom_core import telemetry

# ── 假 OTel：只实现 telemetry 用到的面 ──────────────────────────

class FakeSpan:
    def __init__(self):
        self.attributes = {}
        self.status = None
        self.exceptions = []

    def set_attribute(self, k, v):
        self.attributes[k] = v

    def set_status(self, status, description=""):
        self.status = (status, description)

    def record_exception(self, exc):
        self.exceptions.append(exc)


class FakeSpanKind:
    INTERNAL = "INTERNAL"
    SERVER = "SERVER"
    CLIENT = "CLIENT"


class FakeTracer:
    def __init__(self):
        self.spans = []
        self.last_kind = None

    @contextlib.contextmanager
    def start_as_current_span(self, name, kind=None):
        span = FakeSpan()
        self.last_kind = kind
        self.spans.append((name, span))
        yield span


def _install_fake_otel(monkeypatch, *, inject_raises=False, extract_raises=False):
    otel = types.ModuleType("opentelemetry")
    trace_mod = types.ModuleType("opentelemetry.trace")
    propagate_mod = types.ModuleType("opentelemetry.propagate")

    trace_mod.SpanKind = FakeSpanKind
    trace_mod.StatusCode = types.SimpleNamespace(ERROR="ERROR")
    tracer = FakeTracer()
    trace_mod.get_tracer = lambda *a, **k: tracer

    def inject(payload):
        if inject_raises:
            raise RuntimeError("boom")
        payload["traceparent"] = "00-abc-def-01"

    def extract(payload):
        if extract_raises:
            raise RuntimeError("boom")
        return {"extracted": True}

    propagate_mod.inject = inject
    propagate_mod.extract = extract
    otel.trace = trace_mod
    otel.propagate = propagate_mod

    monkeypatch.setitem(sys.modules, "opentelemetry", otel)
    monkeypatch.setitem(sys.modules, "opentelemetry.trace", trace_mod)
    monkeypatch.setitem(sys.modules, "opentelemetry.propagate", propagate_mod)
    return tracer


# ── no-op 路径（OTel 未安装时的降级面） ─────────────────────────

def test_noop_start_span_yields_none(monkeypatch):
    monkeypatch.setattr(telemetry, "_TRACER", None)
    with telemetry.start_span("agent.handle") as span:
        assert span is None


def test_noop_inject_returns_payload_unchanged(monkeypatch):
    monkeypatch.setattr(telemetry, "_TRACER", None)
    payload = {"k": "v"}
    assert telemetry.inject_trace_context(payload) is payload
    assert payload == {"k": "v"}


def test_noop_extract_returns_none(monkeypatch):
    monkeypatch.setattr(telemetry, "_TRACER", None)
    assert telemetry.extract_trace_context({"k": "v"}) is None
    assert telemetry.extract_trace_context(None) is None


def test_noop_set_span_error_ignores_none_span():
    telemetry.set_span_error(None, RuntimeError("x"))  # 不抛即对


# ── 导入分支：装没装 OTel 决定 _TRACER ──────────────────────────

def test_import_without_otel_degrades_to_noop(monkeypatch):
    monkeypatch.setitem(sys.modules, "opentelemetry", None)  # None ⇒ ImportError
    try:
        mod = importlib.reload(telemetry)
        assert mod.is_telemetry_enabled() is False
        assert mod._TRACER is None
    finally:
        monkeypatch.undo()
        importlib.reload(telemetry)
    telemetry._TRACER = None  # 显式还原 no-op，环境无关


def test_import_with_otel_enables_tracer(monkeypatch):
    tracer = _install_fake_otel(monkeypatch)
    try:
        mod = importlib.reload(telemetry)
        assert mod.is_telemetry_enabled() is True
        assert mod._TRACER is tracer
    finally:
        monkeypatch.undo()
        importlib.reload(telemetry)
    telemetry._TRACER = None


# ── 启用路径（假 tracer 驱动，不依赖真包） ───────────────────────

def test_start_span_sets_attributes_and_maps_kind(monkeypatch):
    tracer = _install_fake_otel(monkeypatch)
    monkeypatch.setattr(telemetry, "_TRACER", tracer)
    with telemetry.start_span("agent.handle", {"agent": "writer"},
                              kind="CLIENT") as span:
        assert span.attributes == {"agent": "writer"}
    assert tracer.last_kind is FakeSpanKind.CLIENT
    assert tracer.spans[0][0] == "agent.handle"


def test_start_span_unknown_kind_defaults_to_internal(monkeypatch):
    tracer = _install_fake_otel(monkeypatch)
    monkeypatch.setattr(telemetry, "_TRACER", tracer)
    with telemetry.start_span("bus.send", kind="NOT_A_KIND"):
        pass
    assert tracer.last_kind is FakeSpanKind.INTERNAL


def test_start_span_without_attributes_skips_set(monkeypatch):
    tracer = _install_fake_otel(monkeypatch)
    monkeypatch.setattr(telemetry, "_TRACER", tracer)
    with telemetry.start_span("dag.node") as span:
        assert span.attributes == {}


def test_inject_calls_through_and_swallows_errors(monkeypatch):
    # inject/extract 是模块级绑定的第三方接缝（导入时绑定），直接 patch 该绑定；
    # 我们要测的是包装层自己的 try/except/return 语义。
    calls = []

    def fake_inject(payload):
        calls.append(payload)
        payload["traceparent"] = "00-abc-def-01"

    monkeypatch.setattr(telemetry, "inject", fake_inject)
    monkeypatch.setattr(telemetry, "_TRACER", FakeTracer())
    payload = {"k": "v"}
    assert telemetry.inject_trace_context(payload) is payload
    assert payload["traceparent"] == "00-abc-def-01"

    def raising_inject(p):
        raise RuntimeError("boom")

    monkeypatch.setattr(telemetry, "inject", raising_inject)
    payload2 = {"k": "v"}
    assert telemetry.inject_trace_context(payload2) is payload2
    assert "traceparent" not in payload2  # 注入失败不污染 payload


def test_extract_returns_context_and_swallows_errors(monkeypatch):
    def fake_extract(payload):
        return {"extracted": True}

    monkeypatch.setattr(telemetry, "extract", fake_extract)
    monkeypatch.setattr(telemetry, "_TRACER", FakeTracer())
    assert telemetry.extract_trace_context({"traceparent": "x"}) == {"extracted": True}

    def raising_extract(p):
        raise RuntimeError("boom")

    monkeypatch.setattr(telemetry, "extract", raising_extract)
    assert telemetry.extract_trace_context({"traceparent": "x"}) is None
    assert telemetry.extract_trace_context(None) is None


def test_set_span_error_records_status_and_exception(monkeypatch):
    monkeypatch.setattr(
        telemetry, "StatusCode", types.SimpleNamespace(ERROR="ERROR"))
    span = FakeSpan()
    err = RuntimeError("node failed")
    telemetry.set_span_error(span, err)
    assert span.status == ("ERROR", "node failed")
    assert span.exceptions == [err]


def test_set_span_error_swallows_span_failures(monkeypatch):
    _install_fake_otel(monkeypatch)

    class BadSpan(FakeSpan):
        def set_status(self, status, description=""):
            raise RuntimeError("span already ended")

    telemetry.set_span_error(BadSpan(), RuntimeError("x"))  # 不抛即对


def test_is_telemetry_enabled_reflects_tracer(monkeypatch):
    monkeypatch.setattr(telemetry, "_TRACER", None)
    assert telemetry.is_telemetry_enabled() is False
    monkeypatch.setattr(telemetry, "_TRACER", FakeTracer())
    assert telemetry.is_telemetry_enabled() is True
    telemetry._TRACER = None


def test_trace_context_key_is_stable():
    """跨进程传播键不许改名——MessageBus 与 BaseAgent 两处都按它取。"""
    assert telemetry._TRACE_CONTEXT_KEY == "_trace_context"


def test_module_restored_to_noop_after_suite():
    """兜底：整文件跑完后模块必须回到 no-op 态，不给其他判据留假 tracer。"""
    assert telemetry._TRACER is None
