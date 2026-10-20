"""
OpenTelemetry 追踪集成 —— 跨 Agent 调用链可视化。

设计原则：
- 可选依赖：opentelemetry-api/sdk 未安装时降级为 no-op，流水线零影响
- 通过 Message.payload["_trace_context"] 传播 W3C traceparent
- 在 BaseAgent.handle() / MessageBus.send() / DAGExecutor 创建 span

用法：
    pip install opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp-proto-http
    OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 python run.py input.md
"""
from __future__ import annotations

import contextlib
import logging
from typing import Any

# 版本号在 `loom_core/__init__` 的子模块导入之前就已绑定（见该文件注释）；
# 放在 try 之外：取不到版本要响亮地坏，而不是被 ImportError 吞成 no-op。
from . import __version__ as _loom_version

logger = logging.getLogger(__name__)

# ── 可选依赖：未安装时降级为 no-op ──────────────────────────────
_TRACER = None
_TRACE_CONTEXT_KEY = "_trace_context"

try:
    from opentelemetry import trace
    from opentelemetry.propagate import extract, inject
    from opentelemetry.trace import StatusCode

    _TRACER = trace.get_tracer("loom", _loom_version)
    logger.info("[Telemetry] OpenTelemetry 已启用")
except ImportError:
    logger.debug("[Telemetry] opentelemetry 未安装，追踪降级为 no-op")


def is_telemetry_enabled() -> bool:
    return _TRACER is not None


@contextlib.contextmanager
def start_span(name: str, attributes: dict | None = None, kind: str = "INTERNAL"):
    """创建 span（no-op 上下文当 OTel 未安装）。"""
    if _TRACER is None:
        yield None
        return
    from opentelemetry.trace import SpanKind

    kind_map = {"INTERNAL": SpanKind.INTERNAL, "SERVER": SpanKind.SERVER, "CLIENT": SpanKind.CLIENT}
    with _TRACER.start_as_current_span(name, kind=kind_map.get(kind, SpanKind.INTERNAL)) as span:
        if attributes:
            for k, v in attributes.items():
                span.set_attribute(k, v)
        yield span


def inject_trace_context(payload: dict) -> dict:
    """向 payload 注入 W3C traceparent（OTel 安装时）。"""
    if _TRACER is None:
        return payload
    try:
        inject(payload)
    except Exception as e:
        logger.debug(f"[Telemetry] inject 失败: {e}")
    return payload


def extract_trace_context(payload: dict) -> Any:
    """从 payload 提取 trace context（OTel 安装时）。"""
    if _TRACER is None:
        return None
    try:
        return extract(payload or {})
    except Exception as e:
        logger.debug(f"[Telemetry] extract 失败: {e}")
        return None


def set_span_error(span, error: Exception) -> None:
    """标记 span 为错误。"""
    if span is None:
        return
    try:
        span.set_status(StatusCode.ERROR, str(error))
        span.record_exception(error)
    except Exception:
        pass
