"""入站 webhook：外部服务 POST → 提交流水线 run（鉴权 + 审计）。

设计要点（都是刻意选择）：
- **鉴权独立于 ADMIN_API_KEY**：`/api/webhooks/<name>` 不走管理端凭据——
  外部服务不该持有管理 key。每个 webhook 用单独密钥，经 `secret_env` 指向
  环境变量（密钥不落配置文件）；两种携带方式：
  1. `X-Webhook-Signature: sha256=<hex>`——HMAC-SHA256 over **原始请求体**
     （GitHub 风格，能防重放体篡改）；
  2. `X-Webhook-Token: <secret>`——常量时间比较的直传令牌（给不会算 HMAC 的服务）。
  **密钥未配置 = fail-closed（503）**，不是"没密钥就放行"。
- **审计留痕**：每次入站调用（无论成败）落一行 JSON 到
  `state_root()/audit/webhooks.jsonl`——时间、webhook 名、来源地址、方案、
  结果与 HTTP 状态、task_id / 拒绝原因。写盘失败不静默（响应带 `audit_error`）。
- **与手动 run 同一条提交路径**：payload 经 `render_inputs_doc` 变输入文档，
  `run_plan(wait=False)` 提交——任务出现在同一任务队列与观测面。
"""
from __future__ import annotations

import contextlib
import hashlib
import hmac
import logging
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from artesian.fast_json import loads as _fast_loads

from . import audit
from . import scheduler as _scheduler_mod
from .ids import new_task_id
from .input_schema import InputValidationError, prepare_inputs

_logger = logging.getLogger(__name__)

DEFAULT_CONFIG_NAME = "webhooks.yaml"
#: 覆盖配置文件路径（双读：LOOM_WEBHOOKS_FILE 优先，旧名回退可读，
#: 规则见 loom_core.envs；调用方用 webhooks_file_env() 取值）
CONFIG_PATH_ENV = "LOOM_WEBHOOKS_FILE"
LEGACY_CONFIG_PATH_ENV = "DOC_PIPELINE_WEBHOOKS_FILE"


def webhooks_file_env() -> str | None:
    """webhooks 配置路径的环境变量取值（LOOM_ 优先，旧名回退）。"""
    from .envs import read

    return read("WEBHOOKS_FILE")

SIGNATURE_HEADER = "X-Webhook-Signature"
TOKEN_HEADER = "X-Webhook-Token"


class WebhookConfigError(ValueError):
    """webhooks.yaml 配置非法（信息里带上出错的条目）。"""


@dataclass
class Webhook:
    name: str
    pipeline: str
    #: 密钥所在的环境变量名（值在**请求时**读取，配置里只存名字）
    secret_env: str
    output: str = ""
    enabled: bool = True


def default_config_path() -> Path:
    """默认配置路径：仓库根 webhooks.yaml。"""
    return Path(__file__).resolve().parent.parent / DEFAULT_CONFIG_NAME


def load_webhooks(path: str | Path,
                  available: list[str] | None = None) -> dict[str, Webhook]:
    """加载并校验 webhooks.yaml；`available` 缺省取 scheduler.installed_pipelines()。

    文件不存在返回 {}（该能力默认休眠，不是错误）；文件存在但写坏则抛
    WebhookConfigError（配置错误不该等到第一个请求进来才发现）。
    """
    p = Path(path)
    if not p.exists():
        return {}
    try:
        import yaml

        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception as e:
        raise WebhookConfigError(f"配置文件解析失败: {e}") from e
    if not isinstance(data, dict) or "webhooks" not in data:
        raise WebhookConfigError("缺少顶层 `webhooks:` 列表")
    items = data.get("webhooks")
    if not isinstance(items, list) or not items:
        raise WebhookConfigError("`webhooks:` 必须是非空列表")

    if available is None:
        available = _scheduler_mod.installed_pipelines()

    result: dict[str, Webhook] = {}
    for idx, item in enumerate(items, start=1):
        where = f"webhooks[{idx}]"
        if not isinstance(item, dict):
            raise WebhookConfigError(f"{where} 必须是映射（含 name/pipeline/secret_env）")
        raw_name = item.get("name")
        if not isinstance(raw_name, str) or not raw_name.strip():
            raise WebhookConfigError(f"{where} 缺少 name（须是非空字符串）")
        name = raw_name.strip()
        if name in result:
            raise WebhookConfigError(f"{where} name 重复: {name!r}")
        pipeline = item.get("pipeline")
        if not isinstance(pipeline, str) or not pipeline.strip():
            raise WebhookConfigError(f"{where}（{name}）缺少 pipeline")
        pipeline = pipeline.strip()
        if pipeline not in available:
            raise WebhookConfigError(
                f"{where}（{name}）pipeline {pipeline!r} 不存在"
                f"（可用: {', '.join(available) or '无'}）")
        secret_env = item.get("secret_env")
        if not isinstance(secret_env, str) or not secret_env.strip():
            raise WebhookConfigError(
                f"{where}（{name}）缺少 secret_env（密钥经环境变量注入，配置里只写变量名）")
        output = item.get("output", "") or ""
        if not isinstance(output, str):
            raise WebhookConfigError(f"{where}（{name}）output 必须是路径字符串")
        enabled = item.get("enabled", True)
        if not isinstance(enabled, bool):
            raise WebhookConfigError(f"{where}（{name}）enabled 必须是布尔值")
        result[name] = Webhook(name=name, pipeline=pipeline,
                               secret_env=secret_env.strip(),
                               output=output, enabled=enabled)
    return result


def _audit_path() -> Path:
    """本通道的审计文件路径（实现收口在 loom_core.audit）。"""
    return audit.audit_path("webhooks")


def write_audit(record: dict) -> str | None:
    """追加一行审计（JSONL，含时间戳）。失败返回错误信息——调用方须如实回报。"""
    return audit.write_audit("webhooks", record)


def _header(headers: Any, name: str) -> str:
    """大小写不敏感取头（email.message.Message 本身就折叠大小写；dict 不会）。"""
    value = headers.get(name)
    if value is None:
        value = headers.get(name.lower())
    return str(value or "")


def verify_credentials(secret: str, body: bytes, headers: Any) -> str:
    """校验入站凭据，返回采用的方案名（"signature"/"token"）；失败返回 ""。"""
    sig = _header(headers, SIGNATURE_HEADER).strip()
    if sig:
        if sig.lower().startswith("sha256="):
            sig = sig[len("sha256="):]
        expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
        return "signature" if hmac.compare_digest(sig.lower(), expected) else ""
    token = _header(headers, TOKEN_HEADER)
    if token:
        return "token" if hmac.compare_digest(token, secret) else ""
    return ""


def handle_inbound(name: str, registry: Mapping[str, Webhook], body: bytes, *,
                   headers: Any, remote: str, orch: Any
                   ) -> tuple[int, dict]:
    """处理一次入站调用，返回 (HTTP 状态, 响应体)。每次调用无论成败都落审计。"""
    def _finish(status: int, payload: dict, extra: dict) -> tuple[int, dict]:
        record = {"event": "webhook.inbound", "webhook": name, "remote": remote,
                  "http_status": status, **extra}
        err = write_audit(record)
        if err:
            payload["audit_error"] = err
            _logger.error("[webhooks] 审计写盘失败: %s", err)
        else:
            payload["audit"] = "ok"
        return status, payload

    webhook = (registry or {}).get(name)
    if webhook is None:
        return _finish(404, {"error": f"unknown webhook: {name!r}"},
                       {"outcome": "rejected", "reason": "unknown"})
    if not webhook.enabled:
        return _finish(403, {"error": "webhook disabled"},
                       {"outcome": "rejected", "reason": "disabled"})
    if not orch:
        return _finish(500, {"error": "orchestrator not set"},
                       {"outcome": "error", "reason": "no_orch"})
    secret = os.environ.get(webhook.secret_env, "")
    if not secret:
        return _finish(
            503,
            {"error": f"webhook secret not configured (env {webhook.secret_env})"},
            {"outcome": "rejected", "reason": "secret_missing"})
    scheme = verify_credentials(secret, body, headers)
    if not scheme:
        return _finish(401, {"error": "unauthorized"},
                       {"outcome": "rejected", "reason": "bad_credentials"})

    # payload → 输入文档：JSON 对象按「## 键」分节，其余（非 JSON / 数组等）原样。
    # **先 parse 再过闸门**：校验要读 plan.inputs_schema，而渲染成文档之后键→值结构
    # 就没了（见 input_schema 模块注释）。没声明 inputs 的流水线走的是与昨天同一份
    # render_inputs_doc 输出——prepare_inputs 在无 schema 时只做透传。
    inputs: Any = body.decode("utf-8", errors="replace")
    with contextlib.suppress(Exception):
        parsed = _fast_loads(body) if body else None
        if isinstance(parsed, (dict, list)):
            inputs = parsed

    task_id = new_task_id()
    try:
        plan = _scheduler_mod.Scheduler().parse(webhook.pipeline)
    except Exception as e:
        return _finish(500, {"error": f"pipeline parse failed: {e}"},
                       {"outcome": "error", "reason": "parse_failed", "scheme": scheme})
    try:
        input_text = prepare_inputs(plan, inputs, title=webhook.name)
    except InputValidationError as e:
        return _finish(400, e.to_dict(),
                       {"outcome": "rejected", "reason": "invalid_inputs",
                        "scheme": scheme})

    input_file = Path(tempfile.gettempdir()) / f"webhook_{webhook.name}_{task_id}.md"
    input_file.write_text(input_text, encoding="utf-8")
    try:
        if webhook.output:
            plan.raw.setdefault("pipeline", {})["output"] = webhook.output
        task = orch.run_plan(plan, input_file=str(input_file),
                             task_id=task_id, wait=False,
                             owner=f"webhook:{webhook.name}")
    except Exception as e:
        return _finish(500, {"error": f"pipeline submit failed: {e}"},
                       {"outcome": "error", "reason": "submit_failed", "scheme": scheme})
    return _finish(
        202,
        {"task_id": str(task.id), "pipeline": webhook.pipeline, "status": "accepted"},
        {"outcome": "accepted", "scheme": scheme, "task_id": str(task.id),
         "bytes": len(body)})
