"""
BaseAgent v3.1 - 增强型 Agent 基类
=================================
核心特性：
  - 结构化日志记录
  - 性能指标自动收集
  - 健康检查接口
  - 配置热重载
  - 优雅关闭
  - 统一消息类型（使用 message_bus_v3.Message）
"""
import json
import logging
import os
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from .cache_manager import CacheManager
from .message_bus_v3 import Message
from .registry import AgentMeta, AgentStatus


class AgentLogger:
    """Agent 专用日志记录器"""

    def __init__(self, agent_name: str, log_dir: str | None = None, quiet: bool = False):
        self.agent_name = agent_name
        self.log_dir = Path(log_dir) if log_dir else Path("logs")
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self._logger = logging.getLogger(f"agent.{agent_name}")
        self._logger.setLevel(logging.DEBUG)

        if not self._logger.handlers:
            # 文件 handler
            log_file = self.log_dir / f"{agent_name}_{time.strftime('%Y%m%d')}.log"
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(logging.DEBUG)

            if not quiet:
                # 控制台 handler (仅当非 quiet 模式)
                console_handler = logging.StreamHandler()
                console_handler.setLevel(logging.INFO)

                # 格式
                formatter = logging.Formatter(
                    '[%(asctime)s] [%(name)s] %(levelname)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                )
                file_handler.setFormatter(formatter)
                console_handler.setFormatter(formatter)

                self._logger.addHandler(file_handler)
                self._logger.addHandler(console_handler)
            else:
                # 仅文件 handler
                formatter = logging.Formatter(
                    '[%(asctime)s] [%(name)s] %(levelname)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                )
                file_handler.setFormatter(formatter)
                self._logger.addHandler(file_handler)

    def debug(self, msg: str):
        self._logger.debug(msg)

    def info(self, msg: str):
        self._logger.info(msg)

    def warning(self, msg: str):
        self._logger.warning(msg)

    def error(self, msg: str):
        self._logger.error(msg)

    def exception(self, msg: str):
        self._logger.exception(msg)


class BaseAgent(ABC):
    """增强型 Agent 基类"""

    # 类属性（子类可覆盖）
    AGENT_NAME = "base"
    AGENT_VERSION = "1.0"
    AGENT_DESC = ""
    AGENT_AUTHOR = ""
    AGENT_PRIORITY = 50
    INPUT_TOPICS = []  # type: ignore[var-annotated]
    OUTPUT_TOPICS = []  # type: ignore[var-annotated]
    DEPENDENCIES = []  # type: ignore[var-annotated]
    CACHE_TTL = 3600
    RESPAWN = False
    RESPAWN_MAX = 3
    HEALTH_CHECK_INTERVAL = 30

    def __init__(self, name: str, meta: AgentMeta, config: dict,
                 message_bus=None, registry=None):
        self.name = name
        self.meta = meta
        self.config = config
        self.bus = message_bus
        self.reg = registry
        self.status = AgentStatus.LOADED

        # 统一缓存管理器（文件后端，跨进程共享）
        self._cache_dir = Path(config.get("cache_dir", "cache")) / name
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._cache = CacheManager(
            name=name, backend="file",
            cache_dir=str(self._cache_dir.parent),
            ttl=self.meta.cache_ttl or 0,
        )

        # 日志
        self._logger = AgentLogger(name, config.get("log_dir", "logs"), quiet=config.get("quiet", False))

        # 性能统计
        self._stats_lock = threading.Lock()
        self._processing_times: list[float] = []
        self._error_count = 0
        self._success_count = 0
        self._last_health_check = time.time()

        # 配置热重载
        self._config_file = config.get("config_file")
        self._config_mtime = 0

        # 自动订阅
        if self.bus:
            for topic in self.meta.input_topics or self.INPUT_TOPICS:
                self.bus.subscribe(topic, self._wrapped_handle)

        self._logger.info(f"Agent {name} v{meta.version} 初始化完成")

    # ─── 生命周期 ───────────────────────────────────

    @abstractmethod
    def handle(self, msg: Message) -> dict | None:
        """处理消息（子类必须实现）"""
        pass

    def _wrapped_handle(self, msg: Message) -> dict | None:
        """包装处理函数，添加性能统计、错误处理和 OTel 追踪。"""
        from .telemetry import set_span_error, start_span

        with start_span(
            f"agent.{self.name}.handle",
            attributes={"agent.name": self.name, "message.topic": msg.topic,
                        "trace.id": msg.trace_id},
        ) as span:
            start_time = time.time()
            self.status = AgentStatus.RUNNING

            try:
                # 检查配置热重载
                self._check_config_reload()

                # 执行实际处理
                result = self.handle(msg)

                # 记录成功
                with self._stats_lock:
                    self._success_count += 1
                processing_time = (time.time() - start_time) * 1000
                self._record_processing_time(processing_time)
                if span is not None:
                    span.set_attribute("duration_ms", processing_time)
                    if isinstance(result, dict):
                        span.set_attribute("result.status", result.get("status", "unknown"))

                # 上报状态
                self.report(AgentStatus.LOADED, f"处理完成，耗时 {processing_time:.1f}ms")

                # 通知 registry
                if self.reg:
                    self.reg.record_processing_time(self.name, processing_time)

                return result

            except Exception as e:
                set_span_error(span, e)
                with self._stats_lock:
                    self._error_count += 1
                self._logger.exception(f"处理消息时出错: {e}")
                self.report(AgentStatus.ERROR, str(e))
                raise

    def on_start(self):
        """Agent 启动时调用（可覆盖）"""
        self._logger.info("Agent 启动")

    def on_stop(self):
        """Agent 停止时调用（可覆盖）"""
        self._logger.info("Agent 停止")
        self.status = AgentStatus.STOPPED

    def on_pause(self):
        """流水线暂停时调用（可覆盖）。清理临时资源、保存中间状态。"""
        self._logger.info("Agent 暂停")

    def on_resume(self):
        """流水线恢复时调用（可覆盖）。重新初始化资源、恢复中间状态。"""
        self._logger.info("Agent 恢复")

    def on_config_update(self, changed_keys: list[str]):
        """配置热更新时调用（可覆盖）。changed_keys 为变更的配置键列表。"""
        self._logger.info(f"配置已更新: {changed_keys}")

    def on_snapshot(self) -> dict:
        """创建检查点时调用（可覆盖）。返回 agent 状态快照，用于断点续传。
        返回的 dict 会被序列化保存到 checkpoint 中。"""
        self._logger.debug("Agent 快照")
        with self._stats_lock:
            success_count = self._success_count
            error_count = self._error_count
        return {
            "name": self.name,
            "status": self.status.value if hasattr(self.status, 'value') else str(self.status),
            "success_count": success_count,
            "error_count": error_count,
        }

    def on_restore(self, state: dict):
        """从检查点恢复时调用（可覆盖）。恢复 agent 到快照时的状态。"""
        self._logger.info("Agent 状态恢复")
        with self._stats_lock:
            self._success_count = state.get("success_count", 0)
            self._error_count = state.get("error_count", 0)

    # ─── 临时文件清理契约 ─────────────────────────────
    # orchestrator 在任务结束的 finally（_cleanup_task_temp）和 shutdown
    # （_cleanup_all_stale_temp）中动态遍历所有已注册 agent 调用以下方法，
    # 需要清理临时文件的 agent 覆写并返回清理条目数。

    def cleanup_task_temp(self, task_id: str) -> int:
        """清理指定任务产生的临时文件（可覆盖）。返回清理的条目数量。"""
        return 0

    def cleanup_stale_temp(self, max_age_hours: int = 24) -> int:
        """清理超过 max_age_hours 的过期临时文件（可覆盖）。返回清理的条目数量。"""
        return 0

    def is_healthy(self) -> bool:
        """健康检查（可覆盖）"""
        # 默认健康检查：错误率不超过 50%
        with self._stats_lock:
            total = self._success_count + self._error_count
            error_count = self._error_count
        if total > 10:
            return error_count / total < 0.5
        return True

    # ─── 配置热重载 ───────────────────────────────────

    def _check_config_reload(self):
        """检查配置文件是否更新"""
        if not self._config_file or not os.path.exists(self._config_file):
            return

        mtime = os.path.getmtime(self._config_file)
        if mtime > self._config_mtime:
            try:
                with open(self._config_file, encoding="utf-8") as f:
                    new_config = json.load(f)
                self.config.update(new_config)
                self._config_mtime = mtime
                self._logger.info("配置已热重载")
            except Exception as e:
                self._logger.error(f"配置热重载失败: {e}")

    # ─── 消息发送 ───────────────────────────────────

    def send_to(self, to_agent: str, topic: str, payload: dict,
                timeout: float = 60) -> dict | None:
        """发送消息到指定 Agent"""
        if not self.bus:
            return None
        return self.bus.request(topic, self.name, to_agent, payload, timeout)  # type: ignore[no-any-return]

    def publish(self, topic: str, payload: dict):
        """发布广播消息"""
        if self.bus:
            self.bus.publish(topic, self.name, payload)

    def emit_event(self, event_type: str, payload: dict):
        """发送事件通知（广播到 agent.event topic）"""
        if self.bus:
            event_payload = {"event": event_type, "from": self.name, **payload}
            self.bus.publish("agent.event", self.name, event_payload)

    def reply(self, original_msg: Message, payload: dict):
        """回复消息"""
        if self.bus:
            self.bus.reply(original_msg, self.name, payload)

    # ─── 缓存 ───────────────────────────────────

    def cache_get(self, key: str) -> Any | None:
        """从缓存读取"""
        return self._cache.get(key)

    def cache_set(self, key: str, data: Any):
        """写入缓存"""
        self._cache.set(key, data)

    def cache_clear(self):
        """清空缓存"""
        self._cache.clear()

    # ─── 状态 ───────────────────────────────────

    def report(self, status: AgentStatus, info: str = ""):
        """上报状态"""
        self.status = status
        if self.bus:
            self.bus.publish("agent.status", self.name, {
                "agent": self.name,
                "status": status.value,
                "info": info,
                "ts": time.time(),
                "stats": self.get_stats(),
            })

    def _record_processing_time(self, ms: float):
        """记录处理时间"""
        with self._stats_lock:
            self._processing_times.append(ms)
            if len(self._processing_times) > 100:
                self._processing_times = self._processing_times[-100:]

    def get_stats(self) -> dict:
        """获取统计信息"""
        with self._stats_lock:
            success_count = self._success_count
            error_count = self._error_count
            processing_times = list(self._processing_times)
        avg_time = sum(processing_times) / len(processing_times) if processing_times else 0
        return {
            "success_count": success_count,
            "error_count": error_count,
            "avg_processing_time_ms": round(avg_time, 2),
            "status": self.status.value,
        }

    # ─── 日志 ───────────────────────────────────

    def log_debug(self, msg: str):
        self._logger.debug(msg)

    def log_info(self, msg: str):
        self._logger.info(msg)

    def log_warning(self, msg: str):
        self._logger.warning(msg)

    def log_error(self, msg: str):
        self._logger.error(msg)

    def log_exception(self, msg: str):
        self._logger.exception(msg)

    def __repr__(self):
        with self._stats_lock:
            success = self._success_count
            errors = self._error_count
        return f"<Agent:{self.name} [{self.status.value}] success={success} errors={errors}>"
