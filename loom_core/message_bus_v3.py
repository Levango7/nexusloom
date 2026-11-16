"""
MessageBus v3 - 持久化消息总线
================================
传输层：发布/订阅、请求/响应、异步广播
存储层：委托给 message_store.PersistentStore
"""
from __future__ import annotations

import contextlib
import logging
import threading
import time
import uuid
from collections.abc import Callable
from queue import Empty, Queue
from typing import Any

from .message_store import (
    DEFAULT_DB_PATH as DEFAULT_DB_PATH,  # re-export for backward compat
)
from .message_store import (
    Message,
    MessageMetrics,
    MessageType,
    PersistentStore,
)

_logger = logging.getLogger(__name__)


# ─── MessageBus ──────────────────────────────

class MessageBus:
    """持久化消息总线 v3"""

    def __init__(self, db_path: str = "", enable_persistence: bool = True,
                 max_queue_depth: int = 500, backpressure_watermark: int = 400):
        """MessageBus v3

        Args:
            db_path: SQLite 数据库路径（空=默认路径）
            enable_persistence: 是否启用持久化
            max_queue_depth: 队列最大深度
            backpressure_watermark: 触发背压的水位线
        """
        self._async_queue = Queue()  # type: ignore[var-annotated]
        self._store = PersistentStore(db_path) if enable_persistence else None
        self._subscribers: dict[str, list[tuple[Callable, int]]] = {}
        self._callbacks: dict[str, Callable] = {}
        self._log: list[Message] = []
        self._max_log = 1000
        self._lock = threading.RLock()
        self._shutdown_event = threading.Event()
        self._metrics = MessageMetrics(start_time=time.time())
        self.max_queue_depth = max_queue_depth
        self.backpressure_watermark = backpressure_watermark
        self._peak_depth = 0
        self._high_watermark_hits = 0
        self._publish_dropped = 0

        # 背压通知条件变量（替代 sleep 轮询）
        self._backpressure_cv = threading.Condition(self._lock)

        # 工作线程
        self._worker_thread = threading.Thread(target=self._process_loop, daemon=True)
        self._worker_running = True
        self._worker_thread.start()

    def _get_store(self):
        """锁内快照读取 store，避免与 shutdown 置 None 竞争出 NoneType。"""
        with self._lock:
            return self._store

    # ─── 订阅 ─────────────────────────────────

    def subscribe(self, topic: str, callback: Callable, priority: int = 50):
        with self._lock:
            if topic not in self._subscribers:
                self._subscribers[topic] = []
            self._subscribers[topic].append((callback, priority))
            self._subscribers[topic].sort(key=lambda x: x[1])

    def unsubscribe(self, topic: str, callback: Callable):
        with self._lock:
            if topic in self._subscribers:
                self._subscribers[topic] = [
                    (cb, p) for cb, p in self._subscribers[topic] if cb != callback
                ]

    # ─── 发送 ─────────────────────────────────

    def send(self, msg: Message, wait_for_delivery: bool = False, persist: bool = True):
        """发送消息

        Args:
            msg: 消息对象
            wait_for_delivery: 是否等待投递完成
            persist: 是否持久化到 SQLite（REQUEST/RESPONSE 热路径可跳过）
        """
        # OTel: 向 payload 注入 traceparent（OTel 安装时）
        from .telemetry import inject_trace_context
        inject_trace_context(msg.payload)

        deliver_now = False
        with self._lock:
            self._log.append(msg)
            if len(self._log) > self._max_log:
                self._log = self._log[-self._max_log:]

            # 修复 P0：幂等检查与持久化必须原子执行。
            # 原代码分两步：check_and_mark_idempotent 成功后 save_message 可能失败，
            # 导致幂等键已标记但消息未落库 → 后续重复消息被静默丢弃（数据丢失）。
            # 改用 check_and_save_atomic 在同事务中完成检查+持久化：
            #   - 有幂等键且需持久化（EVENT）→ 单事务原子执行
            #   - 有幂等键但不持久化（REQUEST/RESPONSE）→ 仅检查标记
            #   - 无幂等键但需持久化 → 直接 save_message
            if msg.idempotency_key and self._store:
                if persist and msg.msg_type == MessageType.EVENT:
                    # 原子：幂等检查 + 消息持久化同事务
                    if self._store.check_and_save_atomic(msg.idempotency_key, msg):
                        return  # 重复消息，跳过
                else:
                    # 仅幂等检查（非 EVENT 或不持久化）
                    if self._store.check_and_mark_idempotent(msg.idempotency_key):
                        return  # 重复消息，跳过
            elif persist and self._store and msg.msg_type == MessageType.EVENT:
                # 无幂等键，直接持久化
                self._store.save_message(msg)

            # 同步投递（REQUEST 类型需要同步等待，直接走回调）；
            # 实际投递移到锁外执行，防止订阅者回调持全局锁同步运行导致多线程停摆
            if msg.msg_type == MessageType.EVENT:
                # EVENT 通过工作队列异步投递
                self._async_queue.put(msg)
            else:
                deliver_now = True

            self._metrics.record_sent()

        if deliver_now:
            self._deliver(msg)

    def request(self, topic: str, from_a: str = "system", to_a: str = "",
                payload: dict = None, timeout: float = 30,
                idempotency_key: str = "") -> Any:
        """发送 REQUEST 并同步等待响应"""
        if payload is None:
            payload = {}

        # 硬上限策略（REQUEST）：队列打满时不投递，直接回覆错误响应
        with self._lock:
            depth = self.queue_depth()
            self._track_peak(depth)
            over_limit = depth >= self.max_queue_depth
        if over_limit:
            self._publish_dropped += 1
            return {
                "status": "error",
                "error": f"queue full: depth={depth} >= max_queue_depth={self.max_queue_depth}",
                "topic": topic,
            }

        # 幂等检查只读不烧键；标记延迟到确认有订阅者接手之后，
        # 无订阅者/投递失败的路径不残留标记，同 key 重试可正常送达
        store = self._get_store()
        if idempotency_key and store and store.is_idempotent(idempotency_key):
            # 去重命中必须让调用方区分"节点没跑"和"节点跑了但没返回值"。
            # 此前这里返回 None，dag_executor 用 `result or {}` 兜底，
            # 于是历史 task_id + 跨进程共享的幂等库会让整条流水线的节点
            # 全部静默空转、却逐个记为 success（实测复现）。
            # 返回体与"订阅者抛异常"的既有约定同形。
            return {
                "status": "error",
                "error": "duplicate_idempotency_key",
                "idempotency_key": idempotency_key,
                "topic": topic,
                "message": "同幂等键已成功投递过，本次请求未触达订阅者",
            }

        msg = Message(
            topic=topic,
            payload=payload,
            msg_type=MessageType.REQUEST,
            from_agent=from_a,
            to_agent=to_a,
            correlation_id=str(uuid.uuid4()),
            idempotency_key=idempotency_key,
        )

        event = threading.Event()
        result = [None]

        def handler(resp_msg: Message):
            result[0] = resp_msg.payload  # type: ignore[call-overload]
            event.set()

        with self._lock:
            self._callbacks[msg.correlation_id] = handler

        self._metrics.record_sent()
        handled, delivered_ok = self._deliver(msg)

        store = self._get_store()
        if idempotency_key and store and handled and delivered_ok:
            store.save_idempotent(idempotency_key)

        if not (handled and delivered_ok):
            # 无订阅者或投递失败（订阅者异常时 _deliver 已回覆含 error 的响应）
            with self._lock:
                self._callbacks.pop(msg.correlation_id, None)
            return result[0]

        # 修复 P0：原代码 event.wait 超时后直接 pop callback 并返回 result[0]，
        # 存在竞态：超时瞬间响应可能已到达 handler 并写入 result[0]，但主线程
        # 未持锁检查，可能返回 None 丢失响应；或 handler 尚未执行，主线程 pop
        # callback 后响应永远丢失（且未进 DLQ）。
        # 修复：handler 先写 result 再 set event，收尾统一 pop callback 并返回
        # result[0]（event 已 set 时允许 payload=None）；超时后到达的响应由
        # _deliver 在 callback 缺失时转入 DLQ。
        event.wait(timeout=timeout)

        with self._lock:
            self._callbacks.pop(msg.correlation_id, None)

        return result[0]

    def reply(self, original: Message, from_a: str, payload: dict):
        """回复一个 REQUEST 消息"""
        resp = Message.response(original, from_a, payload)
        self._deliver(resp)
        return resp

    def publish(self, topic: str, from_a: str, payload: dict) -> dict:
        """
        发布 EVENT 消息（带背压支持）

        水位检查与入队在锁内原子完成（check-and-enforce）：
          - depth >= max_queue_depth：硬上限，记 drop 计数并拒绝入队
          - depth >= backpressure_watermark：背压触发，返回 busy

        Returns:
            {"status": "sent", "msg_id": str}   — 成功
            {"status": "busy", "queue_depth": int} — 背压触发，消息未发送
            {"status": "rejected", "queue_depth": int} — 超过硬上限，消息被丢弃
        """
        msg = Message(topic=topic, payload=payload, msg_type=MessageType.EVENT, from_agent=from_a)
        with self._lock:
            # send() 使用可重入 RLock；深度检查与实际入队同锁串行，
            # 保证 max_queue_depth 是硬约束（并发 publish 不会越限）
            depth = self.queue_depth()
            self._track_peak(depth)

            if depth >= self.max_queue_depth:
                self._publish_dropped += 1
                return {"status": "rejected", "reason": "max_queue_depth", "queue_depth": depth}

            if depth >= self.backpressure_watermark:
                self._metrics.high_watermark_hits += 1
                msg._backpressure_skipped = True  # type: ignore[attr-defined]
                return {"status": "busy", "queue_depth": depth}

            self.send(msg)
        return {"status": "sent", "msg_id": msg.msg_id}

    def publish_blocking(self, topic: str, from_a: str, payload: dict,
                         max_wait: float = 30) -> dict:
        """
        阻塞式发布 EVENT 消息

        当背压触发时，通过 Condition 通知机制等待水位降至
        backpressure_watermark 以下再发布（替代 sleep 轮询）。
        超时时返回 timeout 状态。

        Args:
            topic: 主题
            from_a: 发送方标识
            payload: 消息负载
            max_wait: 最大等待秒数（默认 30）

        Returns:
            {"status": "sent", "msg_id": str}    — 成功
            {"status": "timeout", "queue_depth": int} — 超时，消息未发送
        """
        deadline = time.time() + max_wait
        with self._backpressure_cv:
            while True:
                depth = self.queue_depth()
                self._track_peak(depth)
                if depth < self.backpressure_watermark:
                    break
                remaining = deadline - time.time()
                if remaining <= 0:
                    depth = self.queue_depth()
                    self._track_peak(depth)
                    return {"status": "timeout", "queue_depth": depth}
                # 等待通知或超时，不再 sleep 轮询
                self._backpressure_cv.wait(timeout=min(remaining, 1.0))
        return self.publish(topic, from_a, payload)

    def queue_depth(self) -> int:
        """返回当前未投递的 EVENT 消息数量（背压检测用）"""
        store = self._get_store()
        if not store:
            return 0
        count: int = store.count_undelivered_events()
        return count

    def _track_peak(self, depth: int):
        """更新历史峰值队列深度"""
        if depth > self._metrics.peak_depth:
            self._metrics.peak_depth = depth

    # ─── 投递 ─────────────────────────────────

    def _deliver(self, msg: Message) -> tuple[bool, bool]:
        """投递消息到订阅者（同步执行）

        Returns:
            (handled, delivered_ok)
            handled: 是否有订阅者/callback 接手该消息
            delivered_ok: 接手的订阅者是否全部执行成功
        """
        store = self._get_store()
        if msg.msg_type == MessageType.RESPONSE and msg.correlation_id:
            with self._lock:
                cb = self._callbacks.pop(msg.correlation_id, None)
            if cb:
                try:
                    cb(msg)
                    self._metrics.record_received()
                    if store and msg.msg_id:
                        store.mark_delivered(msg.msg_id)
                    return True, True
                except Exception as e:
                    self._metrics.record_failed()
                    if store:
                        store.move_to_dlq(msg, str(e))
                        self._metrics.record_dlq()
                    return True, False
            else:
                # 修复 P0：callback 不存在（request 已超时被 pop）时，
                # 原代码直接 return 导致响应数据静默丢失。
                # 将孤儿响应转入 DLQ，供后续 replay_dlq 自愈。
                self._metrics.record_failed()
                if store:
                    try:
                        store.move_to_dlq(
                            msg, "orphan response: request timed out or no callback"
                        )
                        self._metrics.record_dlq()
                    except Exception as e:
                        _logger.warning(f"[MessageBus] orphan response DLQ failed: {e}")
                return False, False

        with self._lock:
            callbacks = list(self._subscribers.get(msg.topic, []))

        if not callbacks:
            if store and msg.msg_id:
                store.mark_delivered(msg.msg_id)
            return False, False

        delivered_ok = True
        replied = False
        errors = []
        for callback, _priority in sorted(callbacks, key=lambda x: x[1]):
            try:
                result = callback(msg)
                self._metrics.record_received()

                if msg.msg_type == MessageType.REQUEST and result is not None and not replied:
                    self.reply(msg, msg.from_agent or "unknown", result)
                    replied = True

            except Exception as e:
                delivered_ok = False
                errors.append(str(e) or e.__class__.__name__)
                self._metrics.record_failed()
                if store:
                    store.move_to_dlq(msg, str(e))
                    self._metrics.record_dlq()

        # 修复：REQUEST 订阅者抛异常时立即回覆错误响应，避免等待方空等满 timeout
        if msg.msg_type == MessageType.REQUEST and not replied and errors:
            with self._lock:
                has_waiter = bool(msg.correlation_id) and msg.correlation_id in self._callbacks
            if has_waiter:
                self.reply(msg, msg.from_agent or "unknown", {
                    "error": "; ".join(errors),
                    "status": "error",
                    "topic": msg.topic,
                })

        if delivered_ok and store and msg.msg_id and msg.msg_type != MessageType.EVENT:
            # EVENT 消息由 _process_loop 批量标记，此处仅处理非 EVENT（如 RESPONSE）
            store.mark_delivered(msg.msg_id)
            with self._backpressure_cv:
                self._backpressure_cv.notify_all()

        return True, delivered_ok

    def _process_loop(self):
        """异步事件处理循环（批量 drain 优化：一次取多条，减少锁竞争和 SQLite 写入）"""
        BATCH_SIZE = 50  # 单批最大处理条数
        last_store = None
        try:
            while self._worker_running and not self._shutdown_event.is_set():
                # 持本地引用投递，避免与 shutdown 置 None 竞争出 NoneType
                store = self._get_store()
                if store is not None:
                    last_store = store
                try:
                    # 阻塞等待第一条消息
                    msg = self._async_queue.get(timeout=1)
                    batch = [msg]
                    # 非阻塞 drain：尽可能多取（最多 BATCH_SIZE 条）
                    while len(batch) < BATCH_SIZE:
                        try:
                            batch.append(self._async_queue.get_nowait())
                        except Empty:
                            break
                    # 逐条投递，收集已投递的 msg_id
                    delivered_ids = []
                    for m in batch:
                        self._deliver(m)
                        if m.msg_id and m.msg_type == MessageType.EVENT:
                            delivered_ids.append(m.msg_id)
                    # 批量标记已投递（一次 UPDATE 替代 N 次）
                    if delivered_ids and store:
                        with contextlib.suppress(Exception):
                            store.mark_delivered_batch(delivered_ids)  # 标记失败不影响投递
                    # 通知背压等待者
                    if delivered_ids:
                        with self._backpressure_cv:
                            self._backpressure_cv.notify_all()
                except Empty:
                    continue
                except Exception as e:
                    _logger.error(f"[MessageBus] _process_loop error: {e}", exc_info=True)
        finally:
            # worker 线程退出时关闭自己 thread-local 的 SQLite 连接
            if last_store is not None:
                with contextlib.suppress(Exception):
                    last_store.close_current_thread()

    # ─── 查询 ─────────────────────────────────

    def health(self) -> dict:
        # `running` 是真生命位（worker 线程未 shutdown）：/api/health/deep 用它判
        # message_bus 组件的 healthy/unhealthy。此前只暴露 status 常量、无运行位
        # ⇒ 消费端 .get("running") 恒 None ⇒ 组件恒 unhealthy（2026-11-17 交付演练
        # D1 实证：shipped 3.17.0 与 main 同源）。
        result = {"status": "ok", "running": self._worker_running,
                  "subscribers": len(self._subscribers)}
        store = self._get_store()
        if store:
            result["store"] = store.health()
        result["metrics"] = self._metrics.to_dict()
        result["queue_depth"] = self.queue_depth()
        result["high_watermark_hits"] = self._metrics.high_watermark_hits
        result["peak_depth"] = self._metrics.peak_depth
        result["backpressure_watermark"] = self.backpressure_watermark
        result["max_queue_depth"] = self.max_queue_depth
        result["publish_dropped"] = self._publish_dropped
        return result

    def list_dlq(self, limit: int = 50) -> list[dict]:
        store = self._get_store()
        return store.list_dlq(limit) if store else []

    def replay_dlq(self, dlq_id: int) -> dict | None:
        """重放一条死信：取出数据并真实重新投递到原 topic"""
        store = self._get_store()
        if not store:
            return None
        data = store.replay_dlq(dlq_id)
        if not data:
            return None
        # 真实重投：把原始 payload 重新 publish 到原 topic
        payload = data["payload"]
        original_payload = payload.get("payload", payload)
        from_agent = payload.get("from_agent", "dlq_replay")
        self.publish(data["topic"], from_agent, original_payload)
        replayed: dict = data
        return replayed

    # ─── 关闭 ─────────────────────────────────

    def shutdown(self):
        self._worker_running = False
        self._shutdown_event.set()
        with contextlib.suppress(RuntimeError):
            self._worker_thread.join(timeout=3)
        # 锁内置 None，与 _get_store() 快照读取互斥；
        # worker 自身的 thread-local 连接由其在退出时经 close_current_thread 关闭，
        # 其余线程缓存的连接经 close_all() 按弱引用登记统一关闭
        with self._lock:
            store = self._store
            self._store = None
        if store is not None:
            store.close_all()
