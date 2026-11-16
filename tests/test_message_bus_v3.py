"""MessageBus v3 — pub/sub, backpressure, DLQ, health"""
import threading
import time

from loom_core.message_bus_v3 import Message, MessageBus, MessageType


class TestMessageBusBasics:
    """核心 pub/sub 流程"""

    def test_subscribe_and_publish(self, bus, wait_until):
        msgs = []
        bus.subscribe("test.topic", lambda m: msgs.append(m))
        result = bus.publish("test.topic", "sender", {"key": "value"})

        assert result["status"] == "sent", f"publish failed: {result}"
        assert wait_until(lambda: len(msgs) == 1), f"expected 1 msg, got {len(msgs)}"
        assert msgs[0].payload["key"] == "value"
        assert msgs[0].from_agent == "sender"

    def test_subscribe_priority(self, bus, wait_until):
        """高优先级（低值）先执行 — Unix nice 风格"""
        order = []

        bus.subscribe("prio.topic", lambda m: order.append("high"), priority=10)
        bus.subscribe("prio.topic", lambda m: order.append("low"), priority=100)

        bus.publish("prio.topic", "t", {"x": 1})
        assert wait_until(lambda: len(order) == 2), f"expected 2 callbacks, got {order}"
        assert order == ["high", "low"], f"priority order wrong: {order}"

    def test_unsubscribe(self, bus):
        msgs = []

        def cb(m):
            msgs.append(m)

        bus.subscribe("unsub.topic", cb)
        bus.unsubscribe("unsub.topic", cb)
        bus.publish("unsub.topic", "t", {"x": 1})
        time.sleep(0.15)
        assert len(msgs) == 0, "should not receive after unsubscribe"

    def test_no_subscriber(self, bus):
        """无订阅者应返回 sent（空投递）"""
        result = bus.publish("empty.topic", "t", {"x": 1})
        assert result["status"] == "sent"

    def test_multiple_topics(self, bus, wait_until):
        msgs = []
        bus.subscribe("a.topic", lambda m: msgs.append(m))
        bus.subscribe("b.topic", lambda m: msgs.append(m))
        bus.publish("a.topic", "t", {"topic": "a"})
        bus.publish("b.topic", "t", {"topic": "b"})
        assert wait_until(lambda: len(msgs) == 2), f"expected 2 msgs, got {len(msgs)}"


class TestMessageBusBackpressure:
    """反压机制"""

    def test_publish_returns_sent(self, bus):
        result = bus.publish("bp.topic", "t", {"x": 1})
        assert result["status"] == "sent"

    def test_queue_depth(self, bus):
        """publish 后 queue_depth 应反映待投递消息"""
        bus.subscribe("q.topic", lambda m: time.sleep(0.3))
        bus.publish("q.topic", "s", {"x": 1})
        bus.publish("q.topic", "s", {"x": 1})
        time.sleep(0.05)
        depth = bus.queue_depth()
        assert isinstance(depth, int)

    def test_publish_returns_busy_when_queue_full(self):
        """高水位时 publish 应返回 busy（低水位线）"""
        bus = MessageBus(db_path=":memory:", backpressure_watermark=3)
        bus.subscribe("full.topic", lambda m: time.sleep(0.5))
        results = []
        for i in range(20):
            r = bus.publish("full.topic", "s", {"i": i})
            results.append(r)
            if r["status"] == "busy":
                break
        busy_hits = [r for r in results if r["status"] == "busy"]
        assert len(busy_hits) > 0, f"never got busy: {results[:5]}..."
        bus.shutdown()

    def test_publish_blocking(self):
        """publish_blocking 应等到水位下降"""
        bus = MessageBus(db_path=":memory:", backpressure_watermark=3)
        bus.subscribe("block.topic", lambda m: time.sleep(0.3))
        r = bus.publish_blocking("block.topic", "s", {"x": 1}, max_wait=5)
        assert r["status"] in ("sent", "timeout")
        bus.shutdown()

    def test_health_includes_backpressure_metrics(self, bus):
        """health() 包含反压指标"""
        h = bus.health()
        assert "queue_depth" in h
        assert "high_watermark_hits" in h
        assert "peak_depth" in h
        assert "backpressure_watermark" in h

    def test_health_exposes_running_lifecycle(self):
        """`health()["running"]` 是真生命位：alive=True、shutdown 后 False。

        2026-11-17 交付演练 D1 回归钉：此前 health() 无 running 键，消费端
        （/api/health/deep）`.get("running")` 恒 None ⇒ 组件恒 unhealthy。
        """
        b = MessageBus(db_path=":memory:")
        try:
            assert b.health()["running"] is True
        finally:
            b.shutdown()
        assert b.health()["running"] is False


class TestMessageBusDLQ:
    """死信队列"""

    def test_dlq_list(self, bus):
        dq = bus.list_dlq()
        assert isinstance(dq, list)

    def test_dlq_on_subscriber_error(self, bus, wait_until):
        """订阅者抛异常不应影响总线——其他订阅者仍收到"""
        ok = []

        def failing(m):
            raise ValueError("crash")

        bus.subscribe("fail.topic", failing)
        bus.subscribe("fail.topic", lambda m: ok.append(m))

        result = bus.publish("fail.topic", "s", {"x": 1})
        assert result["status"] == "sent"
        assert wait_until(lambda: len(ok) >= 1), "working callback should receive"


class TestMessageBusLifecycle:
    """生命周期"""

    def test_subscriber_error_doesnt_crash_bus(self, bus, wait_until):
        """一个订阅者抛异常不应影响其他订阅者"""
        ok = []

        def failing(m):
            raise RuntimeError("crash")

        def working(m):
            ok.append(m)

        bus.subscribe("resilient.topic", failing)
        bus.subscribe("resilient.topic", working)

        bus.publish("resilient.topic", "s", {"x": 1})
        assert wait_until(lambda: len(ok) == 1), "working callback should still receive"


class TestMessageBusRequestRobustness:
    """REQUEST/RESPONSE 审计修复：锁外投递、错误回覆、幂等键生命周期"""

    def test_send_request_callback_nested_request_no_deadlock(self):
        """send(REQUEST) 回调内再发 bus.request() 时，其他线程不被全局锁停摆"""
        import os
        import tempfile

        db = os.path.join(tempfile.mkdtemp(), "dl_probe.db")
        bus = MessageBus(db_path=db)
        gate = threading.Event()
        nested_entered = threading.Event()

        def gated(msg):
            nested_entered.set()
            gate.wait(timeout=5)
            return {"g": 1}

        def outer(msg):
            return {"outer": "done", "gate_res": bus.request(
                "dl.gate", "outer", "gate", {"x": 1}, timeout=5)}

        bus.subscribe("dl.gate", gated)
        bus.subscribe("dl.outer", outer)
        bus.subscribe("dl.free", lambda m: {"free": True})

        req = Message(topic="dl.outer", payload={"p": 1},
                      msg_type=MessageType.REQUEST, from_agent="t")
        t1 = threading.Thread(target=bus.send, args=(req,), daemon=True)
        watchdog = threading.Timer(6.0, gate.set)
        watchdog.daemon = True
        watchdog.start()
        t1.start()
        try:
            assert nested_entered.wait(timeout=2), "嵌套回调未进入"

            t0 = time.time()
            probe = bus.request("dl.free", "main", "free", {}, timeout=3)
            elapsed = time.time() - t0
            assert probe == {"free": True}, f"探针请求失败: {probe}"
            assert elapsed < 2.5, f"主线程被全局锁停摆 {elapsed:.2f}s"
        finally:
            gate.set()
            t1.join(timeout=7)
            watchdog.cancel()
        assert not t1.is_alive(), "send 线程未结束"
        bus.shutdown()

    def test_subscriber_crash_returns_error_response_quickly(self, bus):
        """订阅者抛异常 → request 方远小于 timeout 内收到含 error 的响应，DLQ 保留"""
        def failing(m):
            raise ValueError("boom-crash")

        bus.subscribe("err.req", failing)
        t0 = time.time()
        result = bus.request("err.req", "tester", "agent", {"x": 1}, timeout=30)
        elapsed = time.time() - t0
        assert isinstance(result, dict), f"应返回错误 dict，实际 {result}"
        assert "boom-crash" in str(result.get("error"))
        assert result.get("status") == "error"
        assert elapsed < 5, f"应远小于 timeout，实际 {elapsed:.2f}s"
        dlq = bus.list_dlq()
        assert len(dlq) >= 1, "异常仍应进 DLQ"

    def test_partial_failure_still_returns_working_reply(self, bus):
        """多订阅者部分失败：正常订阅者的回复优先返回，异常仍进 DLQ"""
        def failing(m):
            raise ValueError("secondary-boom")

        bus.subscribe("mix.req", failing, priority=10)
        bus.subscribe("mix.req", lambda m: {"ok": True}, priority=90)

        result = bus.request("mix.req", "t", "a", {"x": 1}, timeout=10)
        assert result == {"ok": True}
        assert len(bus.list_dlq()) >= 1

    def test_none_payload_response_returns_immediately(self, bus):
        """payload=None 的响应即时返回，不空等 timeout"""
        def replier(m):
            bus.reply(m, "none-agent", None)
            return None

        bus.subscribe("none.payload", replier)
        t0 = time.time()
        result = bus.request("none.payload", "t", "a", {"x": 1}, timeout=10)
        elapsed = time.time() - t0
        assert result is None
        assert elapsed < 5, f"payload=None 应即时返回，实际 {elapsed:.2f}s"

    def test_idempotency_key_not_burned_when_no_subscriber(self, bus):
        """无订阅者不烧幂等键：注册订阅者后同 key 重试正常送达"""
        r1 = bus.request("ghost.topic", "t", "a", {"n": 1}, timeout=1,
                         idempotency_key="idem-ghost-001")
        assert r1 is None

        got = []

        def sub(m):
            got.append(m)
            return {"ok": True}

        bus.subscribe("ghost.topic", sub)
        r2 = bus.request("ghost.topic", "t", "a", {"n": 1}, timeout=5,
                         idempotency_key="idem-ghost-001")
        assert r2 == {"ok": True}, f"同 key 重试应正常送达，实际 {r2}"
        assert len(got) == 1

    def test_idempotency_key_marked_after_successful_delivery(self, bus):
        """投递成功后幂等键生效：同 key 重复请求被去重并如实报告未触达"""
        calls = []
        bus.subscribe("idem.ok", lambda m: calls.append(m) or {"ok": True})

        r1 = bus.request("idem.ok", "t", "a", {}, timeout=5,
                         idempotency_key="idem-ok-001")
        assert r1 == {"ok": True}
        r2 = bus.request("idem.ok", "t", "a", {}, timeout=5,
                         idempotency_key="idem-ok-001")
        # 不得返回 None：调用方要能区分"节点没跑"与"跑了但没返回值"。
        # 此前返回 None，dag_executor 的 `result or {}` 把空转记成 success。
        assert isinstance(r2, dict) and \
            r2.get("error") == "duplicate_idempotency_key", f"实际响应: {r2}"
        assert r2.get("idempotency_key") == "idem-ok-001"
        assert len(calls) == 1, "重复请求不应再次触达订阅者"

    def test_failed_delivery_does_not_mark_idempotency_key(self, bus):
        """订阅者全挂：key 不标记，订阅者恢复后同 key 重试成功"""
        def failing(m):
            raise RuntimeError("always-fails")

        bus.subscribe("idem.fail", failing)
        r1 = bus.request("idem.fail", "t", "a", {}, timeout=10,
                         idempotency_key="idem-fail-001")
        assert isinstance(r1, dict) and r1.get("status") == "error"

        bus.unsubscribe("idem.fail", failing)
        bus.subscribe("idem.fail", lambda m: {"recovered": True})
        r2 = bus.request("idem.fail", "t", "a", {}, timeout=5,
                         idempotency_key="idem-fail-001")
        assert r2 == {"recovered": True}, f"失败路径不应烧 key，实际 {r2}"
