"""P2 基础设施缺陷修复回归测试。

覆盖：
  1. TaskQueue.recover(stale_seconds) 多进程安全恢复 + close_all 自愈
  2. MessageBus publish/request 硬上限（max_queue_depth）+ shutdown 积压安全
  3. Registry respawn 双 ERROR 竞态（per-name 锁 + 败者 on_stop）
  4. StreamCallback 分级丢弃（边界事件不丢）
  5. WriterAgent 双流并发按 task_id 路由回调
"""
import asyncio
import os
import subprocess
import sys
import threading
import time

import pytest

from loom_core.base_agent import AgentStatus, Message
from loom_core.message_bus_v3 import MessageBus
from loom_core.registry import AgentMeta, Registry
from loom_core.streaming import StreamCallback
from loom_core.task_queue import TaskQueue, _pid_alive
from packs.docgen.agents.writer import WriterAgent

# ─── 1. TaskQueue：recover stale / owner_pid / close_all ──────────────

class TestTaskQueueRecoverStale:
    @pytest.fixture
    def queue(self, tmp_path):
        q = TaskQueue(db_path=str(tmp_path / "tasks.db"))
        yield q
        q.close()

    def _raw_row(self, q, task_id):
        conn = q._get_conn()
        return conn.execute(
            "SELECT status, started_at, owner_pid FROM task_queue WHERE task_id = ?",
            (task_id,),
        ).fetchone()

    def test_acquire_records_owner_pid(self, queue):
        queue.submit("t1", "p", "a.md")
        queue.acquire("w1")
        status, started_at, owner_pid = self._raw_row(queue, "t1")
        assert status == "running"
        assert int(owner_pid) == os.getpid()

    def test_recover_none_keeps_legacy_behavior(self, queue, tmp_path):
        """stale_seconds 缺省保持旧行为：连他进程存活的 running 也照常回收"""
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        try:
            queue.submit("t1", "p", "a.md")
            queue.acquire("w1")
            conn = queue._get_conn()
            conn.execute(
                "UPDATE task_queue SET owner_pid = ?, started_at = ? WHERE task_id = 't1'",
                (child.pid, time.time()),
            )
            conn.commit()
            recovered = queue.recover()
            assert [r["task_id"] for r in recovered] == ["t1"]
            assert queue.get("t1")["status"] == "pending"
        finally:
            child.terminate()
            child.wait(timeout=15)

    def test_recover_stale_skips_fresh_running(self, queue):
        """新鲜（updated_at/started_at 晚于阈值）的 running 不被翻回 pending"""
        queue.submit("fresh", "p", "a.md")
        queue.submit("stale", "p", "b.md")
        queue.acquire("w1")
        queue.acquire("w2")
        conn = queue._get_conn()
        conn.execute(
            "UPDATE task_queue SET started_at = ? WHERE task_id = 'stale'",
            (time.time() - 3600,),
        )
        conn.commit()

        recovered = queue.recover(stale_seconds=60)
        assert [r["task_id"] for r in recovered] == ["stale"]
        assert queue.get("fresh")["status"] == "running"
        assert queue.get("stale")["status"] == "pending"

    def test_recover_skips_alive_foreign_pid_then_recovers_after_death(self, queue):
        """双进程场景：他进程存活时跳过其 running 任务；进程死亡后可回收"""
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        try:
            assert _pid_alive(child.pid)
            queue.submit("t1", "p", "a.md")
            queue.acquire("w1")
            conn = queue._get_conn()
            conn.execute(
                "UPDATE task_queue SET owner_pid = ?, started_at = ? WHERE task_id = 't1'",
                (child.pid, time.time() - 9999),
            )
            conn.commit()

            assert queue.recover(stale_seconds=10) == []
            assert queue.get("t1")["status"] == "running"

            child.terminate()
            child.wait(timeout=15)
            deadline = time.time() + 5
            while time.time() < deadline and _pid_alive(child.pid):
                time.sleep(0.05)

            recovered = queue.recover(stale_seconds=10)
            assert [r["task_id"] for r in recovered] == ["t1"]
            assert queue.get("t1")["status"] == "pending"
        finally:
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=15)

    def test_recover_stale_own_pid_task_recovers_by_age(self, queue):
        """本进程 pid 的陈旧 running 任务按阈值正常回收"""
        queue.submit("t1", "p", "a.md")
        queue.acquire("w1")
        conn = queue._get_conn()
        conn.execute(
            "UPDATE task_queue SET started_at = ? WHERE task_id = 't1'",
            (time.time() - 120,),
        )
        conn.commit()
        recovered = queue.recover(stale_seconds=30)
        assert [r["task_id"] for r in recovered] == ["t1"]


class TestTaskQueueCloseAll:
    def test_close_all_closes_all_registered_connections_and_self_heals(self, tmp_path):
        q = TaskQueue(db_path=str(tmp_path / "tasks.db"))
        try:
            q.submit("seed", "p", "s.md")

            errors = []

            def _worker():
                try:
                    assert q.acquire("wk") is not None
                except Exception as e:  # pragma: no cover
                    errors.append(e)

            t = threading.Thread(target=_worker)
            t.start()
            t.join(timeout=10)
            assert not errors

            # 契约本身而不是引用计数：close_all 只允许关闭当前线程的连接，
            # 绝不跨线程 close（2026-08 CI 实证：跨线程关连接 C 层竞争会
            # SIGSEGV）。工作线程的连接由该线程自身结束时释放——弱引用何时
            # 失效带 GC 时序，不作断言；可断言的是行为：主线程与新线程在
            # close_all 之后都能自愈（见下）。
            q.close_all()

            # close_all 后再操作自愈：主线程与新线程均可继续使用
            assert q.submit("after", "p", "a.md") is True
            t2_errors = []

            def _worker2():
                try:
                    task = q.acquire("wk2")
                    assert task is not None
                    q.complete(task["task_id"], result={})
                except Exception as e:  # pragma: no cover
                    t2_errors.append(e)

            t2 = threading.Thread(target=_worker2)
            t2.start()
            t2.join(timeout=10)
            assert not t2_errors
            assert q.get("after")["status"] == "done"
        finally:
            q.close()


class TestPersistentStoreCloseAll:
    """PersistentStore 与 TaskQueue 同模式：弱引用登记 + close_all 跨线程关闭 + 自愈"""

    def test_close_all_closes_cross_thread_connections_and_self_heals(self, tmp_path):
        from loom_core.message_bus_v3 import PersistentStore

        store = PersistentStore(db_path=str(tmp_path / "store.db"))
        store._get_conn()  # 主线程登记一条连接
        errors = []

        def _worker():
            try:
                store._get_conn().execute("SELECT 1")
            except Exception as e:  # pragma: no cover
                errors.append(e)

        t = threading.Thread(target=_worker)
        t.start()
        t.join(timeout=10)
        assert not errors

        registered = sum(1 for ref in list(store._conn_refs) if ref() is not None)
        assert registered >= 2  # 主线程 + 工作线程各一条

        store.close_all()
        assert all(ref() is None for ref in store._conn_refs)

        # close_all 后自愈：主线程与新线程均可继续使用
        store._get_conn().execute("SELECT 1")
        t2_errors = []

        def _worker2():
            try:
                store._get_conn().execute("SELECT 1")
            except Exception as e:  # pragma: no cover
                t2_errors.append(e)

        t2 = threading.Thread(target=_worker2)
        t2.start()
        t2.join(timeout=10)
        assert not t2_errors
        store.close_all()


# ─── 2. MessageBus：硬上限 / REQUEST 策略 / shutdown 安全 ─────────────

class TestMessageBusHardCap:
    def test_concurrent_publish_never_exceeds_max_depth(self, tmp_path):
        bus = MessageBus(
            db_path=str(tmp_path / "cap.db"),
            enable_persistence=True,
            max_queue_depth=5,
            backpressure_watermark=10000,
        )
        release = threading.Event()
        bus.subscribe("cap.topic", lambda m: release.wait(20))

        n_threads = 32
        barrier = threading.Barrier(n_threads)
        results = []
        lock = threading.Lock()

        def _pub(i):
            barrier.wait()
            r = bus.publish("cap.topic", "t", {"i": i})
            with lock:
                results.append(r["status"])

        threads = [threading.Thread(target=_pub, args=(i,)) for i in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        depth = bus.queue_depth()
        assert depth <= 5, f"越限：depth={depth}"
        statuses = set(results)
        assert "rejected" in statuses, f"应有拒绝发生: {results}"
        assert statuses <= {"sent", "rejected"}
        h = bus.health()
        assert h["max_queue_depth"] == 5
        assert h["publish_dropped"] == results.count("rejected")

        release.set()
        bus.shutdown()

    def test_request_returns_error_response_when_over_max_depth(self, tmp_path):
        bus = MessageBus(
            db_path=str(tmp_path / "req.db"),
            enable_persistence=True,
            max_queue_depth=1,
            backpressure_watermark=10000,
        )
        release = threading.Event()
        entered = threading.Event()

        def _block(msg):
            entered.set()
            return release.wait(10)

        bus.subscribe("blk.topic", _block)

        r = bus.publish("blk.topic", "t", {"occupy": 1})
        assert r["status"] == "sent"
        # 等的是**订阅者被进去阻塞**这个状态，不是 0.4s 的运气：负载轮里 worker 起步
        # 可以远超 0.4s，那时队列还没被占住，下面的断言就是在问一件还没发生的事。
        assert entered.wait(20), "前提：占用消息已被 worker 取走并阻塞在订阅者里"
        assert bus.queue_depth() >= 1, "前提：占用尚未 ack（队列被当成满才是这条用例的对象）"

        resp = bus.request("any.topic", "a", "b", {}, timeout=5)
        assert isinstance(resp, dict)
        assert resp.get("status") == "error"
        assert "max_queue_depth" in str(resp.get("error"))
        with bus._lock:
            assert not bus._callbacks  # 提前返回未残留等待回调

        release.set()
        # 排空同样按可观察状态等——而且是等**被测代码自己读的那个数**归零。
        # ack 走 worker 的批量路径，负载下可以慢过任何固定预算；没等到位就发下一次
        # request，硬上限会把"还没 ack 的占用"当成队列满，于是用例红在产品根本没错的
        # 地方（实测：把 ack 拖慢 1s，原来那句 sleep(0.3) 必红）。
        deadline = time.monotonic() + 20
        while bus.queue_depth() > 0 and time.monotonic() < deadline:
            time.sleep(0.05)
        assert bus.queue_depth() == 0, "占用消息未在 20s 内排空"

        bus.subscribe("ok.topic", lambda m: {"fine": True})
        resp_ok = bus.request("ok.topic", "a", "b", {}, timeout=5)
        assert resp_ok == {"fine": True}
        bus.shutdown()


class TestMessageBusShutdownSafety:
    def test_shutdown_with_backlog_no_nonetype(self, tmp_path):
        """shutdown join 超时（积压未清）后，任何总线操作不再出 NoneType"""
        bus = MessageBus(db_path=str(tmp_path / "bl.db"))
        bus.subscribe("drip.topic", lambda m: time.sleep(0.4))
        for i in range(12):
            r = bus.publish("drip.topic", "t", {"i": i})
            assert r["status"] == "sent"
        time.sleep(0.1)

        t0 = time.monotonic()
        bus.shutdown()  # 积压约 4.8s > join 3s，必然超时返回
        elapsed = time.monotonic() - t0
        assert elapsed < 3.9

        assert bus._get_store() is None
        assert bus.queue_depth() == 0
        h = bus.health()
        assert h["status"] == "ok"
        assert "store" not in h
        assert bus.list_dlq() == []
        r = bus.publish("post.topic", "t", {"late": 1})
        assert r["status"] == "sent"
        resp = bus.request("post.topic", "a", "b", {}, timeout=1)
        assert resp is None

    def test_worker_thread_closes_own_connection_on_exit(self, tmp_path, monkeypatch):
        bus = MessageBus(db_path=str(tmp_path / "wc.db"))
        bus.subscribe("quick.topic", lambda m: None)
        for i in range(3):
            bus.publish("quick.topic", "t", {"i": i})

        store = bus._get_store()
        assert store is not None
        calls = []
        original = store.close_current_thread

        def _spy():
            calls.append(1)
            original()

        monkeypatch.setattr(store, "close_current_thread", _spy)

        bus.shutdown()
        assert bus._worker_thread.is_alive() is False
        assert calls, "worker 线程退出时应调用 close_current_thread 关闭自身连接"


# ─── 3. Registry respawn 竞态 ────────────────────────────────────────

class TestRegistryRespawnRace:
    @staticmethod
    def _make_agent_cls():
        created = []
        stopped = []

        class _FakeAgent:
            def __init__(self, name, meta, config=None, message_bus=None, registry=None):
                self.name = name
                self.meta = meta
                self.bus = message_bus
                created.append(self)

            def on_stop(self):
                stopped.append(self)

        return _FakeAgent, created, stopped

    def test_double_error_single_survivor_losers_stopped(self):
        """并发双 ERROR：单实例存活、所有被弃实例均 on_stop、无泄漏"""
        cls, created, stopped = self._make_agent_cls()
        reg = Registry(enable_health_check=False)
        meta = AgentMeta(name="ra", version="1.0", respawn=True, respawn_max=10)
        a0 = cls(name="ra", meta=meta)
        reg.register(meta, instance=a0)

        barrier = threading.Barrier(2)

        def _fire():
            barrier.wait()
            reg.set_status("ra", AgentStatus.ERROR)

        threads = [threading.Thread(target=_fire) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)

        survivor = reg.get_instance("ra")
        assert survivor is not None
        assert survivor is not a0
        assert reg.get_status("ra") == AgentStatus.LOADED
        assert survivor not in stopped
        discarded = [x for x in created if x is not survivor]
        assert discarded, "双 ERROR 应至少产生一次重建"
        assert all(x in stopped for x in discarded)
        assert len(created) == 3  # 初始 + 两次构建（胜者存活，败者弃置）

    def test_commit_race_loser_stops_own_build_and_discards(self):
        """提交期版本比对：败者对自己构建的实例调用 on_stop 后丢弃"""
        built = threading.Event()
        release = threading.Event()
        created = []
        stopped = []

        class _SlowAgent:
            def __init__(self, name, meta, config=None, message_bus=None, registry=None):
                self.name = name
                self.bus = message_bus
                created.append(self)
                if len(created) > 1:
                    built.set()
                    release.wait(10)

            def on_stop(self):
                stopped.append(self)

        reg = Registry(enable_health_check=False)
        meta = AgentMeta(name="rr", version="1.0", respawn=True, respawn_max=10)
        a0 = _SlowAgent(name="rr", meta=meta)
        reg.register(meta, instance=a0)

        t = threading.Thread(target=reg._check_respawn, args=("rr",))
        t.start()
        assert built.wait(timeout=10), "新实例构建未开始"

        foreign = _SlowAgent.__new__(_SlowAgent)
        foreign.name = "foreign"
        with reg._lock:
            reg._instances["rr"] = foreign  # 提交前实例已被并发替换
        release.set()
        t.join(timeout=10)
        assert not t.is_alive()

        assert reg.get_instance("rr") is foreign
        assert a0 in stopped          # 捕获版本旧实例已停
        assert created[1] in stopped  # 败者构建的实例 on_stop 后丢弃


# ─── 4. StreamCallback 分级丢弃 ──────────────────────────────────────

class TestStreamTieredDrop:
    def test_boundary_events_survive_full_queue_with_slow_consumer(self):
        cb = StreamCallback(max_queue_size=2)
        received = []
        stop = threading.Event()

        def _consume():
            while not stop.is_set():
                received.extend(cb.get_events())
                time.sleep(0.005)

        consumer = threading.Thread(target=_consume, daemon=True)
        consumer.start()

        cb.on_start(3, "Doc")
        for i in range(10):
            cb.on_chunk(f"c{i}")  # 可丢类型：队满即拒
        for i in range(3):
            cb.on_section(i, f"S{i}", "content")  # 边界：阻塞等消费腾位，不丢
        cb.on_complete("full", {})

        deadline = time.time() + 10
        while time.time() < deadline:
            if any(e.event_type == "complete" for e in received):
                break
            time.sleep(0.01)
        stop.set()
        consumer.join(timeout=5)

        types = [e.event_type for e in received]
        assert types.count("start") == 1
        assert types.count("complete") == 1
        assert sorted(e.section_index for e in received if e.event_type == "section") == [0, 1, 2]
        snap = cb.metrics.snapshot()
        boundary_dropped = sum(
            cnt for etype, cnt in snap["drops_by_type"].items() if etype in cb.BOUNDARY_EVENT_TYPES
        )
        assert boundary_dropped == 0
        assert set(snap["drops_by_type"]) <= {"chunk"}

    def test_closed_stream_boundary_emit_returns_without_hang(self):
        cb = StreamCallback(max_queue_size=1)
        cb.on_start(1, "Doc")  # 队列满且队首为边界
        cb.close()
        t0 = time.monotonic()
        cb.on_error("late")  # 已关闭的流：快速返回而非永久阻塞
        assert time.monotonic() - t0 < 1.0
        assert cb.is_closed()
        assert [e.event_type for e in cb.get_events()] == ["start"]


# ─── 5. Writer 双流回调路由 ───────────────────────────────────────────

def _make_writer(tmp_path):
    w = WriterAgent(
        name="writer",
        meta=AgentMeta(name="writer", version="2.0"),
        config={
            "cache_dir": str(tmp_path / "cache"),
            "log_dir": str(tmp_path / "logs"),
            "quiet": True,
        },
        message_bus=None,
        registry=None,
    )
    w._llm_api_key = ""
    return w


class TestWriterStreamRouting:
    def test_stream_callback_registry_and_legacy_slot_compat(self, tmp_path):
        w = _make_writer(tmp_path)
        cb_a = StreamCallback()
        cb_b = StreamCallback()
        cb_legacy = StreamCallback()

        w._register_stream_callback("task-A", cb_a)
        w._register_stream_callback("task-B", cb_b)
        assert w._get_stream_callback("task-A") is cb_a
        assert w._get_stream_callback("task-B") is cb_b

        w._active_stream_callback = cb_legacy  # admin_api 式旧槽写入
        assert w._active_stream_callback is cb_legacy
        assert w._get_stream_callback("") is cb_legacy
        assert w._get_stream_callback("unknown-task") is cb_legacy  # 回退旧键

        w._unregister_stream_callback("task-A")
        assert w._get_stream_callback("task-A") is cb_legacy
        w._active_stream_callback = None
        assert w._get_stream_callback("") is None
        assert w._get_stream_callback("task-B") is cb_b

    def test_dual_streams_sections_routed_to_own_callback(self, tmp_path):
        """两条流并发重构：各自章节经 task_id 命中自己的回调，互不串扰"""
        w = _make_writer(tmp_path)
        w._llm_api_key = "test-key"
        w._load_prompt_template = lambda profile: {
            "system_prompt": "sys",
            "sections": [
                {"name": "S1", "prompt": "# {title} p1"},
                {"name": "S2", "prompt": "# {title} p2"},
            ],
        }

        async def _fake_gen(idx, sec_name, sec_prompt, system_prompt, context):
            if "Title-task-A" in sec_prompt:
                tid = "task-A"
            elif "Title-task-B" in sec_prompt:
                tid = "task-B"
            else:  # pragma: no cover
                tid = "task-X"
            await asyncio.sleep(0)
            return (idx, sec_name, f"CONTENT[{tid}]-{idx}")

        w._generate_section_async = _fake_gen
        outputs = {}

        def _run(tid):
            cb = StreamCallback()
            w._register_stream_callback(tid, cb)
            try:
                doc = w._restructure_document("", [], f"q-{tid}", f"Title-{tid}", task_id=tid)
                outputs[tid] = (cb, doc)
            finally:
                w._unregister_stream_callback(tid)

        threads = [
            threading.Thread(target=_run, args=("task-A",)),
            threading.Thread(target=_run, args=("task-B",)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20)

        for tid in ("task-A", "task-B"):
            cb, doc = outputs[tid]
            other = "task-B" if tid == "task-A" else "task-A"
            sections = [e for e in cb.get_events() if e.event_type == "section"]
            assert len(sections) == 2
            for e in sections:
                assert e.data["content"].startswith(f"CONTENT[{tid}]")
                assert f"CONTENT[{other}]" not in e.data["content"]
            assert f"CONTENT[{other}]" not in (doc or "")
            assert f"CONTENT[{tid}]-0" in doc and f"CONTENT[{tid}]-1" in doc
            w._unregister_stream_callback(tid)
        assert w._get_stream_callback("task-A") is None
        assert w._get_stream_callback("task-B") is None

    def test_handle_streaming_dual_flow_callbacks_hit_own_task(self, tmp_path):
        """handle_streaming 并发两任务：on_start/on_complete 各自命中，无互踩"""
        w = _make_writer(tmp_path)
        results = {}

        def _run(tid, title):
            msg = Message(topic="writer.input", payload={
                "task_id": tid, "query": f"q-{tid}", "title": title,
            })
            cb = StreamCallback()
            out = w.handle_streaming(msg, cb)
            results[tid] = (cb, out)

        threads = [
            threading.Thread(target=_run, args=("t-A", "标题A")),
            threading.Thread(target=_run, args=("t-B", "标题B")),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20)

        for tid, title in (("t-A", "标题A"), ("t-B", "标题B")):
            cb, out = results[tid]
            other_title = "标题B" if tid == "t-A" else "标题A"
            events = [(e.event_type, e.data) for e in cb.get_events()]
            starts = [d for ty, d in events if ty == "start"]
            completes = [d for ty, d in events if ty == "complete"]
            assert len(starts) == 1 and starts[0]["title"] == title
            assert len(completes) == 1
            assert other_title not in str(starts[0]) + str(completes[0])
            assert out["status"] == "ok" and out["task_id"] == tid

    def test_handle_streaming_error_routed_to_own_callback(self, tmp_path):
        w = _make_writer(tmp_path)

        def _boom(msg):
            raise RuntimeError(f"boom-{msg.payload['task_id']}")

        w.handle = _boom
        errors = {}

        def _run(tid):
            msg = Message(topic="writer.input", payload={"task_id": tid, "query": "q"})
            cb = StreamCallback()
            out = w.handle_streaming(msg, cb)
            errors[tid] = (cb, out)

        threads = [
            threading.Thread(target=_run, args=("err-A",)),
            threading.Thread(target=_run, args=("err-B",)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20)

        for tid in ("err-A", "err-B"):
            cb, out = errors[tid]
            assert out["status"] == "error" and out["task_id"] == tid
            err_events = [e for e in cb.get_events() if e.event_type == "error"]
            assert len(err_events) == 1
            assert err_events[0].data["error"] == f"boom-{tid}"
