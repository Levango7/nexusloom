"""CostTracker — LLM 成本追踪测试"""
import threading

import pytest

from loom_core.cost_tracker import (
    DEFAULT_PRICE,
    PRICING,
    BudgetExceededError,
    CostTracker,
    calc_cost,
    estimate_tokens,
    reset_cost_tracker,
)


@pytest.fixture
def tracker(tmp_path):
    reset_cost_tracker()
    return CostTracker(db_path=str(tmp_path / "test_cost.db"))


class TestEstimateTokens:
    def test_english(self):
        assert estimate_tokens("hello world") == 3

    def test_chinese(self):
        assert estimate_tokens("你好世界") == 3  # 4 chars // 2 + 1

    def test_mixed(self):
        tokens = estimate_tokens("hello 你好")
        assert tokens > 0

    def test_empty(self):
        assert estimate_tokens("") == 0


class TestCalcCost:
    def test_known_provider(self):
        cost = calc_cost("cloudflare", 1000, 500)
        assert cost > 0

    def test_ollama_free(self):
        cost = calc_cost("ollama", 10000, 5000)
        assert cost == 0

    def test_unknown_provider_uses_default(self):
        cost = calc_cost("unknown_provider", 1000, 500)
        assert cost > 0

    @pytest.mark.parametrize("provider,price", [
        ("openai", (0.0025, 0.01)),
        ("deepseek", (0.00027, 0.0011)),
        ("moonshot", (0.0006, 0.0025)),
        ("qwen", (0.0004, 0.0012)),
    ])
    def test_new_vendor_specific_price(self, provider, price):
        assert PRICING[provider] == price
        assert PRICING[provider] != DEFAULT_PRICE
        assert calc_cost(provider, 1000, 1000) == pytest.approx(price[0] + price[1])

    def test_default_provider_explicit_entry(self):
        assert PRICING["default"] == DEFAULT_PRICE
        assert calc_cost("default", 1000, 500) == calc_cost("unknown_vendor", 1000, 500)


class TestCostTracker:
    def test_survives_db_file_removed_after_init(self, tmp_path):
        """库文件在 init 之后被删（state 目录迁移/清理）时，下次写要能自愈。

        `_init_db` 只在构造时建表；同进程内文件被换掉后，每个操作都会撞
        `no such table: cost_log`。2026-10-21 在测试隔离场景里实测到这条，
        生产侧的同类场景是"服务在跑、状态目录被清"。

        pool 的 thread-local 连接持有文件句柄，Windows 下必须先 close 再 unlink。
        close 后 pool 的 _probe 检测到文件消失，重建连接并触发 schema_init 自愈。
        """
        db = tmp_path / "wiped.db"
        tracker = CostTracker(db_path=str(db))
        tracker.record("cloudflare", 10, 5, cost=0.01)
        tracker.close()
        db.unlink()
        tracker.record("cloudflare", 10, 5, cost=0.02)
        assert tracker.total_cost() == pytest.approx(0.02)

    def test_record_and_total(self, tracker):
        tracker.record("cloudflare", 500, 200, cost=0.01)
        tracker.record("glm", 300, 100, cost=0.005)
        assert tracker.total_cost() == pytest.approx(0.015)

    def test_record_call_auto_estimate(self, tracker):
        tracker.record_call(
            provider="cloudflare",
            messages=[{"role": "user", "content": "写一篇关于 Python 的文章"}],
            response="Python 是一种编程语言...",
        )
        assert tracker.total_cost() > 0

    def test_budget_not_exceeded(self, tracker):
        tracker.set_budget(10.0)
        tracker.record("cloudflare", 500, 200, cost=0.01)
        assert tracker.check_budget() is True

    def test_budget_exceeded(self, tracker):
        tracker.set_budget(0.005)
        tracker.record("cloudflare", 500, 200, cost=0.01)
        assert tracker.check_budget() is False

    def test_no_budget_unlimited(self, tracker):
        assert tracker.check_budget() is True

    def test_stats_by_provider(self, tracker):
        tracker.record("cloudflare", 500, 200, cost=0.01)
        tracker.record("cloudflare", 300, 100, cost=0.005)
        tracker.record("glm", 200, 50, cost=0.003)
        stats = tracker.stats()
        assert "cloudflare" in stats
        assert stats["cloudflare"]["calls"] == 2
        assert stats["glm"]["calls"] == 1

    def test_stats_by_task(self, tracker):
        tracker.record("cloudflare", 500, 200, cost=0.01, task_id="t1")
        tracker.record("glm", 300, 100, cost=0.005, task_id="t1")
        stats = tracker.stats_by_task("t1")
        assert stats["total_cost"] == pytest.approx(0.015)
        assert "cloudflare" in stats["by_provider"]
        assert "glm" in stats["by_provider"]

    def test_summary(self, tracker):
        tracker.set_budget(1.0)
        tracker.record("cloudflare", 500, 200, cost=0.01)
        s = tracker.summary()
        assert s["total_cost"] == pytest.approx(0.01)
        assert s["budget"] == 1.0
        assert s["budget_remaining"] == pytest.approx(0.99)
        assert s["budget_exceeded"] is False

    def test_cleanup(self, tracker):
        tracker.record("cloudflare", 500, 200, cost=0.01)
        tracker.cleanup(max_age_days=0)
        assert tracker.total_cost() == 0


class TestBudgetEnforce:
    def test_ensure_budget_raises_when_exhausted(self, tracker):
        tracker.set_budget(0.005)
        tracker.record("cloudflare", 500, 200, cost=0.01)
        with pytest.raises(BudgetExceededError):
            tracker.ensure_budget()

    def test_ensure_budget_passes_within_budget(self, tracker):
        tracker.set_budget(10.0)
        tracker.record("cloudflare", 500, 200, cost=0.01)
        tracker.ensure_budget()

    def test_ensure_budget_passes_without_budget(self, tracker):
        tracker.ensure_budget()

    def test_budget_error_is_runtime_error(self):
        assert issubclass(BudgetExceededError, RuntimeError)


class TestConcurrentRecord:
    def test_concurrent_record_total_exact(self, tracker):
        n_threads, per_thread, unit = 8, 25, 0.001
        barrier = threading.Barrier(n_threads)

        def worker():
            barrier.wait()
            for _ in range(per_thread):
                tracker.record("cloudflare", 10, 5, cost=unit)

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert tracker.total_cost() == pytest.approx(n_threads * per_thread * unit)

    def test_concurrent_check_budget_reads_consistent(self, tracker):
        tracker.set_budget(1.0)

        def worker():
            for _ in range(20):
                assert isinstance(tracker.check_budget(), bool)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()


class TestConnectionLifecycle:
    """回归：连接用完即关，不依赖 GC 回收。

    此前 `_get_conn` 每次新建连接且 `with conn` 只提交事务不关闭，
    全量测试累积数百条 ResourceWarning: unclosed database。
    """

    def test_all_connections_closed_after_ops(self, tmp_path, monkeypatch):
        import sqlite3

        import loom_core.db_backend as db_mod

        created = []
        real_connect = sqlite3.connect

        def _spy(*args, **kwargs):
            conn = real_connect(*args, **kwargs)
            created.append(conn)
            return conn

        monkeypatch.setattr(db_mod.sqlite3, "connect", _spy)
        t = CostTracker(db_path=str(tmp_path / "lifecycle.db"))
        t.record("openai", 100, 50, cost=0.001, task_id="t1")
        t.total_cost()
        t.stats()
        t.stats_by_task("t1")
        t.cleanup(max_age_days=9999)
        t.close()  # pool 缓存连接需显式关闭

        assert created, "应创建过 SQLite 连接"
        for conn in created:
            with pytest.raises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")  # 已关闭的连接不可再操作
