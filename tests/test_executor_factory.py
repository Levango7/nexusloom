"""执行器工厂 + ProcessPoolExecutor pickle 兼容性验证 + SmartExecutor 智能切换。"""
import pickle
import time
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool

import pytest

import loom_core.executor_factory as ef
from loom_core.executor_factory import (
    SmartExecutor,
    _estimate_task_cost,
    _is_picklable,
    create_executor,
    is_process_executor,
)

# ── 模块级函数（可 pickle，供 ProcessPoolExecutor 提交） ──

def _square(x: int) -> int:
    return x * x


def _sleep_window(sec: float) -> dict:
    """睡 sec 秒并回报**自己的起止窗口**——判断"真并行"要看同一时刻有几个在跑。"""
    start = time.time()
    time.sleep(sec)
    return {"start": start, "end": time.time(), "sec": sec}


def _max_concurrent(windows: list) -> int:
    """扫描线求最大并发数（窗口重叠的峰值）。"""
    events = []
    for w in windows:
        events.append((w["start"], 1))
        events.append((w["end"], -1))
    events.sort()
    cur = peak = 0
    for _, delta in events:
        cur += delta
        peak = max(peak, cur)
    return peak


# ═══════════════════════════════════════════════════════════

class TestExecutorFactory:
    """执行器工厂基础功能"""

    def test_create_thread_executor(self):
        executor = create_executor(max_workers=4, executor_type="thread")
        assert isinstance(executor, ThreadPoolExecutor)
        executor.shutdown()

    def test_create_process_executor(self):
        executor = create_executor(max_workers=4, executor_type="process")
        assert isinstance(executor, ProcessPoolExecutor)
        executor.shutdown()

    def test_default_is_thread(self):
        executor = create_executor(max_workers=2)
        assert isinstance(executor, ThreadPoolExecutor)
        executor.shutdown()

    def test_is_process_executor_helper(self):
        tp = create_executor(max_workers=2, executor_type="thread")
        pp = create_executor(max_workers=2, executor_type="process")
        assert not is_process_executor(tp)
        assert is_process_executor(pp)
        tp.shutdown()
        pp.shutdown()

    def test_unknown_type_defaults_to_thread(self):
        executor = create_executor(max_workers=2, executor_type="unknown")
        assert isinstance(executor, ThreadPoolExecutor)
        executor.shutdown()


class TestProcessPoolExecution:
    """ProcessPoolExecutor 实际执行验证"""

    def test_module_level_function_executes(self):
        """模块级函数可在 ProcessPoolExecutor 中执行"""
        with ProcessPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(_square, i) for i in range(5)]
            results = [f.result() for f in futures]
        assert results == [0, 1, 4, 9, 16]

    def test_parallel_execution_actually_overlaps(self):
        """进程池的真并行 = 任务窗口重叠，不是"比串行快几倍"。

        旧断言 `parallel_time < serial_time * 3.0`（n=8、sleep=0.3 ⇒ 预算 7.21s）两头都不成立：
        - **零射程**：把池人为串行化（`max_workers=1`）实测 parallel_time=2.73s < 7.21s，
          照样绿——它声称"防止退化"却测不出退化本身（本机实测 2026-10-21）；
        - **假阳性**：Windows 上 4 个进程的 spawn 开销算进 parallel_time，
          机器并行负载下超预算就红（本机全量 34 min 那次红在这条，单跑 3 次全绿）。
        窗口峰值只看"同一时刻有几个在跑"：负载让 spawn 变慢，但不会伪造重叠，
        也不会因为慢而误红。
        """
        n, workers, sec = 8, 4, 1.5
        with ProcessPoolExecutor(max_workers=workers) as pool:
            windows = [f.result() for f in
                       [pool.submit(_sleep_window, sec) for _ in range(n)]]

        assert [w["sec"] for w in windows] == [sec] * n, "有任务没跑完或返回值不对"
        peak = _max_concurrent(windows)
        assert peak >= 2, f"窗口没有重叠，池退化成串行执行：peak={peak} windows={windows}"

    def test_thread_pool_also_works(self):
        """ThreadPoolExecutor 也能执行模块级函数"""
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(_square, i) for i in range(5)]
            results = [f.result() for f in futures]
        assert results == [0, 1, 4, 9, 16]


class TestDAGExecutorPickleCompatibility:
    """DAGExecutor pickle 兼容性验证"""

    def test_execute_node_worker_is_module_level(self):
        """_execute_node_worker 必须是模块级函数（可 pickle）"""
        import types

        from loom_core.dag_executor import _execute_node_worker

        # 模块级函数的 __qualname__ 不含 '.'（不是 bound method）
        assert "." not in _execute_node_worker.__qualname__
        # 确认是函数而非 bound method
        assert not isinstance(_execute_node_worker, types.MethodType)

    def test_execute_node_worker_picklable(self):
        """_execute_node_worker 可被 pickle 序列化"""
        from loom_core.dag_executor import _execute_node_worker

        # 不应抛出 pickle 错误
        data = pickle.dumps(_execute_node_worker)
        restored = pickle.loads(data)
        assert restored is _execute_node_worker

    def test_dag_executor_getstate_setstate(self):
        """DAGExecutor __getstate__/__setstate__ 剥离非可序列化属性"""
        from loom_core.dag_executor import DAGExecutor

        # 验证 __getstate__ 和 __setstate__ 方法存在
        assert hasattr(DAGExecutor, "__getstate__")
        assert hasattr(DAGExecutor, "__setstate__")

        # 验证 from_config 类方法存在且可调用
        assert hasattr(DAGExecutor, "from_config")
        assert callable(DAGExecutor.from_config)

    def test_dag_executor_state_excludes_non_picklable(self):
        """__getstate__ 应排除 registry/bus 等非可序列化属性"""
        from loom_core.dag_executor import DAGExecutor

        # 检查 _NON_PICKLABLE_ATTRS 属性列表存在
        assert hasattr(DAGExecutor, "_NON_PICKLABLE_ATTRS")
        non_picklable = DAGExecutor._NON_PICKLABLE_ATTRS
        assert isinstance(non_picklable, (list, tuple, set))
        # 应包含 registry 和 bus
        assert "registry" in non_picklable
        assert "bus" in non_picklable


# ── SmartExecutor 测试辅助函数 ──

def _cpu_intensive_task(n: int) -> int:
    """CPU 密集型任务（名称含 compute 关键词，估算为高成本）"""
    total = 0
    for i in range(n * 100_000):
        total += i * i
    return total


def _io_task(url: str) -> str:
    """I/O 密集型任务（名称含 fetch 关键词，估算为低成本）"""
    time.sleep(0.001)
    return url


def _fetch_page(url: str) -> str:
    """另一个 I/O 任务"""
    return f"fetched:{url}"


def _score_documents(docs: list) -> float:
    """CPU 密集型（名称含 score 关键词）"""
    return sum(len(d) for d in docs)


# ═══════════════════════════════════════════════════════════

class TestSmartExecutor:
    """SmartExecutor 智能切换功能验证"""

    def test_create_auto_returns_smart_executor(self):
        """executor_type='auto' 返回 SmartExecutor 实例"""
        executor = create_executor(max_workers=4, executor_type="auto")
        assert isinstance(executor, SmartExecutor)
        executor.shutdown()

    def test_smart_executor_context_manager(self):
        """SmartExecutor 支持上下文管理器"""
        with SmartExecutor(max_workers=2) as pool:
            future = pool.submit(_square, 5)
            assert future.result() == 25

    def test_io_task_uses_thread_pool(self):
        """I/O 密集型任务应使用 ThreadPool（不使用 ProcessPool）"""
        with SmartExecutor(max_workers=2) as pool:
            future = pool.submit(_fetch_page, "http://example.com")
            result = future.result()
            assert result == "fetched:http://example.com"
            assert not pool.used_process

    def test_cpu_task_uses_process_pool(self):
        """CPU 密集型任务应使用 ProcessPool"""
        with SmartExecutor(max_workers=2, cost_threshold=0.01) as pool:
            future = pool.submit(_cpu_intensive_task, 5)
            result = future.result()
            assert result > 0
            assert pool.used_process

    def test_map_with_io_tasks(self):
        """map() 批量提交 I/O 任务使用 ThreadPool"""
        urls = [f"http://ex{i}.com" for i in range(5)]
        with SmartExecutor(max_workers=3) as pool:
            results = list(pool.map(_fetch_page, urls))
        assert results == [f"fetched:{u}" for u in urls]

    def test_map_with_cpu_tasks(self):
        """map() 批量提交 CPU 密集任务使用 ProcessPool"""
        with SmartExecutor(max_workers=2, cost_threshold=0.01) as pool:
            results = list(pool.map(_cpu_intensive_task, [2, 3, 4]))
        assert all(r > 0 for r in results)
        assert pool.used_process

    def test_process_pool_reuse(self):
        """多次使用 SmartExecutor 应复用同一个 ProcessPool"""
        with SmartExecutor(max_workers=2, cost_threshold=0.01) as pool1:
            r1 = pool1.submit(_cpu_intensive_task, 2).result()
            assert pool1.used_process

        with SmartExecutor(max_workers=2, cost_threshold=0.01) as pool2:
            r2 = pool2.submit(_cpu_intensive_task, 2).result()
            assert pool2.used_process

        # 结果一致（相同输入）
        assert r1 == r2

    def test_mixed_tasks(self):
        """混合 I/O 和 CPU 任务"""
        with SmartExecutor(max_workers=4, cost_threshold=0.01) as pool:
            io_future = pool.submit(_fetch_page, "http://test.com")
            cpu_future = pool.submit(_cpu_intensive_task, 3)

            io_result = io_future.result()
            cpu_result = cpu_future.result()

        assert io_result == "fetched:http://test.com"
        assert cpu_result > 0

    def test_shutdown_cleans_up_thread_pool(self):
        """shutdown() 关闭线程池但不关闭复用的 ProcessPool"""
        pool = SmartExecutor(max_workers=2)
        pool.submit(_square, 3).result()
        pool.shutdown()
        # 再次使用应正常（创建新线程池）
        pool.submit(_square, 4).result()
        pool.shutdown()


class TestCostEstimation:
    """任务成本估算函数验证"""

    def test_cpu_keyword_estimates_high(self):
        """含 CPU 关键词的函数估算为高成本"""
        cost = _estimate_task_cost(_cpu_intensive_task, (), {})
        assert cost >= 1.0

    def test_io_keyword_estimates_low(self):
        """含 I/O 关键词的函数估算为低成本"""
        cost = _estimate_task_cost(_fetch_page, (), {})
        assert cost <= 0.01

    def test_score_keyword_estimates_high(self):
        """score 关键词估算为高成本"""
        cost = _estimate_task_cost(_score_documents, (), {})
        assert cost >= 1.0

    def test_custom_cost_attribute(self):
        """__cost_estimate__ 属性优先使用"""
        def custom_task():
            pass
        custom_task.__cost_estimate__ = 5.0
        cost = _estimate_task_cost(custom_task, (), {})
        assert cost == 5.0

    def test_default_estimate_for_unknown(self):
        """未知函数默认中性估计"""
        def unknown_task():
            pass
        cost = _estimate_task_cost(unknown_task, (), {})
        assert 0.01 < cost < 1.0


class TestPicklableCheck:
    """_is_picklable 函数验证"""

    def test_module_level_function_is_picklable(self):
        """模块级函数可 pickle"""
        assert _is_picklable(_square)

    def test_lambda_not_picklable(self):
        """lambda 不可 pickle"""
        def f(x):
            return x * 2
        assert not _is_picklable(f)

    def test_local_function_not_picklable(self):
        """局部定义的函数不可 pickle"""
        def local_func(x):
            return x + 1
        assert not _is_picklable(local_func)


# ── BrokenProcessPool 自愈测试辅助 ──

class _FakeProcessPool(ProcessPoolExecutor):
    """不发子进程的假进程池：可配置前 N 次 submit/map 抛 BrokenProcessPool"""

    def __init__(self, max_workers: int = 4, fail_times: int = 0):
        self.max_workers = max_workers
        self._fail_times = fail_times
        self.submit_count = 0

    def submit(self, fn, *args, **kwargs):
        self.submit_count += 1
        if self._fail_times > 0:
            self._fail_times -= 1
            raise BrokenProcessPool()
        future: Future = Future()
        try:
            future.set_result(fn(*args, **kwargs))
        except Exception as exc:
            future.set_exception(exc)
        return future

    def map(self, fn, *iterables, timeout=None, chunksize=1):
        if self._fail_times > 0:
            self._fail_times -= 1
            raise BrokenProcessPool()
        return iter([fn(*combo) for combo in zip(*iterables, strict=True)])

    def shutdown(self, wait: bool = True, cancel_futures: bool = False):
        pass


class TestBrokenProcessPoolRecovery:
    """进程池单例 BrokenProcessPool 自愈（防 P1 回归）"""

    @staticmethod
    def _make_factory(created: list, fail_first: int = 0, fail_all: int | None = None):
        """模拟单例语义的工厂：缓存当前实例；fail_all 时每个新实例都持续失败"""
        def _factory(max_workers: int = 4) -> _FakeProcessPool:
            existing = ef._process_pool_singleton
            if existing is not None:
                return existing
            pool = _FakeProcessPool(max_workers=max_workers,
                                    fail_times=fail_all if fail_all is not None
                                    else (fail_first if not created else 0))
            ef._process_pool_singleton = pool
            created.append(pool)
            return pool
        return _factory

    def test_submit_rebuilds_once_and_retries_success(self, monkeypatch):
        created: list[_FakeProcessPool] = []
        monkeypatch.setattr(ef, "_get_or_create_process_pool", self._make_factory(created, 1))
        monkeypatch.setattr(ef, "_process_pool_singleton", None)

        with SmartExecutor(max_workers=2, cost_threshold=0.01) as pool:
            result = pool.submit(_square, 6).result()

        assert result == 36
        assert pool.used_process
        assert len(created) == 2
        assert created[0].submit_count == 1
        assert created[1].submit_count == 1

    def test_submit_second_failure_raises(self, monkeypatch):
        created: list[_FakeProcessPool] = []
        monkeypatch.setattr(ef, "_get_or_create_process_pool", self._make_factory(created, fail_all=999))
        monkeypatch.setattr(ef, "_process_pool_singleton", None)

        executor = SmartExecutor(max_workers=2, cost_threshold=0.01)
        with pytest.raises(BrokenProcessPool):
            executor.submit(_square, 6)

        assert len(created) == 2

    def test_map_rebuilds_and_retries(self, monkeypatch):
        created: list[_FakeProcessPool] = []
        monkeypatch.setattr(ef, "_get_or_create_process_pool", self._make_factory(created, 1))
        monkeypatch.setattr(ef, "_process_pool_singleton", None)

        with SmartExecutor(max_workers=2, cost_threshold=0.01) as pool:
            results = list(pool.map(_square, [2, 3, 4]))

        assert results == [4, 9, 16]
        assert len(created) == 2

    def test_recovered_singleton_is_reused_after_rebuild(self, monkeypatch):
        created: list[_FakeProcessPool] = []
        monkeypatch.setattr(ef, "_get_or_create_process_pool", self._make_factory(created, 1))
        monkeypatch.setattr(ef, "_process_pool_singleton", None)

        with SmartExecutor(max_workers=2, cost_threshold=0.01) as pool:
            pool.submit(_square, 2).result()
            pool.submit(_square, 3).result()

        assert len(created) == 2
        assert created[1].submit_count == 2
