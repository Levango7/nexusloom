"""Doc-Pipeline 集成测试共享 fixtures"""
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

# 共享路径（先于 loom_core 导入定义，供下面的状态隔离使用）
HERE = Path(__file__).parent
PROJECT = HERE.parent
AGENTS_DIR = str(PROJECT / "agents")
PIPELINES_DIR = str(PROJECT / "pipelines")
CHECKPOINT_DIR = PROJECT / ".test_checkpoints"
OUTPUT_DIR = PROJECT / ".test_outputs"

# ── 状态隔离：测试不许写进 checkout 的真实运行态 ──
# bus_data/（幂等键 + 消息 + 任务队列 + 成本 + 质量反馈）与 versions/ 此前被
# 测试直接落在仓库里，两个后果都是实测过的：
#   1. 幂等键 `{task_id}:{node}:{attempts}` 跨进程共享，同名 task_id 再跑一次
#      会命中历史记录 → 节点静默空转却逐个记 success；
#   2. versions/ 积了 260+ 个指向 .pytest_tmp 的死条目，
#      /api/versions/stats 因此慢到 4.9s（客户端 5s 超时就翻成测试失败）。
# 这些默认值在**导入期**解析，所以环境变量必须在 import loom_core 之前设置。
_STATE_DIR = PROJECT / ".test_state" / f"pid{os.getpid()}"
# 目录按 pid 命名 ⇒ 里面只可能是本进程的数据；但 Windows 会回收 pid，上个会话
# 残留的同名目录里可能还躺着 tasks.db / runs.db / 老 approvals.db 的行，而测试里的
# run_id 是写死的（`run_a` 这种）——回落读会把它们当本会话的事实。会话开始先清。
if _STATE_DIR.exists():
    shutil.rmtree(str(_STATE_DIR), ignore_errors=True)
_STATE_DIR.mkdir(parents=True, exist_ok=True)
os.environ["LOOM_STATE_DIR"] = str(_STATE_DIR)
os.environ["LOOM_VERSIONS_DIR"] = str(_STATE_DIR / "versions")

import contextlib  # noqa: E402

from loom_core import PipelineOrchestrator  # noqa: E402
from loom_core.circuit_breaker import CircuitBreakerRegistry  # noqa: E402
from loom_core.message_bus_v3 import MessageBus  # noqa: E402
from loom_core.rate_limiter import RateLimiterRegistry  # noqa: E402
from loom_core.scheduler import Scheduler  # noqa: E402

# ── 覆盖 pytest 内置 tmp_path，避免 Windows Temp 权限问题 ──
_LOCAL_TMP = PROJECT / ".pytest_tmp"
_tmp_counter = [0]


@pytest.fixture
def tmp_path():
    """使用项目本地 .pytest_tmp 目录替代系统 Temp，规避 WinError 5 权限拒绝"""
    _tmp_counter[0] += 1
    d = _LOCAL_TMP / f"tmp_{os.getpid()}_{_tmp_counter[0]}"
    d.mkdir(parents=True, exist_ok=True)
    yield d
    # 清理：best-effort
    with contextlib.suppress(Exception):
        shutil.rmtree(str(d), ignore_errors=True)


@pytest.fixture(autouse=True)
def clean_checkpoints():
    """每个测试前清理检查点和输出"""
    for d in [CHECKPOINT_DIR, OUTPUT_DIR]:
        if d.exists():
            with contextlib.suppress(OSError):
                shutil.rmtree(str(d))  # Windows 文件锁时忽略
        d.mkdir(parents=True, exist_ok=True)
    yield


# ── MessageBus fixtures ──

@pytest.fixture
def bus():
    """SQLite MessageBus v3（每个测试独立 DB 文件）"""
    db = os.path.join(tempfile.mkdtemp(), "test_bus.db")
    b = MessageBus(db_path=str(db))
    # v3 线程在 __init__ 自动启动
    yield b
    with contextlib.suppress(Exception):
        b.shutdown()


@pytest.fixture
def bus_dlq(bus):
    """带 DLQ 的 MessageBus"""
    # bus already has DLQ active via start()
    return bus


# ── 回调辅助 ──

@pytest.fixture
def collector():
    """收集所有收到的消息"""
    msgs = []

    def cb(msg):
        msgs.append(msg)
    return cb, msgs


@pytest.fixture
def wait_until():
    """轮询等待条件成立，替代「固定 sleep 后断言」的时序敏感写法。

    用法：wait_until(lambda: len(msgs) == 2)
    条件在 timeout 内成立即返回 True；超时返回最后一次求值结果（多为 False）。
    """

    def _wait(cond, timeout: float = 3.0, interval: float = 0.01):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if cond():
                return True
            time.sleep(interval)
        return cond()

    return _wait


# ── Registry fixtures ──

@pytest.fixture
def circuit_breakers():
    """熔断器注册表"""
    return CircuitBreakerRegistry()


@pytest.fixture
def rate_limiters():
    """限流器注册表"""
    return RateLimiterRegistry()


# ── Scheduler fixture ──

@pytest.fixture
def scheduler():
    return Scheduler()


@pytest.fixture
def docgen_plan(scheduler):
    """解析测试用 pipeline（mock 引擎，无网络）"""
    plan = scheduler.parse_file(str(PROJECT / "pipelines" / "test_pipeline.yaml"))
    return plan


# ── Orchestrator fixture ──

@pytest.fixture
def orch():
    """完整初始化的 PipelineOrchestrator"""
    o = PipelineOrchestrator(
        agents_dir=AGENTS_DIR,
        checkpoint_dir=str(CHECKPOINT_DIR),
    )
    o.register_agents()
    yield o
    # 释放 bus/task_queue/registry 持有的线程与 SQLite 连接，避免测试间泄漏
    with contextlib.suppress(Exception):
        o.shutdown()
