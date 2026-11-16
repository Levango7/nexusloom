"""测试新增 API 端点：/api/config, /api/health/deep, /api/cache, /api/agents/<name>, /api/events/hooks"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from loom_core.event_hook import EventHookManager, emit_event, get_hook_manager

# ── EventHook 系统测试 ──────────────────────────

class TestEventHook:
    """EventHook 事件钩子系统"""

    def test_register_and_list(self):
        mgr = EventHookManager()
        hid = mgr.register("task.completed", url="http://example.com/hook")
        hooks = mgr.list_hooks()
        assert len(hooks) == 1
        assert hooks[0]["id"] == hid
        assert hooks[0]["event"] == "task.completed"
        assert hooks[0]["url"] == "http://example.com/hook"

    def test_unregister(self):
        mgr = EventHookManager()
        hid = mgr.register("task.failed", url="http://example.com/fail")
        assert mgr.unregister(hid) is True
        assert len(mgr.list_hooks()) == 0
        assert mgr.unregister("nonexistent") is False

    def test_emit_callback(self):
        mgr = EventHookManager()
        results = []
        mgr.register("task.created", callback=lambda evt, pl: results.append((evt, pl)))
        count = mgr.emit("task.created", {"task_id": "t1"})
        assert count == 1
        assert len(results) == 1
        assert results[0] == ("task.created", {"task_id": "t1"})

    def test_emit_wildcard(self):
        mgr = EventHookManager()
        results = []
        mgr.register("*", callback=lambda evt, pl: results.append(evt))
        mgr.emit("task.created", {})
        mgr.emit("agent.started", {})
        mgr.emit("circuit_breaker.open", {})
        assert len(results) == 3

    def test_emit_prefix_wildcard(self):
        mgr = EventHookManager()
        results = []
        mgr.register("task.*", callback=lambda evt, pl: results.append(evt))
        mgr.emit("task.created", {})
        mgr.emit("task.completed", {})
        mgr.emit("agent.started", {})  # 不匹配
        assert len(results) == 2
        assert "agent.started" not in results

    def test_emit_no_match(self):
        mgr = EventHookManager()
        mgr.register("task.completed", url="http://example.com")
        count = mgr.emit("agent.started", {})
        assert count == 0

    def test_disable_enable(self):
        mgr = EventHookManager()
        results = []
        hid = mgr.register("task.created", callback=lambda evt, pl: results.append(evt))
        mgr.disable(hid)
        assert mgr.emit("task.created", {}) == 0
        mgr.enable(hid)
        assert mgr.emit("task.created", {}) == 1

    def test_call_count_tracking(self):
        mgr = EventHookManager()
        mgr.register("task.created", callback=lambda evt, pl: None)
        mgr.emit("task.created", {})
        mgr.emit("task.created", {})
        hooks = mgr.list_hooks()
        assert hooks[0]["call_count"] == 2

    def test_hook_to_dict_excludes_callback(self):
        mgr = EventHookManager()
        mgr.register("task.created", callback=lambda evt, pl: None)
        hooks = mgr.list_hooks()
        assert "callback" not in hooks[0]

    def test_clear(self):
        mgr = EventHookManager()
        mgr.register("task.created", url="http://example.com")
        mgr.register("task.failed", url="http://example.com")
        mgr.clear()
        assert len(mgr.list_hooks()) == 0

    def test_global_singleton(self):
        mgr1 = get_hook_manager()
        mgr2 = get_hook_manager()
        assert mgr1 is mgr2

    def test_emit_event_helper(self):
        mgr = get_hook_manager()
        mgr.clear()
        results = []
        mgr.register("test.event", callback=lambda evt, pl: results.append(pl))
        emit_event("test.event", {"data": 42})
        assert len(results) == 1
        assert results[0]["data"] == 42
        mgr.clear()


# ── Admin API 新端点测试 ──────────────────────────

class TestAdminAPIEndpoints:
    """新增 API 端点集成测试"""

    @pytest.fixture
    def orch(self):
        from pathlib import Path

        from loom_core import PipelineOrchestrator
        agents_dir = str(Path(__file__).parent.parent / "agents")
        ckpt_dir = str(Path(__file__).parent.parent / ".test_checkpoints")
        o = PipelineOrchestrator(agents_dir=agents_dir, checkpoint_dir=ckpt_dir)
        o.register_agents()
        yield o

    def test_config_get(self, orch):
        """GET /api/config 返回配置字典"""
        config = orch.config.to_dict()
        assert isinstance(config, dict)
        assert "llm" in config
        assert "bus" in config

    def test_config_set(self, orch):
        """POST /api/config 更新配置"""
        old = orch.config.get("llm.model")
        orch.config.set("llm.model", "test-model-123")
        assert orch.config.get("llm.model") == "test-model-123"
        # 恢复
        orch.config.set("llm.model", old)

    def test_agent_detail(self, orch):
        """GET /api/agents/<name> 返回 agent 详情"""
        agents = orch.registry.list()
        if not agents:
            pytest.skip("No agents registered")
        agent_name = agents[0].get("name", "") if isinstance(agents[0], dict) else str(agents[0])
        # registry._agents 存的是 dict（AgentMeta 转 dict）
        agent_meta = orch.registry._agents.get(agent_name)
        assert agent_meta is not None
        # 可能是 dict 或 dataclass
        if isinstance(agent_meta, dict):
            assert agent_meta.get("name") == agent_name
        else:
            assert getattr(agent_meta, "name", None) == agent_name

    def test_agent_detail_not_found(self, orch):
        """GET /api/agents/nonexistent 返回 404"""
        agent = orch.registry._agents.get("nonexistent_agent")
        assert agent is None

    def test_cache_stats(self, orch):
        """GET /api/cache 返回缓存统计"""
        from loom_core.cache_manager import all_stats
        stats = all_stats()
        assert isinstance(stats, dict)

    def test_cache_clear(self, orch):
        """POST /api/cache/clear 清空缓存"""
        from loom_core.cache_manager import all_stats, clear_all_caches
        clear_all_caches()
        stats = all_stats()
        assert isinstance(stats, dict)

    def test_health_deep(self, orch):
        """GET /api/health/deep 返回全组件健康状态"""
        # 模拟 _handle_health_deep 的逻辑
        result = {"components": {}}
        # message_bus
        bus_h = orch.bus.health()
        result["components"]["message_bus"] = {
            "status": "healthy" if bus_h.get("running") else "unhealthy",
        }
        # registry
        agents = orch.registry.list()
        result["components"]["registry"] = {
            "status": "healthy" if len(agents) > 0 else "degraded",
            "agent_count": len(agents),
        }
        # checkpoint
        result["components"]["checkpoint"] = {
            "status": "healthy" if orch.checkpoint_dir.exists() else "degraded",
        }
        # 真总线 + 真判据：alive 的 bus 必须判 healthy。旧断言
        # `in ("healthy", "unhealthy")` 是恒真形态，2026-11-17 交付演练 D1 断言之——
        # 它掩盖了 bus.health() 缺 running 键、组件恒 unhealthy 的缺陷。
        assert result["components"]["message_bus"]["status"] == "healthy"
        assert result["components"]["registry"]["agent_count"] > 0

    def test_hook_register_via_api(self):
        """POST /api/events/hooks 注册钩子"""
        mgr = get_hook_manager()
        mgr.clear()
        mgr.register("task.completed", url="http://test.example.com/hook")
        hooks = mgr.list_hooks()
        assert len(hooks) == 1
        assert hooks[0]["url"] == "http://test.example.com/hook"
        mgr.clear()

    def test_hook_unregister_via_api(self):
        """DELETE /api/events/hooks/<id> 注销钩子"""
        mgr = get_hook_manager()
        mgr.clear()
        hid = mgr.register("task.failed", url="http://test.example.com")
        ok = mgr.unregister(hid)
        assert ok is True
        assert len(mgr.list_hooks()) == 0
