"""维护面的接线与降级判据（`maintenance.py` 的接线层，不是 cleanup 本身）。

`gc_runs` 的逐 store 清理语义已由 `tests/test_maintenance.py` 判过；这里补的是
**它上面那层**——而这一层此前在测试里从没被执行过：三处用例一律把
`gc_from_orchestrator` 换成替身，`_dlq_store` / `_run_cleanup` 零引用。
函数写对了但没被调用，等于接线不存在（同 `feedback-boundary-wiring-criterion`）。

判据只钉 docstring 里写明的契约：
- 配置写坏 → 回退到"不启用"，绝不悄悄变成"删光一切"；
- 某一个观测面取不到 → 只跳过它并告警，其余面照常回收；
- store 没有该清理方法 / 清理抛异常 → 记 failed，不向上抛。
"""
from __future__ import annotations

import hashlib
from types import SimpleNamespace

import pytest

from loom_core.content_digest import digest_of
from loom_core.deliveries import DeliveryLedger, _as_float_or_none
from loom_core.maintenance import (
    AGE_DISABLED,
    Retention,
    _dlq_store,
    _run_cleanup,
    gc_from_orchestrator,
    gc_interval_seconds,
    retention_from_config,
)


# ─── 1. 保留期配置：写坏不许变成"删光一切" ──────────────────────


class TestRetentionFromConfig:
    def test_missing_or_none_is_disabled(self):
        assert retention_from_config(None) == Retention(0, 0)
        assert retention_from_config({}) == Retention(0, 0)

    def test_non_numeric_values_fall_back_to_disabled(self):
        got = retention_from_config({"retention.runs_days": "abc",
                                     "retention.max_runs": None})
        assert got == Retention(0, 0)

    def test_negative_values_are_clamped_not_used(self):
        got = retention_from_config({"retention.runs_days": -5,
                                     "retention.max_runs": "-3"})
        assert got == Retention(0, 0)

    def test_valid_values_pass_through(self):
        got = retention_from_config({"retention.runs_days": "7",
                                     "retention.max_runs": 20})
        assert got == Retention(7, 20)
        assert got.age_days() == 7

    def test_disabled_age_is_far_future(self):
        assert Retention(0, 5).age_days() == AGE_DISABLED


# ─── 2. 常驻服务的节拍：0 是显式选择，读不懂回默认 ──────────────


class TestGcIntervalSeconds:
    @pytest.mark.parametrize("config,expected", [
        (None, 600),
        ({}, 600),
        ({"maintenance.gc_seconds": None}, 600),
        ({"maintenance.gc_seconds": "abc"}, 600),
        ({"maintenance.gc_seconds": 0}, 0),
        ({"maintenance.gc_seconds": -5}, 0),
        ({"maintenance.gc_seconds": "120"}, 120),
    ])
    def test_reads_with_sane_fallbacks(self, config, expected):
        assert gc_interval_seconds(config) == expected


# ─── 3. 死信表取自持久化总线，内存总线要答"没有这一面" ──────────


class TestDlqStore:
    def test_persistent_bus_store_is_used(self):
        store = SimpleNamespace(prune_dlq=lambda *a: 0)
        orch = SimpleNamespace(bus=SimpleNamespace(_store=store))
        assert _dlq_store(orch) is store

    def test_store_without_prune_is_not_mistaken_for_the_dlq(self):
        orch = SimpleNamespace(bus=SimpleNamespace(_store=SimpleNamespace()))
        assert _dlq_store(orch) is None

    def test_in_memory_bus_has_no_store(self):
        assert _dlq_store(SimpleNamespace(bus=SimpleNamespace())) is None
        assert _dlq_store(SimpleNamespace()) is None


# ─── 4. 单个 store 的清理失败只记账不抛 ─────────────────────────


class TestRunCleanup:
    def test_missing_store_is_skipped(self):
        assert _run_cleanup(None, "cleanup", (0,), "task_queue") is None

    def test_store_without_the_method_reports_failed(self):
        got = _run_cleanup(SimpleNamespace(), "cleanup", (0,), "task_queue")
        assert got["failed"] is True
        assert got["store"] == "task_queue"
        assert "cleanup" in got["message"]

    def test_exception_is_captured_with_its_message(self):
        def boom(*_args):
            raise RuntimeError("db closed")
        got = _run_cleanup(SimpleNamespace(cleanup=boom), "cleanup", (0,), "cost")
        assert got == {"failed": True, "store": "cost", "message": "db closed"}

    def test_dict_result_passes_through(self):
        payload = {"per_table": {"a": 2}}
        got = _run_cleanup(SimpleNamespace(cleanup=lambda *a: payload),
                           "cleanup", (0,), "run_store")
        assert got is payload

    def test_scalar_results_normalise_to_int(self):
        assert _run_cleanup(SimpleNamespace(cleanup=lambda *a: 5),
                            "cleanup", (0,), "x") == 5
        assert _run_cleanup(SimpleNamespace(cleanup=lambda *a: None),
                            "cleanup", (0,), "x") == 0


# ─── 5. 组装层：RunStore 取不到就跳过并告警，其余面照常 ─────────


def _capture_gc(monkeypatch):
    """把 gc_runs 换成记录 kwargs 的替身，并把 DeliveryLedger 换成哨兵。"""
    import loom_core.deliveries as deliveries_mod
    import loom_core.maintenance as maintenance_mod

    captured: dict = {}
    sentinel_ledger = SimpleNamespace(name="delivery-ledger")

    def fake_gc_runs(**kwargs):
        captured.update(kwargs)
        return {"enabled": True, "patched": True}

    monkeypatch.setattr(maintenance_mod, "gc_runs", fake_gc_runs)
    monkeypatch.setattr(deliveries_mod, "DeliveryLedger", lambda: sentinel_ledger)
    return captured


class TestGcFromOrchestrator:
    def test_runstore_failure_is_logged_and_skipped(self, monkeypatch):
        captured = _capture_gc(monkeypatch)
        logs: list[str] = []

        def broken():
            raise RuntimeError("run store unavailable")

        queue = SimpleNamespace(name="queue")
        orch = SimpleNamespace(
            config={"retention.runs_days": "7"},
            task_queue=queue,
            _runstore=broken,
        )
        summary = gc_from_orchestrator(orch, now=123.0, log=logs.append)

        assert summary["patched"] is True
        assert captured["run_store"] is None          # 只跳过这一面
        assert captured["queue"] is queue             # 其余面照常接上
        assert captured["retention"].days == 7
        assert captured["now"] == 123.0
        assert any("RunStore 取用失败" in line for line in logs)

    def test_healthy_runstore_is_passed_through(self, monkeypatch):
        captured = _capture_gc(monkeypatch)
        store = SimpleNamespace(name="run-store")
        orch = SimpleNamespace(config={}, task_queue=None,
                               _runstore=lambda: store)
        gc_from_orchestrator(orch)
        assert captured["run_store"] is store
        assert captured["retention"] == Retention(0, 0)


# ─── 6. 摘要编码形态与账本拒收 ───────────────────────────────────


class TestDigestEncoding:
    def test_plain_text_hashes_its_utf8_bytes(self):
        got = digest_of("你好")
        assert got == {"sha256": hashlib.sha256("你好".encode()).hexdigest(),
                       "bytes": 6, "kind": "text"}

    def test_key_order_does_not_change_the_digest(self):
        assert digest_of({"b": 1, "a": 2}) == digest_of({"a": 2, "b": 1})

    def test_unencodable_value_falls_back_to_repr(self):
        circular: list = []
        circular.append(circular)
        got = digest_of(circular)
        expected = repr(circular).encode("utf-8")
        assert got["kind"] == "json"
        assert got["bytes"] == len(expected)
        assert got["sha256"] == hashlib.sha256(expected).hexdigest()


class TestLedgerInputContracts:
    def test_row_without_run_id_is_rejected(self, tmp_path):
        ledger = DeliveryLedger(db_path=str(tmp_path / "deliveries.db"))
        with pytest.raises(ValueError, match="run_id"):
            ledger.record({"pipeline": "demo", "status": "done"})

    def test_empty_ledger_lists_nothing(self, tmp_path):
        ledger = DeliveryLedger(db_path=str(tmp_path / "deliveries.db"))
        assert ledger.list() == []

    def test_non_numeric_duration_reads_as_none(self):
        assert _as_float_or_none(None) is None
        assert _as_float_or_none("not-a-number") is None
        assert _as_float_or_none("3.5") == 3.5
