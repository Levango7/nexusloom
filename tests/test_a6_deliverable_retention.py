"""A6 逐产物保留期判据（2026-11-26 认领）。

治的是什么：GC 此前只回收账本/运行记录，磁盘交付物按"用户所有、
有意保留"原则不删——但磁盘无界增长是真实运维风险，且原文
A6 验收判据要求"GC 只删过期产物"。

本批把"删不删用户文件"做成**显式判据**而不是默认行为：
- 写侧只在显式配置 ``retention.deliverables_days > 0`` 时填
  ``retention_until``；
- GC 只删**带过期 ``retention_until``** 的行的磁盘交付物；
  没有 ``retention_until`` 的行（历史行 / 未配部署）只删账本行、
  不碰磁盘——这是"清理过期记录"不意外外溢成"清理用户产物"的
  安全边界。

判据用假时钟（``now`` 注入），不依赖真实时间。
"""
from __future__ import annotations

from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parent.parent


@pytest.fixture()
def state(tmp_path, monkeypatch):
    d = tmp_path / "state"
    d.mkdir()
    monkeypatch.setenv("LOOM_STATE_DIR", str(d))
    return d


def _led(state):
    from loom_core.deliveries import DeliveryLedger
    return DeliveryLedger()


def _seed(led, run_id, *, finished_at, status="done",
          output_path=None, retention_until=None):
    led.record({"run_id": run_id, "pipeline": "p", "status": status,
                "finished_at": float(finished_at),
                "duration_sec": 1.0,
                "output_path": output_path or f"{run_id}.md",
                "retention_until": retention_until,
                "nodes": []})


class TestDeliverableRetention:
    def test_expired_retention_deletes_file_and_row(self, state, tmp_path):
        """带过期 retention_until 的行：账本行 + 磁盘文件一起删。"""
        doc = tmp_path / "expired.md"
        doc.write_text("# 交付\n正文", encoding="utf-8")
        led = _led(state)
        now = 10_000_000.0
        _seed(led, "expired", finished_at=now - 100 * 86400,
              output_path=str(doc), retention_until=now - 1)
        assert led.cleanup(retention_days=30, now=now) == 1
        assert led.get("expired") is None
        assert not doc.exists(), "过期产物必须被 GC 删除"

    def test_no_retention_until_keeps_file(self, state, tmp_path):
        """没有 retention_until 的历史行：只删账本行，磁盘文件保留。"""
        doc = tmp_path / "legacy.md"
        doc.write_text("# 交付\n正文", encoding="utf-8")
        led = _led(state)
        now = 10_000_000.0
        _seed(led, "legacy", finished_at=now - 100 * 86400,
              output_path=str(doc))
        assert led.cleanup(retention_days=30, now=now) == 1
        assert led.get("legacy") is None
        assert doc.exists(), "未配保留期的产物不许被 GC 删"

    def test_future_retention_keeps_file(self, state, tmp_path):
        """retention_until 未过期：账本行被年龄判据删，磁盘文件保留。"""
        doc = tmp_path / "fresh.md"
        doc.write_text("# 交付\n正文", encoding="utf-8")
        led = _led(state)
        now = 10_000_000.0
        _seed(led, "fresh", finished_at=now - 100 * 86400,
              output_path=str(doc), retention_until=now + 86400)
        assert led.cleanup(retention_days=30, now=now) == 1
        assert led.get("fresh") is None
        assert doc.exists(), "保留期未到的产物不许被 GC 删"

    def test_missing_file_does_not_break_cleanup(self, state, tmp_path):
        """output_path 指向的文件已不在：删除不炸，行照删。"""
        led = _led(state)
        now = 10_000_000.0
        _seed(led, "gone", finished_at=now - 100 * 86400,
              output_path=str(tmp_path / "missing.md"),
              retention_until=now - 1)
        assert led.cleanup(retention_days=30, now=now) == 1
        assert led.get("gone") is None

    def test_retention_until_persisted_in_record(self, state):
        """写侧填的 retention_until 能从账本读回。"""
        led = _led(state)
        _seed(led, "r", finished_at=100.0, retention_until=200.0)
        rec = led.get("r")
        assert rec is not None
        assert rec.get("retention_until") == 200.0


class TestRetentionConfig:
    def test_deliverables_days_read_from_config(self):
        from loom_core.maintenance import retention_from_config
        cfg = {"retention.deliverables_days": "30"}
        r = retention_from_config(cfg)
        assert r.deliverables_days == 30

    def test_deliverables_days_zero_by_default(self):
        from loom_core.maintenance import retention_from_config
        r = retention_from_config({})
        assert r.deliverables_days == 0

    def test_enabled_includes_deliverables_days(self):
        from loom_core.maintenance import Retention
        assert Retention(deliverables_days=30).enabled is True
        assert Retention().enabled is False

    def test_negative_deliverables_days_clamped_to_zero(self):
        from loom_core.maintenance import retention_from_config
        r = retention_from_config({"retention.deliverables_days": "-5"})
        assert r.deliverables_days == 0
        assert r.enabled is False


class TestWriteSide:
    def test_pipeline_record_carries_retention_until(self, state, tmp_path):
        """写侧：配了 deliverables_days 的运行，交付记录带 retention_until。"""
        from loom_core.pipeline import PipelineOrchestrator

        orch = PipelineOrchestrator(
            agents_dir=str(PROJECT / "agents"),
            checkpoint_dir=str(tmp_path / "ckpt"))
        # 直接验证 _build_delivery_record 的保留期计算分支，
        # 不依赖整条流水线跑完（run_plan 需要外部 IO）。
        # 关热加载：get 的 _check_reload 会用 config.json 重置 _data，
        # 把 set 进去的内存值冲掉（config.json 无 retention 段）。
        orch.config._auto_reload = False
        task = _fake_task("r1", "done")
        plan = _fake_plan("p")
        orch.config.set("retention.deliverables_days", 30)
        rec = orch._build_delivery_record(task, plan)
        assert rec["retention_until"] is not None
        assert rec["retention_until"] == rec["finished_at"] + 30 * 86400.0

    def test_pipeline_record_no_retention_when_unconfigured(self, state, tmp_path):
        """未配 deliverables_days：retention_until 为 None（不启用删除）。"""
        from loom_core.pipeline import PipelineOrchestrator

        orch = PipelineOrchestrator(
            agents_dir=str(PROJECT / "agents"),
            checkpoint_dir=str(tmp_path / "ckpt"))
        task = _fake_task("r2", "done")
        plan = _fake_plan("p")
        rec = orch._build_delivery_record(task, plan)
        assert rec.get("retention_until") is None


def _fake_task(run_id, status):
    from types import SimpleNamespace

    from loom_core.pipeline import TaskStatus
    return SimpleNamespace(
        id=run_id,
        status=TaskStatus.DONE if status == "done" else status,
        input_file="in.md",
        output_path=f"{run_id}.md",
        error="",
        started_at=100.0,
        finished_at=200.0)


def _fake_plan(pipeline_name):
    from types import SimpleNamespace
    return SimpleNamespace(
        pipeline_name=pipeline_name,
        plan_id="plan-1",
        levels=[],
        raw={"pipeline": {}})
