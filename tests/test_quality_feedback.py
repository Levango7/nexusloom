"""QualityFeedback — 质量闭环学习测试"""
import pytest

from loom_core.quality_feedback import QualityFeedback


@pytest.fixture
def feedback(tmp_path):
    return QualityFeedback(db_path=str(tmp_path / "test_quality.db"))


class TestQualityFeedback:
    def test_record_and_stats(self, feedback):
        feedback.record("t1", {"completeness": 80, "structure": 90}, ["概述", "核心概念"])
        s = feedback.stats()
        assert s["total_records"] == 2
        assert s["total_tasks"] == 1

    def test_weak_pattern_detection(self, feedback):
        for i in range(5):
            feedback.record(f"t{i}", {"completeness": 60, "structure": 85})
        patterns = feedback.get_weak_patterns(min_samples=3)
        assert "completeness" in patterns
        assert patterns["completeness"]["weak_count"] == 5
        assert patterns["completeness"]["is_problematic"] is True
        assert patterns["structure"]["is_problematic"] is False

    def test_min_samples_filter(self, feedback):
        feedback.record("t1", {"completeness": 60})
        patterns = feedback.get_weak_patterns(min_samples=3)
        assert len(patterns) == 0

    def test_recommendations(self, feedback):
        for i in range(5):
            feedback.record(f"t{i}", {"completeness": 50, "structure": 90})
        recs = feedback.get_recommendations()
        assert len(recs) >= 1
        assert "completeness" in recs[0]

    def test_no_recommendations_when_all_good(self, feedback):
        for i in range(5):
            feedback.record(f"t{i}", {"completeness": 90, "structure": 90})
        recs = feedback.get_recommendations()
        assert len(recs) == 0

    def test_multiple_tasks(self, feedback):
        feedback.record("t1", {"completeness": 80})
        feedback.record("t2", {"completeness": 60})
        feedback.record("t3", {"completeness": 90})
        s = feedback.stats()
        assert s["total_tasks"] == 3
        assert s["total_records"] == 3

    def test_section_types_stored(self, feedback):
        feedback.record("t1", {"completeness": 80}, ["概述", "实践"])
        s = feedback.stats()
        assert s["total_records"] == 1


class TestConnectionLifecycle:
    """回归：连接用完即关，不依赖 GC 回收（同 cost_tracker 的泄漏修复）"""

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
        fb = QualityFeedback(db_path=str(tmp_path / "lifecycle.db"))
        fb.record("t1", {"completeness": 80, "structure": 60})
        fb.get_weak_patterns()
        fb.get_recommendations()
        fb.stats()
        fb.close()  # pool 缓存连接需显式关闭

        assert created, "应创建过 SQLite 连接"
        for conn in created:
            with pytest.raises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")  # 已关闭的连接不可再操作
