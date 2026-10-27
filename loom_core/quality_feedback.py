"""QualityFeedback — 质量闭环学习

记录每次生成的质量评分，积累"哪类章节容易低分"的模式。
下次生成时查询历史，提前加强薄弱环节。

用法：
    from .quality_feedback import record_quality, get_weak_patterns
    record_quality(task_id="t1", scores={"completeness": 65, "structure": 80},
                   section_types=["概述", "核心概念", "实践应用"])
    weak = get_weak_patterns()  # → {"completeness": {"avg": 65, "count": 10, "weak_count": 7}}
"""
from __future__ import annotations

import contextlib
import logging
import threading
import time
from collections.abc import Iterator
from typing import Any

from . import state_paths
from .db_backend import resolve_backend

logger = logging.getLogger(__name__)

WEAK_THRESHOLD = 70  # 低于此分视为弱项


class QualityFeedback:
    """质量评分历史（双方言持久化，pool 管理并发）"""

    def __init__(self, db_path: str = ""):
        self._db_path = db_path or state_paths.store_path("quality.db")
        self._pool = resolve_backend(self._db_path, "quality.db",
                                    schema_init=self._ensure_schema)
        self._lock = threading.Lock()
        self._init_db()

    @contextlib.contextmanager
    def _conn(self) -> Iterator[Any]:
        """当前线程连接（pool 管理生命周期与并发）。

        pool 的 thread-local 连接自带 WAL + busy_timeout（SQLite 侧），
        PG 侧由 MVCC 提供并发控制。事务由 ``with conn:`` 块管理。
        """
        with self._pool.conn() as conn:
            yield conn

    @staticmethod
    def _ensure_schema(conn: Any) -> None:
        """建表 + 索引（幂等；DDL 不可参数化，表名/列名全为常量）"""
        conn.execute(  # nosec B608
            "CREATE TABLE IF NOT EXISTS quality_history "
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "timestamp REAL NOT NULL, task_id TEXT NOT NULL, "
            "dimension TEXT NOT NULL, score REAL NOT NULL, "
            "is_weak INTEGER NOT NULL, section_types TEXT DEFAULT '', "
            "pipeline TEXT DEFAULT '')"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_qh_dim ON quality_history(dimension)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_qh_weak ON quality_history(is_weak)")

    def _init_db(self):
        with self._pool.conn() as conn:
            self._ensure_schema(conn)

    def record(self, task_id: str, scores: dict[str, float],
               section_types: list[str] = None, pipeline: str = ""):
        """记录一次质量评分"""
        sections_str = ",".join(section_types or [])
        now = time.time()
        with self._conn() as conn:
            for dim, score in scores.items():
                conn.execute(
                    "INSERT INTO quality_history "
                    "(timestamp, task_id, dimension, score, is_weak, section_types, pipeline) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (now, task_id, dim, score, 1 if score < WEAK_THRESHOLD else 0,
                     sections_str, pipeline),
                )

    def get_weak_patterns(self, min_samples: int = 3) -> dict[str, dict]:
        """查询历史低分模式

        返回每个维度的统计：平均分、样本数、低分次数、低分率
        只返回 min_samples 以上样本的维度。
        """
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT dimension, COUNT(*), AVG(score), SUM(is_weak) "
                "FROM quality_history GROUP BY dimension HAVING COUNT(*) >= ?",
                (min_samples,),
            ).fetchall()
            result = {}
            for dim, count, avg, weak_count in rows:
                weak_rate = (weak_count or 0) / count if count > 0 else 0
                result[dim] = {
                    "avg_score": round(avg or 0, 1),
                    "sample_count": count,
                    "weak_count": weak_count or 0,
                    "weak_rate": round(weak_rate, 2),
                    "is_problematic": weak_rate > 0.4,
                }
            return result

    def get_recommendations(self) -> list[str]:
        """基于历史低分模式生成改进建议"""
        patterns = self.get_weak_patterns()
        recs = []
        for dim, info in sorted(patterns.items(), key=lambda x: x[1]["weak_rate"], reverse=True):
            if info["is_problematic"]:
                recs.append(
                    f"维度 '{dim}' 历史低分率 {info['weak_rate']:.0%}"
                    f"（均分 {info['avg_score']}/{WEAK_THRESHOLD}，{info['weak_count']}/{info['sample_count']} 次低于阈值），"
                    f"建议加强该维度"
                )
        return recs

    def stats(self) -> dict:
        """总览"""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*), COUNT(DISTINCT task_id), AVG(score), SUM(is_weak)"
                " FROM quality_history"
            ).fetchone()
            return {
                "total_records": row[0] or 0,
                "total_tasks": row[1] or 0,
                "avg_score": round(row[2] or 0, 1),
                "total_weak": row[3] or 0,
                "weak_patterns": self.get_weak_patterns(),
                "recommendations": self.get_recommendations(),
            }

    def cleanup(self, max_age_days: int = 0, max_rows: int = 0,
                now: float | None = None) -> int:
        """清理过期记录（双判据并集）。

        与 RunStore 同口径：**0 = 不启用**（本 store 此前没有清理面，默认
        0=删光会是一次静默的行为变化；age=0 的"删光"语义只保留在既有
        行为的 TaskQueue/CostTracker 上）。worker GC 按 retention 配置调用。
        """
        if max_age_days <= 0 and max_rows <= 0:
            return 0
        dropped = 0
        now_ts = now if now is not None else time.time()
        with self._conn() as conn:
            if max_age_days > 0:
                cur = conn.execute(
                    "DELETE FROM quality_history WHERE timestamp < ?",
                    (now_ts - max_age_days * 86400,))
                dropped = cur.rowcount
            if max_rows > 0:
                cur = conn.execute(
                    "DELETE FROM quality_history WHERE id NOT IN (SELECT id FROM "
                    "quality_history ORDER BY timestamp DESC LIMIT ?)",
                    (max_rows,))
                dropped += cur.rowcount
        return dropped

    def close(self) -> None:
        """关闭当前线程的连接（测试清理用）。"""
        self._pool.close()


_instance: QualityFeedback | None = None
_lock = threading.Lock()


def get_quality_feedback() -> QualityFeedback:
    global _instance
    if _instance is None:
        with _lock:
            if _instance is None:
                _instance = QualityFeedback()
    return _instance


def record_quality(task_id: str, scores: dict[str, float],
                   section_types: list[str] = None, pipeline: str = ""):
    """便捷函数"""
    get_quality_feedback().record(task_id, scores, section_types, pipeline)
