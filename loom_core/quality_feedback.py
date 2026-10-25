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
import os
import sqlite3
import threading
import time
from collections.abc import Iterator
from pathlib import Path

from . import state_paths
from .sqlite_util import enable_wal

logger = logging.getLogger(__name__)
_DEFAULT_DB = os.path.join(Path(__file__).parent.parent.absolute(), "bus_data", "quality.db")

WEAK_THRESHOLD = 70  # 低于此分视为弱项


class QualityFeedback:
    """质量评分历史（SQLite 持久化）"""

    def __init__(self, db_path: str = ""):
        db_path = db_path or state_paths.store_path("quality.db")
        self._db_path = db_path
        self._lock = threading.Lock()
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextlib.contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        """每操作独立连接：with 块内提交事务，退出时保证关闭。

        注意 sqlite3 的 ``with conn`` 只提交事务、不关闭连接，此处用
        contextmanager 包一层确保用完即关，避免连接泄漏
        （ResourceWarning: unclosed database）。
        """
        conn = sqlite3.connect(self._db_path, timeout=5, check_same_thread=False)
        try:
            # 并发首建同一库时 SQLite 切 WAL 不走 busy handler，
            # 直接抛 "database is locked"，必须自带重试（见 sqlite_util 模块头）。
            enable_wal(conn)
            # busy_timeout 防止并发写入时立即抛 SQLITE_BUSY，与 cost_tracker 保持一致
            conn.execute("PRAGMA busy_timeout=3000")
            with conn:
                yield conn
        finally:
            conn.close()

    def _init_db(self):
        with self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS quality_history (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp    REAL NOT NULL,
                    task_id      TEXT NOT NULL,
                    dimension    TEXT NOT NULL,
                    score        REAL NOT NULL,
                    is_weak      INTEGER NOT NULL,
                    section_types TEXT DEFAULT '',
                    pipeline     TEXT DEFAULT ''
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_qh_dim ON quality_history(dimension)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_qh_weak ON quality_history(is_weak)")

    def record(self, task_id: str, scores: dict[str, float],
               section_types: list[str] = None, pipeline: str = ""):
        """记录一次质量评分"""
        sections_str = ",".join(section_types or [])
        now = time.time()
        with self._lock, self._conn() as conn:
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

    def cleanup(self, max_age_days: int = 0, max_rows: int = 0) -> int:
        """清理过期记录（双判据并集）。

        与 RunStore 同口径：**0 = 不启用**（本 store 此前没有清理面，默认
        0=删光会是一次静默的行为变化；age=0 的"删光"语义只保留在既有
        行为的 TaskQueue/CostTracker 上）。worker GC 按 retention 配置调用。
        """
        if max_age_days <= 0 and max_rows <= 0:
            return 0
        dropped = 0
        with self._lock, self._conn() as conn:
            if max_age_days > 0:
                cur = conn.execute(
                    "DELETE FROM quality_history WHERE timestamp < ?",
                    (time.time() - max_age_days * 86400,))
                dropped = cur.rowcount
            if max_rows > 0:
                cur = conn.execute(
                    "DELETE FROM quality_history WHERE id NOT IN (SELECT id FROM "
                    "quality_history ORDER BY timestamp DESC LIMIT ?)",
                    (max_rows,))
                dropped += cur.rowcount
        return dropped


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
