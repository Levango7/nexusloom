"""交付账本 —— 每轮运行的持久化记录（结构化 JSON 落库）。

此前一轮流水线跑完，结构化结果只有两个去处：checkpoint 目录里的诊断报告
（`report_<id>.json`，不是交付物）和 task_queue 的 result_json（只在走队列
时才有）。CLI 直跑的产物没有任何可查询的持久记录。

本模块把"交付"变成账本：每次 run_plan 收尾（含失败）记一行，载荷是完整的
结构化 JSON（节点级状态/耗时/产物路径）。查询接口供 CLI / Admin API / 测试
判据读回。

存储：state_paths.store_path("deliveries.db")（SQLite WAL）——跟随
LOOM_STATE_DIR 隔离，测试互不污染。
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from artesian.fast_json import dumps as _fast_dumps
from artesian.fast_json import loads as _fast_loads

from . import state_paths
from .sqlite_util import enable_wal

_SCHEMA = """
CREATE TABLE IF NOT EXISTS deliveries (
    run_id       TEXT PRIMARY KEY,
    pipeline     TEXT NOT NULL,
    status       TEXT NOT NULL,
    finished_at  REAL NOT NULL DEFAULT 0,
    duration_sec REAL,
    output_path  TEXT DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}'
)
"""


class DeliveryLedger:
    """交付账本（SQLite）。写入失败由调用方决定是否致命——钩子侧永远只警告。"""

    def __init__(self, db_path: str = ""):
        self._db_path = db_path or state_paths.store_path("deliveries.db")
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=5)
        enable_wal(conn)
        conn.execute("PRAGMA busy_timeout=3000")
        return conn

    def _init_db(self) -> None:
        conn = self._conn()
        try:
            with conn:
                conn.executescript(_SCHEMA)
        finally:
            conn.close()

    def record(self, rec: dict[str, Any]) -> str:
        """记一行交付。以 rec["run_id"] 为主键，重复写入覆盖（幂等收尾）。"""
        run_id = str(rec.get("run_id") or "")
        if not run_id:
            raise ValueError("交付记录缺少 run_id")
        conn = self._conn()
        try:
            with conn:
                conn.execute(
                    "INSERT OR REPLACE INTO deliveries "
                    "(run_id, pipeline, status, finished_at, duration_sec, "
                    " output_path, payload_json) VALUES (?,?,?,?,?,?,?)",
                    (run_id,
                     str(rec.get("pipeline") or ""),
                     str(rec.get("status") or ""),
                     float(rec.get("finished_at") or 0.0),
                     _as_float_or_none(rec.get("duration_sec")),
                     str(rec.get("output_path") or ""),
                     _fast_dumps(rec)),
                )
        finally:
            conn.close()
        return run_id

    def get(self, run_id: str) -> dict[str, Any] | None:
        """按 run_id 读回完整结构化记录；没有则 None。"""
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT payload_json FROM deliveries WHERE run_id = ?",
                (run_id,)).fetchone()
        finally:
            conn.close()
        if not row:
            return None
        loaded: dict[str, Any] = _fast_loads(row[0])
        return loaded

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        """最近的交付记录（按 finished_at 降序），返回完整载荷。"""
        limit = max(1, int(limit))
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT payload_json FROM deliveries "
                "ORDER BY finished_at DESC LIMIT ?", (limit,)).fetchall()
        finally:
            conn.close()
        return [_fast_loads(r[0]) for r in rows]

    def cleanup(self, retention_days: int = 0, max_runs: int = 0,
                now: float | None = None) -> int:
        """按保留期判据清理交付账本。返回删除行数。

        与 run_store.cleanup 同一双判据口径：
        - retention_days > 0：删除 finished_at 早于 now - days*86400 的记录
        - max_runs > 0：只保留 finished_at 最新的 N 条，其余删除
        两个判据可同时生效（年龄判据在保留集内也适用）。
        """
        import time as _time
        if retention_days <= 0 and max_runs <= 0:
            return 0
        now_ts = now if now is not None else _time.time()
        conn = self._conn()
        try:
            with conn:
                deleted = 0
                if retention_days > 0:
                    cutoff = now_ts - retention_days * 86400
                    cur = conn.execute(
                        "DELETE FROM deliveries WHERE finished_at < ?", (cutoff,))
                    deleted += cur.rowcount
                if max_runs > 0:
                    cur = conn.execute(
                        "DELETE FROM deliveries WHERE run_id NOT IN "
                        "(SELECT run_id FROM deliveries ORDER BY finished_at DESC LIMIT ?)",
                        (max_runs,))
                    deleted += cur.rowcount
            return deleted
        finally:
            conn.close()


def _as_float_or_none(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None
