"""交付账本 —— 每轮运行的持久化记录（结构化 JSON 落库）。

此前一轮流水线跑完，结构化结果只有两个去处：checkpoint 目录里的诊断报告
（`report_<id>.json`，不是交付物）和 task_queue 的 result_json（只在走队列
时才有）。CLI 直跑的产物没有任何可查询的持久记录。

本模块把"交付"变成账本：每次 run_plan 收尾（含失败）记一行，载荷是完整的
结构化 JSON（节点级状态/耗时/产物路径）。查询接口供 CLI / Admin API / 测试
判据读回。

存储：经 ``db_backend.resolve_backend`` 走 SQLite/Postgres 双方言，跟随
LOOM_STATE_DIR 隔离，测试互不污染。PG 由 ``LOOM_DATABASE_URL`` 或显式
``db_path`` 驱动。
"""
from __future__ import annotations

from typing import Any

from artesian.fast_json import dumps as _fast_dumps
from artesian.fast_json import loads as _fast_loads

from . import state_paths
from .db_backend import resolve_backend


class DeliveryLedger:
    """交付账本（SQLite/Postgres 双方言）。写入失败由调用方决定是否致命——钩子侧永远只警告。"""

    def __init__(self, db_path: str = ""):
        self._db_path = db_path or state_paths.store_path("deliveries.db")
        self._pool = resolve_backend(self._db_path, "deliveries.db")
        self._init_db()

    def _conn(self):
        """当前线程连接（pool 管理生命周期，不需手动关闭）。"""
        return self._pool.conn()

    def _init_db(self) -> None:
        # DDL 不可参数化；表名/列名/类型全为常量，不来自用户输入
        with self._pool.conn() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS deliveries "
                "(run_id TEXT PRIMARY KEY, pipeline TEXT NOT NULL, "
                "status TEXT NOT NULL, finished_at REAL NOT NULL DEFAULT 0, "
                "duration_sec REAL, output_path TEXT DEFAULT '', "
                "payload_json TEXT NOT NULL DEFAULT '{}')"  # nosec B608
            )

    def record(self, rec: dict[str, Any]) -> str:
        """记一行交付。以 rec["run_id"] 为主键，重复写入覆盖（幂等收尾）。

        用 ``INSERT OR IGNORE`` + ``UPDATE`` 而非 ``INSERT OR REPLACE``——
        后者不在 ``db_backend._translate`` 的方言翻译表里，PG 上会原样
        送达 psycopg 报错。先插入忽略冲突，再 UPDATE 覆写非键列，语义
        等价于 REPLACE 但双方言兼容。
        """
        run_id = str(rec.get("run_id") or "")
        if not run_id:
            raise ValueError("交付记录缺少 run_id")
        pipeline = str(rec.get("pipeline") or "")
        status = str(rec.get("status") or "")
        finished_at = float(rec.get("finished_at") or 0.0)
        duration = _as_float_or_none(rec.get("duration_sec"))
        output_path = str(rec.get("output_path") or "")
        payload = _fast_dumps(rec)
        with self._pool.conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO deliveries "
                "(run_id, pipeline, status, finished_at, duration_sec, "
                " output_path, payload_json) VALUES (?,?,?,?,?,?,?)",
                (run_id, pipeline, status, finished_at, duration,
                 output_path, payload))
            conn.execute(
                "UPDATE deliveries SET pipeline=?, status=?, finished_at=?, "
                "duration_sec=?, output_path=?, payload_json=? WHERE run_id=?",
                (pipeline, status, finished_at, duration,
                 output_path, payload, run_id))
        return run_id

    def get(self, run_id: str) -> dict[str, Any] | None:
        """按 run_id 读回完整结构化记录；没有则 None。"""
        row = self._pool.conn().execute(
            "SELECT payload_json FROM deliveries WHERE run_id = ?",
            (run_id,)).fetchone()
        if not row:
            return None
        return _fast_loads(row[0])

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        """最近的交付记录（按 finished_at 降序），返回完整载荷。"""
        limit = max(1, int(limit))
        rows = self._pool.conn().execute(
            "SELECT payload_json FROM deliveries "
            "ORDER BY finished_at DESC LIMIT ?", (limit,)).fetchall()
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
        with self._pool.conn() as conn:
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


def _as_float_or_none(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None
