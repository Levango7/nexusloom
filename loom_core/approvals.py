"""审批台账 —— 跨重启/跨进程的待批与决策记录。

为什么需要它：审批闸门此前只活在内存（`PipelineTask.awaiting_approval`）——
进程一重启，`approve()` 再也找不到任务，"人审"就变成一次性的演示。
台账把"哪个运行在等批、批了没有"落到 SQLite，并让续跑可判定：

  - 闸门挂起时写一行 `pending`（含节点、提示语、principal）；
  - `approve/reject` 更新同一行（**只有 pending 能被决策**，重复决策不覆盖历史）；
  - 续跑（`run_plan(resume=True)`）时 `open_decision()` 把已作出的决定交给执行器，
    闸门不再二次挂起；决定被 `consume()` 后不再参与下一次判定。

存储：`state_paths.store_path("approvals.db")`（SQLite WAL），跟随 LOOM_STATE_DIR
隔离，测试互不污染。
"""
from __future__ import annotations

import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any

from artesian.fast_json import dumps as _fast_dumps
from artesian.fast_json import loads as _fast_loads

from . import state_paths
from .sqlite_util import enable_wal

_SCHEMA = """
CREATE TABLE IF NOT EXISTS approvals (
    request_id    TEXT PRIMARY KEY,
    run_id        TEXT NOT NULL,
    pipeline      TEXT NOT NULL DEFAULT '',
    principal     TEXT NOT NULL DEFAULT '',
    nodes_json    TEXT NOT NULL DEFAULT '[]',
    messages_json TEXT NOT NULL DEFAULT '{}',
    status        TEXT NOT NULL DEFAULT 'pending',
    requested_at  REAL NOT NULL,
    decided_at    REAL NOT NULL DEFAULT 0,
    decided_by    TEXT NOT NULL DEFAULT '',
    note          TEXT NOT NULL DEFAULT '',
    reason        TEXT NOT NULL DEFAULT '',
    consumed      INTEGER NOT NULL DEFAULT 0
)
"""

_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_approvals_run ON approvals(run_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_approvals_pending ON approvals(status, requested_at)",
)

PENDING = "pending"
APPROVED = "approved"
REJECTED = "rejected"


class ApprovalStore:
    """审批台账（SQLite）。写失败由调用方决定是否致命——闸门侧只告警。"""

    def __init__(self, db_path: str = ""):
        self._db_path = db_path or state_paths.store_path("approvals.db")
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=5)
        conn.row_factory = sqlite3.Row
        enable_wal(conn)
        conn.execute("PRAGMA busy_timeout=3000")
        return conn

    def _init_db(self) -> None:
        conn = self._conn()
        try:
            with conn:
                conn.executescript(_SCHEMA)
                for sql in _INDEXES:
                    conn.execute(sql)
        finally:
            conn.close()

    # ── 写入 ─────────────────────────────────────────

    def request(self, run_id: str, pipeline: str, nodes: list[str],
                messages: dict[str, str] | None = None,
                principal: str = "") -> str:
        """登记一次待批。同一运行已有 pending 行时复用（不重复插）。

        复用是必要的：续跑会在决定之前重新 arm 闸门，若每次都插新行，
        收件箱里会出现同一次运行的多个待办。
        """
        existing = self.pending_for_run(run_id)
        if existing is not None:
            return str(existing["request_id"])
        request_id = uuid.uuid4().hex
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO approvals (request_id, run_id, pipeline, principal, "
                "nodes_json, messages_json, status, requested_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (request_id, run_id, pipeline, principal or "",
                 _fast_dumps(list(nodes)), _fast_dumps(dict(messages or {})),
                 PENDING, time.time()),
            )
        return request_id

    def decide(self, request_id: str, decision: str, by: str = "",
               note: str = "", reason: str = "") -> bool:
        """作出决定。只有 pending 行可被决策（幂等：第二次返回 False）。"""
        if decision not in (APPROVED, REJECTED):
            raise ValueError(f"未知审批结论: {decision!r}")
        with self._conn() as conn:
            cur = conn.execute(
                "UPDATE approvals SET status = ?, decided_at = ?, decided_by = ?, "
                "note = ?, reason = ? WHERE request_id = ? AND status = ?",
                (decision, time.time(), by or "", note or "", reason or "",
                 request_id, PENDING),
            )
            return bool(cur.rowcount > 0)

    def consume(self, request_id: str) -> None:
        """标记决定已被执行器消费（续跑越过闸门后调用）。"""
        with self._conn() as conn:
            conn.execute("UPDATE approvals SET consumed = 1 WHERE request_id = ?",
                         (request_id,))

    # ── 读取 ─────────────────────────────────────────

    @staticmethod
    def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        out = dict(row)
        out["nodes"] = list(_fast_loads(out.pop("nodes_json") or "[]"))
        out["messages"] = dict(_fast_loads(out.pop("messages_json") or "{}"))
        return out

    def get(self, request_id: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM approvals WHERE request_id = ?",
                               (request_id,)).fetchone()
        return self._row_to_dict(row)

    def pending_for_run(self, run_id: str) -> dict[str, Any] | None:
        """该运行当前**唯一**的待批行（没有则 None）。"""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM approvals WHERE run_id = ? AND status = ? "
                "ORDER BY requested_at DESC LIMIT 1", (run_id, PENDING)).fetchone()
        return self._row_to_dict(row)

    def open_decision(self, run_id: str,
                      nodes: list[str] | None = None) -> dict[str, Any] | None:
        """该运行已作出但尚未被消费的决定（续跑用）。

        `nodes` 非 None 时**只认节点集合完全一致的决定**：同一运行里多个闸门
        共用 run_id，按"最新决定"取值会让后一个闸门的决定去结算前一个闸门
        （2026-10-20 复核确认的错绑路径）。
        """
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM approvals WHERE run_id = ? AND status IN (?, ?) "
                "AND consumed = 0 ORDER BY decided_at DESC",
                (run_id, APPROVED, REJECTED)).fetchall()
        wanted = set(nodes) if nodes is not None else None
        for row in rows:
            decision = self._row_to_dict(row)
            if decision is None:
                continue
            if wanted is None or set(decision.get("nodes") or []) == wanted:
                return decision
        return None

    def list_pending(self, principal: str | None = None,
                     limit: int = 200) -> list[dict[str, Any]]:
        """待批收件箱。principal=None 表示不过滤（admin）；否则只看自己的。"""
        sql = "SELECT * FROM approvals WHERE status = ?"
        args: list[Any] = [PENDING]
        if principal is not None:
            sql += " AND principal = ?"
            args.append(principal)
        sql += " ORDER BY requested_at ASC LIMIT ?"
        args.append(int(limit))
        with self._conn() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [d for d in (self._row_to_dict(r) for r in rows) if d]

    def list_for_run(self, run_id: str) -> list[dict[str, Any]]:
        """该运行的全部审批历史（新→旧），供详情页/对账。"""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM approvals WHERE run_id = ? ORDER BY requested_at DESC",
                (run_id,)).fetchall()
        return [d for d in (self._row_to_dict(r) for r in rows) if d]
