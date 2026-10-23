"""审批台账 —— 事实源已并入 RunStore（Stage 3a，2026-10-23）。

为什么需要它：审批闸门此前只活在内存（`PipelineTask.awaiting_approval`）——
进程一重启，`approve()` 再也找不到任务，"人审"就变成一次性的演示。台账把
"哪个运行在等批、批了没有"落成持久事实，并让续跑可判定：

  - 闸门挂起时登记一行 `pending`（含节点、提示语、principal）；
  - `approve/reject` 就地决策（**只有 pending 能被决策**，重复决策不覆盖历史）；
  - 续跑（`run_plan(resume=True)`）时 `open_decision()` 把已作出的决定交给执行器，
    闸门不再二次挂起；决定被 `consume()` 后不再参与下一次判定。

**现在这些事实只写进 RunStore**：`runs.db` 的 `approvals` 投影 + 同事务的
`approval.requested/decided/consumed` 事件。本类是它的门面——引擎闸门、Admin
收件箱、审计都从这里走，`approvals.db` 不再是写入面。写不进去时抛
`ApprovalLedgerUnavailable` 而不是静默换一面可写的墙：调用方据此 fail-closed
（写不进台账却声称已批准 = 执行了但没有持久授权，重启后台账仍显示待批）。

老台账 `approvals.db` 的处置是一条**数据边界**，不是开关（RunStore 模块头那条
"不 backfill"在这里同样成立）：

  1. 读侧先看 store，store 里没有这个运行的审批行才回看老台账；
  2. 老台账里**已存在**的 pending 行仍可被就地决策/消费——否则升级到本版本后，
     升级前停在闸门上的运行永远批不掉；
  3. 老台账不再新增行：新的待办只进 store，老行耗尽后该文件只剩历史；
  4. 老台账按需打开：文件不存在就当没有（读返回空、绝不建库），读连接带
     `query_only`，不会因为"查一下"改动它。
"""
from __future__ import annotations

import sqlite3
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from artesian.fast_json import loads as _fast_loads

from . import state_paths
from .run_store import APPROVED, PENDING, REJECTED

__all__ = ["APPROVED", "PENDING", "REJECTED", "ApprovalLedgerUnavailable",
           "ApprovalStore"]

#: 老台账的行形状；列顺序与 `RunStore._APPROVAL_COLS` 一致，两个来源才能并成一份清单
_LEGACY_SELECT = (
    "SELECT request_id, run_id, pipeline, principal, nodes_json, messages_json, "
    "status, requested_at, decided_at, decided_by, note, reason, consumed "
    "FROM approvals"
)


class ApprovalLedgerUnavailable(RuntimeError):
    """审批事实无处可写（RunStore 没接线或建口失败）——写侧唯一的失败形状。"""


class ApprovalStore:
    """审批门面：写一律进 RunStore，读带一条老台账的数据边界回落。

    `run_store` 是**取 RunStore 的可调用**（引擎侧建口本身是惰性的，这里不提前建库），
    也接受直接传实例（单元测试里更顺手）。写操作在拿不到 store 时抛
    `ApprovalLedgerUnavailable`；读操作不抛——没有 store 就只读老台账。
    """

    def __init__(self, run_store: Callable[[], Any] | Any | None = None,
                 db_path: str = ""):
        self._run_store = run_store
        self._legacy_path = db_path or state_paths.store_path("approvals.db")

    # ── 后端解析 ─────────────────────────────────────

    def _store_or_none(self) -> Any:
        provider = self._run_store
        if provider is None:
            return None
        return provider() if callable(provider) else provider

    def _store(self) -> Any:
        store = self._store_or_none()
        if store is None:
            raise ApprovalLedgerUnavailable(
                "RunStore 不可用：审批事实写不进去"
                "（引擎侧未接 run_store provider，或库打不开——"
                "看 /api/health/deep 的 runstore 组件）")
        return store

    @property
    def legacy_path(self) -> str:
        return self._legacy_path

    def _legacy_live(self) -> bool:
        """老台账文件在不在——不在就整条回落当空，绝不因为"查一下"建库。"""
        return Path(self._legacy_path).exists()

    def _legacy_conn(self, read_only: bool) -> sqlite3.Connection:
        conn = sqlite3.connect(self._legacy_path, timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=3000")
        if read_only:
            conn.execute("PRAGMA query_only=ON")
        return conn

    @staticmethod
    def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        out = dict(row)
        out["nodes"] = list(_fast_loads(out.pop("nodes_json") or "[]"))
        out["messages"] = dict(_fast_loads(out.pop("messages_json") or "{}"))
        return out

    # ── 写入（只进 RunStore）──────────────────────────

    def request(self, run_id: str, pipeline: str, nodes: list[str],
                messages: dict[str, str] | None = None,
                principal: str = "") -> str:
        """登记一次待批。同一运行已有 pending 行时复用（不重复插）。

        复用是必要的：续跑会在决定之前重新 arm 闸门，若每次都插新行，
        收件箱里会出现同一次运行的多个待办。老台账里有 pending 时同样复用——
        升级前挂起的闸门不该在升级后变成两条待办。
        """
        store = self._store()
        existing = self.pending_for_run(run_id)
        if existing is not None:
            return str(existing["request_id"])
        request_id = uuid.uuid4().hex
        store.open_approval(request_id=request_id, run_id=run_id, pipeline=pipeline,
                            nodes=list(nodes), messages=dict(messages or {}),
                            principal=principal or "")
        return request_id

    def decide(self, request_id: str, decision: str, by: str = "",
               note: str = "", reason: str = "") -> bool:
        """作出决定。只有 pending 行可被决策（幂等：第二次返回 False）。"""
        return self.decide_recorded(request_id, decision, by=by, note=note,
                                    reason=reason) is not None

    def decide_recorded(self, request_id: str, decision: str, by: str = "",
                        note: str = "", reason: str = "") -> dict[str, Any] | None:
        """决策并返回台账认定的权威行（含 `decided_at`）；None = 没落下去。

        `decided_at` 由台账单点取值，审计 / 内存历史 / 事件读同一个时刻。此前是
        "台账自己 `time.time()` 一次、调用方再取一次"，同一个决定因此有两个时刻，
        对账只能靠容差糊过去（边界 ②：老台账的行由 `_legacy_decide` 就地重读）。
        """
        if decision not in (APPROVED, REJECTED):
            raise ValueError(f"未知审批结论: {decision!r}")
        store = self._store()
        recorded: dict[str, Any] | None = store.decide_approval(
            request_id, decision, by=by, note=note, reason=reason)
        if recorded is not None:
            return recorded
        return self._legacy_decide(request_id, decision, by, note, reason)

    def consume(self, request_id: str) -> None:
        """标记决定已被执行器消费（续跑越过闸门后调用）。"""
        store = self._store_or_none()
        if store is not None and store.consume_approval(request_id) > 0:
            return
        if store is None and not self._legacy_live():
            raise ApprovalLedgerUnavailable(
                "RunStore 不可用且无老台账：决定无法标记消费（会被二次越闸）")
        if not self._legacy_live():
            return
        with self._legacy_conn(False) as conn:
            conn.execute("UPDATE approvals SET consumed = 1 WHERE request_id = ?",
                         (request_id,))

    def _legacy_decide(self, request_id: str, decision: str, by: str, note: str,
                       reason: str) -> dict[str, Any] | None:
        """就地决策老台账里**已存在**的行——只 UPDATE，不 INSERT：老台账没有新写口。"""
        if not self._legacy_live():
            return None
        with self._legacy_conn(False) as conn:
            cur = conn.execute(
                "UPDATE approvals SET status = ?, decided_at = ?, decided_by = ?, "
                "note = ?, reason = ? WHERE request_id = ? AND status = ?",
                (decision, time.time(), by or "", note or "", reason or "",
                 request_id, PENDING))
            if cur.rowcount <= 0:
                return None
        return self._legacy_row(request_id)

    # ── 读取（store 优先，老台账按运行回落）────────────

    def _legacy_sql(self, where: str, args: tuple) -> list[dict[str, Any]]:
        if not self._legacy_live():
            return []
        try:
            with self._legacy_conn(True) as conn:
                rows = conn.execute(_LEGACY_SELECT + where, args).fetchall()
        except sqlite3.OperationalError:
            # 文件在、表还没建（升级中途的半成品状态目录）：按没有处理
            return []
        return [d for d in (self._row_to_dict(r) for r in rows) if d]

    def _legacy_row(self, request_id: str) -> dict[str, Any] | None:
        rows = self._legacy_sql(" WHERE request_id = ?", (request_id,))
        return rows[0] if rows else None

    def _legacy_pending_for_run(self, run_id: str) -> dict[str, Any] | None:
        rows = self._legacy_sql(" WHERE run_id = ? AND status = ? "
                                "ORDER BY requested_at DESC LIMIT 1",
                                (run_id, PENDING))
        return rows[0] if rows else None

    def _legacy_open_decisions(self, run_id: str) -> list[dict[str, Any]]:
        return self._legacy_sql(" WHERE run_id = ? AND status IN (?, ?) "
                                "AND consumed = 0 ORDER BY decided_at DESC",
                                (run_id, APPROVED, REJECTED))

    def _legacy_list_pending(self, principal: str | None) -> list[dict[str, Any]]:
        if principal is None:
            return self._legacy_sql(" WHERE status = ?", (PENDING,))
        return self._legacy_sql(" WHERE status = ? AND principal = ?",
                                (PENDING, principal))

    def _legacy_history(self, run_id: str) -> list[dict[str, Any]]:
        return self._legacy_sql(" WHERE run_id = ?", (run_id,))

    @staticmethod
    def _merged(store_rows: list[dict[str, Any]], legacy_rows: list[dict[str, Any]],
                key: str, newest_first: bool) -> list[dict[str, Any]]:
        """两个来源按同一时间键并成一份清单（按 request_id 去重）。

        去重不是防重复插行——一条 request_id 只可能存在于一个来源——而是防
        "同一次运行既在老台账挂过 pending、升级后又往 store 写了一条"时收件箱出现两行。
        """
        seen: set[str] = set()
        out: list[dict[str, Any]] = []
        for row in [*store_rows, *legacy_rows]:
            rid = str(row.get("request_id") or "")
            if rid in seen:
                continue
            seen.add(rid)
            out.append(row)
        out.sort(key=lambda r: float(r.get(key) or 0.0), reverse=newest_first)
        return out

    def get(self, request_id: str) -> dict[str, Any] | None:
        store = self._store_or_none()
        if store is not None:
            row: dict[str, Any] | None = store.get_approval(request_id)
            if row is not None:
                return row
        return self._legacy_row(request_id)

    def pending_for_run(self, run_id: str) -> dict[str, Any] | None:
        """该运行当前**唯一**的待批行（没有则 None）。"""
        store = self._store_or_none()
        if store is None:
            return self._legacy_pending_for_run(run_id)
        row: dict[str, Any] | None = store.pending_approval(run_id)
        return row if row is not None else self._legacy_pending_for_run(run_id)

    def open_decision(self, run_id: str,
                      nodes: list[str] | None = None) -> dict[str, Any] | None:
        """该运行已作出但尚未被消费的决定（续跑用）。

        `nodes` 非 None 时**只认节点集合完全一致的决定**：同一运行里多个闸门
        共用 run_id，按"最新决定"取值会让后一个闸门的决定去结算前一个闸门
        （2026-10-20 复核确认的错绑路径）。
        """
        store = self._store_or_none()
        store_rows = store.open_approvals(run_id) if store is not None else []
        rows = self._merged(store_rows, self._legacy_open_decisions(run_id),
                            "decided_at", True)
        wanted = set(nodes) if nodes is not None else None
        for decision in rows:
            if wanted is None or set(decision.get("nodes") or []) == wanted:
                return decision
        return None

    def list_pending(self, principal: str | None = None,
                     limit: int = 200) -> list[dict[str, Any]]:
        """待批收件箱。principal=None 表示不过滤（admin）；否则只看自己的。"""
        store = self._store_or_none()
        store_rows = (store.list_pending_approvals(principal, limit)
                      if store is not None else [])
        rows = self._merged(store_rows, self._legacy_list_pending(principal),
                            "requested_at", False)
        return rows[:int(limit)]

    def list_for_run(self, run_id: str) -> list[dict[str, Any]]:
        """该运行的全部审批历史（新→旧），供详情页/对账。"""
        store = self._store_or_none()
        store_rows = store.approvals_for_run(run_id) if store is not None else []
        return self._merged(store_rows, self._legacy_history(run_id),
                            "requested_at", True)
