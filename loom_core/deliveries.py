"""交付账本 —— 每轮运行的持久化记录（结构化 JSON 落库）。

此前一轮流水线跑完，结构化结果只有两个去处：checkpoint 目录里的诊断报告
（`report_<id>.json`，不是交付物）和 task_queue 的 result_json（只在走队列
时才有）。CLI 直跑的产物没有任何可查询的持久记录。

本模块把"交付"变成账本：每次 run_plan 收尾（含失败）记一行，载荷是完整的
结构化 JSON（节点级状态/耗时/产物路径）。查询接口供 CLI / Admin API / 测试
判据读回。

追溯面（#8 遗留，2026-10-27）：每行带**内容摘要**（交付物文件的 sha256/字节
进列，逐节点产物键的摘要进载荷）与**到期时刻** `retention_until`，`cleanup()`
按保留期回收账本行——此前 `deliveries.db` 是唯一没被 A6 GC 覆盖的存储，运行
从 RunStore 删掉了账本还在长。

存储：state_paths.store_path("deliveries.db")（SQLite WAL）——跟随
LOOM_STATE_DIR 隔离，测试互不污染。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from artesian.fast_json import dumps as _fast_dumps
from artesian.fast_json import loads as _fast_loads

from . import state_paths
from .sqlite_util import add_column_if_missing, enable_wal

_SCHEMA = """
CREATE TABLE IF NOT EXISTS deliveries (
    run_id          TEXT PRIMARY KEY,
    pipeline        TEXT NOT NULL,
    status          TEXT NOT NULL,
    finished_at     REAL NOT NULL DEFAULT 0,
    duration_sec    REAL,
    output_path     TEXT DEFAULT '',
    payload_json    TEXT NOT NULL DEFAULT '{}',
    sha256          TEXT DEFAULT '',
    bytes           INTEGER DEFAULT 0,
    retention_until REAL DEFAULT 0
)
"""

#: 与 `run_store.TERMINAL_STATUSES` 同一套三值（判据里钉住两处不得漂）：账本里
#: 只有终态行才是"跑完了的历史"，未终态的行被 GC 碰了就是抹掉正在进行的事实。
TERMINAL_STATUSES = ("done", "failed", "cancelled")

#: 读不到磁盘交付物时的字节数（沿用 `RunHandle._size_of` 的既有约定：-1 要说的是
#: "去查过，查不到"，与 0（空文件）和列缺省 0（从没算过）都不同）。
UNREADABLE = -1


def digest_of(value: Any) -> dict[str, Any]:
    """一段产物内容的稳定摘要：sha256 + 参与摘要的字节数 + 编码形态。

    字符串按 UTF-8 原文摘要——用户拿到的就是这段文本，摘要要能对着文件核；
    非字符串走 `sort_keys` 的规范 JSON：不排序的话同一个 dict 换个键序就换
    一个 sha，"追溯"当场失效。编不动的对象退到 `repr`，宁可摘要一个次优形态，
    也不在这里抛——交付收尾那条路绝不外抛。
    """
    if isinstance(value, str):
        raw = value.encode("utf-8")
        kind = "text"
    else:
        try:
            text = json.dumps(value, ensure_ascii=False, sort_keys=True,
                              default=str)
        except (TypeError, ValueError):
            text = repr(value)
        raw = text.encode("utf-8")
        kind = "json"
    return {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw),
            "kind": kind}


def digest_file(path: str, *, chunk: int = 1 << 16) -> dict[str, Any]:
    """落盘交付物的摘要。读不到时 `sha256` 给空串、`bytes` 给 -1，**不省略这条**：
    "算不出摘要"本身是要能被看见的事实。
    """
    if not path:
        return {"sha256": "", "bytes": 0, "kind": "file"}
    h = hashlib.sha256()
    total = 0
    try:
        with open(path, "rb") as fh:
            while True:
                block = fh.read(chunk)
                if not block:
                    break
                total += len(block)
                h.update(block)
    except OSError:
        return {"sha256": "", "bytes": UNREADABLE, "kind": "file",
                "error": "读不到交付物文件"}
    return {"sha256": h.hexdigest(), "bytes": total, "kind": "file"}


def artifact_digests(result: Any) -> dict[str, dict[str, Any]]:
    """节点结果里每个产物键的摘要，按键名排序。

    只对 dict 结果生效：非 dict（None/字符串）没有"逐键产物"可言，给空表——
    与 `artifact_keys` 那里"没产出的节点给空表"的口径一致。
    """
    if not isinstance(result, dict):
        return {}
    return {str(k): digest_of(result[k]) for k in sorted(result, key=str)}


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
                # 追溯面是后加的三列：老库（`CREATE TABLE IF NOT EXISTS` 不会
                # 动已存在的表）在这里补上，空值即"这一行是在这功能之前写的"。
                add_column_if_missing(conn, "deliveries", "sha256", "TEXT DEFAULT ''")
                add_column_if_missing(conn, "deliveries", "bytes", "INTEGER DEFAULT 0")
                add_column_if_missing(conn, "deliveries", "retention_until",
                                      "REAL DEFAULT 0")
        finally:
            conn.close()

    def record(self, rec: dict[str, Any]) -> str:
        """记一行交付。以 rec["run_id"] 为主键，重复写入覆盖（幂等收尾）。"""
        run_id = str(rec.get("run_id") or "")
        if not run_id:
            raise ValueError("交付记录缺少 run_id")
        out = rec.get("output_digest") or {}
        sha = str(out.get("sha256") or "")
        nbytes = _as_int(out.get("bytes"))
        conn = self._conn()
        try:
            with conn:
                conn.execute(
                    "INSERT OR REPLACE INTO deliveries "
                    "(run_id, pipeline, status, finished_at, duration_sec, "
                    " output_path, payload_json, sha256, bytes, retention_until) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (run_id,
                     str(rec.get("pipeline") or ""),
                     str(rec.get("status") or ""),
                     float(rec.get("finished_at") or 0.0),
                     _as_float_or_none(rec.get("duration_sec")),
                     str(rec.get("output_path") or ""),
                     _fast_dumps(rec),
                     sha,
                     nbytes,
                     _as_float(rec.get("retention_until")),
                     )
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

    def row_digest(self, run_id: str) -> dict[str, Any]:
        """列上的摘要与到期时刻（不解析载荷）；行不存在给空 dict。

        读面单独要这一口的场景是"账本行还在但载荷很大"——摘要用来核对，
        载荷用来展示，两者不必一起拉。
        """
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT sha256, bytes, retention_until FROM deliveries "
                "WHERE run_id = ?", (run_id,)).fetchone()
        finally:
            conn.close()
        if not row:
            return {}
        return {"sha256": str(row[0] or ""), "bytes": int(row[1] or 0),
                "retention_until": float(row[2] or 0.0)}

    def cleanup(self, retention_days: int = 0, max_runs: int = 0,
                now: float | None = None) -> int:
        """按保留期回收账本行，返回删除行数（A6 缺的那一半）。

        判据与 `RunStore.cleanup` 同形，但**到期时刻是写入时冻结的**：

        - `retention_until > 0 且 <= now` 删——这一行落库时就算好了自己几点到期，
          所以事后把保留期从 90 天调成 7 天，不会回头把老行一次性删光（一次
          配置改动触发成片删除，是要人背锅的形状）；
        - `retention_until = 0` 的行（写入时没启用保留期，或本功能之前的老行）
          只受 `max_runs` 约束——不 backfill 历史运行是硬边界，老行既不补摘要
          也不补到期时刻；
        - `max_runs > 0`：按 `finished_at` 新→旧保留最近 N 条，超出的删；
        - 只碰终态行，未终态的一行不动。

        **不删磁盘产物**：`output_path` 指向用户自己声明的输出目录，删它等于
        删用户文件；账本行走了文件还在，追溯靠 `sha256` 列留下的记录。
        """
        stamp = time.time() if now is None else now
        conn = self._conn()
        try:
            # 读判据 + 删除整体放进一个写事务：SQLite 的隐式事务只在 DML 前开启，
            # 裸 SELECT 不排队——两个进程并发 GC 时，"最近 N 条"的边界会在读与删之间
            # 被对方新落库的行挪动，于是多删一条。（同 `run_store._Txn` 的理由。）
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                "SELECT run_id, finished_at, retention_until FROM deliveries "
                "WHERE status IN (?, ?, ?) ORDER BY finished_at DESC",
                TERMINAL_STATUSES).fetchall()
            to_delete: list[str] = []
            if max_runs > 0:
                to_delete += [r[0] for r in rows[max_runs:]]
            if retention_days > 0:
                to_delete += [r[0] for r in rows
                              if float(r[2] or 0.0) > 0
                              and float(r[2]) <= stamp]
            to_delete = sorted(set(to_delete))
            if not to_delete:
                conn.commit()
                return 0
            conn.executemany(
                "DELETE FROM deliveries WHERE run_id = ?",
                [(rid,) for rid in to_delete])
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return len(to_delete)

    def count(self) -> int:
        """账本行数（GC 判据与 `loom --gc` 汇总都要能区分"没删"和"本来就空"）。"""
        conn = self._conn()
        try:
            return int(conn.execute(
                "SELECT COUNT(*) FROM deliveries").fetchone()[0])
        finally:
            conn.close()


def _as_float_or_none(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int:
    """字节数列的值：坏值退到 0（"没算过"），-1（读不到）要原样留着——
    把"查不到"归零等于把这条产物从账本上抹平。"""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
