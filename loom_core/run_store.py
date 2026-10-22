"""RunStore + EventLog —— 一次运行的事实只有**一个写入口**（RunStore 设计 §3）。

治的是什么：运行事实此前散在五处写面（内存 `_running_tasks`、`task_queue`、
checkpoint JSON、`deliveries.db`、`approvals.db`），读侧 20 处各自回落，其中最坏的
几条把"进程没重启过"当前提（见 `docs/runstore-eventlog-design-2026-10-21.md` §1）。
本模块不搬旧数据，而是把**新运行**的每一个事实先落成一条不可变事件，再在
**同一个事务**里更新投影（`runs` / `run_nodes`）——恢复与审计因此共处同一个时间
坐标系，不会出现"事件说完成、投影说运行中"。

四张表（Stage 1 只做这些；`artifacts`/`schedules`/`cost_entries` 属后续批）：

| 表 | 角色 |
|---|---|
| `schema_version` | 结构版本；取代散落的 `add_column_if_missing` 临时迁移 |
| `events` | 追加式事实流，PK `(run_id, seq)`，`seq` 每运行单调 |
| `runs` | 运行投影（列表页与作用域判定的唯一来源），`last_seq` 让 SSE 游标跨进程续传 |
| `run_nodes` | 逐节点投影，把原先埋在 `deliveries.payload_json` 里的事实升为一等行

四条刻意留下的边界（对外要说清，不能读成"什么都齐了"）：

- **不 backfill**：老运行不进这里，仍按原路径经 `deliveries` 读得到终态。
- **有事件必有投影行**：`_append` 先 `INSERT OR IGNORE INTO runs`，所以不存在
  "events 里有 run_id、runs 里查不到"的半条事实。
- **`seq` 由事务内取号**：`MAX(seq)+1`，不用自增 id——多个进程共享一个库文件时，
  跨进程唯一不等于每运行单调，而 SSE 游标要的是后者。SQLite 侧靠 `BEGIN IMMEDIATE`
  把取号排队；Postgres 侧对应 `SELECT ... FOR UPDATE` 行锁。
- **禁用 `INSERT OR REPLACE`**：`db_backend` 的方言翻译表里没有它，写出来就是
  "SQLite 能跑、PG 直接报错"的假可用；这里的 upsert 一律 `INSERT OR IGNORE` + `UPDATE`。

开关：`LOOM_RUNSTORE=0` 显式关掉双写（内部开关，不是产品特性；默认开）。
关掉即回到 Stage 0 之后的形状——读写两侧都不依赖本表，这是 §5 要求的退路。
"""
from __future__ import annotations

import os
import time
from typing import Any, Literal

from artesian.fast_json import dumps as _fast_dumps
from artesian.fast_json import loads as _fast_loads

from . import db_backend

SCHEMA_VERSION = 1

#: 本批的形状化写口；其余事实走 `record()`，同一条流、不动投影
RUN_OPENED = "run.opened"
RUN_STARTED = "run.started"
NODE_FINISHED = "node.finished"
RUN_COMPLETED = "run.completed"
RUN_FAILED = "run.failed"
RUN_CANCELLED = "run.cancelled"

#: 终态投影状态 → 事件类型
_TERMINAL_EVENT = {"done": RUN_COMPLETED, "failed": RUN_FAILED,
                   "cancelled": RUN_CANCELLED}

_DDL = (
    """
CREATE TABLE IF NOT EXISTS schema_version (
    version    INTEGER PRIMARY KEY,
    applied_at REAL NOT NULL
)
""",
    """
CREATE TABLE IF NOT EXISTS events (
    run_id       TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    type         TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    ts           REAL NOT NULL,
    PRIMARY KEY (run_id, seq)
)
""",
    """
CREATE TABLE IF NOT EXISTS runs (
    run_id         TEXT PRIMARY KEY,
    workflow       TEXT NOT NULL DEFAULT '',
    principal      TEXT NOT NULL DEFAULT '',
    status         TEXT NOT NULL DEFAULT 'running',
    trigger        TEXT NOT NULL DEFAULT '',
    inputs_json    TEXT NOT NULL DEFAULT '{}',
    started_at     REAL NOT NULL DEFAULT 0,
    finished_at    REAL NOT NULL DEFAULT 0,
    error          TEXT NOT NULL DEFAULT '',
    cost_usd       REAL NOT NULL DEFAULT 0,
    quality_score  REAL NOT NULL DEFAULT 0,
    last_seq       INTEGER NOT NULL DEFAULT 0,
    artifacts_json TEXT NOT NULL DEFAULT '[]'
)
""",
    """
CREATE TABLE IF NOT EXISTS run_nodes (
    run_id             TEXT NOT NULL,
    node               TEXT NOT NULL,
    status             TEXT NOT NULL DEFAULT '',
    attempts           INTEGER NOT NULL DEFAULT 1,
    started_at         REAL NOT NULL DEFAULT 0,
    finished_at        REAL NOT NULL DEFAULT 0,
    error              TEXT NOT NULL DEFAULT '',
    result_json        TEXT NOT NULL DEFAULT '{}',
    artifact_keys_json TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (run_id, node)
)
""",
)

_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id, seq)",
    "CREATE INDEX IF NOT EXISTS idx_runs_principal ON runs(principal, started_at)",
    "CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status, finished_at)",
    "CREATE INDEX IF NOT EXISTS idx_run_nodes_run ON run_nodes(run_id, status)",
)

#: 双写开关：默认开，`LOOM_RUNSTORE=0` 显式关（§5 要求的退路）
SWITCH_ENV = "LOOM_RUNSTORE"


def enabled() -> bool:
    return (os.environ.get(SWITCH_ENV, "1") or "1").strip().lower() not in (
        "0", "false", "no", "off")


class RunStore:
    """运行事实的唯一写入口 + 读接口。

    写方法一律"事件 + 投影"同事务同提交：要么整套落下去，要么什么都没落。
    读侧因此永远看不到"事件在、投影没跟上"的中间态。
    """

    def __init__(self, db_path: str = ""):
        self._pool = db_backend.resolve_backend(db_path, default_name="runs.db")
        self._init_schema()

    @property
    def dialect(self) -> str:
        return self._pool.dialect

    def _conn(self):
        return self._pool.conn()

    def _init_schema(self) -> None:
        conn = self._conn()
        with _Txn(self._pool, conn):
            for ddl in _DDL:
                conn.execute(ddl)
            for sql in _INDEXES:
                conn.execute(sql)
            conn.execute(
                "INSERT OR IGNORE INTO schema_version (version, applied_at) "
                "VALUES (?, ?)", (SCHEMA_VERSION, time.time()))

    # ── 写入 ───────────────────────────────────────────────
    def open_run(self, run_id: str, workflow: str = "", principal: str = "",
                 trigger: str = "", inputs: dict | None = None,
                 started_at: float | None = None) -> int:
        """登记一次运行的开始，返回 `run.started` 的 seq。

        投影行幂等（同一 run_id 重复 open 不覆写已有列），但**事件照样追加**：
        重放与续跑本身就是事实，抹掉它等于把"这个运行被重开过"从审计里删了。
        """
        ts = started_at or time.time()
        with _Txn(self._pool, self._conn()) as txn:
            conn = txn.conn
            conn.execute(
                "INSERT OR IGNORE INTO runs (run_id, workflow, principal, status, "
                "trigger, inputs_json, started_at) VALUES (?, ?, ?, 'running', ?, ?, ?)",
                (run_id, workflow, principal, trigger, _fast_dumps(inputs or {}), ts))
            self._append(conn, run_id, RUN_OPENED,
                         {"workflow": workflow, "principal": principal,
                          "trigger": trigger}, ts, txn)
            return self._append(conn, run_id, RUN_STARTED, {"started_at": ts},
                                ts, txn)

    def record_nodes(self, run_id: str, rows: list[dict]) -> int:
        """一层节点一次事务（§5 的写放大缓解）。

        逐节点各开一次事务是这个模块最容易写错的地方：事务本身没问题，问题在
        `BEGIN IMMEDIATE` 是**整库写锁**——一层 3 个节点就排队 3 回，并发运行时
        纯付锁切换的钱。这里把一批 upsert + 一批事件收进同一个事务。
        """
        if not rows:
            return 0
        with _Txn(self._pool, self._conn()) as txn:
            conn = txn.conn
            seq = 0
            for row in rows:
                cols = (str(row.get("status") or ""), int(row.get("attempts") or 1),
                        float(row.get("started_at") or 0.0),
                        float(row.get("finished_at") or 0.0),
                        str(row.get("error") or ""),
                        _fast_dumps(row.get("result") or {}),
                        _fast_dumps(row.get("artifact_keys") or []))
                conn.execute(
                    "INSERT OR IGNORE INTO run_nodes (run_id, node, status, attempts, "
                    "started_at, finished_at, error, result_json, artifact_keys_json) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (run_id, str(row.get("node") or "")) + cols)
                conn.execute(
                    "UPDATE run_nodes SET status = ?, attempts = ?, started_at = ?, "
                    "finished_at = ?, error = ?, result_json = ?, artifact_keys_json = ? "
                    "WHERE run_id = ? AND node = ?",
                    cols + (run_id, str(row.get("node") or "")))
                seq = self._append(conn, run_id, NODE_FINISHED,
                                   {"node": row.get("node"), "status": cols[0],
                                    "attempts": cols[1], "error": cols[4]},
                                   cols[3] or time.time(), txn)
            return seq

    def record_node(self, run_id: str, node: str, status: str, *,
                    attempts: int = 1, started_at: float = 0.0,
                    finished_at: float = 0.0, error: str = "",
                    result: Any = None,
                    artifact_keys: list | None = None) -> int:
        """单节点写法（`record_nodes` 的一元特例，留给非层循环的调用方）。"""
        return self.record_nodes(run_id, [{
            "node": node, "status": status, "attempts": attempts,
            "started_at": started_at, "finished_at": finished_at, "error": error,
            "result": result, "artifact_keys": artifact_keys}])

    def close_run(self, run_id: str, status: str, *, finished_at: float = 0.0,
                  error: str = "", cost_usd: float = 0.0,
                  quality_score: float = 0.0,
                  artifacts: list | None = None) -> int:
        """终态：投影盖章 + 一条终态事件，同一事务。

        "每个运行至多一条终态事件"由调用方的 `_claim_terminal_event` 保证（Stage 0）；
        这里不重复裁决，因为幂等性要看整条运行历史，不属存储层职责。
        """
        ts = finished_at or time.time()
        with _Txn(self._pool, self._conn()) as txn:
            conn = txn.conn
            conn.execute(
                "UPDATE runs SET status = ?, finished_at = ?, error = ?, "
                "cost_usd = ?, quality_score = ?, artifacts_json = ? "
                "WHERE run_id = ?",
                (status, ts, error, float(cost_usd or 0), float(quality_score or 0),
                 _fast_dumps(artifacts or []), run_id))
            return self._append(conn, run_id,
                                _TERMINAL_EVENT.get(status, RUN_COMPLETED),
                                {"status": status, "error": error}, ts, txn)

    def record(self, run_id: str, type: str, payload: dict | None = None,
               ts: float | None = None) -> int:
        """不属于上述三种形状的事实（审批、条件跳过……）：只追加事件，不动投影。

        Stage 3 才决定这些要不要升成投影；现在先把它们收进同一条流，免得再添一面总线。
        """
        with _Txn(self._pool, self._conn()) as txn:
            return self._append(txn.conn, run_id, type, payload or {},
                                ts or time.time(), txn)

    def _append(self, conn, run_id: str, type: str, payload: dict,
                ts: float, txn: _Txn) -> int:
        """事务内取号并追加。见模块头对 `seq` 的说明。"""
        txn.acquire_seq_lock(run_id)
        conn.execute("INSERT OR IGNORE INTO runs (run_id) VALUES (?)", (run_id,))
        row = conn.execute("SELECT MAX(seq) FROM events WHERE run_id = ?",
                           (run_id,)).fetchone()
        seq = int(row[0] if row and row[0] is not None else 0) + 1
        conn.execute(
            "INSERT INTO events (run_id, seq, type, payload_json, ts) "
            "VALUES (?, ?, ?, ?, ?)",
            (run_id, seq, type, _fast_dumps(payload), ts))
        conn.execute("UPDATE runs SET last_seq = ? "
                     "WHERE run_id = ? AND last_seq < ?", (seq, run_id, seq))
        return seq

    # ── 读取 ───────────────────────────────────────────────
    def get_run(self, run_id: str) -> dict | None:
        row = self._conn().execute(
            "SELECT run_id, workflow, principal, status, trigger, inputs_json, "
            "started_at, finished_at, error, cost_usd, quality_score, last_seq, "
            "artifacts_json FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if not row:
            return None
        return {"run_id": row[0], "workflow": row[1], "principal": row[2],
                "status": row[3], "trigger": row[4],
                "inputs": _fast_loads(row[5]) or {}, "started_at": row[6],
                "finished_at": row[7], "error": row[8], "cost_usd": row[9],
                "quality_score": row[10], "last_seq": row[11],
                "artifacts": _fast_loads(row[12]) or []}

    def nodes(self, run_id: str) -> list[dict]:
        rows = self._conn().execute(
            "SELECT node, status, attempts, started_at, finished_at, error, "
            "result_json, artifact_keys_json FROM run_nodes WHERE run_id = ? "
            "ORDER BY node", (run_id,)).fetchall()
        return [{"node": r[0], "status": r[1], "attempts": r[2],
                 "started_at": r[3], "finished_at": r[4], "error": r[5],
                 "result": _fast_loads(r[6]) or {},
                 "artifact_keys": _fast_loads(r[7]) or []} for r in rows]

    def events(self, run_id: str, after_seq: int = 0,
               limit: int = 1000) -> list[dict]:
        rows = self._conn().execute(
            "SELECT seq, type, payload_json, ts FROM events "
            "WHERE run_id = ? AND seq > ? ORDER BY seq LIMIT ?",
            (run_id, int(after_seq), int(limit))).fetchall()
        return [{"seq": r[0], "type": r[1], "payload": _fast_loads(r[2]) or {},
                 "ts": r[3]} for r in rows]

    def list_runs(self, principal: str | None = None,
                  limit: int = 100) -> list[dict]:
        """作用域读法：`principal=None` 给全量（admin 面），给了只回自己名下。"""
        cols = ("SELECT run_id, workflow, principal, status, started_at, finished_at, "
                "error ")
        conn = self._conn()
        if principal is None:
            rows = conn.execute(cols + "FROM runs ORDER BY started_at DESC LIMIT ?",
                                (int(limit),)).fetchall()
        else:
            rows = conn.execute(cols + "FROM runs WHERE principal = ? "
                                "ORDER BY started_at DESC LIMIT ?",
                                (principal, int(limit))).fetchall()
        return [{"run_id": r[0], "workflow": r[1], "principal": r[2],
                 "status": r[3], "started_at": r[4], "finished_at": r[5],
                 "error": r[6] or ""}
                for r in rows]


class _Txn:
    """`with _Txn(pool, conn):` —— 成功提交、异常回滚；SQLite 侧提前拿写锁。

    为什么 SQLite 要自己发 `BEGIN IMMEDIATE`：python-sqlite3 的隐式事务只在 **DML**
    之前开启，而取号是 `SELECT MAX(seq)`——读操作不触发 BEGIN，两个进程就可能读到
    同一个 `MAX` 再各自 +1，然后撞主键（或被 busy_timeout 挡下，取决于时序）。
    IMMEDIATE 让"取号 + 插入"整体排队。

    非 SQLite 方言不能照抄这句（`BEGIN IMMEDIATE` 是 SQLite 语法），所以走驱动自带的
    `with conn:` 事务边界 + `SELECT ... FOR UPDATE` 行锁。
    """

    def __init__(self, pool, conn):
        self._pool = pool
        self.conn = conn
        self._sqlite = getattr(pool, "dialect", "sqlite") == "sqlite"

    def __enter__(self) -> _Txn:
        if self._sqlite:
            self.conn.execute("BEGIN IMMEDIATE")
        else:
            self.conn.__enter__()
        return self

    def acquire_seq_lock(self, run_id: str) -> None:
        """取号前的写锁：SQLite 已在 `BEGIN IMMEDIATE` 里，这里只处理其余方言。"""
        if self._sqlite:
            return
        self.conn.execute("SELECT 1 FROM runs WHERE run_id = ? FOR UPDATE", (run_id,))

    def __exit__(self, *exc) -> Literal[False]:
        """永远返回 False：本上下文管理器不吞异常。

        标注成 `bool` 会让 mypy 认为"异常可能被咽掉、控制流落到 with 之后"，
        于是每个 `with _Txn(...): return …` 的写口都报 Missing return——
        而这里确实不该咽异常（提交失败必须让调用方看到）。
        """
        if self._sqlite:
            if exc[0] is None:
                self.conn.commit()
            else:
                self.conn.rollback()
        else:
            self.conn.__exit__(*exc)
        return False
