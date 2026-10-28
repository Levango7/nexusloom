"""
MessageStore - 持久化消息存储
===============================
从 MessageBus 中抽离的独立存储层，负责：
  - SQLite 消息持久化
  - 死信队列 (DLQ) 管理
  - 幂等键管理
  - 健康检查
"""
from __future__ import annotations

import contextlib
import os
import sqlite3
import threading
import time
import uuid
import weakref
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path

from artesian.fast_json import dumps as _fast_dumps
from artesian.fast_json import loads as _fast_loads

from . import state_paths
from .sqlite_util import enable_wal

# ─── 常量 ─────────────────────────────────────

# 兼容旧引用：仅表示"未设置 LOOM_STATE_DIR 时的默认位置"，
# 真实解析走 state_paths.store_path（调用期解析，见该模块 docstring）
DEFAULT_DB_PATH = os.path.join(Path(__file__).parent.parent.absolute(), "bus_data", "message_bus.db")
MAX_PROCESSED_KEYS = 50000


# ─── 数据类型 ────────────────────────────────

class MessageType(Enum):
    EVENT = "event"
    REQUEST = "request"
    RESPONSE = "response"


class MessagePriority(Enum):
    LOW = 0
    NORMAL = 50
    HIGH = 100
    CRITICAL = 200

    @classmethod
    def from_value(cls, v):
        for p in cls:
            if p.value == v:
                return p
        return cls.NORMAL


@dataclass
class Message:
    topic: str
    payload: dict
    msg_type: MessageType = MessageType.EVENT
    from_agent: str = "system"
    to_agent: str = ""
    correlation_id: str = ""
    trace_id: str = ""
    priority: int = 50
    idempotency_key: str = ""
    retry_count: int = 0
    max_retries: int = 3
    msg_id: str = ""
    created_at: float = 0.0
    delivered: bool = False
    error: str = ""

    def __post_init__(self):
        if not self.msg_id:
            self.msg_id = str(uuid.uuid4())
        if not self.created_at:
            self.created_at = time.time()
        if not self.trace_id:
            self.trace_id = self.msg_id[:8]

    @classmethod
    def response(cls, original: Message, from_a: str, payload: dict) -> Message:
        return cls(
            topic=original.topic,
            payload=payload,
            msg_type=MessageType.RESPONSE,
            from_agent=from_a,
            to_agent=original.from_agent,
            correlation_id=original.correlation_id,
            trace_id=original.trace_id,
        )

    def to_dict(self) -> dict:
        d = asdict(self)
        d["msg_type"] = self.msg_type.value
        return d


@dataclass
class MessageMetrics:
    sent: int = 0
    received: int = 0
    failed: int = 0
    retried: int = 0
    dlq_count: int = 0
    high_watermark_hits: int = 0
    peak_depth: int = 0
    start_time: float = 0.0

    def record_sent(self):
        self.sent += 1

    def record_received(self):
        self.received += 1

    def record_failed(self):
        self.failed += 1

    def record_retry(self):
        self.retried += 1

    def record_dlq(self):
        self.dlq_count += 1

    def to_dict(self) -> dict:
        return {
            "sent": self.sent,
            "received": self.received,
            "failed": self.failed,
            "retried": self.retried,
            "dlq": self.dlq_count,
            "high_watermark_hits": self.high_watermark_hits,
            "peak_depth": self.peak_depth,
            "uptime": time.time() - self.start_time if self.start_time else 0,
        }


# ─── SQLite 持久化层 ───────────────────────

class _TrackableConnection(sqlite3.Connection):
    """支持弱引用的连接（sqlite3.Connection 原生不可弱引用，经 factory 子类补槽）"""

    __slots__ = ("__weakref__",)


class PersistentStore:
    """SQLite-backed 消息持久化"""

    def __init__(self, db_path: str = ""):
        self.db_path = db_path or state_paths.store_path("message_bus.db")
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        # thread-local 连接随线程死亡只能靠 GC 回收，close() 只能关当前线程的连接。
        # 创建连接时登记弱引用，close_all() 遍历关闭全部已登记连接（与 task_queue 同模式）。
        self._conn_refs: set = set()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """获取当前线程缓存的连接（被 close_all() 关闭后自动重建，自愈）"""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.execute("SELECT 1")
            except sqlite3.Error:
                conn = None
        if conn is None:
            conn = sqlite3.connect(
                self.db_path,
                timeout=10,
                check_same_thread=False,
                factory=_TrackableConnection,
            )
            # 忙等：并发首建同一库时 SQLite 切 WAL 不走 busy handler，
            # 直接抛 "database is locked"，必须自带重试（见 sqlite_util 模块头）。
            enable_wal(conn)
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            self._local.conn = conn
            self._conn_refs.add(weakref.ref(conn))
        return conn

    def _init_db(self):
        """建表"""
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS messages (
                msg_id       TEXT PRIMARY KEY,
                topic        TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                msg_type     TEXT NOT NULL DEFAULT 'event',
                from_agent   TEXT NOT NULL DEFAULT 'system',
                to_agent     TEXT DEFAULT '',
                correlation_id TEXT DEFAULT '',
                trace_id     TEXT DEFAULT '',
                priority     INTEGER DEFAULT 50,
                retry_count  INTEGER DEFAULT 0,
                max_retries  INTEGER DEFAULT 3,
                idempotency_key TEXT DEFAULT '',
                created_at   REAL NOT NULL,
                delivered    INTEGER DEFAULT 0,
                error        TEXT DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS dlq (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                msg_id       TEXT NOT NULL,
                topic        TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                error_msg    TEXT NOT NULL,
                failed_at    REAL NOT NULL,
                replay_count INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS processed_keys (
                key        TEXT PRIMARY KEY,
                created_at REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages(topic);
            CREATE INDEX IF NOT EXISTS idx_messages_delivered ON messages(delivered);
            CREATE INDEX IF NOT EXISTS idx_messages_created ON messages(created_at);
            CREATE INDEX IF NOT EXISTS idx_dlq_created ON dlq(failed_at);
            CREATE INDEX IF NOT EXISTS idx_processed_created ON processed_keys(created_at);
        """)
        conn.commit()

    # ── 消息 CRUD ──

    def save_message(self, msg: Message):
        conn = self._get_conn()
        conn.execute(
            """INSERT OR REPLACE INTO messages
               (msg_id, topic, payload_json, msg_type, from_agent, to_agent,
                correlation_id, trace_id, priority, retry_count, max_retries,
                idempotency_key, created_at, delivered, error)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                msg.msg_id, msg.topic, _fast_dumps(msg.payload),
                msg.msg_type.value, msg.from_agent, msg.to_agent,
                msg.correlation_id, msg.trace_id, msg.priority,
                msg.retry_count, msg.max_retries,
                msg.idempotency_key, msg.created_at,
                1 if msg.delivered else 0, msg.error,
            ),
        )
        conn.commit()

    def mark_delivered(self, msg_id: str):
        conn = self._get_conn()
        conn.execute("UPDATE messages SET delivered=1 WHERE msg_id=?", (msg_id,))
        conn.commit()

    def mark_delivered_batch(self, msg_ids: list[str]):
        """批量标记已投递（一次 UPDATE + 一次 commit，减少 90% 写 I/O）"""
        if not msg_ids:
            return
        conn = self._get_conn()
        conn.executemany(
            "UPDATE messages SET delivered=1 WHERE msg_id=?",
            [(mid,) for mid in msg_ids],
        )
        conn.commit()

    def move_to_dlq(self, msg: Message, error: str):
        conn = self._get_conn()
        with conn:
            existing = conn.execute(
                "SELECT 1 FROM dlq WHERE msg_id=?", (msg.msg_id,)
            ).fetchone()
            if existing:
                conn.execute("UPDATE messages SET delivered=1, error=? WHERE msg_id=?", (error, msg.msg_id))
                return
            conn.execute(
                """INSERT INTO dlq (msg_id, topic, payload_json, error_msg, failed_at)
                   VALUES (?,?,?,?,?)""",
                (msg.msg_id, msg.topic, _fast_dumps(msg.to_dict()),
                 error, time.time()),
            )
            conn.execute("UPDATE messages SET delivered=1, error=? WHERE msg_id=?", (error, msg.msg_id))

    def prune_dlq(self, max_age_days: int = 0, max_rows: int = 0) -> int:
        """回收死信表：年龄 + 数量双判据，返回删除条数。

        这张表此前**只进不出**（`move_to_dlq` 插入、`list_dlq` 读，没有任何删除
        面），长跑进程的死信会无限堆积——A6 把周围几张表都收了，唯独漏了这一张。
        两个参数都 <= 0 时返回 0：这张表从来没有过清理面，不沿用
        `task_queue` 那种"age=0 就是删光"的历史语义（0 = 没启用）。
        """
        if max_age_days <= 0 and max_rows <= 0:
            return 0
        conn = self._get_conn()
        cutoff = time.time() - max_age_days * 86400.0
        rows = conn.execute(
            "SELECT id, failed_at FROM dlq ORDER BY failed_at DESC, id DESC"
        ).fetchall()
        targets: list[int] = []
        if max_rows > 0:
            targets += [r[0] for r in rows[max_rows:]]
        if max_age_days > 0:
            targets += [r[0] for r in rows if float(r[1] or 0.0) < cutoff]
        targets = sorted(set(targets))
        if not targets:
            return 0
        with conn:
            conn.executemany("DELETE FROM dlq WHERE id = ?",
                             [(i,) for i in targets])
        return len(targets)

    def replay_dlq(self, dlq_id: int) -> dict | None:
        """取一条死信数据并更新重放计数（真实重投由调用方完成）"""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT msg_id, topic, payload_json, error_msg, replay_count FROM dlq WHERE id=?",
            (dlq_id,),
        ).fetchone()
        if not row:
            return None
        conn.execute("UPDATE dlq SET replay_count=replay_count+1 WHERE id=?", (dlq_id,))
        conn.commit()
        return {
            "msg_id": row[0],
            "topic": row[1],
            "payload": _fast_loads(row[2]),
            "error": row[3],
            "replay_count": row[4] + 1,
        }

    def get_dlq_entry(self, dlq_id: int) -> dict | None:
        """仅取一条死信数据，不更新计数、不重投（供 orchestrator 解析）"""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT id, msg_id, topic, payload_json, error_msg, failed_at, replay_count FROM dlq WHERE id=?",
            (dlq_id,),
        ).fetchone()
        if not row:
            return None
        return {
            "id": row[0], "msg_id": row[1], "topic": row[2],
            "payload": _fast_loads(row[3]), "error": row[4],
            "failed_at": row[5], "replay_count": row[6],
        }

    def list_dlq(self, limit: int = 50) -> list[dict]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT id, msg_id, topic, error_msg, failed_at, replay_count FROM dlq ORDER BY failed_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [
            {"id": r[0], "msg_id": r[1], "topic": r[2], "error": r[3],
             "failed_at": r[4], "replay_count": r[5]}
            for r in rows
        ]

    # ── 幂等键 ──

    def is_idempotent(self, key: str) -> bool:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT 1 FROM processed_keys WHERE key=?", (key,)
        ).fetchone()
        return row is not None

    def save_idempotent(self, key: str):
        conn = self._get_conn()
        conn.execute(
            "INSERT OR IGNORE INTO processed_keys (key, created_at) VALUES (?,?)",
            (key, time.time()),
        )
        conn.commit()
        self._trim_processed_keys()

    def check_and_mark_idempotent(self, key: str) -> bool:
        """原子检查并标记幂等键。返回 True=已存在(重复)，False=首次(已标记)。"""
        conn = self._get_conn()
        cursor = conn.execute(
            "INSERT OR IGNORE INTO processed_keys (key, created_at) VALUES (?,?)",
            (key, time.time()),
        )
        conn.commit()
        # affected rows = 0 means key already existed (duplicate)
        return cursor.rowcount == 0

    def check_and_save_atomic(self, key: str, msg: Message) -> bool:
        """修复 P0：原子地执行幂等检查 + 消息持久化（同事务）。

        原 send() 分两步调用 check_and_mark_idempotent + save_message，
        若 check 成功后 save_message 失败（磁盘满/IO 错误），幂等键已标记
        但消息未落库 → 后续重复消息被静默丢弃，数据丢失。

        本方法在单个 SQLite 事务中执行：
          1. 检查幂等键是否存在 → 存在则返回 True（重复，不写任何数据）
          2. 插入幂等键 + 保存消息 → 任一步失败则整个事务回滚，幂等键不残留
        返回 True=重复跳过，False=首次已保存。
        """
        conn = self._get_conn()
        try:
            with conn:
                # 1. 幂等检查
                existing = conn.execute(
                    "SELECT 1 FROM processed_keys WHERE key=?", (key,)
                ).fetchone()
                if existing:
                    return True  # 重复，事务无写入直接结束
                # 2. 标记幂等键
                conn.execute(
                    "INSERT INTO processed_keys (key, created_at) VALUES (?,?)",
                    (key, time.time()),
                )
                # 3. 保存消息（INSERT OR REPLACE 语义与 save_message 一致）
                conn.execute(
                    """INSERT OR REPLACE INTO messages
                       (msg_id, topic, payload_json, msg_type, from_agent, to_agent,
                        correlation_id, trace_id, priority, retry_count, max_retries,
                        idempotency_key, created_at, delivered, error)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        msg.msg_id, msg.topic, _fast_dumps(msg.payload),
                        msg.msg_type.value, msg.from_agent, msg.to_agent,
                        msg.correlation_id, msg.trace_id, msg.priority,
                        msg.retry_count, msg.max_retries,
                        msg.idempotency_key, msg.created_at,
                        1 if msg.delivered else 0, msg.error,
                    ),
                )
            # 事务提交成功后异步修剪（不阻塞主路径）
            self._trim_processed_keys()
            return False
        except Exception:
            # with conn 上下文已自动回滚，幂等键不会残留
            raise

    def _trim_processed_keys(self):
        """保持幂等键数量在限制内"""
        conn = self._get_conn()
        count = conn.execute("SELECT COUNT(*) FROM processed_keys").fetchone()[0]
        if count > MAX_PROCESSED_KEYS:
            conn.execute(
                """DELETE FROM processed_keys WHERE rowid IN (
                    SELECT rowid FROM processed_keys ORDER BY created_at ASC LIMIT ?
                )""",
                (count - MAX_PROCESSED_KEYS,),
            )
            conn.commit()

    # ── 健康检查 ──

    def count_undelivered_events(self) -> int:
        """返回未投递的 EVENT 消息数量（背压检测用）"""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE delivered=0 AND msg_type='event'"
        ).fetchone()
        return row[0] if row else 0

    def health(self) -> dict:
        try:
            conn = self._get_conn()
            msg_count = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            dlq_count = conn.execute("SELECT COUNT(*) FROM dlq").fetchone()[0]
            pk_count = conn.execute("SELECT COUNT(*) FROM processed_keys").fetchone()[0]
            return {
                "status": "ok",
                "messages": msg_count,
                "dlq": dlq_count,
                "processed_keys": pk_count,
                "db_path": self.db_path,
                "db_size": os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0,
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def close(self):
        """显式关闭当前线程缓存的 SQLite 连接。"""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            with contextlib.suppress(Exception):
                conn.close()
            self._local.conn = None

    def close_current_thread(self):
        """关闭调用线程自己的 thread-local SQLite 连接（供工作线程退出时自清理）。"""
        self.close()

    def close_all(self):
        """使本实例登记的全部 thread-local 连接失效（长驻进程/测试关停用）。

        安全性（P0，2026-08 CI 段错误修复）：sqlite3 连接被另一线程 close()
        时，若拥有线程正在其上执行语句（_get_conn 探测/mark_delivered 等），
        会在 C 层竞争触发 SIGSEGV（ubuntu 多版本 Python 稳定复现，CI exit 139）。
        因此绝不跨线程 close：只关调用线程自己的连接，其余仅移出登记表；
        拥有线程下次 _get_conn() 时 "SELECT 1" 探测失败自动重建（自愈，即
        本方法的原设计意图）。失效连接由 GC 在其线程结束后回收。
        """
        refs = list(self._conn_refs)
        self._conn_refs.clear()
        for ref in refs:
            conn = ref()
            if conn is None:
                continue
            with contextlib.suppress(Exception):
                # 仅关闭属于当前线程的连接；其余线程的连接只从登记表移除
                if conn is getattr(self._local, "conn", None):
                    conn.close()
