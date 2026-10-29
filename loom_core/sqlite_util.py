"""SQLite 并发初始化收口：WAL 切换与加列两处竞态的忙等/重试。

两个都是**首建期**竞态，只在"两个进程同时第一次建同一批库"时现形，
因此单进程测试与隔离跑都看不见——CI 的 3.14 腿在全量负载下命中过
（`e2e-0 终态是 failed: database is locked`）。本机探针复现口径：
两进程 × 80 轮同建四库，命中 3 次 `database is locked` + 1 次
`duplicate column name: owner_pid`。

1. **WAL 切换不走 busy handler**：SQLite 官方文档明示 `PRAGMA
   journal_mode=WAL` 在需要短暂独占锁时遇到竞争会直接返回
   SQLITE_BUSY（Python 文案 "database is locked"），不看 `busy_timeout`。
   所以必须自己重试。
2. **`ALTER TABLE ... ADD COLUMN` 的 check-then-act**：两个进程都按
   `PRAGMA table_info` 判断"列不存在"，然后都执行 ALTER，后到者拿
   "duplicate column name"。目标状态其实已达成，吞掉这个错误即可。
"""
from __future__ import annotations

import re
import sqlite3
import time

#: WAL 重试次数与退避（首建期竞争窗口极短，8 次 × 0.05s 起步足够）
WAL_RETRY_ATTEMPTS = 8
WAL_RETRY_BASE_DELAY = 0.05
WAL_RETRY_MAX_DELAY = 1.0

#: 标识符白名单：PRAGMA/ALTER 的插值点无法参数化（SQL 标识符不支持占位符），
#: 这里把"调用方只传字面常量"从约定升级为强制——动态拼出的表名/列名在这里
#: 直接抛 ValueError，而不是等 SQLite 原样执行。
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
#: DDL 类型片段白名单：字母/数字/下划线/空格/单引号/逗号/括号——够表达
#: "INTEGER DEFAULT 0"、"TEXT DEFAULT ''" 这类字面量，拒掉 ; -- /* 等结构字符。
_DDL_TYPE_RE = re.compile(r"^[A-Za-z0-9_ '(),]+$")


def _checked(kind: str, value: str, pattern: re.Pattern[str]) -> str:
    if not pattern.match(value):
        raise ValueError(
            f"sqlite_util: {kind} 必须是字面常量，不接受动态拼出的值: {value!r}")
    return value


def enable_wal(conn: sqlite3.Connection, attempts: int = WAL_RETRY_ATTEMPTS) -> None:
    """带重试地切换到 WAL 模式。

    非锁类错误（如库损坏）立即抛出；锁类错误退避重试，耗尽后仍抛
    ——"没切成 WAL"必须是可见故障，不许静默降级。
    """
    delay = WAL_RETRY_BASE_DELAY
    for attempt in range(1, attempts + 1):
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            return
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "locked" not in msg and "busy" not in msg:
                raise
            if attempt == attempts:
                raise
            time.sleep(delay)
            delay = min(delay * 2, WAL_RETRY_MAX_DELAY)


def column_names(conn: sqlite3.Connection, table: str) -> set:
    """表的现有列名集合。"""
    _checked("表名", table, _IDENTIFIER_RE)
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def add_column_if_missing(conn: sqlite3.Connection, table: str, column: str,
                          ddl_type: str) -> bool:
    """幂等加列。返回是否真的执行了 ALTER。

    并发下 check-then-act 会撞 "duplicate column name"，此时目标状态已达成，
        视为成功（返回 False）；其他 OperationalError 照抛。
    """
    _checked("表名", table, _IDENTIFIER_RE)
    _checked("列名", column, _IDENTIFIER_RE)
    _checked("DDL 类型", ddl_type, _DDL_TYPE_RE)
    if column in column_names(conn, table):
        return False
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_type}")
        return True
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            return False
        raise
