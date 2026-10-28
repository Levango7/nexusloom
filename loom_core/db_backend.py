"""状态后端工厂：SQLite / Postgres 双方言，队列可换（spec §5.4 P1-B）。

设计：方言差异收敛在本模块的**四处翻译**，业务模块（TaskQueue 等）继续用
`?` 占位符与常量 DDL，不写分支：

  1. **占位符**：SQLite 用 `?`，psycopg 用 `%s`——`translate()` 统一转；
  2. **UPSERT**：SQLite 的 `INSERT OR IGNORE` 在 PG 是
     `INSERT ... ON CONFLICT DO NOTHING`——同一句 SQL 两个方言；
     判断"是否真插入"仍看 `cursor.rowcount`（两边都是受影响行数）；
  3. **浮点类型**：SQLite `REAL` = 8 字节 double，PG `REAL` = 4 字节 →
     PG 侧统一用 `DOUBLE PRECISION`；
  4. **自增主键**：SQLite `INTEGER PRIMARY KEY AUTOINCREMENT` →
     PG `SERIAL PRIMARY KEY`。

其余对齐（2026-10-12 实测）：
  - `CREATE TABLE IF NOT EXISTS` / 列名检查 / 事务块语义两边一致；
  - 时间戳列必须 `DOUBLE PRECISION`（PG 的 `REAL` 是 4 字节浮点，
    unix 时间戳精度不够，`ORDER BY created_at` 会错序）；
  - WAL 只属 SQLite：PG 的多进程并发由 MVCC 提供，不需要 pragma；
  - 连接生命周期与既有 TaskQueue 同构：thread-local 缓存 + 弱引用登记，
    `close_all()` 绝不跨线程 close（跨线程关连接在 C 层竞争会 SIGSEGV——
    2026-08 CI 段错误的教训）。

启用 Postgres：`export LOOM_DATABASE_URL=postgresql://user:pass@host:5432/db`
（旧名 `DOC_PIPELINE_DATABASE_URL` 过渡期仍可读）。不设 = 既有 SQLite 路径，
行为零变化。依赖 `psycopg[binary]`（`pip install loom[postgres]`）——未装且
选了 PG DSN 时启动即报错，不静默降级回 SQLite。
"""
from __future__ import annotations

import logging
import re
import sqlite3
import threading
import weakref
from contextlib import suppress as _suppress
from pathlib import Path
from typing import Any

from .envs import read as _env_read

logger = logging.getLogger(__name__)

#: DSN 环境变量（新名 + 旧名回退，见 envs 模块双读规则）
DB_URL_ENV = "LOOM_DATABASE_URL"
LEGACY_DB_URL_ENV = "DOC_PIPELINE_DATABASE_URL"

_PG_SCHEMES = ("postgres://", "postgresql://")


def is_postgres_dsn(dsn: str) -> bool:
    return (dsn or "").strip().lower().startswith(_PG_SCHEMES)


def _translate(sql: str) -> str:
    """SQLite 方言 SQL → PG 方言（已穷举的四处替换）。"""
    # 占位符：库内所有 SQL 都只把 `?` 当参数位，不存在字面量 `?`
    sql = sql.replace("?", "%s")
    # 幂等 UPSERT：INSERT OR IGNORE INTO t (...) → INSERT INTO t (...) ON CONFLICT DO NOTHING
    # 目标表均以 TEXT 主键为唯一约束，无需指定冲突列
    if "INSERT OR IGNORE INTO" in sql.upper():
        sql = re.sub(r"(?i)INSERT OR IGNORE INTO", "INSERT INTO", sql)
        sql = sql.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
    # DDL 浮点类型：SQLite REAL = 8 字节（IEEE754 double），PG REAL = 4 字节
    sql = re.sub(r"\bREAL\b", "DOUBLE PRECISION", sql, flags=re.IGNORECASE)
    # 自增主键：SQLite 的 INTEGER PRIMARY KEY AUTOINCREMENT → PG 的 SERIAL PRIMARY KEY
    sql = re.sub(r"(?i)\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b",
                 "SERIAL PRIMARY KEY", sql)
    return sql


class _ConnPool:
    """thread-local 连接池（SQLite/PG 共用骨架，connect 由子类实现）。"""

    def __init__(self, dialect: str, schema_init=None):
        self.dialect = dialect
        self._local = threading.local()
        self._conn_refs: set = set()
        self._lock = threading.Lock()
        # 新建连接后回调（自愈：文件被删重建连接时重跑建表）
        self._schema_init = schema_init

    # ── 子类实现 ──
    def _new_conn(self) -> Any:  # pragma: no cover - 抽象
        raise NotImplementedError

    def _probe(self, conn: Any) -> bool:  # pragma: no cover - 抽象
        raise NotImplementedError

    def add_column_if_missing(self, conn: Any, table: str, column: str,
                              ddl_type: str) -> bool:  # pragma: no cover - 抽象
        """幂等加列；conn 可为包装体（内部解包取原生连接）。"""
        raise NotImplementedError

    # ── 共用 ──
    def conn(self) -> Any:
        """当前线程连接（复用；失效自愈——probe 失败即重建）。"""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                if self._probe(conn):
                    return conn
            except Exception:
                pass  # 连接已断（服务器重启/网络抖动）→ 下面重建
            conn = None
        conn = _wrap(self, self._new_conn())
        if self._schema_init is not None:
            try:
                self._schema_init(conn)
            except Exception:
                logger.warning("schema_init 回调失败", exc_info=True)
        self._local.conn = conn
        with self._lock:
            self._conn_refs.add(weakref.ref(conn))
        return conn

    def close(self) -> None:
        """关闭当前线程的连接（TaskQueue.close 的对应物）。"""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            with _suppress(Exception):
                conn.close()
        self._local.conn = None

    def close_all(self) -> None:
        """关闭当前线程的连接；其余只移出登记（拥有线程下次自愈）。

        绝不跨线程 close——C 层连接竞争会 SIGSEGV（2026-08 CI 实证）。
        """
        refs = list(self._conn_refs)
        self._conn_refs.clear()
        for ref in refs:
            conn = ref()
            if conn is None:
                continue
            with _suppress(Exception):
                if conn is getattr(self._local, "conn", None):
                    conn.close()  # type: ignore[union-attr]  # 上已判非 None
        self._local.conn = None


class SqlitePool(_ConnPool):
    """SQLite 连接池：WAL + busy_timeout，既有行为原样保留。"""

    def __init__(self, db_path: str, schema_init=None):
        super().__init__("sqlite", schema_init=schema_init)
        self._db_path = db_path
        # 父目录不存在时 sqlite 直接 "unable to open database file"——
        # LOOM_STATE_DIR 指向未创建目录是常见启动形态，工厂负责兜底
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    def _new_conn(self):
        from .sqlite_util import enable_wal

        conn = sqlite3.connect(self._db_path, timeout=5, check_same_thread=False)
        # WAL 切换自带忙等重试（切模式不走 busy handler，多进程首建同库时
        # "database is locked"——见 sqlite_util 模块头）。
        enable_wal(conn)
        conn.execute("PRAGMA busy_timeout=3000")
        return conn

    def _probe(self, conn) -> bool:
        # 文件被删后连接仍可用（Linux 下 unlink 不影响已打开的 fd），
        # 但数据已丢失——探测磁盘文件存在性，触发重建 + schema_init 自愈。
        if not Path(self._db_path).exists():
            return False
        conn.execute("SELECT 1")
        return True

    def add_column_if_missing(self, conn, table: str, column: str,
                              ddl_type: str) -> bool:
        from .sqlite_util import add_column_if_missing

        raw = getattr(conn, "_raw", conn)
        return add_column_if_missing(raw, table, column, ddl_type)


class PostgresPool(_ConnPool):
    """Postgres 连接池（psycopg3）。事务由 `with conn:` 块管理，与 SQLite 同义。"""

    def __init__(self, dsn: str, schema_init=None):
        super().__init__("postgres", schema_init=schema_init)
        self._dsn = dsn

    def _new_conn(self):
        try:
            import psycopg
        except ImportError as e:
            raise RuntimeError(
                "LOOM_DATABASE_URL 指向 Postgres，但 psycopg 未安装："
                "pip install \"nexusloom[postgres]\"（或 pip install psycopg[binary]）"
            ) from e
        return psycopg.connect(self._dsn)

    def _probe(self, conn) -> bool:
        conn.execute("SELECT 1")
        return True

    def add_column_if_missing(self, conn, table: str, column: str,
                              ddl_type: str) -> bool:
        # PG 9.6+ 原生幂等加列；列名不来自用户输入（调用方全是常量）
        raw = getattr(conn, "_raw", conn)
        row = raw.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = %s AND column_name = %s",
            (table, column),
        ).fetchone()
        if row:
            return False
        # nosec B608：DDL 不可参数化；table/column/ddl_type 全为调用方常量，
        # 不来自请求或用户输入（PG 的 ON CONFLICT 同理，不拼用户值）
        raw.execute(  # nosec B608
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {ddl_type}")
        return True


def resolve_backend(db_path: str = "", default_name: str = "",
                    schema_init=None) -> _ConnPool:
    """按 DSN/路径选后端（业务侧拿到的 pool 用 `pool.conn()` 取连接）。

    优先级：显式 db_path > 环境变量 `LOOM_DATABASE_URL`（旧名回退）>
    `default_name` 的 SQLite 默认路径（既有行为，零变化）。

    `schema_init`：新建连接后的回调（幂等建表），用于文件被删后的自愈。
    """
    dsn = (db_path or "").strip()
    if not dsn:
        dsn = (_env_read("DATABASE_URL") or "").strip()
    if is_postgres_dsn(dsn):
        return PostgresPool(dsn, schema_init=schema_init)
    from . import state_paths

    path = dsn or state_paths.store_path(default_name)
    return SqlitePool(path, schema_init=schema_init)


class _Conn:
    """连接包装：`execute` 自动按方言翻译 SQL——业务模块 SQL 字符串零改动。

    事务语义与两边原生一致：sqlite3/psycopg 的 `with conn:` 都是
    "成功提交、异常回滚"，包装只转发 `__exit__`。
    """

    def __init__(self, pool: _ConnPool, raw: Any):
        self._pool = pool
        self._raw = raw

    def execute(self, sql: str, *args: Any) -> Any:
        return self._raw.execute(translate(self._pool, sql), *args)

    def executemany(self, sql: str, seq_of_parameters: Any) -> Any:
        """批量执行也过方言翻译（否则 PG 上占位符原样送达 psycopg 会挂）。

        参数与 SQL 文本分离（参数化），不拼接。"""
        return self._raw.executemany(translate(self._pool, sql),
                                     seq_of_parameters)

    def __enter__(self) -> _Conn:
        # sqlite3/psycopg 的 __enter__ 都返回连接自身；这里返回包装体，
        # 让 `with conn:` 之后仍走翻译层
        self._raw.__enter__()
        return self

    def __exit__(self, *exc) -> Any:
        return self._raw.__exit__(*exc)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._raw, name)


def _wrap(pool: _ConnPool, raw: Any) -> _Conn:
    return _Conn(pool, raw)


def translate(pool: _ConnPool, sql: str) -> str:
    """按后端方言翻译 SQL（SQLite 原样返回）。"""
    if pool.dialect == "postgres":
        return _translate(sql)
    return sql
