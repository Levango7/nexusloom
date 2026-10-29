"""sqlite_util 的标识符白名单判据。

PRAGMA / ALTER 的插值点无法参数化（SQL 标识符不支持占位符），sqlite_util 把
「调用方只传字面常量」从约定升级为强制校验。这组判据钉两件事：
真 DDL 照常工作；动态拼出的结构字符（; -- /*）在进入 SQLite 之前就被拒。
"""
import sqlite3

import pytest

from loom_core.sqlite_util import add_column_if_missing, column_names


def _conn():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, note TEXT)")
    return conn


def test_column_names_returns_existing_columns():
    conn = _conn()
    assert column_names(conn, "t1") == {"id", "note"}


def test_column_names_rejects_non_identifier_table():
    conn = _conn()
    with pytest.raises(ValueError, match="表名"):
        column_names(conn, "t1; DROP TABLE t1")


def test_add_column_rejects_non_identifier_table_or_column():
    conn = _conn()
    with pytest.raises(ValueError, match="表名"):
        add_column_if_missing(conn, "t1 -- x", "extra", "TEXT DEFAULT ''")
    with pytest.raises(ValueError, match="列名"):
        add_column_if_missing(conn, "t1", "extra; DROP TABLE t1", "TEXT DEFAULT ''")


def test_add_column_rejects_structural_chars_in_ddl_type():
    """分号 / 行注释 / 块注释这类结构字符一律拒收——它们改变语句结构。"""
    conn = _conn()
    for bad in ("TEXT; DROP TABLE t1", "TEXT -- x", "TEXT /* y */"):
        with pytest.raises(ValueError, match="DDL 类型"):
            add_column_if_missing(conn, "t1", "extra", bad)


def test_add_column_idempotent_with_real_ddl_types():
    """真 DDL 照常：首加 True、重加 False（幂等），带引号/空格的类型片段合法。"""
    conn = _conn()
    assert add_column_if_missing(conn, "t1", "owner_pid", "INTEGER DEFAULT 0") is True
    assert add_column_if_missing(conn, "t1", "owner_pid", "INTEGER DEFAULT 0") is False
    assert add_column_if_missing(conn, "t1", "principal", "TEXT DEFAULT ''") is True
    assert column_names(conn, "t1") >= {"owner_pid", "principal", "id", "note"}
