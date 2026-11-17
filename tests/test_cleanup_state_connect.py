"""cleanup_state 连接卫生判据（板面 §五十 规格）。

钉三件事：
1. `_connect` 是本模块唯一 connect 点，且连接的 busy_timeout=3000（与
   approvals/message_store/db_backend 同口径）——运行中的 loom 持写锁时，
   扫描/清理不再被 "database is locked" 在收尾半途炸掉；
2. 源码形状：除 `_connect` 定义内外，模块不再出现裸 sqlite3.connect；
3. 路由行为：扫描与样本两条路径真的走 `_connect`（monkeypatch 计数钉）。
"""
from __future__ import annotations

import ast
import importlib.util
import sqlite3
from pathlib import Path

import pytest

MODULE = Path(__file__).resolve().parent.parent / "scripts" / "cleanup_state.py"


def _load_module(state_dir: Path):
    """按目标 state 目录加载 cleanup_state（模块级 STATE_DIR 在导入时定死）。"""
    import os

    prev = os.environ.get("LOOM_STATE_DIR")
    os.environ["LOOM_STATE_DIR"] = str(state_dir)
    try:
        spec = importlib.util.spec_from_file_location("cleanup_state_under_test", MODULE)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    finally:
        if prev is None:
            os.environ.pop("LOOM_STATE_DIR", None)
        else:
            os.environ["LOOM_STATE_DIR"] = prev


@pytest.fixture
def state(tmp_path):
    """最小运行态目录：三个库各建被测表，各插一行夹具命名空间＋一行非夹具。"""
    d = tmp_path / "state"
    d.mkdir()
    con = sqlite3.connect(d / "message_bus.db")
    con.execute("CREATE TABLE processed_keys (key TEXT)")
    con.execute("CREATE TABLE messages (payload_json TEXT)")
    con.execute("INSERT INTO processed_keys VALUES ('test_a:writer:0')")
    con.execute("INSERT INTO messages VALUES ('{\"task_id\": \"smoke_run\"}')")
    con.commit()
    con.close()
    con = sqlite3.connect(d / "tasks.db")
    con.execute("CREATE TABLE task_queue (task_id TEXT)")
    con.execute("INSERT INTO task_queue VALUES ('user_task_1')")
    con.commit()
    con.close()
    con = sqlite3.connect(d / "quality.db")
    con.execute("CREATE TABLE quality_history (task_id TEXT, id INTEGER)")
    con.commit()
    con.close()
    return d


def test_connect_sets_busy_timeout(tmp_path):
    mod = _load_module(tmp_path)
    db = tmp_path / "t.db"
    db.touch()
    con = mod._connect(db)
    try:
        row = con.execute("PRAGMA busy_timeout").fetchone()
        assert row is not None and int(row[0]) == 3000
    finally:
        con.close()


def test_shape_connect_only_inside_helper():
    tree = ast.parse(MODULE.read_text(encoding="utf-8"))
    calls = [n for n in ast.walk(tree)
             if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Attribute) and n.func.attr == "connect"
             and isinstance(n.func.value, ast.Name) and n.func.value.id == "sqlite3"]
    assert len(calls) == 1, "裸 sqlite3.connect 必须只出现在 _connect 内"
    helper = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.FunctionDef) and n.name == "_connect")
    assert helper.lineno <= calls[0].lineno <= (helper.end_lineno or helper.lineno)


def test_scan_and_samples_route_via_connect(state):
    """扫描与样本两条路径都走 _connect（对 _connect 计数，且仍返回真连接）。"""
    mod = _load_module(state)
    real = mod._connect
    seen: list[str] = []

    def counting(db):
        seen.append(str(db))
        return real(db)

    mod._connect = counting
    report, hits, kept = mod._scan_stores()
    dropped, keep = mod._task_id_samples()
    mod._connect = real

    assert hits == 2 and kept == 1, "fixture 前提：2 行命中夹具命名空间、1 行保留"
    assert report["message_bus.db"] != [] and report["tasks.db"] != []
    # 样本路径只读 processed_keys＋task_queue（messages 的 payload 不在其列），
    # 故 dropped 里是 processed_keys 解析出的 test_a，而非 smoke_run。
    assert "test_a" in dropped and "user_task_1" in keep
    assert seen, "扫描/样本路径必须经过 _connect"
    # 3 库扫描（message_bus/tasks/quality 均存在）＋2 样本路径（bus+tdb 存在）＝5 次；
    # 计数钉让任何单点退化（某一处改回裸 connect）直接红，而不是被其余点掩护。
    assert len(seen) == 5, f"期望 5 次 _connect，实得 {len(seen)}"
    assert str(state / "message_bus.db") in seen
    assert str(state / "tasks.db") in seen
