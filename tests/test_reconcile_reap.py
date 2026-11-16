"""A7-C 僵尸回收判据（板面 §四十八 规格）。

钉六件事：
1. 正例端到端：分类①僵尸 → 队列 cancelled＋error 前缀、投影 cancelled＋
   finished_at、事件序列 run.reaped → run.cancelled、健康位升格 ok；
2. 负控不碰：新鲜/存活 pid/阈值边界/②③类/中间态一律不动（回收零写入）；
3. 幂等：二次回收 checked=0、零写（快照比对）；
4. dry-run：零写 + 预测行；
5. 竞态与冲突：finish 条件 UPDATE 0 行 ⇒ skipped；投影已终态 ⇒ 不覆写、
   不发第二条终态事件（projection_closed=False）；投影写失败 ⇒ failed 且
   队列停在 incomplete（会回到 ② 类漂移、reconcile 可见）；
6. 通道与门：CLI 人类/JSON 双通道（--reap）、旗标脱离 reconcile 动词拒收、
   LOOM_REAP_STALE_SECONDS env 门（未设/非法/≤0 不跑，合法值回收）。

漂移侧一律字面量 SQL 直插（同 test_reconcile_report.py 的手法：公共 API 的
open_run 会用 time.time() 盖 ts，会污染注入时钟 NOW 的判据）。
"""
from __future__ import annotations

import hashlib
import importlib
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parent.parent

NOW = 1_000_000.0
STALE = 900.0


@pytest.fixture
def state_dir(monkeypatch, tmp_path):
    """LOOM_STATE_DIR 指到 tmp：tasks.db / runs.db 都跟它走，并预建表。"""
    d = tmp_path / "state"
    monkeypatch.setenv("LOOM_STATE_DIR", str(d))
    _ensure_schema()
    return d


def _ensure_schema():
    from loom_core.run_store import RunStore
    from loom_core.task_queue import TaskQueue

    TaskQueue()
    RunStore()


def _dead_pid():
    """真死 pid：起一个秒退子进程并等它收尸（同 test_reconcile_report.py）。"""
    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(0.1)"])
    child.wait()
    return child.pid


def _queue_row(state_dir, *, task_id, status, started_at=0.0, finished_at=0.0,
               owner_pid=0, projection="ok"):
    conn = sqlite3.connect(state_dir / "tasks.db")
    try:
        conn.execute(
            "INSERT INTO task_queue "
            "(task_id, pipeline_name, input_file, status, config_json, projection, "
            "error, created_at, started_at, finished_at, worker_id, owner_pid) "
            "VALUES (?, ?, ?, ?, '{}', ?, '', ?, ?, ?, '', ?)",
            (task_id, "docgen", "in.md", status, projection, NOW - 10000.0,
             started_at, finished_at, owner_pid))
        conn.commit()
    finally:
        conn.close()


def _run_row(state_dir, *, run_id, status, started_at=NOW - 5000.0,
             finished_at=0.0):
    conn = sqlite3.connect(state_dir / "runs.db")
    try:
        conn.execute(
            "INSERT INTO runs "
            "(run_id, workflow, principal, status, trigger, inputs_json, "
            "started_at, finished_at, error, cost_usd, quality_score, last_seq, "
            "artifacts_json) "
            "VALUES (?, ?, '', ?, '', '{}', ?, ?, '', 0, 0, 0, '[]')",
            (run_id, "docgen", status, started_at, finished_at))
        conn.commit()
    finally:
        conn.close()


def _event(state_dir, *, run_id, seq, type_, ts):
    conn = sqlite3.connect(state_dir / "runs.db")
    try:
        conn.execute(
            "INSERT INTO events (run_id, seq, type, payload_json, ts) "
            "VALUES (?, ?, ?, '{}', ?)", (run_id, seq, type_, ts))
        conn.commit()
    finally:
        conn.close()


def _queue_fetch(state_dir, task_id):
    conn = sqlite3.connect(state_dir / "tasks.db")
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM task_queue WHERE task_id = ?", (task_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _run_fetch(state_dir, run_id):
    conn = sqlite3.connect(state_dir / "runs.db")
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT * FROM runs WHERE run_id = ?",
                           (run_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _events(state_dir, run_id):
    conn = sqlite3.connect(state_dir / "runs.db")
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT seq, type, payload_json, ts FROM events WHERE run_id = ? "
            "ORDER BY seq", (run_id,)).fetchall()
        return [{"seq": r["seq"], "type": r["type"],
                 "payload": json.loads(r["payload_json"]), "ts": r["ts"]}
                for r in rows]
    finally:
        conn.close()


def _snapshot(state_dir):
    """库内容全量指纹（WAL 归一 + 逐表 dump + 主库字节哈希）。

    与 test_reconcile_report.py 同法：先 wal_checkpoint(TRUNCATE) 归一物理
    口径，再逐表全量 dump + 主库 sha256，任何逻辑写都无处遁形。
    """
    for name in ("tasks.db", "runs.db"):
        conn = sqlite3.connect(state_dir / name)
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.commit()
        finally:
            conn.close()
    dumps = {}
    conn = sqlite3.connect(state_dir / "tasks.db")
    try:
        dumps["tasks"] = repr(conn.execute(
            "SELECT * FROM task_queue ORDER BY rowid").fetchall())
    finally:
        conn.close()
    conn = sqlite3.connect(state_dir / "runs.db")
    try:
        dumps["runs"] = repr(conn.execute(
            "SELECT * FROM runs ORDER BY rowid").fetchall())
        dumps["events"] = repr(conn.execute(
            "SELECT * FROM events ORDER BY rowid").fetchall())
    finally:
        conn.close()
    hashes = {}
    for name in ("tasks.db", "runs.db"):
        db = state_dir / name
        if db.is_file():
            hashes[name] = hashlib.sha256(db.read_bytes()).hexdigest()
    return {"dumps": dumps, "hashes": hashes}


def _reap(state_dir, **kw):
    from loom_core.reconcile import reap_stale_running
    from loom_core.run_store import RunStore
    from loom_core.task_queue import TaskQueue

    kw.setdefault("now", NOW)
    kw.setdefault("stale_seconds", STALE)
    return reap_stale_running(TaskQueue(), RunStore(), **kw)


def _zombie(state_dir, task_id="t-zombie", *, with_projection=True):
    dead = _dead_pid()
    _queue_row(state_dir, task_id=task_id, status="running",
               started_at=NOW - STALE - 5, owner_pid=dead)
    if with_projection:
        _run_row(state_dir, run_id=task_id, status="running",
                 started_at=NOW - STALE - 5)
        _event(state_dir, run_id=task_id, seq=1, type_="run.started",
               ts=NOW - STALE - 5)
    return dead


# ─── 正例端到端 ─────────────────────────────

def test_reap_zombie_end_to_end(state_dir):
    from loom_core.reconcile import REAP_ERROR_PREFIX

    dead = _zombie(state_dir)
    res = _reap(state_dir)

    assert res["checked"] == 1 and res["other_categories"] == 0
    assert len(res["reaped"]) == 1 and res["skipped"] == [] and res["failed"] == []
    r = res["reaped"][0]
    assert r["action"] == "reaped" and r["owner_pid"] == dead
    assert r["projection_closed"] is True and r["marker_upgraded"] is True

    q = _queue_fetch(state_dir, "t-zombie")
    assert q["status"] == "cancelled"
    assert q["error"].startswith(REAP_ERROR_PREFIX)
    assert q["projection"] == "ok"
    # 队列时间戳由 finish 内部盖真实时钟（无 now 注入参数）；投影/事件侧才用
    # 注入时钟。两侧同为「回收发生的时刻」，仅测试口径不同——这里只钉非零。
    assert q["finished_at"] > 0

    run = _run_fetch(state_dir, "t-zombie")
    assert run["status"] == "cancelled"
    assert run["finished_at"] == NOW

    evs = _events(state_dir, "t-zombie")
    assert [e["type"] for e in evs][-2:] == ["run.reaped", "run.cancelled"]
    reap_payload = evs[-2]["payload"]
    assert reap_payload["category"] == "stale-owner-dead"
    assert reap_payload["owner_pid"] == dead
    assert reap_payload["stale_seconds"] == STALE
    assert "reconcile" in reap_payload["source"]
    assert reap_payload["queue_started_at"] == NOW - STALE - 5
    assert evs[-2]["ts"] == NOW and evs[-1]["ts"] == NOW
    assert evs[-1]["payload"]["error"].startswith(REAP_ERROR_PREFIX)


def test_reap_missing_projection_row_created(state_dir):
    """投影行缺失的僵尸：审计事件先行由 _append 补出行，再盖章终态。"""
    _zombie(state_dir, "t-norun", with_projection=False)
    res = _reap(state_dir)

    assert len(res["reaped"]) == 1
    run = _run_fetch(state_dir, "t-norun")
    assert run is not None and run["status"] == "cancelled"
    assert run["finished_at"] == NOW
    evs = _events(state_dir, "t-norun")
    assert [e["type"] for e in evs] == ["run.reaped", "run.cancelled"]
    assert evs[0]["payload"]["queue_started_at"] == NOW - STALE - 5


# ─── 负控：一律不碰 ─────────────────────────

def test_negative_controls_untouched(state_dir):
    alive = os.getpid()
    # 新鲜 running（未过期）
    _queue_row(state_dir, task_id="fresh", status="running",
               started_at=NOW - 10, owner_pid=_dead_pid())
    # 过期但 pid 存活（慢任务，绝不能误杀）
    _queue_row(state_dir, task_id="stale-alive", status="running",
               started_at=NOW - STALE - 1, owner_pid=alive)
    # 阈值边界：恰为 cutoff 不算过期（判据为严格 <）
    _queue_row(state_dir, task_id="boundary", status="running",
               started_at=NOW - STALE, owner_pid=_dead_pid())
    # ②队列终态 + 投影 running（归补账/人工，不归 reaper）
    _queue_row(state_dir, task_id="projrun", status="done",
               started_at=NOW - 100, finished_at=NOW - 90)
    _run_row(state_dir, run_id="projrun", status="running")
    # ③队列终态 + 投影缺失
    _queue_row(state_dir, task_id="nostamp", status="failed",
               started_at=NOW - 100, finished_at=NOW - 90)
    # 中间态 pending
    _queue_row(state_dir, task_id="pending1", status="pending")

    before = _snapshot(state_dir)
    res = _reap(state_dir)
    after = _snapshot(state_dir)

    assert res["reaped"] == [] and res["failed"] == [] and res["skipped"] == []
    assert res["checked"] == 2 and res["other_categories"] == 2  # ②1 + ③1
    assert after == before  # 零写入


# ─── 幂等 / dry-run / 竞态 / 冲突 / 失败隔离 ────────────

def test_reap_idempotent(state_dir):
    _zombie(state_dir)
    first = _reap(state_dir)
    assert len(first["reaped"]) == 1
    mid = _snapshot(state_dir)
    second = _reap(state_dir)
    assert second["reaped"] == [] and second["checked"] == 0
    assert _snapshot(state_dir) == mid  # 二次零写


def test_reap_dry_run_writes_nothing(state_dir):
    _zombie(state_dir)
    before = _snapshot(state_dir)
    res = _reap(state_dir, dry_run=True)
    after = _snapshot(state_dir)
    assert after == before
    assert res["dry_run"] is True
    assert res["reaped"][0]["action"] == "dry-run"
    assert _queue_fetch(state_dir, "t-zombie")["status"] == "running"  # 队列未动


def test_reap_skips_when_queue_state_changes(state_dir):
    """竞态：finish 条件 UPDATE 0 行（写前状态已变）⇒ skipped、零写入。"""
    from loom_core.reconcile import reap_stale_running
    from loom_core.run_store import RunStore
    from loom_core.task_queue import TaskQueue

    _zombie(state_dir)
    before = _snapshot(state_dir)
    tq = TaskQueue()
    tq.finish = lambda *a, **k: False  # type: ignore[method-assign]
    res = reap_stale_running(tq, RunStore(), now=NOW, stale_seconds=STALE)

    assert res["reaped"] == [] and res["failed"] == []
    assert "写前队列状态已变" in res["skipped"][0]["reason"]
    assert _snapshot(state_dir) == before


def test_reap_projection_already_terminal_not_overwritten(state_dir):
    """①行而投影已终态：队列收尾，投影不覆写、不发第二条终态事件。"""
    dead = _dead_pid()
    _queue_row(state_dir, task_id="t-conflict", status="running",
               started_at=NOW - STALE - 5, owner_pid=dead)
    _run_row(state_dir, run_id="t-conflict", status="done",
             started_at=NOW - STALE - 5, finished_at=12345.0)

    res = _reap(state_dir)
    assert len(res["reaped"]) == 1
    assert res["reaped"][0]["projection_closed"] is False
    run = _run_fetch(state_dir, "t-conflict")
    assert run["status"] == "done" and run["finished_at"] == 12345.0
    types = [e["type"] for e in _events(state_dir, "t-conflict")]
    assert "run.reaped" in types and "run.cancelled" not in types
    assert _queue_fetch(state_dir, "t-conflict")["status"] == "cancelled"


def test_reap_projection_failure_isolated(state_dir):
    """投影写失败：该行 failed 且停在 incomplete，其余行照常回收。"""
    from loom_core.reconcile import reap_stale_running
    from loom_core.run_store import RunStore
    from loom_core.task_queue import TaskQueue

    _zombie(state_dir, "t-bad")
    _zombie(state_dir, "t-good")

    rs = RunStore()
    real_close = rs.close_run

    def flaky_close(run_id, *a, **k):
        if run_id == "t-bad":
            raise RuntimeError("boom")
        return real_close(run_id, *a, **k)

    rs.close_run = flaky_close  # type: ignore[method-assign]
    res = reap_stale_running(TaskQueue(), rs, now=NOW, stale_seconds=STALE)

    assert len(res["reaped"]) == 1 and res["reaped"][0]["task_id"] == "t-good"
    assert len(res["failed"]) == 1 and res["failed"][0]["task_id"] == "t-bad"
    assert "投影写入失败" in res["failed"][0]["reason"]
    bad_q = _queue_fetch(state_dir, "t-bad")
    assert bad_q["status"] == "cancelled" and bad_q["projection"] == "incomplete"
    bad_run = _run_fetch(state_dir, "t-bad")
    assert bad_run["status"] == "running"  # 停在漂移里，reconcile 报告可见


# ─── env 门 ─────────────────────────────

def test_env_gate(state_dir):
    from loom_core.reconcile import reap_from_env
    from loom_core.run_store import RunStore
    from loom_core.task_queue import TaskQueue

    tq, rs = TaskQueue(), RunStore()
    assert reap_from_env(tq, rs, env={}) is None
    assert reap_from_env(tq, rs, env={"LOOM_REAP_STALE_SECONDS": ""}) is None
    assert reap_from_env(tq, rs, env={"LOOM_REAP_STALE_SECONDS": "abc"}) is None
    assert reap_from_env(tq, rs, env={"LOOM_REAP_STALE_SECONDS": "0"}) is None
    assert reap_from_env(tq, rs, env={"LOOM_REAP_STALE_SECONDS": "-5"}) is None
    # RunStore 建口失败（None）：不猜不跑
    assert reap_from_env(tq, None,
                         env={"LOOM_REAP_STALE_SECONDS": "900"}) is None

    _zombie(state_dir)
    res = reap_from_env(tq, rs, env={"LOOM_REAP_STALE_SECONDS": str(STALE)})
    assert res is not None and len(res["reaped"]) == 1


# ─── CLI 端到端 与 旗标翻译 ─────────────────

def _cli(state_dir, *argv):
    return subprocess.run(
        [sys.executable, "run.py", "reconcile", *argv],
        cwd=str(PROJECT), capture_output=True, text=True, timeout=120,
        env={**os.environ, "LOOM_STATE_DIR": str(state_dir)})


def test_cli_reap_human(state_dir):
    _zombie(state_dir)
    proc = _cli(state_dir, "--reap")
    assert proc.returncode == 0
    assert "[reap] 回收 1 行" in proc.stdout
    assert "[reaped] task=t-zombie" in proc.stdout
    assert "②③ 类未动" in proc.stdout
    assert _queue_fetch(state_dir, "t-zombie")["status"] == "cancelled"


def test_cli_reap_json(state_dir):
    _zombie(state_dir)
    proc = _cli(state_dir, "--reap", "--json-output")
    assert proc.returncode == 0
    lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert isinstance(data, dict)
    assert len(data["reaped"]) == 1
    assert data["reaped"][0]["task_id"] == "t-zombie"


def test_cli_reap_flag_requires_reconcile_verb(state_dir):
    """带输入文件但没走 reconcile 动词：--reap 拒收（防「以为回收了」）。"""
    proc = subprocess.run(
        [sys.executable, "run.py", "no-such-input.md", "--reap"],
        cwd=str(PROJECT), capture_output=True, text=True, timeout=120,
        env={**os.environ, "LOOM_STATE_DIR": str(state_dir)})
    assert proc.returncode == 2
    assert "只在 loom reconcile 动词下生效" in proc.stderr


def test_reap_flag_parses_and_independent(state_dir):
    run = importlib.import_module("run")
    verb, action, argv = run._split_verb(["reconcile", "--reap"])
    assert (verb, action) == ("reconcile", "reconcile")
    args = run.build_arg_parser().parse_args(argv)
    assert args.reap is True
    # 缺席默认 False（普通 reconcile 仍只读）
    args2 = run.build_arg_parser().parse_args([])
    assert args2.reap is False
