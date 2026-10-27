"""GC 的活性与账目判据（A6 在"真实运行形态"下的缺口）。

这一批盯的不是"GC 函数对不对"，而是**它到底会不会被跑到、账报得对不对**：

  A. worker 有活时也要回收 —— 原实现把 `_maybe_gc()` 放在 `if item is None:`
     分支里，持续有活的 worker（最需要回收的那一个）永远不回收；
  B. 常驻服务（`--daemon`/`--admin`/`--dashboard`）的回收节拍 —— 服务路径上根本没有
     worker 循环，保留期配置此前等于摆设；
  C. 死信表 `dlq` 的回收面 —— 只进不出的表（A6 漏掉的最后一张）；
  D. GC 的账：`enabled: True` 曾被算成"删了 1 行"、`run_store` 的逐表 dict 被整个
     跳过（本机实测：删 18 行、日志写"删除 1 行"），以及 `loom --gc` 的逐表打印。
"""
from __future__ import annotations

import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

PROJECT = Path(__file__).resolve().parent.parent


def _fake_cfg(mapping: dict):
    """ConfigCenter 的最小替身：点号键取值，缺键给 default。"""
    return SimpleNamespace(get=lambda key, default=None: mapping.get(key, default))


def _worker(monkeypatch, tmp_path, *, gc_ticks=2, retention_days=30):
    from loom_core.worker import TaskWorker

    monkeypatch.setenv("LOOM_STATE_DIR", str(tmp_path / "state"))
    orch = SimpleNamespace(
        config=_fake_cfg({"maintenance.gc_ticks": gc_ticks,
                          "retention.runs_days": retention_days}),
        task_queue=SimpleNamespace())
    w = TaskWorker(queue=SimpleNamespace(recover=lambda **kw: [],
                                         acquire=lambda **kw: None),
                   scheduler=SimpleNamespace())
    w._orch = orch
    return w


def _run_for(worker, seconds: float, **kw) -> int:
    stop = threading.Event()
    t = threading.Thread(target=lambda: (time.sleep(seconds), stop.set()),
                        daemon=True)
    t.start()
    return worker.run_forever(stop_event=stop, **kw)


@pytest.fixture()
def gc_calls(monkeypatch):
    """把 gc_from_orchestrator 换成记账替身（worker 与 daemon 走同一处 import）。"""
    import loom_core.maintenance as m

    calls: list = []

    def fake(orch, now=None, log=None):
        calls.append(orch)
        return {"enabled": True}

    monkeypatch.setattr(m, "gc_from_orchestrator", fake)
    return calls


# ── A. worker 有活也要回收 ────────────────────────────────

class TestBusyWorkerStillGcs:
    def test_gc_runs_while_the_worker_is_busy(self, monkeypatch, tmp_path, gc_calls):
        """每一轮都有活儿时，GC 仍要按 tick 跑。

        退化形状（原实现）：GC 在 `if item is None:` 分支里 ⇒ 这条判据 0 次调用。
        """
        w = _worker(monkeypatch, tmp_path, gc_ticks=2)
        monkeypatch.setattr(w, "run_once", lambda: ("job",))
        _run_for(w, 0.6, poll_interval=0.02)
        assert len(gc_calls) >= 2, f"有活时 GC 也应每 2 轮跑一次，实际 {len(gc_calls)}"

    def test_gc_does_not_run_before_its_interval(self, monkeypatch, tmp_path,
                                                 gc_calls):
        """反向对照：间隔没到就一次都不许跑（否则"每轮都 GC"也能骗绿）。

        空转取件（`run_once → None`）才有 sleep，tick 的推进是可控的；有活的分支
        不带 sleep，几百毫秒就能把 1000 tick 跑完——那会把"间隔门"测成"跑得够快"。
        """
        w = _worker(monkeypatch, tmp_path, gc_ticks=1000)
        monkeypatch.setattr(w, "run_once", lambda: None)
        _run_for(w, 0.3, poll_interval=0.05)
        assert gc_calls == []

    def test_idle_worker_still_exits_on_idle_timeout(self, monkeypatch, tmp_path,
                                                     gc_calls):
        """把 GC 挪出空转分支不许弄坏空转超时退出。"""
        w = _worker(monkeypatch, tmp_path, gc_ticks=1)
        monkeypatch.setattr(w, "run_once", lambda: None)
        processed = _run_for(w, 0.8, poll_interval=0.02, idle_timeout=0.1)
        assert processed == 0
        assert gc_calls, "空转时同样要回收（这条是挪分支时的回归对照）"


# ── B. 常驻服务的回收节拍 ─────────────────────────────────

class TestDaemonGcDue:
    def _due(self, monkeypatch, *, interval, calls):
        import loom_core.maintenance as m
        import run as run_mod

        monkeypatch.setattr(m, "gc_from_orchestrator",
                            lambda orch, now=None, log=None: calls.append(1))
        monkeypatch.setattr(m, "gc_interval_seconds", lambda cfg: interval)
        orch = SimpleNamespace(config=SimpleNamespace())
        return lambda last, now: run_mod._daemon_gc_due(orch, last, now=now)

    def test_runs_when_the_interval_elapsed(self, monkeypatch):
        calls: list = []
        due = self._due(monkeypatch, interval=600, calls=calls)
        assert due(1000.0, 1000.0 + 601) == 1000.0 + 601
        assert calls == [1]
        # 没到点：不跑，也不推进"上次回收"时刻（否则节拍会被抖没）
        assert due(1000.0 + 601, 1000.0 + 700) == 1000.0 + 601
        assert calls == [1]

    def test_zero_interval_is_an_explicit_off_switch(self, monkeypatch):
        calls: list = []
        due = self._due(monkeypatch, interval=0, calls=calls)
        assert due(0.0, 10 ** 9) == 0.0
        assert calls == [], "gc_seconds=0 是显式关闭，不是忘了接"

    def test_gc_failure_does_not_kill_the_service(self, monkeypatch):
        import loom_core.maintenance as m
        import run as run_mod

        def boom(orch, now=None, log=None):
            raise RuntimeError("db closed")

        monkeypatch.setattr(m, "gc_from_orchestrator", boom)
        monkeypatch.setattr(m, "gc_interval_seconds", lambda cfg: 1)
        orch = SimpleNamespace(config=SimpleNamespace())
        assert run_mod._daemon_gc_due(orch, 0.0, now=10.0) == 10.0


# ── C. 死信表回收 ────────────────────────────────────────

class TestDlqPrune:
    def _store(self, tmp_path):
        from loom_core.message_store import PersistentStore

        return PersistentStore(db_path=str(tmp_path / "bus.db"))

    def _seed(self, store, n: int, *, ages: list[float] | None = None):
        from loom_core.message_store import Message, MessageType

        ages = ages or [0.0] * n
        conn = store._get_conn()
        now = time.time()
        for i in range(n):
            store.move_to_dlq(Message(topic=f"t{i}", payload={"i": i},
                                      msg_type=MessageType.EVENT), "boom")
        ids = [r[0] for r in conn.execute(
            "SELECT id FROM dlq ORDER BY id ASC").fetchall()]
        with conn:
            for i, rid in enumerate(ids):
                conn.execute("UPDATE dlq SET failed_at = ? WHERE id = ?",
                             (now - ages[i], rid))
        return ids

    def _count(self, store) -> int:
        conn = store._get_conn()
        return int(conn.execute("SELECT COUNT(*) FROM dlq").fetchone()[0])

    def test_zero_means_disabled_not_delete_all(self, tmp_path):
        """这张表从来没有清理面：0 不许被读成"删光"（与 task_queue 的历史语义不同）。"""
        store = self._store(tmp_path)
        self._seed(store, 3)
        assert store.prune_dlq(0, 0) == 0
        assert self._count(store) == 3

    def test_max_rows_keeps_the_newest(self, tmp_path):
        store = self._store(tmp_path)
        self._seed(store, 4, ages=[40, 30, 20, 10])
        assert store.prune_dlq(0, 2) == 2
        conn = store._get_conn()
        left = [r[0] for r in conn.execute(
            "SELECT topic FROM dlq ORDER BY failed_at DESC").fetchall()]
        assert left == ["t3", "t2"]

    def test_age_deletes_only_the_old_ones(self, tmp_path):
        store = self._store(tmp_path)
        self._seed(store, 3, ages=[30 * 86400, 20 * 86400, 60])
        assert store.prune_dlq(7, 0) == 2
        assert self._count(store) == 1

    def test_prune_is_idempotent(self, tmp_path):
        store = self._store(tmp_path)
        self._seed(store, 2)
        assert store.prune_dlq(0, 1) == 1
        assert store.prune_dlq(0, 1) == 0

    def test_gc_sweeps_the_dlq(self, tmp_path):
        from loom_core.maintenance import Retention, gc_runs

        store = self._store(tmp_path)
        self._seed(store, 2)
        summary = gc_runs(dlq=store, retention=Retention(days=30, max_runs=1))
        assert summary["dlq"] == 1

    def test_gc_skips_memory_only_buses(self):
        """内存总线没有 store：这一面跳过，不许为此报 failed。"""
        from loom_core.maintenance import Retention, gc_runs

        summary = gc_runs(dlq=None, retention=Retention(days=30))
        assert summary["dlq"] is None


# ── D. GC 的账 ──────────────────────────────────────────

class TestGcAccounting:
    def test_count_dropped_adds_dicts_and_ignores_flags(self):
        from loom_core.maintenance import count_dropped

        summary = {"enabled": True, "queue": 0, "cost": None,
                   "run_store": {"runs": 3, "run_nodes": 5, "events": 9,
                                 "approvals": 1},
                   "deliveries": 2, "dlq": 4}
        assert count_dropped(summary) == 24
        assert count_dropped({"enabled": True, "queue": 0}) == 0
        # 布尔不是行数（`True` 是 int 的子类，这一条单独钉住 bool 守卫）
        assert count_dropped({"queue": True, "cost": False}) == 0

    def test_gc_log_reports_the_real_row_count(self):
        from loom_core.maintenance import Retention, gc_runs

        logs: list[str] = []
        rs = SimpleNamespace(cleanup=lambda days, rows, now=None: {
            "runs": 3, "run_nodes": 5, "events": 9, "approvals": 1})
        gc_runs(run_store=rs,
                deliveries=SimpleNamespace(
                    cleanup=lambda days, rows, now=None: 2),
                retention=Retention(days=30, max_runs=500), now=1.0,
                log=logs.append)
        assert any("删除 20 行" in m for m in logs), logs

    def test_format_summary_lists_each_store(self):
        from loom_core.maintenance import Retention, format_summary

        summary = {"enabled": True, "queue": 0,
                   "run_store": {"runs": 3, "events": 9}, "deliveries": 2}
        out = format_summary(summary, Retention(days=30, max_runs=500))
        assert "保留 30 天 / 最近 500 条" in out
        assert "run_store: events=9 runs=3" in out
        assert "合计删除 14 行" in out
        assert "{" not in out, "打印里不该再出现原始 dict"

    def test_format_summary_shows_a_failed_store(self):
        from loom_core.maintenance import format_summary

        out = format_summary({"enabled": True,
                              "dlq": {"failed": True, "message": "db is locked"}})
        assert "dlq: 失败（db is locked）" in out

    def test_format_summary_disabled_message(self):
        from loom_core.maintenance import format_summary

        assert "retention 未启用" in format_summary({"enabled": False})

    def test_gc_cli_end_to_end_enabled_prints_per_store(self, tmp_path):
        """启用态的 CLI 输出逐表可读。

        走 `DOCPIPE_` 环境覆写（`DOCPIPE_RETENTION__RUNS_DAYS` → `retention.runs_days`），
        不碰仓库配置；状态目录另指，用例之间互不污染。
        """
        import os
        import subprocess
        import sys

        env = dict(os.environ,
                   DOCPIPE_RETENTION__RUNS_DAYS="7",
                   DOCPIPE_RETENTION__MAX_RUNS="500",
                   LOOM_STATE_DIR=str(tmp_path / "state"))
        r = subprocess.run(
            [sys.executable, str(PROJECT / "run.py"), "--gc"],
            capture_output=True, text=True, timeout=180, cwd=str(tmp_path), env=env)
        assert r.returncode == 0, r.stderr[-800:]
        out = r.stdout
        assert "保留 7 天 / 最近 500 条" in out, out[-800:]
        assert "合计删除" in out, out[-800:]
        assert "run_store:" in out, out[-800:]
        tail = out[out.index("保留 7 天"):]
        assert "{" not in tail, f"启用态还在打印原始 dict：{tail[-400:]}"
