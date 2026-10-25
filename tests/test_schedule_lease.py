"""schedules 触发租约（run_store.py 的 schedules 表 + triggers.py 的 B4 接线）。

覆盖三层：
  A. 存储层原子语义——互斥、本人续租、到期接管、只放自己的约、登记不碰租约列；
  B. 真跨进程——两个独立解释器进程抢同一触发，恰好一个拿到（`loom --triggers`
     双开不再重复触发的那条验收判据）；
  C. 真 Postgres（`LOOM_TEST_PG_URL` 未设则 skip）——租约语义在 PG 方言下同义。

诚实边界：本文件不测"两个真 `--triggers` 循环整晚只跑一遍"（那是 24h 级的
观测题）；tick 级的接管正确性（被拒后 last_check 前移防重复提交）在
`tests/test_triggers.py::TestTickLease`。
"""
import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loom_core.run_store import RunStore  # noqa: E402

PG_URL = os.environ.get("LOOM_TEST_PG_URL", "").strip()

try:
    import psycopg  # noqa: F401

    _HAS_PSYCOPG = True
except ImportError:
    _HAS_PSYCOPG = False


@pytest.fixture()
def store(tmp_path, monkeypatch) -> RunStore:
    monkeypatch.setenv("LOOM_STATE_DIR", str(tmp_path))
    return RunStore()


class TestScheduleStore:
    def test_register_and_list_roundtrip(self, store):
        store.register_schedule("nightly", pipeline="docgen", cron="@daily")
        rows = store.list_schedules()
        assert len(rows) == 1
        r = rows[0]
        assert (r["name"], r["pipeline"], r["cron"],
                r["enabled"]) == ("nightly", "docgen", "@daily", True)
        assert r["lease_owner"] == "" and r["lease_until"] == 0
        assert r["last_fire"] == 0 and r["next_fire"] == 0

    def test_register_update_keeps_lease(self, store):
        """crontab 轮换覆盖配置列，但**不碰租约列**——清了别人的约就有两个触发者。"""
        store.register_schedule("job", pipeline="docgen", cron="@daily")
        assert store.acquire_schedule_lease("job", owner="p1", ttl=60.0, now=1000.0)
        store.register_schedule("job", pipeline="report", cron="@hourly")
        r = store.list_schedules()[0]
        assert (r["pipeline"], r["cron"]) == ("report", "@hourly")
        assert r["lease_owner"] == "p1"

    def test_two_owners_mutually_exclusive(self, store):
        store.register_schedule("job", pipeline="docgen", cron="@daily")
        assert store.acquire_schedule_lease("job", owner="p1", ttl=60.0, now=1000.0)
        assert not store.acquire_schedule_lease("job", owner="p2", ttl=60.0, now=1010.0)

    def test_holder_renews(self, store):
        """持有者每轮续租都要成功（TTL 被推远），这正是"活着就持续持有"的机制。"""
        store.register_schedule("job", pipeline="docgen", cron="@daily")
        assert store.acquire_schedule_lease("job", owner="p1", ttl=60.0, now=1000.0)
        assert store.acquire_schedule_lease("job", owner="p1", ttl=60.0, now=1050.0)
        assert store.list_schedules()[0]["lease_until"] == pytest.approx(1110.0)

    def test_expired_lease_taken_over(self, store):
        store.register_schedule("job", pipeline="docgen", cron="@daily")
        assert store.acquire_schedule_lease("job", owner="p1", ttl=10.0, now=1000.0)
        assert not store.acquire_schedule_lease("job", owner="p2", ttl=10.0, now=1005.0)
        assert store.acquire_schedule_lease("job", owner="p2", ttl=10.0, now=1010.5)

    def test_release_only_own_lease(self, store):
        store.register_schedule("job", pipeline="docgen", cron="@daily")
        store.acquire_schedule_lease("job", owner="p1", ttl=60.0, now=1000.0)
        assert not store.release_schedule_lease("job", owner="p2")
        assert store.list_schedules()[0]["lease_owner"] == "p1"
        assert store.release_schedule_lease("job", owner="p1")
        assert store.list_schedules()[0]["lease_owner"] == ""
        assert store.acquire_schedule_lease("job", owner="p2", ttl=60.0, now=1001.0)

    def test_acquire_unknown_schedule_refused_not_created(self, store):
        """没登记过的触发不能被"顺手"建出来——登记是配置侧的事。"""
        assert not store.acquire_schedule_lease("ghost", owner="p1",
                                                ttl=60.0, now=1000.0)
        assert store.list_schedules() == []

    def test_record_fire_updates_observability(self, store):
        store.register_schedule("job", pipeline="docgen", cron="@daily")
        store.record_schedule_fire("job", fired_at=1700.0, next_fire=5300.0)
        r = store.list_schedules()[0]
        assert r["last_fire"] == 1700.0 and r["next_fire"] == 5300.0

    def test_schema_version_covers_schedules(self, store):
        assert store.schema_version() >= 3


_DRIVER = '''
import sys
sys.path.insert(0, {project!r})
from loom_core.run_store import RunStore
store = RunStore()
store.register_schedule({name!r}, pipeline="docgen", cron="@daily")
ok = store.acquire_schedule_lease({name!r}, owner={owner!r}, ttl=30.0)
print("ACQUIRED" if ok else "DENIED")
'''


class TestCrossProcess:
    def test_two_processes_single_holder(self, tmp_path, monkeypatch):
        """两个独立解释器进程抢同一触发，恰好一个拿到（真跨进程互斥）。

        双方各走一次完整的 register → acquire，SQLite 侧由 BEGIN IMMEDIATE
        串行化，`ttl=30` 覆盖整个测试窗口——第二个进程不可能等到过期。
        """
        monkeypatch.setenv("LOOM_STATE_DIR", str(tmp_path))
        env = {**os.environ, "LOOM_STATE_DIR": str(tmp_path)}
        project = str(Path(__file__).resolve().parent.parent)
        procs = [
            subprocess.Popen(
                [sys.executable, "-c", _DRIVER.format(project=project,
                                                      name="job", owner=f"p{i}")],
                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, cwd=project)
            for i in range(2)]
        outs = [p.communicate(timeout=180) for p in procs]
        for p, (_out, err) in zip(procs, outs, strict=True):
            assert p.returncode == 0, err[-500:]
        results = [out.strip() for out, _ in outs]
        assert results.count("ACQUIRED") == 1, results
        assert results.count("DENIED") == 1, results


@pytest.mark.skipif(not (PG_URL and _HAS_PSYCOPG),
                    reason="LOOM_TEST_PG_URL 未设置或 psycopg 未安装（真 PG 判据）")
class TestScheduleLeasePostgres:
    def test_register_acquire_release_takeover_on_real_pg(self):
        store = RunStore(PG_URL)
        store.register_schedule("job", pipeline="docgen", cron="@daily")
        assert store.acquire_schedule_lease("job", owner="pg-p1", ttl=60.0)
        assert not store.acquire_schedule_lease("job", owner="pg-p2", ttl=60.0)
        assert store.release_schedule_lease("job", owner="pg-p1")
        assert store.acquire_schedule_lease("job", owner="pg-p2", ttl=60.0)
        assert store.list_schedules()[0]["lease_owner"] == "pg-p2"
