"""交付追溯面判据（#8 遗留三件里的写侧两件：产物 sha256/保留期 + 逐运行锁指纹）。

设计复核 2026-10-20 §3.1 A2 那一行要的是"交付物浏览器（sha256/字节/来源节点/
保留期）"，读侧（`GET /api/deliveries*`）在续64 已经落地，剩下两半都卡在写侧：

  - **摘要**：产物值只活到进程结束，账本里没有 sha256 的话，"当时交付的到底是
    哪一份内容"事后无法核对；
  - **保留期**：`deliveries.db` 曾是唯一没被 A6 GC 覆盖的存储——运行从 RunStore
    删了，账本行还在长；
  - **逐运行锁指纹**：`config_hash`/`topology_hash` 此前只在 `verify_lockfile`
    那一瞬存在于内存，`GET /api/locks` 只能回答"现在这两份文件对不对"，回答
    不了"某次运行时用的锁对不对"。

本文件钉五组：
  A. 摘要函数本身（规范形态、键序无关、读不到时的形状）；
  B. 账本新增三列 + 老库升级 + 与 RunStore 的终态口径不分叉；
  C. 真跑一次：摘要与磁盘字节一致、锁指纹是**当时算的**（事后改锁不动它）、
     到期时刻按写入时的配置冻结；三种"没核对/没锁"形状彼此可区分；
  D. `DeliveryLedger.cleanup`：冻结到期、**不 backfill 历史行**、数量上限、
     未终态不碰、**不删磁盘产物**；
  E. GC 接线：账本被扫到、`count_dropped` 那处历史错账（dict 漏计 + `enabled`
     布尔被当成一行）、`loom --gc` 的逐表打印。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.test_delivery_forms import DEMO_YAML, _plan

PROJECT = Path(__file__).resolve().parent.parent


# ── 夹具 ────────────────────────────────────────────────

@pytest.fixture()
def state(tmp_path, monkeypatch):
    """账本按测试独立：LOOM_STATE_DIR 指到 tmp，避免跨用例读到残留行。"""
    d = tmp_path / "gc-state"
    d.mkdir()
    monkeypatch.setenv("LOOM_STATE_DIR", str(d))
    return d


def _ledger(state):
    from loom_core.deliveries import DeliveryLedger
    return DeliveryLedger(db_path=str(state / "deliveries.db"))


def _rec(run_id: str, *, status="done", finished_at=1_000_000.0,
         retention_until=0.0, sha="a" * 64, nbytes=128, output_path=""):
    return {
        "run_id": run_id,
        "pipeline": "tracing-demo",
        "status": status,
        "finished_at": finished_at,
        "duration_sec": 1.0,
        "output_path": output_path,
        "output_digest": {"sha256": sha, "bytes": nbytes, "kind": "file"},
        "retention_until": retention_until,
        "nodes": [],
    }


def _locked_plan(yaml_text: str, tmp_path: Path, name: str):
    """写临时流水线**并生成锁**，再按执行侧口径（verify_lock=True）解析。

    `tests.test_delivery_forms._plan` 走的是 verify_lock=False——临时流水线没有
    锁，那条路径正是"没核对"的形状；这里要的是"有锁且一致"的形状，得先 --write-lock。
    """
    from loom_core.scheduler import Scheduler

    out = tmp_path / name
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{name}.yaml"
    path.write_text(yaml_text, encoding="utf-8")
    sched = Scheduler(agents_dir=str(PROJECT / "agents"), pipeline_dir=str(out))
    plan = sched.parse_file(str(path), verify_lock=False)
    lock = Path(sched.generate_lockfile(plan, output_dir=str(out)))
    return sched, plan, lock, out


def _demo_yaml(tmp_path: Path, tag: str):
    """离线 demo 流水线，且**输出目录与 `_plan` 建的目录同名**。

    `tests.test_delivery_forms` 那套夹具里 name 就是目录名；这里若让两者不一致，
    `pipeline.deliver.json` 会写到另一个地方，判据读的是"没写出来的那个文件"——
    第一轮红就是这么来的（FileNotFoundError: .../result.json）。
    """
    name = f"tracing-{tag}"
    out_dir = tmp_path / name
    yaml_text = DEMO_YAML.replace("deliver-demo", name).format(out=str(out_dir))
    return name, out_dir, yaml_text


def _demo_plan(tmp_path: Path, tag: str):
    """不核对锁的解析（verify_lock=False）：账本里的 `lock` 应当是空 dict。"""
    name, out_dir, yaml_text = _demo_yaml(tmp_path, tag)
    plan, _out = _plan(yaml_text, tmp_path, name)
    return plan, out_dir


def _demo_verified(tmp_path: Path, tag: str):
    """执行侧口径（verify_lock=True）解析一条**没有锁**的流水线：不抛，
    指纹如实记 `lock_present: false` / `matches: null`。"""
    from loom_core.scheduler import Scheduler

    name, out_dir, yaml_text = _demo_yaml(tmp_path, tag)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{name}.yaml"
    path.write_text(yaml_text, encoding="utf-8")
    sched = Scheduler(agents_dir=str(PROJECT / "agents"), pipeline_dir=str(out_dir))
    return sched.parse_file(str(path), verify_lock=True), out_dir


def _demo_locked(tmp_path: Path, tag: str):
    """同 `_demo_verified`，但先写锁再解析：要的是"有锁且一致"那一种形状。"""
    name, out_dir, yaml_text = _demo_yaml(tmp_path, tag)
    sched, _unlocked, _lock, _out = _locked_plan(yaml_text, tmp_path, name)
    plan = sched.parse_file(str(out_dir / f"{name}.yaml"), verify_lock=True)
    return plan, out_dir / f"{name}.lock", name


# ── A. 摘要函数 ─────────────────────────────────────────

class TestDigest:
    def test_string_is_digested_over_its_utf8_bytes(self):
        """文本摘要要能对着文件核：按 UTF-8 原文，不是按 Python 的 repr。"""
        from loom_core.deliveries import digest_of

        value = "# 标题\n中文正文"
        got = digest_of(value)
        assert got["sha256"] == hashlib.sha256(value.encode("utf-8")).hexdigest()
        assert got["bytes"] == len(value.encode("utf-8"))
        assert got["kind"] == "text"

    def test_dict_digest_is_key_order_invariant(self):
        """同一个 dict 换个键序就换一个 sha，"追溯"当场失效——规范 JSON 是必须的。"""
        from loom_core.deliveries import digest_of

        a = digest_of({"x": 1, "y": {"p": 2, "q": 3}})
        b = digest_of({"y": {"q": 3, "p": 2}, "x": 1})
        assert a == b
        assert a["kind"] == "json"

    def test_unserialisable_value_still_gets_a_digest(self):
        """编不动的对象退到 repr 而不是抛：交付收尾那条路绝不外抛。"""
        from loom_core.deliveries import digest_of

        got = digest_of({(1, 2), 3})          # set 进不了 JSON
        assert got["kind"] == "json"
        assert len(got["sha256"]) == 64 and got["bytes"] > 0

    def test_digest_file_matches_the_bytes_on_disk(self, tmp_path):
        from loom_core.deliveries import digest_file

        p = tmp_path / "result.md"
        p.write_bytes(b"# doc\nDELIVERY-OK\n")
        got = digest_file(str(p))
        assert got["sha256"] == hashlib.sha256(p.read_bytes()).hexdigest()
        assert got["bytes"] == p.stat().st_size
        assert got["kind"] == "file"

    def test_unreadable_file_is_reported_not_dropped(self, tmp_path):
        """读不到 → sha 空串 + bytes=-1，这条仍在账本上（沿用 _size_of 的约定）。"""
        from loom_core.deliveries import UNREADABLE, digest_file

        missing = str(tmp_path / "gone.md")
        got = digest_file(missing)
        assert got["sha256"] == ""
        assert got["bytes"] == UNREADABLE
        assert "error" in got

    def test_empty_path_gives_zero_bytes_not_an_error(self):
        from loom_core.deliveries import digest_file

        assert digest_file("") == {"sha256": "", "bytes": 0, "kind": "file"}

    def test_artifact_digests_only_for_dict_results(self):
        """非 dict 结果没有"逐键产物"可言：给空表，与 artifact_keys 口径一致。"""
        from loom_core.deliveries import artifact_digests, digest_of

        assert artifact_digests(None) == {}
        assert artifact_digests("正文") == {}
        got = artifact_digests({"content": "abc", "meta": {"n": 1}})
        assert list(got) == ["content", "meta"]        # 键名有序
        assert got["content"]["sha256"] == digest_of("abc")["sha256"]


# ── B. 账本的列与迁移 ───────────────────────────────────

class TestLedgerColumns:
    def test_digest_columns_round_trip(self, state):
        led = _ledger(state)
        led.record(_rec("row-1"))
        row = led.row_digest("row-1")
        assert row == {"sha256": "a" * 64, "bytes": 128, "retention_until": 0.0}
        assert led.row_digest("nope") == {}

    def test_columns_exist_on_the_table(self, state):
        """载荷里有不算数——列要在，账本才能力求"不解析大 JSON 也能核对"。"""
        _ledger(state).record(_rec("row-2"))
        conn = sqlite3.connect(str(state / "deliveries.db"))
        try:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(deliveries)")}
        finally:
            conn.close()
        assert {"sha256", "bytes", "retention_until"} <= cols

    def test_old_database_gets_the_new_columns(self, state):
        """老库（七个列）不会被 `CREATE TABLE IF NOT EXISTS` 动到——必须补 ALTER。

        这条同时是"不 backfill"的现场：升级后老行的摘要如实为空。
        """
        db = state / "deliveries.db"
        conn = sqlite3.connect(str(db))
        with conn:
            conn.execute(
                "CREATE TABLE deliveries ("
                " run_id TEXT PRIMARY KEY, pipeline TEXT NOT NULL,"
                " status TEXT NOT NULL, finished_at REAL NOT NULL DEFAULT 0,"
                " duration_sec REAL, output_path TEXT DEFAULT '',"
                " payload_json TEXT NOT NULL DEFAULT '{}')")
            conn.execute(
                "INSERT INTO deliveries (run_id, pipeline, status, finished_at,"
                " payload_json) VALUES (?,?,?,?,'{}')",
                ("legacy-1", "p", "done", 500.0))
            conn.execute("INSERT INTO deliveries (run_id, pipeline, status,"
                         " finished_at, payload_json) VALUES (?,?,?,?,'{}')",
                         ("legacy-running", "p", "running", 0.0))
        conn.close()

        led = _ledger(state)
        assert led.row_digest("legacy-1") == {"sha256": "", "bytes": 0,
                                              "retention_until": 0.0}
        assert led.count() == 2
        # 老行 retention_until=0 → 年龄判据一行不删（不 backfill 的代价照实说）
        assert led.cleanup(retention_days=1, now=10 ** 12) == 0
        assert led.count() == 2

    def test_terminal_statuses_agree_with_run_store(self):
        """两处"终态"必须是同一套三值：判据漂移会让一张表删了、另一张表没删。"""
        from loom_core import run_store
        from loom_core.deliveries import TERMINAL_STATUSES

        assert tuple(TERMINAL_STATUSES) == tuple(run_store.TERMINAL_STATUSES)

    def test_record_accepts_a_digest_that_is_absent(self, state):
        """没有 output_path 的运行（失败运行）照样记一行，摘要列留空。"""
        led = _ledger(state)
        rec = _rec("no-output", status="failed")
        rec["output_path"] = ""
        rec["output_digest"] = {}
        led.record(rec)
        assert led.row_digest("no-output") == {"sha256": "", "bytes": 0,
                                               "retention_until": 0.0}

    def test_broken_digest_values_do_not_blow_up_the_write(self, state):
        """摘要字段坏值退到 0，但 -1（读不到）要原样留着——归零等于抹掉这条产物。"""
        led = _ledger(state)
        rec = _rec("bad-digest")
        rec["output_digest"] = {"sha256": None, "bytes": "not-a-number"}
        led.record(rec)
        assert led.row_digest("bad-digest")["bytes"] == 0
        rec2 = _rec("minus-one")
        rec2["output_digest"] = {"sha256": "", "bytes": -1}
        led.record(rec2)
        assert led.row_digest("minus-one")["bytes"] == -1


# ── C. 真跑一次：摘要与锁指纹确实落到账本 ───────────────

class TestRunWritesTracingFacts:
    def _run(self, orch, tmp_path, plan, task_id):
        (tmp_path / "in.md").write_text("# 输入\n追溯判据输入。", encoding="utf-8")
        return orch.run_plan(plan, input_file=str(tmp_path / "in.md"),
                             task_id=task_id, wait=True)

    def test_output_digest_matches_the_delivered_file(self, orch, tmp_path, state):
        plan, out = _demo_plan(tmp_path, "doc")
        task = self._run(orch, tmp_path, plan, "trace-1")
        assert task.status.value == "done", task.error

        rec = _ledger(state).get("trace-1")
        assert rec is not None
        sha = rec["output_digest"]["sha256"]
        assert sha == hashlib.sha256(Path(task.output_path).read_bytes()).hexdigest()
        assert rec["output_digest"]["bytes"] == Path(task.output_path).stat().st_size

    def test_node_artifact_digests_cover_the_recorded_keys(self, orch, tmp_path,
                                                           state):
        """每个产物键都要有摘要，且摘要对象就是引擎当时记下的那个值。"""
        from loom_core.deliveries import digest_of

        plan, out = _demo_plan(tmp_path, "nodes")
        task = self._run(orch, tmp_path, plan, "trace-2")
        rec = _ledger(state).get("trace-2")
        node_map = {n["node"]: n for n in rec["nodes"]}
        assert node_map, "账本里没有逐节点记录"
        for name, node in node_map.items():
            assert set(node["artifacts"]) == set(node["artifact_keys"]), name
            dag_node = task.dag_nodes.get(name)
            for key, digest in node["artifacts"].items():
                assert digest["sha256"] == digest_of(dag_node.result[key])["sha256"]

    def test_deliver_json_carries_the_same_facts_as_the_ledger(self, orch,
                                                               tmp_path, state):
        """两个出口（deliver.json 与账本行）同形——分叉就是两份互相矛盾的交付事实。"""
        plan, out = _demo_plan(tmp_path, "json")
        self._run(orch, tmp_path, plan, "trace-3")
        file_rec = json.loads((out / "result.json").read_text(encoding="utf-8"))
        row = _ledger(state).get("trace-3")
        assert file_rec["output_digest"] == row["output_digest"]
        assert file_rec["nodes"] == row["nodes"]
        assert file_rec["lock"] == row["lock"]

    def test_locked_run_records_the_fingerprint_it_ran_with(self, orch, tmp_path,
                                                           state):
        """有锁且一致：指纹是逐节点 config_hash + 拓扑 hash，且当场算。"""
        from loom_core.scheduler import Scheduler

        plan, lock, name = _demo_locked(tmp_path, "locked")
        assert plan.lock_fingerprint["matches"] is True

        task = self._run(orch, tmp_path, plan, "trace-4")
        assert task.status.value == "done", task.error
        lock_rec = _ledger(state).get("trace-4")["lock"]
        assert lock_rec["checked"] is True
        assert lock_rec["lock_present"] is True
        assert lock_rec["matches"] is True
        assert lock_rec["lock_file"] == f"{name}.lock"
        assert len(lock_rec["topology_hash"]) == 12
        assert lock_rec["topology_hash"] == Scheduler._topology_hash(plan)
        for level in plan.levels:
            for node in level:
                entry = lock_rec["nodes"][node.agent_name]
                assert entry["config_hash"] == Scheduler._node_config_hash(
                    node.agent_config)
                assert entry["version"] == node.agent_config.version

    def test_fingerprint_is_frozen_at_run_time(self, orch, tmp_path, state):
        """跑完之后改锁文件，账本里那一行的指纹不许跟着变。

        这条盯的是"事后拿今天的值冒充当时的"——回答"某次运行时用的锁对不对"
        只能靠写入时冻结的那一份。
        """
        plan, lock, _name = _demo_locked(tmp_path, "frozen")
        before = dict(plan.lock_fingerprint)
        self._run(orch, tmp_path, plan, "trace-5")

        import yaml as pyyaml
        data = pyyaml.safe_load(lock.read_text(encoding="utf-8"))
        data["topology_hash"] = "deadbeefcafe"
        lock.write_text(pyyaml.dump(data), encoding="utf-8")

        after = _ledger(state).get("trace-5")["lock"]
        assert after["topology_hash"] == before["topology_hash"]
        assert after["topology_hash"] != "deadbeefcafe"

    def test_run_without_a_lock_records_matches_none(self, orch, tmp_path, state):
        """没有锁可比时给 None，不给 True——"没发现问题"与"没法判断"是两件事。"""
        plan, _out = _demo_verified(tmp_path, "nolock")
        task = self._run(orch, tmp_path, plan, "trace-6")
        assert task.status.value == "done", task.error

        rec = _ledger(state).get("trace-6")["lock"]
        assert rec["checked"] is True
        assert rec["lock_present"] is False
        assert rec["matches"] is None

    def test_unverified_plan_records_an_empty_lock(self, orch, tmp_path, state):
        """verify_lock=False 的 plan 是第三种形状：没核对过（空 dict），
        与"核对过但没锁"必须能分辨。"""
        plan, _out = _demo_plan(tmp_path, "unverified")
        task = self._run(orch, tmp_path, plan, "trace-7")
        assert task.status.value == "done", task.error
        assert _ledger(state).get("trace-7")["lock"] == {}

    def test_retention_until_is_frozen_from_the_config_at_write(self, orch,
                                                                tmp_path, state):
        led = _ledger(state)
        plan, _out = _demo_plan(tmp_path, "no-retention")
        self._run(orch, tmp_path, plan, "trace-8")
        assert led.row_digest("trace-8")["retention_until"] == 0.0, \
            "没启用保留期时到期时刻必须是 0（不过期），不能拿默认值凑一个日期"

        orch.config.set("retention.runs_days", 30)
        plan2, _out2 = _demo_plan(tmp_path, "with-retention")
        task2 = self._run(orch, tmp_path, plan2, "trace-9")
        got = led.row_digest("trace-9")["retention_until"]
        assert got == pytest.approx(float(task2.finished_at) + 30 * 86400.0)
        orch.config.set("retention.runs_days", 0)

    def test_record_builder_survives_a_broken_config_value(self, orch, tmp_path):
        """配置里的保留期读成坏值 → 到期时刻退到 0，而不是把整条交付记录丢掉。

        外层 `_deliver` 把"构建记录失败"只记成 warning：这里要是往外抛，
        代价是账本少一行——为一条注脚丢一次交付事实，不对称。
        """
        plan, _out = _demo_plan(tmp_path, "broken-cfg")

        class _Cfg:
            def get(self, key, default=None):
                raise RuntimeError("配置中心炸了")

        orch.config = _Cfg()
        task = SimpleNamespace(id="t-cfg", finished_at=1.0, started_at=0.0,
                               status=SimpleNamespace(value="done"),
                               input_file="", output_path="", error="",
                               dag_nodes={})
        rec = orch._build_delivery_record(task, plan)
        assert rec["retention_until"] == 0.0


# ── D. 账本回收 ─────────────────────────────────────────

class TestLedgerCleanup:
    def test_expired_rows_are_deleted(self, state):
        led = _ledger(state)
        now = 2_000_000.0
        led.record(_rec("old", finished_at=now - 100, retention_until=now - 1))
        led.record(_rec("fresh", finished_at=now, retention_until=now + 100))
        assert led.cleanup(retention_days=30, now=now) == 1
        assert led.row_digest("old") == {}
        assert led.row_digest("fresh") != {}

    def test_rows_without_a_frozen_expiry_are_never_age_deleted(self, state):
        """不 backfill 历史运行：老行没有到期时刻，年龄判据一行都不许碰。"""
        led = _ledger(state)
        now = 2_000_000.0
        led.record(_rec("legacy", finished_at=1.0, retention_until=0.0))
        assert led.cleanup(retention_days=1, now=now) == 0
        assert led.count() == 1

    def test_shortening_retention_does_not_retro_delete(self, state):
        """写入时冻结的第二个含义：事后把保留期调短，不该回头清掉老行。"""
        led = _ledger(state)
        now = 2_000_000.0
        led.record(_rec("kept", finished_at=now - 90 * 86400,
                        retention_until=now + 86400))
        assert led.cleanup(retention_days=7, now=now) == 0
        assert led.count() == 1

    def test_max_runs_keeps_the_newest(self, state):
        led = _ledger(state)
        for i, rid in enumerate(["k1", "k2", "k3"]):
            led.record(_rec(rid, finished_at=1_000.0 + i))
        assert led.cleanup(max_runs=2) == 1
        assert led.row_digest("k1") == {}
        assert led.row_digest("k3") != {}

    def test_non_terminal_rows_are_untouched(self, state):
        led = _ledger(state)
        now = 2_000_000.0
        led.record(_rec("running", status="running", finished_at=0.0,
                        retention_until=1.0))
        assert led.cleanup(retention_days=1, max_runs=0, now=now) == 0
        assert led.count() == 1

    def test_cleanup_does_not_delete_the_delivered_file(self, state, tmp_path):
        """账本行走了，磁盘产物还在——删用户声明的输出目录里的文件不在此权内。"""
        doc = tmp_path / "result.md"
        doc.write_text("# 交付\n正文", encoding="utf-8")
        led = _ledger(state)
        now = 2_000_000.0
        led.record(_rec("with-file", finished_at=now - 10,
                        retention_until=now - 1, output_path=str(doc)))
        assert led.cleanup(retention_days=30, now=now) == 1
        assert doc.exists(), "GC 不该删磁盘上的交付物"
        assert led.row_digest("with-file") == {}

    def test_cleanup_is_idempotent_and_empty_db_gives_zero(self, state):
        led = _ledger(state)
        now = 2_000_000.0
        led.record(_rec("gone", finished_at=now - 10, retention_until=now - 1))
        assert led.cleanup(retention_days=1, now=now) == 1
        assert led.cleanup(retention_days=1, now=now) == 0
        assert _ledger(state).cleanup(retention_days=1, now=now) == 0

    def test_disabled_returns_zero_without_touching_rows(self, state):
        led = _ledger(state)
        led.record(_rec("stay", finished_at=1.0, retention_until=1.0))
        assert led.cleanup(retention_days=0, max_runs=0, now=10 ** 12) == 0
        assert led.count() == 1


# ── E. GC 接线与打印 ────────────────────────────────────

class TestGcWiring:
    def test_gc_runs_sweeps_the_ledger_with_the_same_criteria(self):
        from loom_core.maintenance import Retention, gc_runs

        seen: list[tuple] = []

        class Led:
            def cleanup(self, days, rows, now=None):
                seen.append((days, rows, now))
                return 3

        out = gc_runs(deliveries=Led(), retention=Retention(days=30, max_runs=10),
                      now=1234.0)
        assert out["deliveries"] == 3
        assert seen == [(30, 10, 1234.0)], "账本清理要拿得到假时钟"

    def test_count_dropped_adds_per_table_dicts_and_ignores_the_flag(self):
        """历史错账：`enabled: True` 是 int 的子类被算成"删了 1 行"，
        而 run_store 的逐表 dict（真删了几十行）因为不是 int 被整个跳过。"""
        from loom_core.maintenance import count_dropped

        summary = {"enabled": True, "queue": 0, "cost": None, "feedback": None,
                   "run_store": {"runs": 3, "run_nodes": 5, "events": 9,
                                 "approvals": 1},
                   "deliveries": 2, "checkpoints": 0}
        assert count_dropped(summary) == 20
        # 反向对照：一行没删时不许报出 1 行
        assert count_dropped({"enabled": True, "queue": 0}) == 0
        assert count_dropped({"enabled": False}) == 0
        # 布尔不是行数：`True` 是 int 的子类，这一条单独钉住那条 bool 守卫
        # （`enabled` 的跳过是第二道保险，只拆它不会有可观测差异）
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

    def test_format_summary_lists_each_store_and_the_total(self):
        from loom_core.maintenance import Retention, format_summary

        summary = {"enabled": True, "queue": 0,
                   "run_store": {"runs": 3, "events": 9}, "deliveries": 2}
        text = format_summary(summary, Retention(days=30, max_runs=500))
        assert "保留 30 天 / 最近 500 条" in text
        assert "run_store: events=9 runs=3" in text  # 键名有序输出
        assert "deliveries: 2 行" in text
        assert "合计删除 14 行" in text
        assert "{" not in text, "打印里不该再出现原始 dict"

    def test_format_summary_shows_a_failed_store(self):
        from loom_core.maintenance import format_summary

        text = format_summary({"enabled": True,
                               "deliveries": {"failed": True,
                                              "message": "db is locked"}})
        assert "deliveries: 失败（db is locked）" in text

    def test_format_summary_disabled_message(self):
        from loom_core.maintenance import format_summary

        assert "retention 未启用" in format_summary({"enabled": False})

    def test_gc_from_orchestrator_does_not_create_the_ledger(self, tmp_path,
                                                             monkeypatch):
        """没跑过任何东西时，一次 GC 不该在磁盘上 mkdir + 建出一个空账本。"""
        from loom_core.maintenance import gc_from_orchestrator

        d = tmp_path / "state"
        monkeypatch.setenv("LOOM_STATE_DIR", str(d))
        cfg = SimpleNamespace(get=lambda key, default=0: 30)
        orch = SimpleNamespace(config=cfg, task_queue=None, _checkpoint=None)
        summary = gc_from_orchestrator(orch)
        assert summary["deliveries"] is None
        assert not (d / "deliveries.db").exists()

    def test_gc_from_orchestrator_sweeps_an_existing_ledger(self, tmp_path,
                                                            monkeypatch, state):
        from loom_core.maintenance import gc_from_orchestrator

        now = 2_000_000.0
        led = _ledger(state)
        led.record(_rec("swept", finished_at=now - 10, retention_until=now - 1))
        assert led.count() == 1
        cfg = SimpleNamespace(get=lambda key, default=0: 30)
        orch = SimpleNamespace(config=cfg, task_queue=None, _checkpoint=None)
        summary = gc_from_orchestrator(orch, now=now)
        assert summary["deliveries"] == 1
        assert led.count() == 0


# ── F. 只有一处摘要实现 ─────────────────────────────────

class TestOneDigestImplementation:
    def test_content_digest_has_a_single_definition(self):
        """内容摘要只许有一处实现——算第二遍就会算出两个 sha，追溯当场失效。

        口径收窄到**定义**：`loom_core` 里另有拿 sha256 算凭据指纹的模块
        （`tokens.py`、`admin_api` 的身份摘要），那是另一件事，不该被这条
        判据误伤；判据打的是"再写一个 digest_* 函数"这个真实退化形状。
        """
        import re

        pattern = re.compile(r"^\s*def ((?:_?digest|artifact_digests)\w*)\(", re.M)
        hits: dict[str, list[str]] = {}
        for path in (PROJECT / "loom_core").glob("*.py"):
            names = [m.group(1) for m in pattern.finditer(
                path.read_text(encoding="utf-8"))]
            if names:
                hits[path.name] = names
        assert hits == {"deliveries.py": ["digest_of", "digest_file",
                                          "artifact_digests"]}, hits

    def test_pipeline_uses_the_ledger_digest_helpers(self):
        """写侧不许自己算：从账本那处唯一实现 import 进来用。"""
        src = (PROJECT / "loom_core" / "pipeline.py").read_text(encoding="utf-8")
        assert "from .deliveries import artifact_digests, digest_file" in src
        assert "hashlib.sha256" not in src, "pipeline 里出现了第二处摘要实现"


# ── G. 穿过 HTTP 才算数 ────────────────────────────────

class TestTracingOverHTTP:
    """写侧算对了还不够：`GET /api/deliveries*` 没把摘要与锁指纹带上，
    对读的人就等于这一切不存在（续64 那批判据钉的是读面形状，这一组钉新字段
    确实从磁盘走到线上）。"""

    NL = chr(10)

    def _serve(self, orch, monkeypatch):
        from loom_core.admin_api import AdminAPI

        # 端口只绑回环，但鉴权开关会被环境变量牵着走：删掉它，否则本机设过
        # ADMIN_API_KEY 时这四条会一起 401（那是环境噪声，不是契约变化）
        monkeypatch.delenv("ADMIN_API_KEY", raising=False)
        api = AdminAPI(host="127.0.0.1", port=0, api_key="", tokens_file="")
        assert api.start(orch) is True
        port = api._server.server_address[1]
        return api, f"http://127.0.0.1:{port}"

    def _get(self, url: str):
        import urllib.request

        with urllib.request.urlopen(url, timeout=20) as resp:
            return resp.status, json.loads(resp.read().decode())

    def _stop(self, api):
        """停服务 + 复位 `AdminHandler` 的**类**属性（`stop()` 自己不清）。

        不复位就会把"我这台 orch / 空 api_key"留给后面所有起 AdminAPI 的用例。
        本批第一次全量就是这么红的：`test_identity_tokens` 与
        `test_runstore_readside` 的成本作用域两条在后面跑，读到的是我这台，
        单独跑却全绿——典型的顺序依赖。
        """
        from loom_core.admin_api import AdminHandler

        api.stop()
        AdminHandler.orch = None
        AdminHandler.api_key = None
        AdminHandler.tokens = None
        AdminHandler.principal = None

    def _run(self, orch, tmp_path, tag, task_id):
        plan, _lock, _name = _demo_locked(tmp_path, tag)
        inp = tmp_path / "in.md"
        inp.write_text("# 输入" + self.NL + "HTTP 契约判据。", encoding="utf-8")
        task = orch.run_plan(plan, input_file=str(inp), task_id=task_id, wait=True)
        assert task.status.value == "done", task.error
        return task

    def test_list_row_exposes_the_digest_that_the_disk_carries(self, orch, tmp_path,
                                                              state, monkeypatch):
        task = self._run(orch, tmp_path, "http-list", "trace-http-1")
        expected = hashlib.sha256(
            Path(task.output_path).read_bytes()).hexdigest()
        api, base = self._serve(orch, monkeypatch)
        try:
            code, body = self._get(base + "/api/deliveries?limit=20")
            assert code == 200, body
            rows = [d for d in body["deliveries"] if d["run_id"] == "trace-http-1"]
            assert rows, f"列表里没有这一行：{body}"
            item = rows[0]
            assert item["sha256"] == expected, item
            assert item["bytes"] == Path(task.output_path).stat().st_size, item
            # 保留期没启用 ⇒ 如实报 0，而不是凑一个日期
            assert item["retention_until"] == 0.0, item
        finally:
            self._stop(api)

    def test_detail_exposes_lock_fingerprint_and_node_digests(self, orch, tmp_path,
                                                             state, monkeypatch):
        self._run(orch, tmp_path, "http-detail", "trace-http-2")
        rec = _ledger(state).get("trace-http-2")
        api, base = self._serve(orch, monkeypatch)
        try:
            code, body = self._get(base + "/api/deliveries/trace-http-2")
            assert code == 200, body
            delivery = body["delivery"]
            # 与账本逐字段一致：读面不重算、不改写、不挑键
            assert delivery["output_digest"] == rec["output_digest"], delivery
            assert delivery["lock"] == rec["lock"], delivery
            assert delivery["lock"]["checked"] is True
            assert delivery["lock"]["matches"] is True
            assert delivery["retention_until"] == rec["retention_until"]
            with_digests = [n for n in delivery["nodes"] if n.get("artifacts")]
            assert with_digests, "逐节点产物摘要没穿过 HTTP"
            assert with_digests[0]["artifacts"] == next(
                n["artifacts"] for n in rec["nodes"] if n.get("artifacts"))
        finally:
            self._stop(api)

    def test_row_without_a_digest_crosses_as_null_not_as_zero(self, orch, tmp_path,
                                                             state, monkeypatch):
        """没有摘要的行：HTTP 给 null，不给 0 或空串冒充"算过了"。

        三种形状（null / -1 / 0）在读侧必须还分得开——合并了就没法据此判断
        "这行是老数据"还是"文件被人删了"。
        """
        led = _ledger(state)
        rec = _rec("legacy-http", finished_at=2_000_000.0)
        rec["output_digest"] = {}
        led.record(rec)
        api, base = self._serve(orch, monkeypatch)
        try:
            code, body = self._get(base + "/api/deliveries?limit=50")
            assert code == 200, body
            item = next(d for d in body["deliveries"]
                        if d["run_id"] == "legacy-http")
            assert item["sha256"] is None, item
            assert item["bytes"] is None, item
        finally:
            self._stop(api)

    def test_unreadable_output_crosses_as_minus_one_bytes(self, orch, tmp_path,
                                                          state, monkeypatch):
        """文件读不到（bytes=-1）与"没算过"（null）是两个事实，不许在 HTTP 上合体。"""
        led = _ledger(state)
        rec = _rec("unreadable-http")
        rec["output_digest"] = {"sha256": "", "bytes": -1, "kind": "file",
                               "error": "读不到交付物文件"}
        led.record(rec)
        api, base = self._serve(orch, monkeypatch)
        try:
            code, body = self._get(base + "/api/deliveries?limit=50")
            assert code == 200, body
            item = next(d for d in body["deliveries"]
                        if d["run_id"] == "unreadable-http")
            assert item["bytes"] == -1, item
            assert item["sha256"] is None, item
        finally:
            self._stop(api)
