"""定时触发（loom_core/triggers.py）测试。

时钟与 sleep 全程注入——"不等真实时间的调度器测试"的前提；提交路径用
stub orchestrator 断言 run_plan 入参，证明定时任务与手动 run 走同一入口。
"""
import re
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_core.cron import parse_cron
from loom_core.triggers import (
    Trigger,
    TriggerConfigError,
    _Runtime,
    describe_triggers,
    input_file_for,
    load_triggers,
    render_trigger_input,
    run_trigger_loop,
    submit_trigger_run,
    tick,
)

BASIC = """
triggers:
  - name: weekly
    pipeline: docgen
    cron: "0 9 * * 1"
    inputs: |
      本周周报
      output/last.md
  - name: nightly
    pipeline: docgen
    cron: "@daily"
    enabled: false
"""


def _write_cfg(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "triggers.yaml"
    p.write_text(body, encoding="utf-8")
    return p


class FakeOrch:
    def __init__(self, task_id="deadbeefdeadbeef"):
        self.calls: list[dict] = []
        self._task_id = task_id

    def run_plan(self, plan, input_file=None, task_id=None, wait=True, owner=""):
        self.calls.append({"plan": plan, "input_file": input_file,
                           "task_id": task_id, "wait": wait, "owner": owner})
        return SimpleNamespace(id=self._task_id)


class TestLoadTriggers:
    def test_loads_and_parses(self, tmp_path):
        triggers = load_triggers(_write_cfg(tmp_path, BASIC), available=["docgen"])
        assert [t.name for t in triggers] == ["weekly", "nightly"]
        assert triggers[0].spec.hours == frozenset({9})
        assert triggers[0].enabled is True
        assert triggers[1].enabled is False
        assert "本周周报" in triggers[0].inputs

    def test_missing_file(self, tmp_path):
        with pytest.raises(TriggerConfigError, match="不存在"):
            load_triggers(tmp_path / "nope.yaml", available=["docgen"])

    def test_missing_triggers_key(self, tmp_path):
        with pytest.raises(TriggerConfigError, match="triggers"):
            load_triggers(_write_cfg(tmp_path, "other: 1"), available=[])

    def test_empty_list(self, tmp_path):
        with pytest.raises(TriggerConfigError, match="非空"):
            load_triggers(_write_cfg(tmp_path, "triggers: []"), available=[])

    def test_unknown_pipeline_lists_available(self, tmp_path):
        with pytest.raises(TriggerConfigError, match="可用: docgen"):
            load_triggers(_write_cfg(tmp_path, """
triggers:
  - {name: x, pipeline: nope, cron: "@daily"}
"""), available=["docgen"])

    def test_duplicate_name(self, tmp_path):
        with pytest.raises(TriggerConfigError, match="重复"):
            load_triggers(_write_cfg(tmp_path, """
triggers:
  - {name: x, pipeline: docgen, cron: "@daily"}
  - {name: x, pipeline: docgen, cron: "@hourly"}
"""), available=["docgen"])

    def test_bad_cron_reported_with_name(self, tmp_path):
        with pytest.raises(TriggerConfigError, match="x"):
            load_triggers(_write_cfg(tmp_path, """
triggers:
  - {name: x, pipeline: docgen, cron: "61 * * * *"}
"""), available=["docgen"])

    @pytest.mark.parametrize("bad_value", ["{a: 1}", "123", "[1, 2]"])
    def test_inputs_must_be_string(self, tmp_path, bad_value):
        with pytest.raises(TriggerConfigError, match="inputs"):
            load_triggers(_write_cfg(tmp_path, f"""
triggers:
  - {{name: x, pipeline: docgen, cron: "@daily", inputs: {bad_value}}}
"""), available=["docgen"])

    def test_enabled_must_be_bool(self, tmp_path):
        with pytest.raises(TriggerConfigError, match="enabled"):
            load_triggers(_write_cfg(tmp_path, """
triggers:
  - {name: x, pipeline: docgen, cron: "@daily", enabled: "yes"}
"""), available=["docgen"])

    def test_missing_name(self, tmp_path):
        with pytest.raises(TriggerConfigError, match="name"):
            load_triggers(_write_cfg(tmp_path, """
triggers:
  - {pipeline: docgen, cron: "@daily"}
"""), available=["docgen"])

    def test_boolean_like_name_rejected_with_hint(self, tmp_path):
        """YAML 会把 off/on/yes/no 解析成布尔值——必须显式拦下并给提示。"""
        with pytest.raises(TriggerConfigError, match="布尔"):
            load_triggers(_write_cfg(tmp_path, """
triggers:
  - {name: off, pipeline: docgen, cron: "@daily"}
"""), available=["docgen"])


def _trigger(**kw) -> Trigger:
    return Trigger(
        name=kw.get("name", "t1"),
        pipeline=kw.get("pipeline", "docgen"),
        cron=kw.get("cron", "@daily"),
        spec=parse_cron(kw.get("cron", "@daily")),
        inputs=kw.get("inputs", ""),
        output=kw.get("output", ""),
        enabled=kw.get("enabled", True),
    )


class TestInputDoc:
    def test_empty_inputs_title_only(self):
        assert render_trigger_input(_trigger()) == "# t1\n"

    def test_body_gets_title_prefixed(self):
        doc = render_trigger_input(_trigger(inputs="主题行\n资料.pdf\n"))
        assert doc == "# t1\n\n主题行\n资料.pdf\n"

    def test_explicit_heading_kept(self):
        doc = render_trigger_input(_trigger(inputs="# 自定义标题\n正文"))
        assert doc.startswith("# 自定义标题") and not doc.startswith("# t1")

    def test_input_file_written_under_state_dir(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LOOM_STATE_DIR", str(tmp_path))
        p = input_file_for(_trigger(name="wf", inputs="X"))
        assert p == tmp_path / "trigger_inputs" / "wf.md"
        assert p.read_text(encoding="utf-8") == "# wf\n\nX\n"


def _rt(name="t1", cron="@hourly", last_check=None) -> _Runtime:
    return _Runtime(_trigger(name=name, cron=cron),
                    last_check or datetime(2026, 10, 8, 8, 0))


class TestTick:
    def test_not_due_no_submit(self):
        calls: list = []
        tick([_rt()], datetime(2026, 10, 8, 8, 30),
             lambda t, f: calls.append(f) or "x", lambda m: None)
        assert calls == []

    def test_due_submits_once(self):
        calls: list = []
        out = tick([_rt()], datetime(2026, 10, 8, 9, 0, 10),
                   lambda t, f: (calls.append((t.name, f)), "tid-1")[1],
                   lambda m: None)
        assert calls == [("t1", datetime(2026, 10, 8, 9, 0))]
        assert out == ["tid-1"]

    def test_due_not_repeated_on_next_tick(self):
        calls: list = []
        rt = _rt()

        def submit(t, f):
            calls.append(f)
            return "tid"

        tick([rt], datetime(2026, 10, 8, 9, 0, 10), submit, lambda m: None)
        tick([rt], datetime(2026, 10, 8, 9, 0, 40), submit, lambda m: None)
        assert len(calls) == 1

    def test_missed_windows_reported_and_latest_fired(self):
        logs: list = []
        calls: list = []
        rt = _rt()                                  # 每小时；last_check 08:00
        tick([rt], datetime(2026, 10, 8, 12, 30),
             lambda t, f: (calls.append(f), "tid")[1], logs.append)
        # 09/10/11/12 四个窗口都到了：只跑最近的 12:00，缺 3 个如实计数
        assert calls == [datetime(2026, 10, 8, 12, 0)]
        assert any("跳过 3 个" in m for m in logs)

    def test_disabled_skipped(self):
        rt = _rt()
        rt.trigger.enabled = False
        calls: list = []
        tick([rt], datetime(2026, 10, 8, 9, 1),
             lambda t, f: calls.append(f) or "x", lambda m: None)
        assert calls == []

    def test_submit_failure_does_not_break_loop(self):
        logs: list = []

        def boom(t, f):
            raise RuntimeError("boom")

        tick([_rt()], datetime(2026, 10, 8, 9, 0, 5), boom, logs.append)
        assert any("提交失败" in m for m in logs)


class TestTickLease:
    def test_denied_lease_skips_submit_but_advances_clock(self):
        """被拒后 last_check 仍前移：不前移的话，持约进程一死，接管者会把
        它已提交过的旧窗口再算成"到期"而重复提交（B4 的接管正确性判据）。"""
        calls: list = []
        rt = _rt()                                  # 每小时；last_check 08:00
        tick([rt], datetime(2026, 10, 8, 9, 0, 10),
             lambda t, f: calls.append(f) or "tid", lambda m: None,
             lease=lambda t: False)
        assert calls == []
        tick([rt], datetime(2026, 10, 8, 9, 0, 40),
             lambda t, f: calls.append(f) or "tid", lambda m: None,
             lease=lambda t: True)
        assert calls == []          # 09:00 窗口已记账过去，下一窗 10:00 未到

    def test_granted_lease_submits(self):
        calls: list = []
        out = tick([_rt()], datetime(2026, 10, 8, 9, 0, 10),
                   lambda t, f: (calls.append((t.name, f)), "tid-1")[1],
                   lambda m: None, lease=lambda t: True)
        assert calls == [("t1", datetime(2026, 10, 8, 9, 0))]
        assert out == ["tid-1"]

    def test_lease_exception_isolated_per_trigger(self):
        """租约面故障按 trigger 隔离（与提交失败同款纪律），不炸整个 tick。"""
        logs: list = []
        submitted: list = []

        def lease_boom(t: Trigger) -> bool:
            raise RuntimeError("store gone")

        tick([_rt()], datetime(2026, 10, 8, 9, 0, 5),
             lambda t, f: submitted.append(f) or "tid", logs.append,
             lease=lease_boom)
        assert submitted == []
        assert any("租约判定失败" in m for m in logs)

    def test_on_fire_records_after_submit_and_survives_failure(self):
        fires: list = []
        tick([_rt()], datetime(2026, 10, 8, 9, 0, 10),
             lambda t, f: "tid", lambda m: None,
             lease=lambda t: True, on_fire=lambda t, f: fires.append((t.name, f)))

        def broken(t, f):
            raise RuntimeError("ledger down")

        tick([_rt(name="t2")], datetime(2026, 10, 8, 9, 0, 20),
             lambda t, f: "tid2", lambda m: None,
             lease=lambda t: True, on_fire=broken)
        assert fires == [("t1", datetime(2026, 10, 8, 9, 0))]


class TestSubmitTriggerRun:
    def test_submits_via_run_plan_with_output_override(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LOOM_STATE_DIR", str(tmp_path))
        t = _trigger(name="wf", inputs="主题", output="output/wf.md")
        orch = FakeOrch()
        got = submit_trigger_run(orch, t, datetime(2026, 10, 9, 0, 0))
        assert got == "deadbeefdeadbeef"
        call = orch.calls[0]
        assert call["owner"] == "trigger:wf", "触发任务的归属必须记 trigger:<名>（多租户可见性）"
        assert call["wait"] is False                       # 定时触发不阻塞调度器
        assert re.fullmatch(r"[0-9a-f]{16}", call["task_id"])
        assert Path(call["input_file"]).read_text(encoding="utf-8") == "# wf\n\n主题\n"
        assert call["plan"].raw["pipeline"]["output"] == "output/wf.md"


class TestRunTriggerLoop:
    def test_loop_fires_with_fake_clock(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LOOM_STATE_DIR", str(tmp_path))
        triggers = load_triggers(_write_cfg(tmp_path, """
triggers:
  - {name: job, pipeline: docgen, cron: "0 9 * * *"}
"""), available=["docgen"])
        orch = FakeOrch()
        clock = {"now": datetime(2026, 10, 8, 8, 59)}
        logs: list = []

        def sleep_fn(sec: float) -> None:
            clock["now"] += timedelta(seconds=sec)

        run_trigger_loop(orch, triggers, poll_seconds=60, now_fn=lambda: clock["now"],
                         sleep_fn=sleep_fn, max_ticks=3, log=logs.append)
        assert len(orch.calls) == 1                        # 09:00 窗口在第二轮命中
        assert any("下次触发" in m for m in logs)

    def test_all_disabled_returns_immediately(self, tmp_path):
        triggers = load_triggers(_write_cfg(tmp_path, """
triggers:
  - {name: nightly, pipeline: docgen, cron: "@daily", enabled: false}
"""), available=["docgen"])
        logs: list = []
        orch = FakeOrch()
        run_trigger_loop(orch, triggers, max_ticks=1, log=logs.append)
        assert orch.calls == []
        assert any("没有启用的触发" in m for m in logs)


class TestLoopLeaseWiring:
    """run_trigger_loop 的租约接线：真 RunStore（SQLite）走通登记/抢约/记账/释放。"""

    def test_loop_uses_runstore_lease_and_releases_on_exit(
            self, tmp_path, monkeypatch):
        monkeypatch.setenv("LOOM_STATE_DIR", str(tmp_path))
        triggers = load_triggers(_write_cfg(tmp_path, """
triggers:
  - {name: job, pipeline: docgen, cron: "0 9 * * *"}
"""), available=["docgen"])

        from loom_core.run_store import RunStore

        class LeaseOrch(FakeOrch):
            def __init__(self):
                super().__init__()
                self._store = RunStore()

            def _runstore(self):
                return self._store

        orch = LeaseOrch()
        clock = {"now": datetime(2026, 10, 8, 8, 59)}
        logs: list = []
        run_trigger_loop(orch, triggers, poll_seconds=60,
                         now_fn=lambda: clock["now"],
                         sleep_fn=lambda s: clock.__setitem__(
                             "now", clock["now"] + timedelta(seconds=s)),
                         max_ticks=2, log=logs.append)
        assert len(orch.calls) == 1                        # 09:00 窗口第二轮命中
        assert any("租约已接 RunStore" in m for m in logs)
        row = orch._store.list_schedules()[0]
        assert (row["name"], row["pipeline"], row["cron"]) == ("job", "docgen", "0 9 * * *")
        assert row["last_fire"] > 0, "on_fire 记账必须落表（观测面可读 last_fire/next_fire）"
        assert row["lease_owner"] == "", "循环退出必须放掉本人租约（另一进程可立即接管）"

    def test_loop_degrades_without_runstore(self, tmp_path):
        triggers = load_triggers(_write_cfg(tmp_path, """
triggers:
  - {name: job, pipeline: docgen, cron: "0 9 * * *"}
"""), available=["docgen"])
        logs: list = []
        run_trigger_loop(FakeOrch(), triggers, max_ticks=1, log=logs.append)
        assert any("租约停用" in m for m in logs)


class TestDescribe:
    def test_lines_show_next_fires_and_disabled(self, tmp_path):
        triggers = load_triggers(_write_cfg(tmp_path, BASIC), available=["docgen"])
        lines = describe_triggers(triggers, datetime(2026, 10, 8, 9, 30))
        assert "weekly" in lines[0] and "最近 3 次" in lines[0]
        assert "停用" in lines[1]

    def test_cli_flags_registered(self):
        import run as run_mod

        parser = run_mod.build_arg_parser()
        dests = {a.dest for a in parser._actions}
        assert {"triggers", "triggers_file", "triggers_dry_run"} <= dests

    def test_dry_run_cli_end_to_end(self, tmp_path):
        import subprocess
        import sys

        cfg = _write_cfg(tmp_path, """
triggers:
  - {name: job, pipeline: docgen, cron: "0 9 * * *"}
""")
        repo = Path(__file__).parent.parent
        proc = subprocess.run(
            [sys.executable, "run.py", "--triggers-dry-run", "--triggers-file", str(cfg)],
            capture_output=True, text=True, timeout=120, cwd=str(repo))
        assert proc.returncode == 0, proc.stderr[-800:]
        assert "job" in proc.stdout and "最近 3 次" in proc.stdout
