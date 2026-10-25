"""跨进程配置热更新（docs/product-spec.md §5.4 P1 验收第三条：
「/api/config 热更新不再只对本进程生效」）。

被测契约：
- `set_persisted` 把运行时键值写进 `<状态目录>/config_overrides.json`（revision
  单调递增、点号平铺键），**原子写**（临时文件 + os.replace，无残留 .tmp）；
- 覆写层位于环境变量**之后**——与既往"运行时 set 胜过一切"的内存语义一致；
- 其他进程两条路径拿到新值：**构造时**（新 ConfigCenter 直接读到）与
  **轮询时**（`poll_overlay` 查到 revision 变化 → 热加载 → True）；
- 覆写文件损坏 = 保留旧配置 + `last_overlay_error` 非空 + 不反复重解析；
- `reload()` 仍然保留覆写层值（不是 reload 一次就丢运行时配置）；
- `publish_reload()` 广播 revision，让其他进程重新读取全部配置层；
- 写盘失败**不静默**：`set_persisted` 返回错误串，调用方如实回报
  （Admin API 响应带 `propagation_error`，见 TestAdminApiPropagation）；
- 真跨进程判据：两个真实子进程共享一个状态目录（非 mock、非同进程两实例）。

另覆盖消费端接线：worker `run_forever` 循环里轮询到变更即热加载并通知
（但 orchestrator 未构建时不提前加载——空转 worker 不应为此拉起全部 Agent）。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from loom_core.admin_api import AdminHandler
from loom_core.config import ConfigCenter
from loom_core.worker import TaskWorker

PROJECT = Path(__file__).resolve().parent.parent


@pytest.fixture()
def state_dir(tmp_path, monkeypatch):
    d = tmp_path / "state"
    d.mkdir()
    monkeypatch.setenv("LOOM_STATE_DIR", str(d))
    return d


def _overlay(state_dir: Path) -> dict:
    p = state_dir / "config_overrides.json"
    return json.loads(p.read_text(encoding="utf-8"))


# ── 覆写文件本身 ──────────────────────────────────────────


class TestOverlayFile:
    def test_set_persisted_writes_flattened_keys(self, state_dir):
        c = ConfigCenter()
        assert c.set_persisted("llm.model", "m1") is None
        assert c.set_persisted("execution.max_workers", 4) is None
        payload = _overlay(state_dir)
        assert payload["revision"] == 2
        assert payload["values"] == {"llm.model": "m1", "execution.max_workers": 4}
        assert payload["updated"]

    def test_no_tmp_left_behind(self, state_dir):
        ConfigCenter().set_persisted("llm.model", "m1")
        assert not list(state_dir.glob("*.tmp")), "原子写留下了临时文件"

    def test_second_key_updates_instead_of_replacing(self, state_dir):
        c = ConfigCenter()
        c.set_persisted("llm.model", "m1")
        c.set_persisted("llm.temperature", 0.2)
        assert _overlay(state_dir)["values"] == {
            "llm.model": "m1", "llm.temperature": 0.2}
        assert _overlay(state_dir)["revision"] == 2

    def test_nested_key_creates_intermediate_dicts(self, state_dir):
        ConfigCenter().set_persisted("a.b.c", 7)
        assert ConfigCenter().get("a.b.c") == 7


# ── 跨进程/跨实例生效 ──────────────────────────────────────


class TestCrossInstance:
    def test_second_instance_sees_value_at_construction(self, state_dir):
        ConfigCenter().set_persisted("llm.model", "m1")
        assert ConfigCenter().get("llm.model") == "m1"

    def test_poll_returns_false_when_nothing_changed(self, state_dir):
        c = ConfigCenter()
        c.set_persisted("llm.model", "m1")
        assert c.poll_overlay() is False
        assert c.poll_overlay() is False

    def test_poll_picks_up_other_process_write(self, state_dir):
        a = ConfigCenter()
        b = ConfigCenter()
        a.set_persisted("llm.model", "from-a")
        assert b.get("llm.model") != "from-a"
        assert b.poll_overlay() is True
        assert b.get("llm.model") == "from-a"

    def test_overlay_wins_over_environment(self, state_dir, monkeypatch):
        monkeypatch.setenv("DOCPIPE_LLM__MODEL", "env-model")
        assert ConfigCenter().get("llm.model") == "env-model"
        ConfigCenter().set_persisted("llm.model", "runtime-model")
        assert ConfigCenter().get("llm.model") == "runtime-model"

    def test_reload_keeps_overlay_values(self, state_dir):
        c = ConfigCenter()
        c.set_persisted("llm.model", "kept")
        c.reload()
        assert c.get("llm.model") == "kept"

    def test_publish_reload_bumps_revision_and_peer_reloads(self, state_dir):
        a = ConfigCenter()
        b = ConfigCenter()
        before = b.revision
        assert a.publish_reload() is None
        assert _overlay(state_dir)["revision"] == before + 1
        assert b.poll_overlay() is True

    def test_in_memory_set_is_not_propagated(self, state_dir):
        """set() 保持旧契约：只改本进程内存，不写覆写文件。"""
        c = ConfigCenter()
        c.set("llm.model", "memory-only")
        assert not (state_dir / "config_overrides.json").exists()
        assert ConfigCenter().get("llm.model") != "memory-only"


class TestOverlayCorruption:
    def test_corrupt_file_keeps_old_config(self, state_dir, caplog):
        c = ConfigCenter()
        c.set_persisted("llm.model", "good")
        (state_dir / "config_overrides.json").write_text("{not json", encoding="utf-8")
        d = ConfigCenter()
        assert d.get("llm.model") != "good"  # 回落到默认值，而非崩溃
        assert d.last_overlay_error

    def test_corrupt_file_not_reparsed_every_poll(self, state_dir, monkeypatch):
        c = ConfigCenter()
        (state_dir / "config_overrides.json").write_text("{not json", encoding="utf-8")
        c.poll_overlay()
        reads = []
        real_read = Path.read_text

        def _counting(self, *a, **kw):
            if self.name == "config_overrides.json":
                reads.append(1)
            return real_read(self, *a, **kw)

        monkeypatch.setattr(Path, "read_text", _counting)
        assert c.poll_overlay() is False
        assert c.poll_overlay() is False
        assert not reads, "损坏文件被反复重解析"

    def test_values_not_object_is_rejected(self, state_dir):
        (state_dir / "config_overrides.json").write_text(
            json.dumps({"revision": 5, "values": ["bad"]}), encoding="utf-8")
        c = ConfigCenter()
        assert "values" in (c.last_overlay_error or "")


class TestWriteFailureReported:
    def test_error_returned_not_raised_when_overlay_unwritable(self, state_dir):
        # 让覆写路径本身成为目录：读会炸，os.replace 也会炸
        target = state_dir / "config_overrides.json"
        target.mkdir()
        c = ConfigCenter()
        err = c.set_persisted("llm.model", "x")
        assert err, "写盘失败必须返回错误串，不静默"
        assert c.get("llm.model") == "x"  # 本进程内存仍然生效

    def test_publish_reload_reports_failure(self, state_dir):
        target = state_dir / "config_overrides.json"
        c = ConfigCenter()
        assert c.publish_reload() is None
        target.unlink()
        target.mkdir()
        assert c.publish_reload()


# ── 真跨进程判据（spec P1 验收口径，非 mock） ──────────────


class TestRealProcesses:
    #: 子进程源码（写成临时文件再跑——一行式 -c 带不了 for，且引号/转义易被 shell 吃掉）
    #: 注意顺序：ConfigCenter 构造**之后**才创建输出文件——否则“文件已存在”会被
    #: 父进程当成 readiness 信号，抢在子进程构造前写入，子进程把新值当初始值、此后再
    #: 无人改动，轮询永远检不到（实测：跑满 600 轮空转）。
    CHILD_SRC = '''\
import json, sys, time
sys.path.insert(0, {root!r})
from loom_core.config import ConfigCenter, overlay_path
from loom_core.state_paths import state_root
c = ConfigCenter()
out = open(sys.argv[1], "w", encoding="utf-8")
diag = {{"state_root": str(state_root()), "overlay": str(overlay_path()),
         "tracked_mtime": c._overlay_mtime, "tracked_rev": c._overlay_revision}}
out.write(json.dumps({{"initial": c.get("llm.model"), "diag": diag}}))
out.flush()
detected = None
for i in range(600):
    if c.poll_overlay():
        detected = i
        break
    time.sleep(0.05)
out.write(json.dumps({{"after": c.get("llm.model"), "detected_at": detected,
                       "diag2": {{"tracked_mtime": c._overlay_mtime,
                                  "tracked_rev": c._overlay_revision}}}}))
out.close()
'''

    @staticmethod
    def _read_two_objects(text: str) -> tuple[dict, dict]:
        """读回两个拼接的 JSON 对象（子进程分两次写入，无分隔符）。"""
        dec = json.JSONDecoder()
        first, idx = dec.raw_decode(text)
        second, _ = dec.raw_decode(text[idx:].lstrip())
        return first, second

    def test_two_processes_share_one_state_dir(self, state_dir, tmp_path):
        script = tmp_path / "poll_child.py"
        script.write_text(self.CHILD_SRC.format(root=str(PROJECT)), encoding="utf-8")
        out = state_dir / "child_out.json"
        child = subprocess.Popen(
            [sys.executable, str(script), str(out)],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        try:
            parent = ConfigCenter()
            deadline = time.time() + 20
            while not out.exists() and time.time() < deadline:
                time.sleep(0.05)
            assert out.exists(), "子进程没有在合理时间内构造完 ConfigCenter 并产出首轮读数"
            assert parent.set_persisted("llm.model", "from-parent-process") is None
            child.wait(timeout=60)
            first, second = self._read_two_objects(out.read_text(encoding="utf-8"))
            assert second["detected_at"] is not None, (
                f"子进程没有轮询到覆写变化: {first} → {second}")
            assert second["after"] == "from-parent-process"
            assert first["initial"] != "from-parent-process", \
                "父进程写入抢在子进程构造之前——readiness 信号不可靠"
            # 同目录铁证：子进程解析到同一个覆写文件，且构造时它还不存在
            assert first["diag"]["overlay"] == str(state_dir / "config_overrides.json")
            assert first["diag"]["tracked_mtime"] == 0.0
            assert second["diag2"]["tracked_rev"] == 1
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=10)
            err = child.stderr.read() if child.stderr else ""
            assert "Traceback" not in err, f"子进程崩溃: {err}"


# ── B6：覆写写的 revision CAS ─────────────────────

class TestOverlayCAS:
    """配置覆写写口的乐观锁（B6）：多进程写不同键不许互相覆盖。

    此前的"读 revision → 写 revision+1"在两个进程并发时会静默丢键：
    后写者整份覆盖先写者，两边都报成功。CAS 写回 = 写后读回验证 + 重试。
    """

    #: 子进程源码：写一个键后把结果打到 stdout（不落文件——子进程不写
    #: 路径参数，PYTHONPATH 由父进程环境传入，子进程里没有 sys.path 操作）。
    CHILD_SRC = '''\
import json
from loom_core.config import ConfigCenter
c = ConfigCenter()
err = c.set_persisted({key!r}, {value!r})
print(json.dumps({{"error": err}}))
'''

    def test_two_processes_writing_different_keys_both_land(self, state_dir, tmp_path):
        """两个真进程并发写不同键：都成功，且两个键都留在文件里。"""
        script = tmp_path / "cas_child.py"
        script.write_text(self.CHILD_SRC.format(key="a.k", value=1), encoding="utf-8")
        script_b = tmp_path / "cas_child_b.py"
        script_b.write_text(self.CHILD_SRC.format(key="b.k", value=2), encoding="utf-8")
        procs = [
            subprocess.Popen(
                [sys.executable, str(script)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                env={**os.environ, "PYTHONPATH": str(PROJECT)}),
            subprocess.Popen(
                [sys.executable, str(script_b)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                env={**os.environ, "PYTHONPATH": str(PROJECT)}),
        ]
        try:
            out_texts = []
            for p in procs:
                out, err = p.communicate(timeout=120)
                assert p.returncode == 0, err[-800:]
                assert json.loads(out.strip())["error"] is None
                out_texts.append(out)
            overlay = _overlay(state_dir)
            assert overlay["values"] == {"a.k": 1, "b.k": 2}, overlay
            assert overlay["revision"] == 2, overlay
        finally:
            for p in procs:
                if p.poll() is None:
                    p.kill()

    def test_conflict_reported_after_retries(self, state_dir, monkeypatch):
        """读回永远不一致（模拟持续抢跑）→ 如实报冲突，不假装成功。"""
        from loom_core import config as cfg_mod

        real_write = cfg_mod._write_overlay
        state = {"n": 0}

        def raced_write(path, revision, values):
            err = real_write(path, revision, values)
            if err:
                return err
            # 每次抢跑都换一个新键——读回永远多出一个键，恒不一致
            state["n"] += 1
            return real_write(path, revision,
                              {f"ghost.k{state['n']}": "stolen", **values})

        monkeypatch.setattr(cfg_mod, "_write_overlay", raced_write)
        cfg = ConfigCenter(auto_reload=False)
        err = cfg.set_persisted("a.k", 1)
        assert err is not None and "冲突" in err, err

    def test_retry_converges_after_one_race(self, state_dir, monkeypatch):
        """抢跑只发生一次 → 重试读到抢跑者的值并进来，最终成功且两键都在。"""
        from loom_core import config as cfg_mod

        real_write = cfg_mod._write_overlay
        state = {"first": True}

        def raced_write(path, revision, values):
            err = real_write(path, revision, values)
            if err:
                return err
            if state["first"]:
                state["first"] = False
                return real_write(path, revision, {"ghost.k": "stolen", **values})
            return None

        monkeypatch.setattr(cfg_mod, "_write_overlay", raced_write)
        cfg = ConfigCenter(auto_reload=False)
        err = cfg.set_persisted("a.k", 1)
        assert err is None, err
        overlay = _overlay(state_dir)
        assert overlay["values"] == {"a.k": 1, "ghost.k": "stolen"}, overlay


# ── Orchestrator 侧：通知循环真身 ──────────────────────────


class TestNotifyConfigUpdate:
    """续44 把"遍历注册表通知 Agent"从 admin_api 抽到 orchestrator——真方法真遍历。"""

    def _orch(self):
        from loom_core.pipeline import PipelineOrchestrator

        o = PipelineOrchestrator.__new__(PipelineOrchestrator)
        o.registry = MagicMock()
        o._log = lambda *a, **kw: None
        return o

    def test_notifies_only_agents_implementing_hook(self):
        orch = self._orch()
        good, silent = MagicMock(), MagicMock(spec=[])
        orch.registry.list.return_value = [{"name": "good"}, {"name": "silent"}]
        orch.registry.get_instance.side_effect = lambda n: {"good": good, "silent": silent}[n]
        assert orch.notify_config_update(["llm.model"]) == 1
        good.on_config_update.assert_called_once_with(changed_keys=["llm.model"])
        assert not hasattr(silent, "on_config_update")

    def test_one_agent_failure_does_not_block_others(self):
        orch = self._orch()
        bad, good = MagicMock(), MagicMock()
        bad.on_config_update.side_effect = RuntimeError("boom")
        orch.registry.list.return_value = [{"name": "bad"}, {"name": "good"}]
        orch.registry.get_instance.side_effect = lambda n: {"bad": bad, "good": good}[n]
        assert orch.notify_config_update() == 1
        good.on_config_update.assert_called_once_with(changed_keys=["*"])

    def test_missing_instance_is_skipped(self):
        orch = self._orch()
        orch.registry.list.return_value = [{"name": "ghost"}]
        orch.registry.get_instance.return_value = None
        assert orch.notify_config_update() == 0


# ── Admin API 接线 ────────────────────────────────────────


def _handler(orch):
    h = AdminHandler.__new__(AdminHandler)
    h.orch = orch
    h.client_address = ("10.0.0.7", 5555)
    h.path = "/api/config"
    h._json = MagicMock()
    h.headers = {}
    return h


class _StubOrch:
    """只带 config 与通知计数的最小 orchestrator（不拉真实 Agent 注册表）。"""

    def __init__(self):
        from loom_core.config import ConfigCenter as _CC

        self.config = _CC()
        self.notified: list = []
        self.logs: list = []

    def _log(self, level, message, **kw):
        self.logs.append((level, message, kw))

    def notify_config_update(self, changed_keys=None) -> int:
        self.notified.append(changed_keys or ["*"])
        return 1

    def refresh_config_if_changed(self) -> bool:
        from loom_core.pipeline import PipelineOrchestrator

        return PipelineOrchestrator.refresh_config_if_changed(self)


class TestAdminApiPropagation:
    def test_set_reports_propagated_and_revision(self, state_dir):
        orch = _StubOrch()
        h = _handler(orch)
        h._handle_config_set(b'{"key": "llm.model", "value": "api-model"}')
        payload = h._json.call_args[0][0]
        assert payload["applied"] is True
        assert payload["propagated"] is True
        assert payload["revision"] == 1
        assert _overlay(state_dir)["values"] == {"llm.model": "api-model"}
        # 另一个进程（新实例）立刻能读到
        assert ConfigCenter().get("llm.model") == "api-model"

    def test_set_reports_propagation_error_when_unwritable(self, state_dir):
        (state_dir / "config_overrides.json").mkdir()
        orch = _StubOrch()
        h = _handler(orch)
        h._handle_config_set(b'{"key": "llm.model", "value": "api-model"}')
        payload = h._json.call_args[0][0]
        assert payload["applied"] is True, "本进程内存仍应生效"
        assert payload["propagation_error"], "写盘失败必须带错误串"

    def test_reload_broadcasts_revision(self, state_dir):
        orch = _StubOrch()
        h = _handler(orch)
        h._handle_config_reload()
        payload = h._json.call_args[0][0]
        assert payload["reloaded"] is True
        assert payload["propagated"] is True
        assert payload["agents_notified"] == 1
        assert orch.notified == [["*"]]
        assert _overlay(state_dir)["revision"] >= 1

    def test_request_picks_up_other_process_change(self, state_dir):
        """第二个 Admin API 实例：别人的 /api/config 变更，本进程请求时随之生效。"""
        orch = _StubOrch()
        ConfigCenter().set_persisted("llm.model", "from-elsewhere")
        # 请求入口的 _sync_shared_config 就是走的这条链
        from loom_core.pipeline import PipelineOrchestrator

        assert PipelineOrchestrator.refresh_config_if_changed(orch) is True
        assert orch.config.get("llm.model") == "from-elsewhere"
        assert orch.notified == [["*"]]


# ── Worker 接线 ───────────────────────────────────────────


class _FakeQueue:
    def __init__(self):
        self.acquire = MagicMock(return_value=None)
        self.recover = MagicMock(return_value=[])


class TestWorkerPolling:
    def test_run_forever_picks_up_config_change(self, state_dir):
        orch = _StubOrch()
        worker = TaskWorker(orchestrator=orch, queue=_FakeQueue(), scheduler=MagicMock())
        stop = threading.Event()

        def _writer():
            time.sleep(0.3)
            ConfigCenter().set_persisted("llm.model", "worker-visible")

        t = threading.Thread(target=_writer, daemon=True)
        t.start()
        worker.run_forever(poll_interval=0.05, stop_event=stop, idle_timeout=1.5)
        t.join(timeout=5)
        assert orch.notified, "worker 循环没有把配置变更热加载进来"
        assert orch.config.get("llm.model") == "worker-visible"

    def test_idle_worker_does_not_build_orchestrator_for_polling(self, state_dir):
        worker = TaskWorker(queue=_FakeQueue(), scheduler=MagicMock())
        assert worker._orch is None
        assert worker.refresh_config() is False
        assert worker._orch is None, "空转轮询不该提前构建 orchestrator"
