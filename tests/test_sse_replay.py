"""RunStore Stage 2b：`/stream` 重放改读 `events`，跨进程/重启可重放。

Stage 2a 把列表与详情切到了投影，但 SSE 重连仍只认进程内的 `StreamCallback`：
发起流的那条进程一退出，重连就只能报"缺 query"或**重新跑一遍**（把一次重连变成
第二次运行）。本文件钉两件事：

1. **重放以投影为准**：`Last-Event-ID: r<seq>` 是运行级游标；重放帧带 `id: r<seq>`，
   逐帧取 `store.events(after_seq)`，终态运行放完即收；
2. **重连不再起第二条流水线**：`task_id` 在投影里存在就只重放/尾随；查不到回调
   不等于"从头再跑"。

边界：重放的是运行级事实（`run.opened/node.finished/run.completed`），不是逐 chunk
的正文流——后者从不落盘。
"""
from __future__ import annotations

import http.client
import json
import threading
import time
from pathlib import Path

import pytest

from loom_core.pipeline import PipelineOrchestrator
from loom_core.run_store import RunStore

PROJECT = Path(__file__).resolve().parent.parent

TOK_ALICE = "tok-alice-secret"
TOK_BOB = "tok-bob-secret"
TOK_OPS = "tok-ops-secret"


def _state_dir(monkeypatch, tmp_path: Path) -> Path:
    d = tmp_path / "state"
    monkeypatch.setenv("LOOM_STATE_DIR", str(d))
    return d


def _orch(tmp_path: Path) -> PipelineOrchestrator:
    orch = PipelineOrchestrator(agents_dir=str(PROJECT / "agents"),
                                checkpoint_dir=str(tmp_path / "ckpt"))
    orch.register_agents()
    return orch


def _seed_run(run_id: str, *, principal: str = "alice", status: str = "done",
              error: str = "", nodes: tuple[str, ...] = ("tldr",)) -> RunStore:
    store = RunStore()
    store.open_run(run_id, workflow="rs_read_plain", principal=principal)
    for i, node in enumerate(nodes):
        store.record_node(run_id, node, "success",
                          started_at=1.0 + i, finished_at=2.0 + i,
                          artifact_keys=["summary"])
    store.close_run(run_id, status, error=error)
    return store


def _tokens_yaml(tmp_path: Path) -> Path:
    import hashlib
    f = tmp_path / "tokens.yaml"
    f.write_text(f"""
tokens:
  - id: alice
    token_sha256: {hashlib.sha256(TOK_ALICE.encode()).hexdigest()}
  - id: bob
    token_sha256: {hashlib.sha256(TOK_BOB.encode()).hexdigest()}
  - id: ops
    admin: true
    token_sha256: {hashlib.sha256(TOK_OPS.encode()).hexdigest()}
""", encoding="utf-8")
    return f


def _start(orch, tmp_path: Path, *, with_tokens: bool = False):
    from loom_core.admin_api import AdminAPI
    tokens = str(_tokens_yaml(tmp_path)) if with_tokens else None
    api = AdminAPI(host="127.0.0.1", port=0, api_key="", tokens_file=tokens)
    assert api.start(orch) is True
    return api._server.server_address[1], api


def _reset_handler():
    from loom_core.admin_api import AdminHandler
    AdminHandler.tokens = None
    AdminHandler.orch = None
    AdminHandler.principal = None


def _read_frames(port: int, path: str, *, token: str | None = None,
                 last_event_id: str | None = None,
                 timeout: float = 25.0) -> list[tuple[str, dict]]:
    """读一条 SSE 连接直到服务端关闭或超时，返回 [(frame_id, payload), ...]。"""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if last_event_id is not None:
        headers["Last-Event-ID"] = last_event_id
    conn.request("GET", path, headers=headers)
    resp = conn.getresponse()
    frames: list[tuple[str, dict]] = []
    frame_id = ""
    data_lines: list[str] = []

    def _flush() -> None:
        nonlocal frame_id, data_lines
        if data_lines:
            frames.append((frame_id, json.loads("\n".join(data_lines))))
        frame_id, data_lines = "", []

    while True:
        line = resp.readline()
        if not line:
            break
        text = line.decode("utf-8").rstrip("\r\n")
        if text == "":
            _flush()
            continue
        if text.startswith(":"):
            continue
        if text.startswith("id:"):
            frame_id = text[3:].strip()
        elif text.startswith("data:"):
            data_lines.append(text[5:].strip())
    _flush()
    conn.close()
    return frames


class TestReplayFromProjection:
    @pytest.fixture
    def served(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        store = _seed_run("sse-done")
        expected = [e["seq"] for e in store.events("sse-done")]
        orch = _orch(tmp_path)
        port, api = _start(orch, tmp_path)
        yield port, api, orch, expected
        api.stop()
        orch.shutdown()
        _reset_handler()

    def test_reconnect_replays_the_whole_event_log(self, served):
        port, _api, _orch, expected = served
        frames = _read_frames(port, "/stream?task_id=sse-done")
        types = [p["type"] for _id, p in frames]
        assert types[0] == "connected"
        assert "run_event" in types and "resumed" in types
        run_frames = [(fid, p) for fid, p in frames if p["type"] == "run_event"]
        assert [int(fid[1:]) for fid, _p in run_frames] == expected, run_frames
        assert [p["data"]["event"] for _f, p in run_frames][0] == "run.opened"
        assert types[-1] == "complete", types

    def test_resume_from_run_cursor_only_sends_later_events(self, served):
        port, _api, _orch, expected = served
        frames = _read_frames(port, "/stream?task_id=sse-done",
                              last_event_id="r2")
        run_frames = [p for _fid, p in frames if p["type"] == "run_event"]
        assert [p["data"]["seq"] for p in run_frames] == expected[2:], run_frames
        resumed = [p for _f, p in frames if p["type"] == "resumed"][0]
        assert resumed["data"]["last_seq"] == expected[-1]

    def test_progress_frame_is_derived_from_projection(self, served):
        port, _api, _orch, _expected = served
        frames = _read_frames(port, "/stream?task_id=sse-done")
        progress = [p for _f, p in frames if p["type"] == "progress"]
        assert progress, "重放没有合成进度帧"
        assert progress[0]["data"]["total"] == 1
        assert progress[0]["data"]["current"] == 1

    def test_failed_run_ends_with_error_frame(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        _seed_run("sse-fail", status="failed", error="boom")
        orch = _orch(tmp_path)
        port, api = _start(orch, tmp_path)
        try:
            frames = _read_frames(port, "/stream?task_id=sse-fail")
            last = frames[-1][1]
            assert last["type"] == "error"
            assert "boom" in last["data"]["error"]
        finally:
            api.stop()
            orch.shutdown()
            _reset_handler()


class TestNoSecondRun:
    def test_reconnect_with_query_does_not_start_a_new_run(self, tmp_path, monkeypatch):
        """旧行为：查不到进程内回调就**再跑一遍**。这条钉住"重连 ≠ 第二次运行"。"""
        _state_dir(monkeypatch, tmp_path)
        _seed_run("sse-dup")
        orch = _orch(tmp_path)
        port, api = _start(orch, tmp_path)
        try:
            frames = _read_frames(port, "/stream?task_id=sse-dup&query=topic")
            assert any(p["type"] == "run_event" for _f, p in frames), frames
            # 没有第二条运行落进队列/投影
            assert orch.task_queue.get("sse-dup") is None, "重连把一个已有运行重新入了队"
            rows = {r["run_id"] for r in RunStore().list_runs()}
            assert rows == {"sse-dup"}, rows
        finally:
            api.stop()
            orch.shutdown()
            _reset_handler()

    def test_unknown_run_without_query_is_still_400(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        orch = _orch(tmp_path)
        port, api = _start(orch, tmp_path)
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
            conn.request("GET", "/stream?task_id=never-existed")
            resp = conn.getresponse()
            body = json.loads(resp.read().decode())
            assert resp.status == 400 and "query" in body["error"]
            conn.close()
        finally:
            api.stop()
            orch.shutdown()
            _reset_handler()


class TestTailWhileRunning:
    def test_live_run_events_arrive_until_terminal(self, tmp_path, monkeypatch):
        """运行还在别的进程里跑：先重放已有事件，再尾随到终态才收流。"""
        _state_dir(monkeypatch, tmp_path)
        store = RunStore()
        store.open_run("sse-live", workflow="rs_read_plain", principal="alice")
        store.record_node("sse-live", "tldr", "success",
                          started_at=1.0, finished_at=2.0, artifact_keys=["k"])
        orch = _orch(tmp_path)
        port, api = _start(orch, tmp_path)

        def _finish() -> None:
            time.sleep(0.4)
            later = RunStore()
            later.record_node("sse-live", "readability", "success",
                              started_at=2.0, finished_at=3.0, artifact_keys=[])
            later.close_run("sse-live", "done")

        writer = threading.Thread(target=_finish, daemon=True)
        writer.start()
        try:
            frames = _read_frames(port, "/stream?task_id=sse-live", timeout=30)
            nodes = [p["data"]["payload"].get("node")
                     for _f, p in frames
                     if p["type"] == "run_event" and p["data"]["event"] == "node.finished"]
            assert nodes == ["tldr", "readability"], nodes
            assert frames[-1][1]["type"] == "complete", frames[-1]
        finally:
            writer.join(timeout=10)
            api.stop()
            orch.shutdown()
            _reset_handler()


class TestTenantScope:
    @pytest.fixture
    def served(self, tmp_path, monkeypatch):
        _state_dir(monkeypatch, tmp_path)
        _seed_run("sse-alice", principal="alice")
        orch = _orch(tmp_path)
        port, api = _start(orch, tmp_path, with_tokens=True)
        yield port, api, orch
        api.stop()
        orch.shutdown()
        _reset_handler()

    def test_owner_can_replay(self, served):
        port, _api, _orch = served
        frames = _read_frames(port, "/stream?task_id=sse-alice", token=TOK_ALICE)
        assert any(p["type"] == "run_event" for _f, p in frames), frames

    def test_cross_tenant_gets_the_standard_not_found_shape(self, served):
        port, _api, _orch = served
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        conn.request("GET", "/stream?task_id=sse-alice",
                     headers={"Authorization": f"Bearer {TOK_BOB}"})
        resp = conn.getresponse()
        body = json.loads(resp.read().decode())
        conn.close()
        assert resp.status == 404, body
        # 与 /tasks/<id> 的"不存在"逐字段同形（SSE 不能成为存在性旁路）
        assert body == {"error": "task not found"}, body

    def test_admin_can_replay(self, served):
        port, _api, _orch = served
        frames = _read_frames(port, "/stream?task_id=sse-alice", token=TOK_OPS)
        assert any(p["type"] == "run_event" for _f, p in frames), frames
