"""LoomClient 的 HTTP 契约判据：登记的 16 条路由，每条都得真被打通。

`tests/test_client_sdk.py` 测的是"SDK 发出去服务端认不认"（真 Admin API 集成链，
7 条）。这里补的是**另一半**：`docs/sdk.md` 列了 16 个方法，但其中 8 个的
method/path/query/body 形状从没被断言过，**非 JSON 响应体退成 `{"raw": ...}`**
这条写进文档的错误模型也没有判据。

用 stdlib 桩服务器（不起流水线、不碰 LLM）：判的是"客户端发出去的那一帧长什么样"，
与后端实现无关，所以必须能离线跑、也必须稳定。凭证头同理（docstring 承诺
token 优先、都不给时不带 Authorization）。
"""
from __future__ import annotations

import json
import socket
import threading
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from loom_core import LoomClient
from loom_core.client import LoomError, routes

TASK_ID = "t-1"
PIPELINE = "demo"
RUN_ID = "r-1"


class _Stub(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), _Handler)
        self.status = 200
        self.content_type = "application/json"
        self.body = b"{}"
        self.requests: list[dict] = []


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):        # 静音，别污染测试输出
        pass

    def _record(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length).decode("utf-8") if length else ""
        self.server.requests.append({
            "method": self.command,
            "path": self.path,
            "body": raw,
            "authorization": self.headers.get("Authorization"),
        })

    def _reply(self):
        self.send_response(self.server.status)
        self.send_header("Content-Type", self.server.content_type)
        self.send_header("Content-Length", str(len(self.server.body)))
        self.end_headers()
        self.wfile.write(self.server.body)

    def do_GET(self):
        self._record()
        self._reply()

    def do_POST(self):
        self._record()
        self._reply()


@pytest.fixture()
def stub():
    server = _Stub()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    server.base_url = f"http://127.0.0.1:{server.server_address[1]}"
    yield server
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def _client(server) -> LoomClient:
    return LoomClient(server.base_url, token="tok-x", timeout=5.0)


def _set_json(server, payload: dict) -> None:
    server.status = 200
    server.content_type = "application/json"
    server.body = json.dumps(payload).encode("utf-8")


def _last(server) -> dict:
    assert server.requests, "客户端一个请求都没发出去"
    return server.requests[-1]


# ─── 1. 每条登记路由都得真被打通 ─────────────────────────────────


class TestRouteWiring:
    def test_every_registered_route_is_actually_hit(self, stub):
        """逐个调用公开方法，断言真打了 `_ROUTES` 登记的那条 (method, path)。

        路由表与 OpenAPI 的同源由 test_client_sdk 的另一条判据钉；这里钉的是
        **"登记了但方法根本没发这个请求"**那一类——方法存在≠调用能通过。
        """
        calls = {
            "health": lambda c: c.health(),
            "submit": lambda c: c.submit(query="q"),
            "run_workflow": lambda c: c.run_workflow(PIPELINE, {"a": 1}),
            "status": lambda c: c.status(TASK_ID),
            "wait": lambda c: c.wait(TASK_ID, timeout=1, poll_interval=0),
            "artifacts": lambda c: c.artifacts(TASK_ID),
            "cancel": lambda c: c.cancel(TASK_ID),
            "pause": lambda c: c.pause(TASK_ID),
            "resume": lambda c: c.resume(TASK_ID),
            "rerun": lambda c: c.rerun(TASK_ID),
            "approve": lambda c: c.approve(TASK_ID, note="ok"),
            "reject": lambda c: c.reject(TASK_ID, reason="no"),
            "approvals": lambda c: c.approvals(),
            "deliveries": lambda c: c.deliveries(),
            "delivery": lambda c: c.delivery(RUN_ID),
            "locks": lambda c: c.locks(),
        }
        assert set(calls) == set(routes()), (
            f"路由表与方法清单不同源：只在表里 {set(routes()) - set(calls)}，"
            f"只在测试里 {set(calls) - set(routes())}")

        for name, (verb, template) in sorted(routes().items()):
            expected = template.format(task_id=TASK_ID, name=PIPELINE, run_id=RUN_ID)
            stub.requests.clear()
            _set_json(stub, {"status": "done", "artifacts": [], "approvals": [],
                             "deliveries": [], "delivery": {"run_id": RUN_ID},
                             "pipelines": []})
            calls[name](_client(stub))
            got = _last(stub)
            assert got["method"] == verb, f"{name} 用了 {got['method']}，登记的是 {verb}"
            # 路由登记的是路径形状，过滤条件走 query（TestQueryShape 单独判）
            assert got["path"].split("?")[0] == expected, (
                f"{name} 打到 {got['path']}，登记的是 {expected}")


# ─── 2. 载荷形状：没给的字段不该出现在 body 里 ──────────────────


class TestBodyShape:
    def test_submit_sends_only_what_was_given(self, stub):
        _set_json(stub, {"task_id": TASK_ID})
        _client(stub).submit(query="写一篇指南")
        assert json.loads(_last(stub)["body"]) == {"query": "写一篇指南"}

    def test_submit_carries_typed_payload_and_options(self, stub):
        _set_json(stub, {"task_id": TASK_ID})
        _client(stub).submit(inputs={"topic": "rebase"}, pipeline=PIPELINE,
                             title="标题", wait=True, output="out.md")
        assert json.loads(_last(stub)["body"]) == {
            "inputs": {"topic": "rebase"}, "pipeline": PIPELINE,
            "title": "标题", "wait": True, "output": "out.md"}

    def test_submit_rejects_empty_payload_locally(self, stub):
        """缺载荷要在本地拦住，不该白跑一次 HTTP 往返。"""
        with pytest.raises(ValueError, match="至少给一个"):
            _client(stub).submit()
        assert stub.requests == []

    def test_run_workflow_minimal_and_optional_flags(self, stub):
        _set_json(stub, {"task_id": TASK_ID})
        client = _client(stub)
        client.run_workflow(PIPELINE, {"a": 1})
        assert json.loads(_last(stub)["body"]) == {"inputs": {"a": 1}}
        client.run_workflow(PIPELINE, {"a": 1}, wait=True, output="out.md")
        assert json.loads(_last(stub)["body"]) == {
            "inputs": {"a": 1}, "wait": True, "output": "out.md"}

    def test_lifecycle_posts_send_empty_object_not_null(self, stub):
        _set_json(stub, {"ok": True})
        _client(stub).cancel(TASK_ID)
        assert _last(stub)["body"] == "{}"

    def test_decision_payloads_carry_note_and_reason(self, stub):
        _set_json(stub, {"ok": True})
        client = _client(stub)
        client.approve(TASK_ID, note="放行")
        assert json.loads(_last(stub)["body"]) == {"note": "放行"}
        client.reject(TASK_ID, reason="不合口径")
        assert json.loads(_last(stub)["body"]) == {"reason": "不合口径"}


# ─── 3. query 参数：None 不该拼进 URL ───────────────────────────


class TestQueryShape:
    def test_deliveries_limit_is_a_query_param(self, stub):
        _set_json(stub, {"deliveries": [{"run_id": RUN_ID}]})
        assert _client(stub).deliveries(limit=5) == [{"run_id": RUN_ID}]
        assert _last(stub)["path"] == "/api/deliveries?limit=5"

    def test_deliveries_limit_none_sends_no_query(self, stub):
        """`limit=None` 是"不筛"，不该拼出 `?limit=None` 让服务端自己猜。"""
        _set_json(stub, {"deliveries": []})
        _client(stub).deliveries(limit=None)
        assert _last(stub)["path"] == "/api/deliveries"

    def test_locks_omit_query_when_pipeline_absent(self, stub):
        _set_json(stub, {"pipelines": ["demo"]})
        client = _client(stub)
        assert client.locks() == ["demo"]
        assert _last(stub)["path"] == "/api/locks"
        assert client.locks(PIPELINE) == ["demo"]
        assert _last(stub)["path"] == "/api/locks?pipeline=demo"

    def test_empty_lists_read_back_as_empty_not_none(self, stub):
        _set_json(stub, {})
        client = _client(stub)
        assert client.approvals() == []
        assert client.deliveries() == []
        assert client.locks() == []
        assert client.artifacts(TASK_ID) == []


# ─── 4. 错误模型（docs/sdk.md 承诺的那四条）─────────────────────


class TestEnvelopeUnwrap:
    def test_delivery_unwraps_the_envelope(self, stub):
        payload = {"run_id": RUN_ID, "output_sha256": "a" * 64}
        _set_json(stub, {"delivery": payload})
        assert _client(stub).delivery(RUN_ID) == payload

    def test_approvals_unwraps_a_non_empty_list(self, stub):
        _set_json(stub, {"approvals": [{"task_id": TASK_ID}]})
        assert _client(stub).approvals() == [{"task_id": TASK_ID}]

    def test_wait_returns_the_last_status_snapshot(self, stub):
        _set_json(stub, {"status": "done", "result": {"ok": True}})
        assert _client(stub).wait(TASK_ID, timeout=1, poll_interval=0)["status"] == "done"

    def test_artifacts_reads_the_status_payload(self, stub):
        _set_json(stub, {"artifacts": [{"kind": "deliverable", "path": "out.md"}]})
        assert _client(stub).artifacts(TASK_ID) == [
            {"kind": "deliverable", "path": "out.md"}]


class TestErrorModel:
    def test_empty_success_body_is_an_empty_dict(self, stub):
        stub.status, stub.content_type, stub.body = 200, "application/json", b""
        assert _client(stub).health() == {}

    def test_non_json_success_body_becomes_raw(self, stub):
        stub.status, stub.content_type = 200, "text/html"
        stub.body = b"<html>proxy error</html>"
        assert _client(stub).health() == {"raw": "<html>proxy error</html>"}

    def test_non_json_error_body_becomes_raw_with_status(self, stub):
        stub.status, stub.content_type = 502, "text/plain"
        stub.body = b"bad gateway"
        with pytest.raises(LoomError) as got:
            _client(stub).health()
        assert got.value.status == 502
        assert got.value.body == {"raw": "bad gateway"}

    def test_json_error_body_keeps_its_fields(self, stub):
        stub.status, stub.content_type = 409, "application/json"
        stub.body = json.dumps({"error": "任务不在审批等待中"}).encode("utf-8")
        with pytest.raises(LoomError) as got:
            _client(stub).approve(TASK_ID)
        assert got.value.status == 409
        assert got.value.body == {"error": "任务不在审批等待中"}

    def test_unreachable_server_is_not_wrapped(self):
        """连不上抛 urllib.error.URLError——SDK 不替调用方把传输错误伪装成 HTTP。"""
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        dead_port = probe.getsockname()[1]
        probe.close()
        with pytest.raises(urllib.error.URLError):
            LoomClient(f"http://127.0.0.1:{dead_port}", token="t",
                       timeout=2.0).health()


# ─── 5. 凭证头（docstring 承诺的二选一与本机信任）───────────────


class TestCredentialHeaders:
    def test_token_wins_when_both_given(self, stub):
        _set_json(stub, {})
        LoomClient(stub.base_url, api_key="key-y", token="tok-x",
                   timeout=5.0).health()
        assert _last(stub)["authorization"] == "Bearer tok-x"

    def test_api_key_is_used_when_no_token(self, stub):
        _set_json(stub, {})
        LoomClient(stub.base_url, api_key="key-y", timeout=5.0).health()
        assert _last(stub)["authorization"] == "Bearer key-y"

    def test_no_credentials_sends_no_authorization_header(self, stub):
        _set_json(stub, {})
        LoomClient(stub.base_url, timeout=5.0).health()
        assert _last(stub)["authorization"] is None
