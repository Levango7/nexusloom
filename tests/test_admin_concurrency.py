"""常驻服务并发模型（docs/product-spec.md §3 P1 决策 A：保留 stdlib，消灭
spec 点名的两个缺陷——HTTP/1.0 无连接复用、线程-per-连接无并发上限）。

被测契约（全部真 HTTP，随机端口起真 server，非 mock handler）：
- **HTTP/1.1 keep-alive**：同一条 TCP 连接上连续发两个请求都成功——前提是
  每个响应都带 `Content-Length`（旧 HTTP/1.0 靠连接关闭界定响应体，直接切
  1.1 会让客户端永久挂起；判据把这一点钉住）；
- **Content-Length 全家**：JSON / 文本 / Prometheus / OPTIONS 403 分支 /
  静态文件，一个都不能漏；
- **并发上限**：池满即 **503 + Retry-After**，不排队、不无限起线程
  （`LOOM_ADMIN_MAX_CONCURRENCY` 可调）；拒答的连接还要**优雅关闭**——
  接收队列里没回收的请求字节会让内核发 RST，把刚写的 503 一起吞掉；
- **socket 超时**：半开连接（只发请求行不发完）在超时后由服务端关闭，
  不再永久占着一条（有界）线程（`LOOM_ADMIN_SOCKET_TIMEOUT` 可调）；
- **SSE 不参与连接复用**：`_handle_stream` 置 `close_connection`。
"""
from __future__ import annotations

import http.client
import json
import socket
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from loom_core.admin_api import (
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_SOCKET_TIMEOUT,
    AdminAPI,
    AdminHandler,
    BoundedThreadingHTTPServer,
)


@pytest.fixture
def orch_mock():
    o = MagicMock()
    o.list_tasks.return_value = []
    o.registry.list.return_value = []
    o.bus.health.return_value = {"status": "ok", "subscribers": 0}
    o.get_task.return_value = None
    return o


def _start(orch, **env):
    """起真 server（随机端口），env 以 monkeypatch 注入。返回 (port, api)。"""
    import os
    for k, v in env.items():
        os.environ[k] = str(v)
    try:
        api = AdminAPI(host="127.0.0.1", port=0, api_key="")
        assert api.start(orch) is True
        return api._server.server_address[1], api
    finally:
        for k in env:
            os.environ.pop(k, None)


def _wait_inflight(api, expected: int, timeout: float = 10.0) -> bool:
    """轮询服务端自己的占用计数，直到池里确有 `expected` 个请求。

    这类用例的前提是"池已被占满"，而前提只能由被观测方证实：`_inflight` 在
    accept 线程派发时递增，是服务端的真实状态。调用方必须对返回值判红，
    不能只等——否则后面"几条 200"就成了没有前提的代理指标。
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if api._server.inflight >= expected:
            return True
        time.sleep(0.02)
    return False


@pytest.fixture(autouse=True)
def reset_handler_state():
    yield
    AdminHandler.api_key = None
    AdminHandler.server_host = "127.0.0.1"
    AdminHandler.dashboard_dir = None
    AdminHandler.orch = None
    AdminHandler.timeout = None


# ── HTTP/1.1 keep-alive 与 Content-Length ────────────────────


class TestKeepAliveAndContentLength:
    def test_two_requests_on_one_connection(self, orch_mock):
        """同一连接连续两请求——HTTP/1.1 复用的最小证明。"""
        port, api = _start(orch_mock)
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/tasks")
            r1 = conn.getresponse()
            b1 = r1.read()
            assert r1.status == 200 and json.loads(b1) is not None
            conn.request("GET", "/health")
            r2 = conn.getresponse()
            b2 = r2.read()
            assert r2.status == 200 and json.loads(b2) is not None
            # 服务端没有关闭连接（否则第二次请求会走到新连接也仍能成功，
            # 因此这里额外断言服务端视连接为可复用）
            assert not r2.will_close
            conn.close()
        finally:
            api.stop()

    def test_json_response_has_content_length(self, orch_mock):
        port, api = _start(orch_mock)
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/tasks")
            r = conn.getresponse()
            assert r.getheader("Content-Length") is not None, \
                "JSON 响应缺 Content-Length——keep-alive 下客户端会挂死"
            assert r.getheader("Content-Length") == str(len(r.read()))
            conn.close()
        finally:
            api.stop()

    def test_prometheus_and_text_have_content_length(self, orch_mock):
        orch_mock._metrics = None
        port, api = _start(orch_mock)
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            for path in ("/metrics", "/"):
                conn.request("GET", path)
                r = conn.getresponse()
                body = r.read()
                assert r.getheader("Content-Length") == str(len(body)), \
                    f"{path} 缺 Content-Length（{r.status}）"
            conn.close()
        finally:
            api.stop()

    def test_options_403_branch_has_content_length(self, orch_mock):
        port, api = _start(orch_mock)
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("OPTIONS", "/tasks", headers={"Origin": "https://evil.example"})
            r = conn.getresponse()
            body = r.read()
            assert r.status == 403
            assert r.getheader("Content-Length") == str(len(body))
            conn.close()
        finally:
            api.stop()

    def test_static_file_has_content_length(self, orch_mock, tmp_path):
        (tmp_path / "index.html").write_text("<h1>ok</h1>", encoding="utf-8")
        port, api = _start(orch_mock)
        api._server  # noqa: B018
        AdminHandler.dashboard_dir = str(tmp_path)
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/index.html")
            r = conn.getresponse()
            body = r.read()
            assert r.status == 200
            assert r.getheader("Content-Length") == str(len(body))
            conn.close()
        finally:
            api.stop()

    def test_protocol_version_is_http11(self):
        assert AdminHandler.protocol_version == "HTTP/1.1"


# ── 有界并发 ──────────────────────────────────────────────


class TestBoundedConcurrency:
    def test_pool_full_returns_503_with_retry_after(self, orch_mock):
        """max_workers=2：两个请求占满池，第三个必须当场拿到 503 + Retry-After。

        "池满"这一步用服务端的占用计数确认，不是 `time.sleep(0.6)` 猜窗口：
        同机有并行负载时线程起步慢，第三个请求会在前两个还没派发时到达 ⇒
        池没满 ⇒ 三条全 200，红得像是并发上限本身失效（本轮实测正是这样错位的）。
        """
        gate = threading.Event()

        def _slow(self, *a, **kw):
            # 挂到 gate 被显式放行（`finally` 一定 set），不给占位请求设短超时：
            # 超时自动放行会让"池满"这个前提在断言中途自己消失。
            gate.wait(timeout=30)
            self._json({"ok": True})

        port, api = _start(orch_mock, LOOM_ADMIN_MAX_CONCURRENCY=2)
        assert api.max_concurrency == 2
        try:
            with patch.object(AdminHandler, "_handle_list_tasks", _slow):
                held: list[int] = []
                lock = threading.Lock()

                def _hit():
                    try:
                        c = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
                        c.request("GET", "/tasks")
                        r = c.getresponse()
                        r.read()
                        with lock:
                            held.append(r.status)
                        c.close()
                    except Exception:  # noqa: BLE001
                        with lock:
                            held.append(0)

                holders = [threading.Thread(target=_hit) for _ in range(2)]
                for t in holders:
                    t.start()
                assert _wait_inflight(api, 2), "两个请求没能占满池，前提不成立"

                third = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
                third.request("GET", "/tasks")
                r = third.getresponse()
                r.read()
                assert r.status == 503, f"池满没有拒绝：{r.status}"
                assert r.getheader("Retry-After") == "1", "503 没带 Retry-After"
                third.close()

                gate.set()
                for t in holders:
                    t.join(timeout=30)
                assert held == [200, 200], f"占位的两条不该被拒：{held}"
        finally:
            gate.set()
            api.stop()

    def test_503_body_and_retry_after_shape(self, orch_mock):
        gate = threading.Event()

        def _slow(self, *a, **kw):
            gate.wait(timeout=30)
            self._json({"ok": True})  # 放行后照常回应，别让读取线程干等

        port, api = _start(orch_mock, LOOM_ADMIN_MAX_CONCURRENCY=1)
        try:
            with patch.object(AdminHandler, "_handle_health", _slow):
                c2 = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
                c2.request("GET", "/health")  # 健康端点免鉴权，先占住唯一工位
                t = threading.Thread(target=lambda: c2.getresponse().read())
                t.start()
                assert _wait_inflight(api, 1), "唯一工位没被占住，前提不成立"
                rejected = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
                rejected.request("GET", "/tasks")
                r = rejected.getresponse()
                body = r.read()
                assert r.status == 503
                assert r.getheader("Retry-After") == "1"
                assert json.loads(body) == {"error": "server busy"}
                rejected.close()
                # 先放行占着工位的慢请求，再等它的读取线程收尾
                gate.set()
                t.join(timeout=30)
                c2.close()
        finally:
            gate.set()
            api.stop()

    def test_default_concurrency_is_sane(self):
        assert DEFAULT_MAX_CONCURRENCY >= 16

    def test_saturated_reject_waits_for_the_bytes_it_must_not_leave_behind(self):
        """拒答要等到请求字节落地再关：关早了内核发 RST，写好的 503 客户端读不到。

        构造是确定的，不靠碰运气：客户端先连着**不发**，等服务端进入回收步骤
        （`_drain_received` 入口）后 5ms 才发。缺陷版从回收到关闭约 0.1ms ⇒ 发送时
        连接已关 ⇒ 客户端 recv 报 10053/10054 ⇒ 判红；修复版的窗口是 50ms ⇒ 这 5ms
        到达的字节被回收 ⇒ 503 完整可读并以 FIN 收尾 ⇒ 判绿。两侧余量 10~50 倍，
        所以 5ms 是夹在两个已知时长之间，不是"睡一下看看"。
        """
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        server = BoundedThreadingHTTPServer(("127.0.0.1", 0), AdminHandler, max_workers=1)
        client = socket.create_connection(listener.getsockname(), timeout=10)
        client.settimeout(10)
        entered = threading.Event()
        real_drain = BoundedThreadingHTTPServer._drain_received

        def _spy(self, request, wait):  # noqa: ANN001
            entered.set()
            return real_drain(self, request, wait)

        srv, _addr = listener.accept()
        try:
            with patch.object(BoundedThreadingHTTPServer, "_drain_received",
                              autospec=True, side_effect=_spy):
                worker = threading.Thread(target=lambda: server._reject_saturated(srv))
                worker.start()
                assert entered.wait(5), "拒答路径没进入请求字节回收，前提不成立"
                time.sleep(0.005)
                try:
                    client.sendall(b"GET /tasks HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
                except OSError as exc:
                    pytest.fail(f"请求发不出去（连接已被服务端中止：{exc}），503 也读不到")
                worker.join(timeout=10)
                assert not worker.is_alive(), "拒答线程没收尾，服务端还挂在这条连接上"

            try:
                head = client.recv(4096)
                tail = client.recv(1)  # 期望 b''：FIN 收尾
            except OSError as exc:  # 10053/10054：RST 把 503 一起吞了
                pytest.fail(f"拒答连接被 RST 中止（{exc}），客户端读不到 503")
            assert head.startswith(b"HTTP/1.1 503"), head[:64]
            assert b"Retry-After: 1" in head, head[:256]
            assert tail == b"", "关闭不是优雅 FIN：接收队列里还有没回收的请求字节"
        finally:
            client.close()
            listener.close()
            server.server_close()

    def test_reject_wait_degrades_to_instant_shed_once_budget_is_gone(self):
        """那点等待是给真客户端的，不是给刷连接者的：配额耗尽后必须立刻硬关。

        去掉配额闸门（每条拒答都等满窗口）时，第二条静默连接也会等 50ms ⇒ 判红。
        这条钉的是"修复没顺手把拒答路径变成 accept 线程的新 DoS 面"：静默连接最多
        让 accept 线程每秒牺牲一个窗口（50ms），其余连接照旧即时 shed。
        """
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        listener.listen(8)
        server = BoundedThreadingHTTPServer(("127.0.0.1", 0), AdminHandler, max_workers=1)
        silent = [socket.create_connection(listener.getsockname(), timeout=10)
                  for _ in range(2)]
        held = [listener.accept() for _ in range(2)]
        try:
            t0 = time.perf_counter()
            server._reject_saturated(held[0][0])
            first = time.perf_counter() - t0
            assert 0.04 <= first < 0.5, f"第一条静默连接没等满窗口：{first:.3f}s"

            t0 = time.perf_counter()
            server._reject_saturated(held[1][0])
            second = time.perf_counter() - t0
            assert second < 0.02, f"配额耗尽后仍在等（{second:.3f}s）：accept 线程可被刷连接拖死"
        finally:
            for sock in silent:
                sock.close()
            for conn, _addr in held:
                conn.close()
            listener.close()
            server.server_close()

    def test_inflight_returns_to_zero_after_close(self, orch_mock):
        """keep-alive 语义：连接开着工位就占着；客户端关连接后工位必须归还。"""
        port, api = _start(orch_mock, LOOM_ADMIN_SOCKET_TIMEOUT=30)
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/tasks")
            r = conn.getresponse()
            r.read()
            assert _wait_inflight(api, 1), "keep-alive 下连接应仍占一个工位"
            conn.close()
            deadline = time.time() + 5
            while api._server.inflight and time.time() < deadline:
                time.sleep(0.05)
            assert api._server.inflight == 0, "客户端关闭后工位没有归还"
        finally:
            api.stop()

    def test_idle_keepalive_connection_reclaimed_by_socket_timeout(self, orch_mock):
        """连接开着不发请求：超过 socket 超时后服务端收回工位（不永久占线程）。"""
        port, api = _start(orch_mock, LOOM_ADMIN_SOCKET_TIMEOUT=1)
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/tasks")
            r = conn.getresponse()
            r.read()
            assert api._server.inflight == 1
            # 不再发请求，静置到超过 1s 超时——服务端应关闭连接并收回工位
            deadline = time.time() + 8
            while api._server.inflight and time.time() < deadline:
                time.sleep(0.1)
            assert api._server.inflight == 0, "空闲 keep-alive 连接没有被超时回收"
            conn.close()
        finally:
            api.stop()


# ── socket 超时（半开连接不占线程） ─────────────────────────


class TestSocketTimeout:
    def test_half_open_connection_is_closed(self, orch_mock):
        """只发半个请求行：服务端应在超时后主动关闭，而不是永久挂着。"""
        port, api = _start(orch_mock, LOOM_ADMIN_SOCKET_TIMEOUT=1)
        assert api.socket_timeout == 1.0
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=5)
            s.sendall(b"GET /tasks HTTP/1.1\r\n")  # 头没发完
            s.settimeout(8)
            closed = False
            deadline = time.time() + 8
            while time.time() < deadline:
                try:
                    if not s.recv(1024):
                        closed = True
                        break
                except TimeoutError:
                    break
            s.close()
            assert closed, "半开连接没有被服务端超时关闭"
        finally:
            api.stop()

    def test_default_timeout_is_sane(self):
        assert DEFAULT_SOCKET_TIMEOUT >= 5


# ── SSE 不参与连接复用 ─────────────────────────────────────


class TestSseConnectionSemantics:
    def test_stream_handler_marks_connection_close(self):
        """SSE 没有 Content-Length 可依，必须显式不复用连接。"""
        import io
        from unittest.mock import patch as _patch

        h = AdminHandler.__new__(AdminHandler)
        h.path = "/stream?task_id=x&query=y"
        h.headers = {}
        h.close_connection = False
        h.requestline = "GET /stream HTTP/1.1"
        h.request_version = "HTTP/1.1"
        h.wfile = io.BytesIO()
        AdminHandler.orch = None  # orchestrator 缺失 → connected/error 后即返回
        with _patch.object(AdminHandler, "send_response", lambda self, code: None), \
             _patch.object(AdminHandler, "send_header", lambda self, *a, **k: None), \
             _patch.object(AdminHandler, "end_headers", lambda self: None):
            h._handle_stream()
        assert h.close_connection is True
        assert b"orchestrator not set" in h.wfile.getvalue()
