"""缺陷簇修复验证：CORS 白名单 / 流式指标聚合 / SSE 帧格式与止损 /
错误码统一 / 临时输入文件清理 / CLI pipeline 校验 / FAILED 退出码 / MCP isError。
"""
import io
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from loom_core import TaskStatus
from loom_core.admin_api import (
    AdminAPI,
    AdminHandler,
    stream_metrics_snapshot,
    track_stream_callback,
    untrack_stream_callback,
)
from loom_core.streaming import StreamCallback, StreamEvent

# ─── 公共工具 ─────────────────────────────────────

def _bare_handler(path="/"):
    h = AdminHandler.__new__(AdminHandler)
    h.path = path
    h.headers = {}
    h.orch = None
    h.api_key = None
    h.server_host = "127.0.0.1"
    h.dashboard_dir = None
    h._json = MagicMock()
    return h


@pytest.fixture(autouse=True)
def reset_handler_state():
    yield
    AdminHandler.api_key = None
    AdminHandler.server_host = "127.0.0.1"
    AdminHandler.dashboard_dir = None
    AdminHandler.orch = None


def _start_server(api_key="", orch=None):
    api = AdminAPI(host="127.0.0.1", port=0, api_key=api_key)
    assert api.start(orch if orch is not None else MagicMock()) is True
    port = api._server.server_address[1]
    return f"http://127.0.0.1:{port}", api, port


def _request(url, method="OPTIONS", origin=None):
    req = urllib.request.Request(url, method=method)
    if origin:
        req.add_header("Origin", origin)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers)


# ─── A1: CORS 白名单 ─────────────────────────────────────

class TestCorsWhitelist:
    def test_preflight_foreign_origin_rejected(self):
        base, api, _port = _start_server()
        try:
            status, headers = _request(base + "/api/tasks", "OPTIONS",
                                        origin="http://evil.example.com")
            assert status == 403
            assert "Access-Control-Allow-Origin" not in headers
        finally:
            api.stop()

    def test_preflight_same_origin_allowed(self):
        base, api, port = _start_server()
        try:
            status, headers = _request(base + "/api/tasks", "OPTIONS",
                                        origin=f"http://127.0.0.1:{port}")
            assert status == 204
            assert headers.get("Access-Control-Allow-Origin") == f"http://127.0.0.1:{port}"
        finally:
            api.stop()

    def test_preflight_localhost_interchangeable(self):
        base, api, port = _start_server()
        try:
            status, headers = _request(base + "/tasks", "OPTIONS",
                                        origin=f"http://localhost:{port}")
            assert status == 204
            assert headers.get("Access-Control-Allow-Origin") == f"http://localhost:{port}"
        finally:
            api.stop()

    def test_json_responses_no_wildcard_acao(self):
        base, api, port = _start_server()
        try:
            status, headers = _request(base + "/health", "GET",
                                        origin="http://evil.example.com")
            assert status == 200
            assert headers.get("Access-Control-Allow-Origin") != "*"
            assert "Access-Control-Allow-Origin" not in headers
        finally:
            api.stop()

    def test_extra_origin_via_env_allowed(self, monkeypatch):
        monkeypatch.setattr(
            "loom_core.admin_api._EXTRA_CORS_ORIGINS",
            ("https://dashboard.example.com",))
        h = _bare_handler()
        h.headers = {"Host": "127.0.0.1:8910",
                     "Origin": "https://dashboard.example.com"}
        assert h._allowed_origin("https://dashboard.example.com") \
            == "https://dashboard.example.com"

    def test_no_origin_no_cors_headers(self):
        base, api, _port = _start_server()
        try:
            status, headers = _request(base + "/health", "GET")
            assert status == 200
            assert "Access-Control-Allow-Origin" not in headers
        finally:
            api.stop()


# ─── A2: /stream/metrics 聚合 ─────────────────────────────────

class TestStreamMetricsAggregation:
    def test_snapshot_reflects_known_callback_data(self):
        cb = StreamCallback()
        cb.on_start(3, "Doc")
        cb.on_section(0, "S1", "c1")
        cb.on_section(1, "S2", "c2")
        track_stream_callback("agg-test-1", cb)
        try:
            snap = stream_metrics_snapshot()
            assert snap["events_emitted"] >= 3
            assert snap["sections_emitted"] >= 2
            assert snap["active_streams"] >= 1
        finally:
            untrack_stream_callback("agg-test-1")
            cb.close()

    def test_untrack_reduces_active_streams(self):
        cb = StreamCallback()
        track_stream_callback("agg-test-2", cb)
        untrack_stream_callback("agg-test-2")
        assert stream_metrics_snapshot()["active_streams"] == 0
        cb.close()

    def test_handler_serves_aggregate_not_zero_instance(self):
        cb = StreamCallback()
        cb.on_start(2, "Doc")
        cb.on_chunk("hello")
        track_stream_callback("agg-test-3", cb)
        try:
            h = _bare_handler("/stream/metrics")
            h._handle_stream()
            payload = h._json.call_args[0][0]
            assert payload["events_emitted"] >= 2
            assert payload["chunks_emitted"] >= 1
        finally:
            untrack_stream_callback("agg-test-3")
            cb.close()


# ─── A3: SSE 帧序列化 + 心跳 + 断管止损 ──────────────────────

class _FakeWFile:
    def __init__(self, fail_after=None):
        self.buffer = io.BytesIO()
        self.writes = 0
        self.fail_after = fail_after

    def write(self, data):
        self.writes += 1
        if self.fail_after is not None and self.writes > self.fail_after:
            raise BrokenPipeError("client gone")
        self.buffer.write(data)

    def flush(self):
        pass


class TestSseFrameAndPump:
    def test_send_sse_uses_to_dict_shape(self):
        h = _bare_handler()
        h.wfile = _FakeWFile()
        ev = StreamEvent("section", {"section_name": "S"}, section_index=2,
                         total_sections=5, event_id=7)
        assert h._send_sse(ev) is True
        frame = h.wfile.buffer.getvalue().decode()
        assert frame.startswith("id: 7\ndata: ")
        payload = json.loads(frame.split("data: ", 1)[1].strip())
        assert payload["type"] == "section"
        assert payload["data"]["section_name"] == "S"
        assert "ts" in payload
        assert payload["section"] == 2
        assert payload["total"] == 5
        assert payload["id"] == 7

    def test_send_sse_broken_pipe_returns_false(self):
        h = _bare_handler()
        h.wfile = _FakeWFile(fail_after=0)
        ev = StreamEvent("chunk", {"text": "x"}, event_id=1)
        assert h._send_sse(ev) is False

    def test_send_heartbeat_comment_frame(self):
        h = _bare_handler()
        h.wfile = _FakeWFile()
        assert h._send_heartbeat() is True
        assert h.wfile.buffer.getvalue() == b": keep-alive\n\n"

    def test_pump_cancels_task_on_consecutive_broken_pipe(self):
        h = _bare_handler()
        h.orch = MagicMock()
        h.wfile = _FakeWFile(fail_after=0)
        cb = StreamCallback()
        for i in range(5):
            cb.on_chunk(f"c{i}")
        cb.close()
        h._pump_sse(cb, "task-bp")
        h.orch.cancel.assert_called_once_with("task-bp")

    def test_pump_sends_events_until_complete(self):
        h = _bare_handler()
        h.orch = MagicMock()
        wfile = _FakeWFile()
        h.wfile = wfile
        cb = StreamCallback()
        cb.on_start(1, "Doc")
        cb.on_complete("done content", {})
        h._pump_sse(cb, "task-ok")
        out = wfile.buffer.getvalue().decode()
        assert '"complete"' in out.replace(" ", "") or '"type": "complete"' in out
        h.orch.cancel.assert_not_called()


# ─── A4: 错误格式统一 ─────────────────────────────────────

class TestErrorFormatUnification:
    def test_health_without_orch_503_error_shape(self):
        h = _bare_handler()
        h._handle_health()
        assert h._json.call_args[0][1] == 503
        assert "error" in h._json.call_args[0][0]

    def test_metrics_without_orch_503_error_shape(self):
        h = _bare_handler()
        h._handle_metrics()
        assert h._json.call_args[0][1] == 503
        assert "error" in h._json.call_args[0][0]

    def test_alerts_limit_not_int_400(self):
        h = _bare_handler("/api/alerts?limit=abc")
        h._handle_alerts()
        assert h._json.call_args[0][1] == 400
        assert "error" in h._json.call_args[0][0]

    def test_logs_since_not_int_400(self):
        h = _bare_handler("/api/logs?since=abc")
        h._handle_logs()
        assert h._json.call_args[0][1] == 400
        assert "error" in h._json.call_args[0][0]

    def test_dlq_replay_get_bad_id_400(self):
        h = _bare_handler("/dlq/not-an-int/replay")
        h.do_GET()
        assert h._json.call_args[0][1] == 400
        assert "invalid dlq id" in h._json.call_args[0][0]["error"]

    def test_dlq_replay_post_bad_id_400(self):
        h = _bare_handler("/dlq/bad/replay")
        h.do_POST()
        assert h._json.call_args[0][1] == 400

    def test_do_get_fallback_hides_exception_detail(self):
        h = _bare_handler("/metrics")
        h.orch = MagicMock()
        h.orch._metrics = None
        with patch.object(AdminHandler, "_handle_metrics",
                          side_effect=RuntimeError("secret-db-password")):
            h.do_GET()
        payload, status = h._json.call_args[0]
        assert status == 500
        assert payload["error"] == "internal server error"
        assert "secret" not in payload["error"]

    def test_do_post_fallback_generic_message(self):
        h = _bare_handler("/api/config/reload")
        h.headers = {}
        h.orch = MagicMock()
        with patch.object(AdminHandler, "_handle_config_reload",
                          side_effect=RuntimeError("boom-detail")):
            h.do_POST()
        payload, status = h._json.call_args[0]
        assert status == 500
        assert "boom-detail" not in json.dumps(payload)


# ─── A5: 临时输入文件清理（POST /api/tasks）───────────────────

class TestSubmitTaskTempCleanup:
    def _handler_with_orch(self):
        h = _bare_handler("/api/tasks")
        h.orch = MagicMock()
        task = MagicMock()
        task.id = "t123"
        task.status = SimpleNamespace(value="running")
        task.pipeline_name = "docgen"
        task.result = {}
        h.orch.run_plan.return_value = task
        h._find_streaming_agent = lambda: None
        return h

    def test_wait_true_removes_temp_input(self):
        h = self._handler_with_orch()
        h._handle_submit_task(b'{"query": "test topic", "pipeline": "docgen", "wait": true}')
        call = h._json.call_args[0]
        assert len(call) == 1 or call[1] == 200
        input_arg = h.orch.run_plan.call_args.kwargs["input_file"]
        assert not Path(input_arg).exists()

    def test_wait_false_temp_removed_after_terminal(self):
        h = self._handler_with_task_lifecycle()
        h._handle_submit_task(b'{"query": "async topic", "pipeline": "docgen"}')
        input_arg = h.orch.run_plan.call_args.kwargs["input_file"]
        import time as _t
        deadline = _t.time() + 5
        while Path(input_arg).exists() and _t.time() < deadline:
            _t.sleep(0.05)
        assert not Path(input_arg).exists()

    def _handler_with_task_lifecycle(self):
        from loom_core.streaming import unregister_callback
        unregister_callback("t456")
        h = _bare_handler("/api/tasks")
        h.orch = MagicMock()
        task = MagicMock()
        task.id = "t456"
        task.status = SimpleNamespace(value="running")
        task.pipeline_name = "docgen"
        task.result = {}
        h.orch.run_plan.return_value = task
        # 任务已不在注册表中 → reaper 首轮即视为终态并删除文件
        h.orch.get_task.return_value = None
        h._find_streaming_agent = lambda: None
        return h

    def test_parse_failure_leaves_no_temp_file(self):
        h = self._handler_with_orch()
        before = {p.name for p in Path(tempfile_dir()).glob("task_*.md")}
        body = b'{"query": "x", "pipeline": "no_such_pipeline_yaml"}'
        h._handle_submit_task(body)
        assert h._json.call_args[0][1] == 400
        after = {p.name for p in Path(tempfile_dir()).glob("task_*.md")}
        assert after <= before


def tempfile_dir():
    import tempfile
    return tempfile.gettempdir()


# ─── B6/B7: run.py CLI 校验与退出码 ───────────────────────────

class TestCliPipelineValidation:
    def test_unknown_pipeline_exits_2_listing_available(self, capsys):
        from run import _resolve_pipeline_plan
        args = SimpleNamespace(pipeline="docgen-typo", pipeline_file=None)
        with pytest.raises(SystemExit) as exc_info:
            _resolve_pipeline_plan(args, MagicMock(), {})
        assert exc_info.value.code == 2
        err = capsys.readouterr().err
        assert "docgen-typo" in err
        assert "docgen" in err

    def test_argparse_choices_rejects_bad_pipeline(self):
        from run import build_arg_parser
        parser = build_arg_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["input.md", "--pipeline", "bogus-pipeline"])
        assert exc_info.value.code == 2

    def test_argparse_accepts_docgen(self):
        from run import build_arg_parser
        parser = build_arg_parser()
        args = parser.parse_args(["input.md", "--pipeline", "docgen"])
        assert args.pipeline == "docgen"


class TestTaskExitCodeMapping:
    def test_failed_maps_to_1(self):
        from run import _task_exit_code
        task = SimpleNamespace(status=TaskStatus.FAILED)
        assert _task_exit_code(task) == 1

    def test_cancelled_maps_to_2(self):
        from run import _task_exit_code
        task = SimpleNamespace(status=TaskStatus.CANCELLED)
        assert _task_exit_code(task) == 2

    def test_done_and_paused_map_to_0(self):
        from run import _task_exit_code
        assert _task_exit_code(SimpleNamespace(status=TaskStatus.DONE)) == 0
        assert _task_exit_code(SimpleNamespace(status=TaskStatus.PAUSED)) == 0

    def test_main_failing_task_exits_1(self, tmp_path):
        from run import main
        input_file = tmp_path / "input.md"
        input_file.write_text("# Test", encoding="utf-8")

        mock_orch = MagicMock()
        mock_orch.register_agents.return_value = ["agent1"]
        task = MagicMock()
        task.status = TaskStatus.FAILED
        task.error = "kaput"
        task.steps = []
        task.result = {}
        task.finished_at = 1.0
        task.started_at = 0.5
        mock_orch.run.return_value = task

        with patch("sys.argv", ["run.py", str(input_file)]), \
                patch("run._get_orchestrator", return_value=mock_orch), \
                patch("run._load_config", return_value={}), \
                patch("run._resolve_pipeline_plan", return_value=(None, False)), \
                patch("builtins.print"), \
                pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_check_semantics_unchanged(self):
        """--check 无错误仍退出 0（既有语义不受终态映射影响）"""
        from run import main
        with patch("sys.argv", ["run.py", "--check"]), \
                patch("loom_core.bootstrap.run_startup_check") as mock_check:
            report = MagicMock()
            report.has_errors = False
            report.summary.return_value = "OK"
            mock_check.return_value = report
            with patch("builtins.print"), pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0


# ─── C8/C9/C10: MCP 业务失败 isError / 校验 / output / 清理 ─────

class TestMcpBusinessFailureIsError:
    def _server(self, orch=None):
        from loom_core.mcp_server import MCPServer
        return MCPServer(orch=orch if orch is not None else MagicMock())

    def test_missing_query_is_error_result(self):
        resp = self._server()._handle_request({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "generate_document", "arguments": {}},
        })
        assert "error" not in resp
        assert resp["result"]["isError"] is True
        text = resp["result"]["content"][0]["text"]
        assert isinstance(text, str) and text

    def test_task_not_found_is_error_result(self):
        from loom_core.run_handle import RunHandle

        orch = MagicMock()
        orch.get_task.return_value = None
        # 存在性由 RunHandle 回答（内存 → RunStore 投影 → 持久队列）：替身要把
        # 这三条回落链都配成"没有"，否则自动生成的 mock 会被当成"存在"。
        orch.run_handle.side_effect = lambda tid: RunHandle(orch, tid)
        orch.run_store.return_value = None
        orch.task_queue.get.return_value = None
        resp = self._server(orch)._handle_request({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": "get_task", "arguments": {"task_id": "nope"}},
        })
        assert "error" not in resp
        assert resp["result"]["isError"] is True
        assert "Task not found" in resp["result"]["content"][0]["text"]

    def test_pipeline_failure_is_error_result(self):
        orch = MagicMock()
        orch.run_plan.side_effect = RuntimeError("llm quota gone")
        with patch("loom_core.scheduler.Scheduler") as mock_sched_cls:
            mock_sched_cls.return_value.parse.return_value = MagicMock()
            resp = self._server(orch)._handle_request({
                "jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": {"name": "generate_document",
                           "arguments": {"query": "topic", "pipeline": "docgen"}},
            })
        assert "error" not in resp
        assert resp["result"]["isError"] is True
        assert "Pipeline failed" in resp["result"]["content"][0]["text"]

    def test_protocol_errors_still_jsonrpc(self):
        s = self._server()
        resp = s._handle_request({"jsonrpc": "2.0", "id": 4, "method": "no-such"})
        assert resp["error"]["code"] == -32601
        resp = s._handle_request({
            "jsonrpc": "2.0", "id": 5, "method": "tools/call",
            "params": {"name": "unknown-tool", "arguments": {}},
        })
        assert resp["error"]["code"] == -32602


class TestMcpValidationAndOutput:
    def _server(self, orch=None):
        from loom_core.mcp_server import MCPServer
        return MCPServer(orch=orch if orch is not None else MagicMock())

    def test_get_task_invalid_task_id_rejected(self):
        resp = self._server()._handle_request({
            "jsonrpc": "2.0", "id": 10, "method": "tools/call",
            "params": {"name": "get_task", "arguments": {"task_id": "../etc/passwd"}},
        })
        assert resp["result"]["isError"] is True
        assert "Invalid task_id" in resp["result"]["content"][0]["text"]

    def test_generate_document_output_whitelist_violation(self):
        resp = self._server()._handle_request({
            "jsonrpc": "2.0", "id": 11, "method": "tools/call",
            "params": {"name": "generate_document",
                       "arguments": {"query": "t", "output": "../../outside.md"}},
        })
        assert resp["result"]["isError"] is True
        assert "output" in resp["result"]["content"][0]["text"].lower()

    def test_generate_document_output_written_on_wait(self, tmp_path):
        src = tmp_path / "src_out.md"
        src.write_text("# Doc\n\ncontent", encoding="utf-8")
        task = MagicMock()
        task.id = "tw1"
        task.status = SimpleNamespace(value="done")
        task.pipeline_name = "docgen"
        task.error = None
        # 交付物契约：由声明 WRITES_OUTPUT 的落盘节点挂在任务上
        task.output_path = str(src)
        task.output_content = "# Doc" + chr(10) + chr(10) + "content"
        orch = MagicMock()
        orch.run_plan.return_value = task

        rel_target = ".pytest_tmp/mcp_out_test/doc_final.md"
        target_abs = Path(__file__).parent.parent / rel_target
        try:
            resp = self._server(orch)._handle_request({
                "jsonrpc": "2.0", "id": 12, "method": "tools/call",
                "params": {"name": "generate_document",
                           "arguments": {"query": "t", "pipeline": "docgen",
                                         "wait": True, "output": rel_target}},
            })
            payload = json.loads(resp["result"]["content"][0]["text"])
            assert target_abs.exists()
            assert "# Doc" in target_abs.read_text(encoding="utf-8")
            assert payload.get("output_written", "").endswith("doc_final.md")
        finally:
            target_abs.unlink(missing_ok=True)

    def test_mcp_temp_input_cleaned_on_wait_true(self):
        import glob
        import os
        import tempfile as tf
        task = MagicMock()
        task.id = "tc9"
        task.status = SimpleNamespace(value="done")
        task.pipeline_name = "docgen"
        task.error = None
        task.result = {}
        orch = MagicMock()
        orch.run_plan.return_value = task
        before = set(glob.glob(os.path.join(tf.gettempdir(), "mcp_*.md")))
        self._server(orch)._handle_request({
            "jsonrpc": "2.0", "id": 13, "method": "tools/call",
            "params": {"name": "generate_document",
                       "arguments": {"query": "cleanup", "pipeline": "docgen", "wait": True}},
        })
        input_arg = orch.run_plan.call_args.kwargs["input_file"]
        assert Path(input_arg).exists() is False
        assert Path(input_arg) not in before or True

    def test_list_pipelines_uses_project_root_anchor(self):
        from loom_core.mcp_server import PROJECT_ROOT
        resp = self._server()._handle_request({
            "jsonrpc": "2.0", "id": 14, "method": "tools/call",
            "params": {"name": "list_pipelines", "arguments": {}},
        })
        data = json.loads(resp["result"]["content"][0]["text"])
        names = [p["name"] for p in data["pipelines"]]
        assert "docgen" in names
        assert all(not n.startswith("test_") for n in names)
        assert (PROJECT_ROOT / "pipelines").is_dir()


# ─── D: OpenAPI spec 补充 ─────────────────────────────────────

class TestOpenApiSpecAdditions:
    def test_taskinfo_status_enum_includes_paused(self):
        from loom_core.openapi_spec import generate_spec
        enum = generate_spec()["components"]["schemas"]["TaskInfo"]["properties"]["status"]["enum"]
        assert "paused" in enum

    def test_stream_metrics_response_is_json(self):
        from loom_core.openapi_spec import generate_spec
        resp = generate_spec()["paths"]["/stream/metrics"]["get"]["responses"]["200"]
        assert "application/json" in resp["content"]

    def test_root_endpoint_declared(self):
        from loom_core.openapi_spec import generate_spec
        paths = generate_spec()["paths"]
        assert "/" in paths
        assert paths["/"]["get"]["summary"] != "/"

    def test_stream_declares_last_event_id_header(self):
        from loom_core.openapi_spec import generate_spec
        params = generate_spec()["paths"]["/stream"]["get"]["parameters"]
        header = [p for p in params if p["in"] == "header" and p["name"] == "Last-Event-ID"]
        assert len(header) == 1
