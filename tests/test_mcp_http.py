"""Real Streamable HTTP tests for the Admin API MCP adapter."""
import http.client
import json
from unittest.mock import MagicMock

import pytest

from loom_core.admin_api import AdminAPI, AdminHandler
from loom_core.tokens import Principal


@pytest.fixture
def http_api():
    previous_tokens = AdminHandler.tokens
    api = AdminAPI(host="127.0.0.1", port=0, api_key="sekret")
    orch = MagicMock()
    assert api.start(orch)
    yield api, orch
    api.stop()
    AdminHandler.tokens = previous_tokens


def _post(api, payload, token="sekret", raw=None):
    body = raw if raw is not None else json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    conn = http.client.HTTPConnection("127.0.0.1", api._server.server_address[1], timeout=5)
    try:
        conn.request("POST", "/mcp", body=body, headers=headers)
        resp = conn.getresponse()
        text = resp.read().decode()
        return resp.status, json.loads(text) if text else None
    finally:
        conn.close()


def test_initialize_and_notification_roundtrip(http_api):
    api, _ = http_api
    status, data = _post(api, {"jsonrpc": "2.0", "id": 1, "method": "initialize",
                               "params": {"protocolVersion": "2025-03-26"}})
    assert status == 200
    assert data["result"]["protocolVersion"] == "2025-03-26"
    status, data = _post(api, {"jsonrpc": "2.0", "method": "notifications/initialized"})
    assert status == 202
    assert data is None


def test_tools_list_over_real_http(http_api):
    api, _ = http_api
    status, data = _post(api, {"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    assert status == 200
    tools = data["result"]["tools"]
    assert len(tools) == 9
    assert {t["name"] for t in tools} >= {"approve_task", "reject_task", "resume_task"}


def test_malformed_json_is_jsonrpc_parse_error(http_api):
    api, _ = http_api
    status, data = _post(api, None, raw=b"{broken")
    assert status == 200
    assert data["error"]["code"] == -32700
    assert data["id"] is None


def test_unknown_method_is_jsonrpc_error(http_api):
    api, _ = http_api
    status, data = _post(api, {"jsonrpc": "2.0", "id": 3, "method": "no/such"})
    assert status == 200
    assert data["error"]["code"] == -32601


def test_mcp_requires_authentication(http_api):
    api, _ = http_api
    status, data = _post(api, {"jsonrpc": "2.0", "id": 4, "method": "tools/list"}, token="")
    assert status == 401
    assert data["error"] == "unauthorized"


def test_mcp_rejects_non_admin_principal(http_api):
    api, _ = http_api
    store = MagicMock()
    store.enabled = True
    store.resolve.return_value = Principal(id="user", admin=False, source="test")
    AdminHandler.tokens = store
    status, data = _post(api, {"jsonrpc": "2.0", "id": 5, "method": "tools/list"}, token="user")
    assert status == 403
    assert data["error"] == "admin required"


def test_gate_run_resume_reject_and_approve_chain(tmp_path, monkeypatch):
    import contextlib
    import time

    from loom_core import PipelineOrchestrator
    from loom_core.scheduler import Scheduler

    gate_yaml = tmp_path / "mcp_gate.yaml"
    gate_yaml.write_text("""name: mcp_gate
agents:
  - name: tldr
    version: "0.1"
    dependencies: []
    config: {max_sentences: 2}
  - name: release_gate
    approval: {message: "确认对外发布"}
    dependencies: ["tldr"]
topology:
  levels: [["tldr"], ["release_gate"]]
  edges: [["tldr", "release_gate"]]
""", encoding="utf-8")
    monkeypatch.setattr("loom_core.pack_manifest.pipeline_dirs", lambda: [tmp_path])
    monkeypatch.setattr("loom_core.scheduler.Scheduler",
                        lambda: Scheduler(pipeline_dir=str(tmp_path)))
    orch = PipelineOrchestrator(agents_dir="agents", checkpoint_dir=str(tmp_path / "ckpt"))
    orch.register_agents()
    previous_tokens = AdminHandler.tokens
    api = AdminAPI(host="127.0.0.1", port=0, api_key="sekret")
    assert api.start(orch)

    def wait_for(task_id, wanted, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            _, frame = _post(api, {"jsonrpc": "2.0", "id": 90, "method": "tools/call",
                "params": {"name": "get_task", "arguments": {"task_id": task_id}}})
            payload = json.loads(frame["result"]["content"][0]["text"])
            if payload["status"] == wanted:
                return payload
            time.sleep(0.1)
        raise AssertionError(f"task {task_id} did not reach {wanted}: {payload}")

    def start_gate():
        _, frame = _post(api, {"jsonrpc": "2.0", "id": 91, "method": "tools/call",
            "params": {"name": "run_workflow", "arguments": {
                "name": "mcp_gate", "inputs": "这是审批链测试输入，用于生成摘要。", "wait": False}}})
        return json.loads(frame["result"]["content"][0]["text"])["task_id"]

    try:
        task_id = start_gate()
        paused = wait_for(task_id, "paused")
        assert paused["awaiting_approval"]["nodes"] == ["release_gate"]
        _, resume = _post(api, {"jsonrpc": "2.0", "id": 92, "method": "tools/call",
            "params": {"name": "resume_task", "arguments": {"task_id": task_id}}})
        assert resume["result"]["isError"] is True
        _, approved = _post(api, {"jsonrpc": "2.0", "id": 93, "method": "tools/call",
            "params": {"name": "approve_task", "arguments": {"task_id": task_id, "note": "ok"}}})
        assert approved["result"].get("isError") is not True
        assert wait_for(task_id, "done")["status"] == "done"

        rejected_id = start_gate()
        wait_for(rejected_id, "paused")
        _, rejected = _post(api, {"jsonrpc": "2.0", "id": 94, "method": "tools/call",
            "params": {"name": "reject_task", "arguments": {"task_id": rejected_id, "reason": "no"}}})
        assert rejected["result"].get("isError") is not True
        assert wait_for(rejected_id, "cancelled")["status"] == "cancelled"
    finally:
        api.stop()
        AdminHandler.tokens = previous_tokens
        with contextlib.suppress(Exception):
            orch.shutdown()
