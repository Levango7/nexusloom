"""High-level embedding SDK: from_env + generate one-call contract."""
import hashlib
from pathlib import Path

import pytest

from loom_core import (
    LoomApprovalRequired,
    LoomClient,
    LoomTaskError,
)
from loom_core.admin_api import AdminAPI, AdminHandler

PROJECT = Path(__file__).resolve().parent.parent
TOK_ADMIN = "tok-admin-sdk-generate"


def _tokens_yaml(tmp_path):
    path = tmp_path / "tokens.yaml"
    path.write_text(f"""
tokens:
  - id: admin
    admin: true
    token_sha256: {hashlib.sha256(TOK_ADMIN.encode()).hexdigest()}
""", encoding="utf-8")
    return path


def _new_orch(tmp_path):
    from loom_core import PipelineOrchestrator
    orch = PipelineOrchestrator(agents_dir=str(PROJECT / "agents"),
                                checkpoint_dir=str(tmp_path / "ckpt"))
    orch.register_agents()
    return orch


def _start(orch, tokens_file):
    api = AdminAPI(host="127.0.0.1", port=0, api_key="", tokens_file=str(tokens_file))
    assert api.start(orch) is True
    return f"http://127.0.0.1:{api._server.server_address[1]}", api


@pytest.fixture(autouse=True)
def reset_handler_state():
    yield
    AdminHandler.api_key = None
    AdminHandler.server_host = "127.0.0.1"
    AdminHandler.orch = None
    AdminHandler.tokens = None
    AdminHandler.principal = None
    AdminHandler.dashboard_dir = None


def test_from_env_uses_standard_runtime_configuration():
    client = LoomClient.from_env({
        "LOOM_RUNTIME_URL": "http://127.0.0.1:9999/",
        "LOOM_RUNTIME_API_KEY": "fake-test-token",
    })
    assert client.base_url == "http://127.0.0.1:9999"
    assert client._headers["Authorization"] == "Bearer fake-test-token"


def test_generate_returns_done_output(tmp_path):
    orch = _new_orch(tmp_path)
    base, api = _start(orch, _tokens_yaml(tmp_path))
    try:
        client = LoomClient.from_env({
            "LOOM_RUNTIME_URL": base,
            "LOOM_RUNTIME_API_KEY": TOK_ADMIN,
        })
        result = client.generate(query="SDK 一次调用验收", pipeline="demo")
        assert result["status"] == "done"
        assert result["id"]
        assert result["output_content"]
    finally:
        api.stop()
        orch.shutdown()


def test_generate_failed_task_raises_structured_error(tmp_path):
    orch = _new_orch(tmp_path)
    base, api = _start(orch, _tokens_yaml(tmp_path))
    try:
        client = LoomClient(base, token=TOK_ADMIN)
        with pytest.raises(LoomTaskError) as exc_info:
            client.generate(query="离线硬底线失败样例", pipeline="docgen")
        assert exc_info.value.task_id
        assert exc_info.value.task_status == "failed"
        assert exc_info.value.body["status"] == "failed"
    finally:
        api.stop()
        orch.shutdown()


def test_generate_approval_required_and_explicit_callback(tmp_path, monkeypatch):
    gate_yaml = tmp_path / "sdk_gate.yaml"
    gate_yaml.write_text("""name: sdk_gate
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
    from loom_core.scheduler import Scheduler
    monkeypatch.setattr("loom_core.pack_manifest.pipeline_dirs", lambda: [tmp_path])
    monkeypatch.setattr("loom_core.scheduler.Scheduler",
                        lambda: Scheduler(pipeline_dir=str(tmp_path)))
    orch = _new_orch(tmp_path)
    base, api = _start(orch, _tokens_yaml(tmp_path))
    try:
        client = LoomClient(base, token=TOK_ADMIN)
        with pytest.raises(LoomApprovalRequired) as exc_info:
            client.generate(inputs="这是 SDK 审批链测试输入，用于生成摘要。", pipeline="sdk_gate")
        assert exc_info.value.task_status == "paused"
        assert exc_info.value.body["awaiting_approval"]["nodes"] == ["release_gate"]

        result = client.generate(inputs="这是 SDK 审批链测试输入，用于生成摘要。",
                                 pipeline="sdk_gate", approval_handler=lambda _: "approve")
        assert result["status"] == "done"
        assert result["approvals"][-1]["decision"] == "approved"
    finally:
        api.stop()
        orch.shutdown()
