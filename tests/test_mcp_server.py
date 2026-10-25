"""MCPServer — JSON-RPC 协议测试"""
import json
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from loom_core.mcp_server import PROTOCOL_VERSION, SERVER_NAME, MCPServer


@pytest.fixture
def server():
    s = MCPServer(orch=MagicMock())
    return s


class TestMCPServer:
    def test_server_identity_is_loom(self):
        """serverInfo.name = loom（spec §7.1 同名行；旧值 doc-pipeline 已更）。"""
        assert SERVER_NAME == "loom"

    def test_initialize(self, server):
        resp = server._handle_request({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2024-11-05"},
        })
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 1
        assert resp["result"]["protocolVersion"] == PROTOCOL_VERSION
        assert resp["result"]["serverInfo"]["name"] == SERVER_NAME

    def test_initialize_echoes_client_protocol_version(self, server):
        """MCP 规范：result.protocolVersion 回显客户端请求的版本"""
        resp = server._handle_request({
            "jsonrpc": "2.0", "id": 10, "method": "initialize",
            "params": {"protocolVersion": "2025-06-18"},
        })
        assert resp["result"]["protocolVersion"] == "2025-06-18"

    def test_generate_document_uses_new_task_id(self):
        """task_id 统一由 loom_core.ids.new_task_id 生成（16 位 hex）"""
        from types import SimpleNamespace

        from loom_core.ids import new_task_id
        task = SimpleNamespace(id="tid", status=SimpleNamespace(value="running"),
                               pipeline_name="docgen", result={}, error=None)
        orch = MagicMock()
        orch.resolve_pipeline_name.return_value = ("docgen", "")
        orch.run_plan.return_value = task
        s = MCPServer(orch=orch)
        with patch("loom_core.mcp_server.new_task_id",
                   side_effect=new_task_id) as mock_gen:
            s._tool_generate_document(11, {"query": "t", "pipeline": "docgen"})
            mock_gen.assert_called_once()
        input_arg = orch.run_plan.call_args.kwargs["input_file"]
        stem = Path(input_arg).stem
        tid = stem.removeprefix("mcp_")
        assert re.fullmatch(r"[0-9a-f]{16}", tid)

    def test_tools_list(self, server):
        resp = server._handle_request({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        tools = resp["result"]["tools"]
        assert len(tools) == 6
        names = [t["name"] for t in tools]
        assert "generate_document" in names
        assert "run_workflow" in names
        assert "get_task" in names
        assert "list_tasks" in names
        assert "list_pipelines" in names
        assert "get_pipeline_info" in names

    def test_tool_has_input_schema(self, server):
        resp = server._handle_request({"jsonrpc": "2.0", "id": 3, "method": "tools/list"})
        for tool in resp["result"]["tools"]:
            assert "inputSchema" in tool
            assert tool["inputSchema"]["type"] == "object"

    def test_generate_document_missing_query(self, server):
        """业务失败（缺 query）→ MCP 规范的 result.content + isError:true，非 JSON-RPC error"""
        resp = server._handle_request({
            "jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": {"name": "generate_document", "arguments": {}},
        })
        assert "error" not in resp
        assert resp["result"]["isError"] is True
        assert "query" in resp["result"]["content"][0]["text"]

    def test_unknown_method(self, server):
        resp = server._handle_request({"jsonrpc": "2.0", "id": 5, "method": "unknown"})
        assert "error" in resp
        assert resp["error"]["code"] == -32601

    def test_unknown_tool(self, server):
        resp = server._handle_request({
            "jsonrpc": "2.0", "id": 6, "method": "tools/call",
            "params": {"name": "nonexistent", "arguments": {}},
        })
        assert "error" in resp
        assert resp["error"]["code"] == -32602

    def test_ping(self, server):
        resp = server._handle_request({"jsonrpc": "2.0", "id": 7, "method": "ping"})
        assert resp["result"] == {}

    def test_initialized_notification(self, server):
        resp = server._handle_request({"jsonrpc": "2.0", "method": "initialized"})
        assert resp is None

    def test_list_pipelines(self, server):
        resp = server._handle_request({
            "jsonrpc": "2.0", "id": 8, "method": "tools/call",
            "params": {"name": "list_pipelines", "arguments": {}},
        })
        assert "result" in resp
        assert "content" in resp["result"]
        data = json.loads(resp["result"]["content"][0]["text"])
        assert "pipelines" in data

    def test_tool_result_format(self, server):
        resp = server._handle_request({
            "jsonrpc": "2.0", "id": 9, "method": "tools/call",
            "params": {"name": "list_pipelines", "arguments": {}},
        })
        content = resp["result"]["content"]
        assert isinstance(content, list)
        assert content[0]["type"] == "text"
        assert isinstance(content[0]["text"], str)


class TestRunWorkflowTool:
    """通用入口 run_workflow：任意流水线 + 任意 inputs。

    输入文档是引擎的运行接口（首层 Agent 从它提取查询词），序列化规则单独
    直测 `_render_inputs_doc`；提交路径用 MagicMock orchestrator 断言，
    并 patch 掉 watcher 线程以便读临时输入文件（不 patch 会被后台清理删走）。
    """

    def _server(self, pipeline="api-digest"):
        from types import SimpleNamespace
        task = SimpleNamespace(id="tid", status=SimpleNamespace(value="running"),
                               pipeline_name=pipeline, result={}, error=None)
        orch = MagicMock()
        orch.run_plan.return_value = task
        return MCPServer(orch=orch), orch

    def test_tool_listed_with_schema(self, server):
        resp = server._handle_request({"jsonrpc": "2.0", "id": 30, "method": "tools/list"})
        tool = next(t for t in resp["result"]["tools"] if t["name"] == "run_workflow")
        assert tool["inputSchema"]["required"] == ["name"]
        assert "inputs" in tool["inputSchema"]["properties"]

    def test_missing_name(self, server):
        resp = server._handle_request({
            "jsonrpc": "2.0", "id": 31, "method": "tools/call",
            "params": {"name": "run_workflow", "arguments": {}},
        })
        assert resp["result"]["isError"] is True
        assert "name" in resp["result"]["content"][0]["text"]

    def test_unknown_pipeline_lists_available(self, server):
        resp = server._handle_request({
            "jsonrpc": "2.0", "id": 32, "method": "tools/call",
            "params": {"name": "run_workflow", "arguments": {"name": "no-such-wf"}},
        })
        text = resp["result"]["content"][0]["text"]
        assert resp["result"]["isError"] is True
        assert "no-such-wf" in text and "可用" in text

    def test_object_inputs_serialized_into_input_doc(self):
        s, orch = self._server()
        with patch("loom_core.mcp_server.threading.Thread"):
            resp = s._handle_request({
                "jsonrpc": "2.0", "id": 33, "method": "tools/call",
                "params": {"name": "run_workflow", "arguments": {
                    "name": "api-digest",
                    "inputs": {"topic": "季度营收", "files": ["a.pdf", "b.pdf"]},
                }},
            })
        assert resp["result"].get("isError") is not True
        data = json.loads(resp["result"]["content"][0]["text"])
        assert data["pipeline"] == "api-digest"
        assert data["inputs"]["topic"] == "季度营收"      # inputs 原样回显
        input_file = Path(orch.run_plan.call_args.kwargs["input_file"])
        text = input_file.read_text(encoding="utf-8")
        assert "## topic" in text and "季度营收" in text
        assert "- a.pdf" in text and "- b.pdf" in text
        input_file.unlink(missing_ok=True)

    def test_string_inputs_passthrough(self):
        s, orch = self._server("kb-docgen")
        with patch("loom_core.mcp_server.threading.Thread"):
            resp = s._handle_request({
                "jsonrpc": "2.0", "id": 34, "method": "tools/call",
                "params": {"name": "run_workflow", "arguments": {
                    "name": "kb-docgen", "inputs": "季度复盘\n资料A.pdf",
                }},
            })
        assert resp["result"].get("isError") is not True
        input_file = Path(orch.run_plan.call_args.kwargs["input_file"])
        text = input_file.read_text(encoding="utf-8")
        assert "季度复盘" in text and "资料A.pdf" in text
        input_file.unlink(missing_ok=True)

    def test_render_inputs_doc_shapes(self):
        # 渲染器归 input_docs 管；mcp_server 里那个别名已随"闸门上移"删掉，
        # 判据本身（文档形状）不变——从真实出处 import，不再测一个转发别名。
        from loom_core.input_docs import render_inputs_doc as _render_inputs_doc
        assert _render_inputs_doc("wf", {}) == "# wf\n"
        assert _render_inputs_doc("wf", "") == "# wf\n"
        doc = _render_inputs_doc("wf", {"n": 3, "nested": {"a": 1}, "items": [1, "x"]})
        assert "## n" in doc and "\n3\n" in doc
        assert "- 1" in doc and "- x" in doc
        assert re.search(r'\{"a":\s*1\}', doc), f"嵌套对象应走 JSON: {doc!r}"


class TestMCPOverRealStdio:
    """真实 stdio 往返：上面 12 条用例都在进程内调 _handle_request，
    于是"banner 打到 stdout 污染 JSON-RPC 通道"和"serverInfo.version 恒为
    unknown"这两件事没有任何测试能发现（2026-10-06 实测复现）。
    """

    REPO = Path(__file__).parent.parent

    def _roundtrip(self, tmp_path):
        import subprocess
        import sys
        requests = [
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        ]
        payload = "\n".join(json.dumps(r) for r in requests) + "\n"
        env = dict(__import__("os").environ,
                   # 状态目录隔离：幂等库按检出绝对路径共享，跑在真 bus_data/ 上会串台
                   LOOM_STATE_DIR=str(tmp_path))
        proc = subprocess.run([sys.executable, "run.py", "--mcp"], input=payload,
                              capture_output=True, text=True, timeout=120,
                              cwd=str(self.REPO), env=env)
        assert proc.returncode == 0, proc.stderr[-2000:]
        frames = [line for line in proc.stdout.splitlines() if line.strip()]
        return frames

    def test_stdout_carries_only_jsonrpc_frames(self, tmp_path):
        frames = self._roundtrip(tmp_path)
        assert len(frames) == 2, f"每个请求应回一帧，实得: {frames}"
        for line in frames:
            parsed = json.loads(line)          # 非 JSON 行（banner 等）在这里炸
            assert parsed.get("jsonrpc") == "2.0", parsed
        assert "Doc-Pipeline" not in "\n".join(frames), "banner 又回到 stdout 了"

    def test_server_info_reports_real_version(self, tmp_path):
        from loom_core import __version__
        frames = self._roundtrip(tmp_path)
        info = json.loads(frames[0])["result"]["serverInfo"]
        assert info["name"] == SERVER_NAME
        assert info["version"] == __version__, \
            f"版本回显必须是真实值，历史上当成 'unknown' 出厂过: {info}"
        assert json.loads(frames[1])["result"]["tools"], "tools/list 必须给出工具表"
