"""OpenAPI Spec — 结构有效性测试"""
import pytest

from loom_core.openapi_spec import generate_spec


@pytest.fixture
def spec():
    return generate_spec()


class TestOpenAPISpec:
    def test_version(self, spec):
        assert spec["openapi"] == "3.0.3"

    def test_info(self, spec):
        assert spec["info"]["title"] == "Doc-Pipeline Admin API"
        assert "version" in spec["info"]

    def test_has_servers(self, spec):
        assert len(spec["servers"]) >= 1

    def test_security_scheme(self, spec):
        assert "BearerAuth" in spec["components"]["securitySchemes"]
        assert spec["components"]["securitySchemes"]["BearerAuth"]["scheme"] == "bearer"

    def test_has_schemas(self, spec):
        schemas = spec["components"]["schemas"]
        assert "TaskSubmit" in schemas
        assert "TaskInfo" in schemas
        assert "CostSummary" in schemas
        assert "Error" in schemas

    def test_task_submit_schema(self, spec):
        schema = spec["components"]["schemas"]["TaskSubmit"]
        assert "query" in schema["properties"]
        assert "inputs" in schema["properties"]
        # #7 之后 query 不再是无条件必填：只带合法 inputs 的提交也得是合法的。
        # 这里验的是"spec 没有反过来禁止真实允许的载荷形状"。
        branches = [set(b.get("required", [])) for b in schema["anyOf"]]
        assert {"query"} in branches and {"inputs"} in branches, schema.get("anyOf")
        assert "required" not in schema, "顶层 required 会把 anyOf 的两条路压回一条"

    def test_paths_not_empty(self, spec):
        assert len(spec["paths"]) >= 15

    def test_key_endpoints_exist(self, spec):
        paths = spec["paths"]
        assert "/health" in paths
        assert "/tasks" in paths
        assert "/api/tasks" in paths
        assert "/api/cost" in paths
        assert "/api/cost/budget" in paths
        assert "/api/alerts" in paths
        assert "/api/logs" in paths
        assert "/api/openapi.json" not in paths  # 自身不需要
        assert "/stream" in paths

    def test_post_endpoints_have_request_body(self, spec):
        for path, methods in spec["paths"].items():
            for method, detail in methods.items():
                if method == "post" and path in ("/api/tasks", "/api/config", "/api/cost/budget", "/api/events/hooks"):
                    assert "requestBody" in detail, f"POST {path} 应有 requestBody"

    def test_task_id_path_param(self, spec):
        path = spec["paths"]["/tasks/{task_id}"]["get"]
        params = path["parameters"]
        assert any(p["name"] == "task_id" and p["in"] == "path" for p in params)

    def test_stream_has_query_params(self, spec):
        path = spec["paths"]["/stream"]["get"]
        params = path["parameters"]
        assert any(p["name"] == "query" and p["in"] == "query" for p in params)

    def test_responses_exist(self, spec):
        """每个操作都要声明 responses，且至少含一个 2xx 成功响应。

        2026-10-08：入站 webhook 的成功响应是语义正确的 202 Accepted（异步受理），
        原先"只认 200"的写法会把它误判成"没声明成功响应"——判据按意图收宽到 2xx，
        而不是去规范里补一个端点根本不会返回的 200。
        """
        for path, methods in spec["paths"].items():
            for method, detail in methods.items():
                assert "responses" in detail, f"{method.upper()} {path} 缺少 responses"
                keys = str(detail["responses"].keys())
                assert any(code in keys for code in ("200", "201", "202")),                     f"{method.upper()} {path} 没有声明 2xx 成功响应: {keys}"
