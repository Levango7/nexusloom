"""Run dashboard browser regressions with Playwright and an installed browser."""
from __future__ import annotations

import argparse
import functools
import json
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        pass


def check(browser, base: str) -> None:
    from playwright.sync_api import expect

    page = browser.new_page(viewport={"width": 1280, "height": 900})
    errors: list[str] = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    responses = {
        "/health": {"status": "ok", "metrics": {}},
        "/api/dashboard": {"tasks": [{"id": "sample", "pipeline": "demo", "status": "paused"}], "agents": []},
        "/api/cost": {"total_cost": 0.125, "budget": 5, "budget_remaining": 4.875},
        "/api/quality/feedback": {"total_records": 1, "total_tasks": 1, "avg_score": 83.5},
        "/api/logs": {"logs": [{"level": "info", "message": "browser-log-sample"}]},
        "/api/approvals": {"approvals": []},
        "/api/pipeline": {"pipeline_files": ["demo.yaml"]},
        "/api/deliveries": {"deliveries": [{
            "run_id": "run-browser-1", "pipeline": "demo", "status": "done",
            "finished_at": 1700000000, "output_sha256": "a" * 64,
            "node_count": 2, "artifact_count": 3,
        }]},
        "/api/deliveries/run-browser-1": {"delivery": {
            "run_id": "run-browser-1", "pipeline": "demo", "status": "done",
            "output_path": "/tmp/out/runner-output.md", "output_sha256": "a" * 64,
            "node_count": 2, "artifact_count": 3, "duration_sec": 12,
            "topology_hash": "b" * 64, "config_hashes": {"alpha": "c" * 12},
            "nodes": [{
                "node": "alpha", "status": "done", "artifact_keys": ["doc"],
                "artifacts": {"doc": {"sha256": "d" * 64, "bytes": 2048, "kind": "str"}},
            }],
        }},
        "/tasks/sample": {
            "id": "sample", "pipeline": "demo", "status": "paused",
            "dag": {"nodes": [], "layers": []},
            "awaiting_approval": {"nodes": ["gate"], "messages": {}},
        },
        # Settings 面（评审 §4.2，只读 v1）的四源。**按后端真形状 mock**：
        # `/api/webhooks` 只给 name/pipeline/enabled/secret_env/output，没有 url——
        # 用作者想象的形状 mock，正好会放过"渲染一个不存在的字段"这类缺陷。
        "/api/config": {"llm": {"model": "demo-model"}, "security": {"api_key": "sk-very-secret"},
                        "runner": {"pool_size": 4}},
        "/api/webhooks": {"webhooks": [{"name": "demo-hook", "pipeline": "demo.yaml",
                                        "enabled": True, "secret_env": "HOOK_SECRET",
                                        "output": "out"}]},
        "/api/versions/stats": {"status": "ok", "tracked_files": 7, "total_versions": 21,
                               "total_size_bytes": 4096, "max_versions_per_file": 5},
        "/api/config/audit": {"audit_file": "audit/config.jsonl", "count": 2, "records": []},
    }
    statuses: dict[str, int] = {}
    posts: list[str] = []

    def respond(route):
        from urllib.parse import urlsplit

        path = urlsplit(route.request.url).path
        if path not in responses and not path.startswith("/tasks/"):
            route.continue_()
            return
        if route.request.method == "POST":
            posts.append(path)
        route.fulfill(status=statuses.get(path, 200), content_type="application/json",
                      body=json.dumps(responses.get(path, {})))

    page.route("**/*", respond)
    try:
        page.goto(base + "/index.html")
        expect(page.locator("#badge-cost")).to_have_text("$0.1250")
        expect(page.locator("#badge-quality")).to_have_text("avg 83.5")
        expect(page.locator("#panel-logs")).to_contain_text("browser-log-sample")
        print("PASS: cost, quality and logs reach the rendered panels")

        # 账本面板此前只有静态源断言，M10b 变异（入参被覆写成空态）0 failed。
        # 这里从真实渲染面判：列表行来自 /api/deliveries，明细来自 /api/deliveries/<run_id>。
        expect(page.locator("#badge-deliveries")).to_have_text("1")
        expect(page.locator("#panel-deliveries")).to_contain_text("run-browser-1")
        expect(page.locator("#panel-deliveries")).to_contain_text("2 / 3")
        page.locator("#panel-deliveries tr.row-clickable").click()
        expect(page.locator("#panel-deliveries")).to_contain_text("runner-output.md")
        expect(page.locator("#panel-deliveries")).to_contain_text("节点 alpha 的产物")
        expect(page.locator("#panel-deliveries")).to_contain_text("2.0 KB")
        page.locator("#panel-deliveries .run-back").click()
        expect(page.locator("#panel-deliveries")).to_contain_text("run-browser-1")
        print("PASS: delivery ledger list and detail come from the API payload")

        statuses.update({"/api/quality/feedback": 403, "/api/logs": 403})
        page.reload()
        for panel in ("quality", "logs"):
            expect(page.locator(f"#panel-{panel}")).to_contain_text("需要 admin 权限")
        expect(page.locator("#last-updated")).not_to_contain_text("部分数据不可用")
        print("PASS: denied access is distinct from empty data and service errors")

        statuses.clear()
        statuses.update({path: 503 for path in ("/api/cost", "/api/quality/feedback", "/api/logs", "/api/approvals")})
        page.reload()
        for panel in ("cost", "quality", "logs", "approvals"):
            expect(page.locator(f"#panel-{panel}")).to_contain_text("读取失败：HTTP 503")
            expect(page.locator(f"#badge-{panel}")).to_have_text("--")
        print("PASS: failed requests cannot masquerade as zero cost or an empty inbox")

        statuses.clear()
        responses["/api/cost"] = {"total_cost": 0}
        responses["/api/quality/feedback"] = {"total_records": 0, "avg_score": 0}
        responses["/api/logs"] = {"logs": []}
        page.reload()
        expect(page.locator("#badge-cost")).to_have_text("$0.0000")
        expect(page.locator("#badge-quality")).to_have_text("avg 0")
        expect(page.locator("#panel-logs")).to_contain_text("暂无日志")
        expect(page.locator("#panel-approvals")).to_contain_text("没有等待审批的运行")
        print("PASS: successful empty responses still render as empty")

        page.locator('tr[data-task-id="sample"]').click()
        reject = page.locator(".btn-reject")
        expect(reject).to_be_enabled()
        dialogs: list[str] = []

        def dismiss(dialog):
            dialogs.append(dialog.type)
            dialog.dismiss()

        page.on("dialog", dismiss)
        reject.click()
        assert dialogs == ["prompt"], dialogs
        assert not posts, posts
        page.remove_listener("dialog", dismiss)
        print("PASS: cancelling rejection sends no decision and no confirmation")

        def accept(dialog):
            dialogs.append(dialog.type)
            dialog.accept("") if dialog.type == "prompt" else dialog.accept()

        page.on("dialog", accept)
        reject.click()
        expect(page.locator(".approval-bar")).to_be_visible()
        assert posts == ["/tasks/sample/reject"], posts
        page.remove_listener("dialog", accept)
        print("PASS: explicit confirmation still submits rejection")

        statuses["/api/pipeline"] = 401
        page.reload()
        expect(page.locator("#token-input")).to_be_visible()
        statuses.clear()
        responses["/api/pipeline"] = {"pipeline_files": ["after-login.yaml"]}
        page.locator("#token-input").fill("test-only-token")
        page.locator("#token-save").click()
        expect(page.locator("#nt-pipeline option")).to_have_text(["after-login"])
        print("PASS: saving credentials reloads the workflow catalog")

        # Settings 面（评审 §4.2，只读 v1）：四块真渲染 + 敏感键掩码 + 只读边界。
        # 这一步是 PR #90 声称"真 Chrome 11 项全过"的**可复现版本**——那次是一次性脚本，
        # 没进仓，别人没法重跑；而且它按想象的字段 mock，漏掉了 `/api/webhooks` 没有 url。
        panel = page.locator("#panel-settings")
        expect(panel).to_contain_text("运行配置")
        expect(panel).to_contain_text("llm.model")
        expect(panel).to_contain_text("***（已掩码）")
        expect(panel).not_to_contain_text("sk-very-secret")
        expect(panel).to_contain_text("demo-hook")
        expect(panel).to_contain_text("demo.yaml")
        expect(panel).to_contain_text("受跟踪文件")
        expect(panel).to_contain_text("配置审计记录")
        expect(panel).to_contain_text("不触发回滚")
        print("PASS: settings face renders four sources, masks secrets and states read-only")
        assert not errors, errors
    finally:
        page.close()


def main() -> None:
    from playwright.sync_api import sync_playwright

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--channel", default=None, help="Installed browser channel, e.g. chrome or msedge")
    parser.add_argument("--dashboard-dir", type=Path, default=ROOT / "loom_core" / "dashboard")
    args = parser.parse_args()
    handler = functools.partial(QuietHandler, directory=str(args.dashboard_dir))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(channel=args.channel, headless=True)
            try:
                check(browser, f"http://127.0.0.1:{server.server_port}")
            finally:
                browser.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


if __name__ == "__main__":
    main()
