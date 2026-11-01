"""dashboard S2 运行详情屏的结构护栏（docs/product-spec.md §4.2）。

仓库内**没有浏览器级证据**（2026-10-20 设计复核记为缺口；此前"无头 Chrome
走查留证"的说法已撤回）；这里钉住**wiring 契约**，防止"后端加了字段、前端忘了接"
或"接了一半"这类静默退化：

- `index.html` 有 `#panel-run`（默认 hidden）与返回按钮 + S2 图标；
- `app.js` 的真接线：任务行 click → `openRun`；`openRun` 隐藏其他卡并显示
  运行详情；`closeRun` 恢复；审批按钮打 `/tasks/<id>/approve|reject`；
  节点四态与 when 条件跳过的虚线语义有对应渲染分支；
- `style.css` 有 `[hidden]` 显式压过 `.card{display:flex}` 的规则
  （作者样式优先于 UA 的 `[hidden]{display:none}`——不写这条"详情态隐藏
  其他卡片"根本不生效，无头 Chrome 实测踩过）与四态/虚线样式类。
"""
from __future__ import annotations

from pathlib import Path

DASH = Path(__file__).resolve().parent.parent / "loom_core" / "dashboard"


def _read(name: str) -> str:
    return (DASH / name).read_text(encoding="utf-8")


class TestRunPanelMarkup:
    def setup_method(self):
        self.html = _read("index.html")

    def test_panel_exists_and_hidden_by_default(self):
        assert 'id="panel-run"' in self.html
        # 同一行里 hidden 与 id 相邻（默认不占位，点任务才打开）
        assert 'id="panel-run"' in self.html and "hidden" in self.html

    def test_back_button_and_meta_slot(self):
        assert 'id="run-back"' in self.html
        assert 'id="run-meta"' in self.html
        assert 'id="run-body"' in self.html

    def test_s2_icons_present(self):
        assert 'id="i-run"' in self.html
        assert 'id="i-check"' in self.html


class TestAppWiring:
    def setup_method(self):
        self.js = _read("app.js")

    def test_task_row_opens_run_detail(self):
        assert "openRun(String(t.id))" in self.js

    def test_open_hides_other_cards_and_shows_run(self):
        assert "c.id !== 'panel-run'" in self.js
        assert "c.hidden = true" in self.js
        assert "runPanel.hidden = false" in self.js

    def test_close_restores_all_cards(self):
        assert "runPanel.hidden = true" in self.js
        assert "c.hidden = false" in self.js

    def test_fetches_task_detail_endpoint(self):
        assert "API_BASE + '/tasks/'" in self.js

    def test_approval_buttons_hit_decision_endpoints(self):
        assert "/approve'" in self.js or 'approve' in self.js
        assert "'/tasks/' + encodeURIComponent(taskId) + '/' + action" in self.js

    def test_four_states_and_condition_skip_rendered(self):
        """四态经 nodeVisualState 归一后拼成 dag-<state> 类名（不是字面罗列）。"""
        assert "'dag-node dag-' + state" in self.js or '"dag-node dag-" + state' in self.js
        for state in ("'success'", "'failed'", "'skipped'", "'retried'"):
            assert state in self.js, f"nodeVisualState 缺状态 {state}"
        for label in ("'成功'", "'失败'", "'跳过'", "'重试后成功'"):
            assert label in self.js, f"NODE_STATE_LABEL 缺中文标签 {label}"
        assert "dag-skip-condition" in self.js
        assert "skip_reason === 'condition'" in self.js

    def test_node_opens_inspector(self):
        """S3：点 DAG 节点 → 右侧检查器抽屉（spec §4.2 统一骨架）。"""
        assert "openInspector(taskId, n.name)" in self.js
        assert "/nodes/" in self.js
        assert "node-inspector" in self.js

    def test_inspector_renders_detail_kinds(self):
        for token in ("artifact", "idempotency", "circuit_breaker", "rate_limit", "quality"):
            assert token in self.js, f"检查器缺 {token} 区块"
        # “取不到就不编”：null 时如实显示未建立，而不是假读数
        assert "未建立" in self.js

    def test_checkpoint_marker_on_dag(self):
        """S2 缺的检查点续跑标记：dag.checkpoint 驱动角标。"""
        assert "renderCheckpointMarkers" in self.js
        assert "dag-checkpoint-resumed" in self.js
        assert "断点续跑" in self.js

    def test_no_emoji_in_new_markup(self):
        """用户审美铁律：功能位置不用 emoji 指示符。"""
        for ch in ("✅", "❌", "⚠️", "🚀"):
            assert ch not in self.js


class TestStyleContract:
    def setup_method(self):
        self.css = _read("style.css")

    def test_hidden_overrides_card_display_flex(self):
        assert ".card[hidden]" in self.css and "display: none" in self.css

    def test_dag_state_classes_defined(self):
        for token in (".dag-success", ".dag-failed", ".dag-skipped",
                      ".dag-retried", ".dag-running", ".dag-pending"):
            assert token in self.css, f"缺少 {token} 样式"

    def test_condition_skip_is_dashed(self):
        assert ".dag-skip-condition" in self.css and "dashed" in self.css

    def test_inspector_drawer_styles(self):
        for token in (".node-inspector", ".insp-art-row", ".dag-checkpoint-badge",
                      ".dag-checkpoint-resumed", ".dag-node-clickable"):
            assert token in self.css, f"缺少 {token} 样式"


class TestConsoleShipping:
    """控制台必须随包发布（2026-10-20 复核：3.12.0 wheel 里 0 个 dashboard 资源）。"""

    def test_dashboard_lives_inside_the_package(self):
        from loom_core import paths

        d = paths.dashboard_dir()
        for name in ("index.html", "app.js", "style.css"):
            assert (d / name).is_file(), f"控制台资源缺失：{name}"

    def test_package_data_declares_the_dashboard(self):
        import tomllib

        root = Path(__file__).resolve().parent.parent
        data = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
        assert "dashboard/*" in data["tool"]["setuptools"]["package-data"]["loom_core"]

    def test_root_serves_the_console_shell(self):
        """`GET /` 直接给控制台入口页（免鉴权，只发静态壳）。"""
        import urllib.request
        from unittest.mock import MagicMock

        from loom_core import paths
        from loom_core.admin_api import AdminAPI, AdminHandler

        api = AdminAPI(host="127.0.0.1", port=0, api_key="",
                       serve_static=True,
                       dashboard_dir=str(paths.dashboard_dir()))
        try:
            assert api.start(MagicMock()) is True
            port = api._server.server_address[1]
            with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/", timeout=5) as r:
                body = r.read().decode("utf-8")
                assert r.status == 200
                assert r.headers.get_content_type() == "text/html"
            assert "Loom" in body
        finally:
            api.stop()
            # start() 写的是类属性，清掉避免污染后续用例
            AdminHandler.dashboard_dir = None


class TestDeliveriesDetailSurvivesPolling:
    """账本明细跨轮询存活（2026-10-31 实测缺陷回归）。

    缺陷现场：`openDelivery` 点开明细后，`render()` 的 5 秒轮询无条件重画
    交付面板 → 明细 ≤5 秒被列表覆盖（点开即消失）。修法：明细开着时置
    `deliveriesDetailOpen`，`renderDeliveries` 遇旗只更新数据不回画；
    「返回账本列表」显式清旗。本判据静态钉住三处接线（零浏览器）。
    """

    def setup_method(self):
        self.js = _read("app.js")

    def test_flag_declared_and_guarded(self):
        assert "deliveriesDetailOpen" in self.js
        assert "if (deliveriesDetailOpen) return;" in self.js, \
            "renderDeliveries 缺轮询守卫——明细会被 5 秒轮询覆盖"

    def test_open_sets_flag_before_rendering_detail(self):
        open_body = self.js.split("async function openDelivery", 1)[1]
        set_at = open_body.index("deliveriesDetailOpen = true;")
        render_at = open_body.index("renderDeliveryDetail(")
        assert set_at < render_at, "必须先置旗再渲染明细"

    def test_back_button_clears_flag(self):
        detail_body = self.js.split("function renderDeliveryDetail", 1)[1]
        assert "deliveriesDetailOpen = false;" in detail_body, \
            "返回账本列表必须清旗，否则列表再也回不来"


class TestRunsDeepLinkAndFilter:
    """Runs 面 P0（评审 §4.2）：深链 `#/runs/{id}` 与状态过滤的接线护栏（零浏览器）。

    评审原文："运行列表（状态/工作流/principal/耗时/成本/质量分）｜过滤、新建运行、
    深链 `/#/runs/{id}`｜P0"。深链的价值是"把某一次运行发给同事/从书签直达"——
    因此 hash→视图只能有一个真源（`routeFromHash`），且浏览器前进/后退必须同步。
    """

    def setup_method(self):
        self.js = _read("app.js")
        self.html = _read("index.html")

    def test_hash_route_helpers_exist(self):
        assert "RUN_HASH_PREFIX = '#/runs/'" in self.js
        for token in ("runIdFromHash", "setRunHash", "clearRunHash", "routeFromHash"):
            assert token in self.js, f"缺深链接线 {token}"
        assert "addEventListener('hashchange', routeFromHash)" in self.js

    def test_open_sets_hash_and_close_clears_it(self):
        open_body = self.js.split("async function openRun", 1)[1]
        assert "setRunHash(taskId);" in open_body, "点开运行必须写深链"
        close_body = self.js.split("function closeRun", 1)[1]
        assert "clearRunHash();" in close_body, "关闭运行必须清深链（否则后退链错乱）"
        # hashchange 驱动的路径不得反向再写历史（防：每 3s 刷新写一条历史）
        assert "openRun(id, { fromHash: true })" in self.js
        assert "closeRun({ fromHash: true })" in self.js

    def test_filter_element_and_render_wiring(self):
        assert 'id="task-filter"' in self.html
        assert "els.taskFilter" in self.js
        assert "state.taskFilter" in self.js
        # 动词级：过滤必须真按状态分类走，而不是拼个假计数
        assert "all.filter(t => statusClass(t.status) === filter)" in self.js
        assert "该状态下没有任务" in self.js, "过滤后空态要说清是'筛选空'，不是'没任务'"
