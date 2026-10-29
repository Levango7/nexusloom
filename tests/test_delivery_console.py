"""#8 第三件（控制台呈现）的接线判据：交付账本在控制台上要看得见、说得清。

A2 那一行验收要的是"交付物列表（sha256/字节/来源节点/保留期）+ 点开看来源运行"。
读面（`GET /api/deliveries*`）在续64 落地了，写侧摘要与逐运行锁指纹在续66 落地了，
这一文件钉的是**中间那段接线**——仓库里没有浏览器级证据（设计复核 §4 记为缺口），
所以钉的是 wiring 契约与字段契约，防止两类静默退化：

  1. 后端加了字段、前端忘了接（或接了一半）；
  2. 前端读了一个账本里根本没有的字段（改名/删字段时两端各自 grep 一遍都不会红）。

覆盖五组：卡片在 DOM 里、真调了两个端点、每轮刷新都渲染、点行开明细并能返回、
以及"摘要缺失的三种真相不许合并成一个'未知'"。
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.test_delivery_tracing import _demo_locked, _ledger

PROJECT = Path(__file__).resolve().parent.parent
DASH = PROJECT / "loom_core" / "dashboard"


@pytest.fixture()
def state(tmp_path, monkeypatch):
    """与 test_delivery_tracing 同一个隔离口径：账本按用例独立。"""
    d = tmp_path / "gc-state"
    d.mkdir()
    monkeypatch.setenv("LOOM_STATE_DIR", str(d))
    return d


def _js() -> str:
    return (DASH / "app.js").read_text(encoding="utf-8")


def _html() -> str:
    return (DASH / "index.html").read_text(encoding="utf-8")


def _css() -> str:
    return (DASH / "style.css").read_text(encoding="utf-8")


def _console_region() -> str:
    """app.js 里账本面板那一段（格式化助手 + 三个渲染函数 + 产物行辅助函数）。"""
    js = _js()
    # 从 fmtBytes 起：三种"取不到"的文案就在那几个格式化函数里。区域要是从
    # renderDeliveries 起，那几条判据量的就是自己射程之外的东西。
    start = js.index("function fmtBytes (n) {")
    end = js.index("function renderError ()")
    return js[start:end]


class TestConsoleMarkup:
    def test_card_and_badge_exist(self):
        html = _html()
        assert '<div class="card wide" id="panel-deliveries">' in html
        assert 'id="badge-deliveries"' in html
        assert '<use href="#i-download"/>' in html

    def test_card_sits_before_the_run_detail_card(self):
        """账本卡排在运行详情卡之前：详情卡默认 hidden，排在它后面等于压进折叠区。"""
        html = _html()
        assert html.index('id="panel-deliveries"') < html.index('id="panel-run"')

    def test_body_follows_the_shared_panel_selector(self):
        """卡体走各面板同一套选择器（`#panel-x .card-body`），cacheDom 才拿得到。"""
        html = _html()
        seg = html[html.index('id="panel-deliveries"'):]
        assert '<div class="card-body">' in seg[:400]


class TestAppWiring:
    def test_fetches_the_ledger_endpoints(self):
        js = _js()
        assert "fetchJson(API_BASE + '/api/deliveries?limit=' + DELIVERIES_LIMIT)" in js
        assert ("fetchJson(API_BASE + '/api/deliveries/' + "
                "encodeURIComponent(runId))") in js

    def test_panel_is_rendered_each_refresh(self):
        js = _js()
        assert "renderDeliveries(state.deliveries)" in js
        assert "state.deliveries = data.deliveries;" in js
        # 结果集的第 7 路必须真映射到账本：漏一行 settle 就是把面板钉死在空态
        assert "deliveries: settle(results[6])" in js

    def test_row_click_opens_detail_and_back_restores_list(self):
        region = _console_region()
        assert "openDelivery(String(d.run_id || ''))" in region
        assert "renderDeliveryDetail((data && data.delivery) || {})" in region
        assert "back.onclick = () => renderDeliveries(state.deliveries)" in region

    def test_missing_digest_has_three_separate_truths(self):
        """摘要缺失有三种真相，合并成一个"未知"就是把账本的诚实性抹平。"""
        region = _console_region()
        assert "未记录" in region        # 这一行写在摘要面之前（bytes/sha 为 null）
        assert "读不到文件" in region     # bytes=-1：去查过，文件不在了
        assert "不过期" in region        # 保留期没启用，或老行没冻结到期时刻

    def test_lock_verdict_is_three_valued(self):
        region = _console_region()
        assert "lock.matches === null ? '没有锁可比'" in region
        assert "'有漂移'" in region
        assert "'一致'" in region
        assert "未核对" in region        # verify_lock=False 的 plan：没算过

    def test_artifact_rows_reuse_the_inspector_row_style(self):
        """产物行沿用运行详情检查器的三列版式：同一个"产物"两处必须长得一样。"""
        region = _console_region()
        for token in ("artifactDigestBlock", "insp-art-row", "insp-art-key",
                      "insp-art-type", "insp-art-val"):
            assert token in region, f"控制台缺 {token}"

    def test_no_produced_node_is_not_reported_as_empty(self):
        """有产物键但没摘要（老行）与"这个节点没产出物"是两件事。"""
        region = _console_region()
        assert "产物键有但摘要没记" in region
        assert "这个节点没有产出物" in region

    def test_no_emoji_in_the_deliveries_panel(self):
        """用户审美铁律：功能位置不用 emoji 指示符。"""
        for ch in ("\u2705", "\u274c", "\u26a0\ufe0f", "\U0001f680",
                   "\U0001f4e6", "\U0001f512"):
            assert ch not in _console_region()
            assert ch not in _html()


class TestStyleContract:
    def test_deliveries_styles_exist(self):
        css = _css()
        for token in (".deliv-sha", ".deliv-detail", ".deliv-bar"):
            assert token in css, f"缺少 {token} 样式"

    def test_new_classes_are_used_by_the_js(self):
        """样式不是写了就算——每个 .deliv-* 选择器都要有对应的 className 使用点。"""
        region = _console_region()
        for cls in re.findall(r"^\.(deliv-[a-z-]+)", _css(), re.M):
            assert cls in region, f"样式 .{cls} 没有使用者（写了个死样式）"


class TestConsoleApiContract:
    """前端读的字段必须在**真记录**里存在。

    建一条真流水线跑一遍，拿账本真实返回的载荷作为参照系，而不是在测试里抄一份
    字段表——抄的那份会跟着代码一起漂。
    """

    #: JS 里的局部变量/方法名，不是账本字段
    NOT_FIELDS = {"run", "length", "map", "sort", "slice", "join", "push",
                  "toFixed", "toLocaleString", "toLocaleTimeString", "now"}

    def _fields_read(self, region: str) -> set[str]:
        found: set[str] = set()
        for pattern in (r"\bd\.([a-z_][a-z_0-9]*)",
                        r"\bdigest\.([a-z_][a-z_0-9]*)",
                        r"\barts\[k\]\.([a-z_][a-z_0-9]*)",
                        r"\block\.([a-z_][a-z_0-9]*)",
                        r"\bn\.([a-z_][a-z_0-9]*)"):
            found |= {m.group(1) for m in re.finditer(pattern, region)}
        return found - self.NOT_FIELDS

    def test_console_reads_only_fields_the_ledger_writes(self, orch, tmp_path,
                                                        state):
        # 带锁那条才有 lock 的全形状：未核对的 plan 里 lock = {}，拿空集合当参照系
        # 等于开一扇对着空气的门
        plan, _lock, _name = _demo_locked(tmp_path, "console")
        (tmp_path / "in.md").write_text("# 输入\n控制台契约。", encoding="utf-8")
        task = orch.run_plan(plan, input_file=str(tmp_path / "in.md"),
                             task_id="trace-console", wait=True)
        assert task.status.value == "done", task.error

        rec = _ledger(state).get("trace-console")
        assert rec is not None
        assert rec["lock"], "前提：这条参照记录必须带锁指纹"
        # `GET /api/deliveries/<run_id>` 的载荷 = 账本行 + admin 侧的投影字段
        readable = set(rec) | {"node_count", "artifact_count", "sha256", "bytes"}
        for node in rec["nodes"]:
            readable |= set(node)
            for digest in (node.get("artifacts") or {}).values():
                readable |= set(digest)
        readable |= set(rec["output_digest"]) | set(rec["lock"])
        # 逐节点 config_hash 挂在 lock.nodes 的条目上，不在 plan 的节点对象里
        for entry in (rec["lock"].get("nodes") or {}).values():
            readable |= set(entry)

        used = self._fields_read(_console_region())
        assert used, "判据没抓到任何字段引用，护栏需要改写法"
        missing = sorted(used - readable)
        assert missing == [], f"控制台读了账本里不存在的字段：{missing}"

    def test_every_tracing_field_is_presented(self):
        """写侧新加的字段不许只活在 JSON 里：四样都要能在台面上看到。"""
        region = _console_region()
        for token in ("d.sha256", "d.retention_until", "d.output_digest",
                      "d.lock", "n.artifacts", "lock.topology_hash",
                      "n.config_hash", "lock.matches", "lock.lock_file"):
            assert token in region, f"控制台没呈现追溯字段 {token}"
