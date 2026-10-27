"""交付形态判据：结构化 JSON 落库 / HTML 单页 / HTML 站点 —— spec §2.1 第 4 条。

第 4 条原文要求五种交付形态：md / docx / pdf / **结构化 JSON 落库** / **HTML 站点**。
此前后两种不存在：结构化结果只有 checkpoint 里的诊断报告（非交付物、查不了），
HTML 只在 run.py --export 里有一把单页转换（进不了流水线声明）。

本文件钉四件事：
  1. 交付账本（deliveries.db）：每轮运行（含失败）都有一行可查询的结构化记录；
  2. `pipeline.deliver.json`：声明即写结构化 JSON 文件（与账本同形状）；
  3. `pipeline.deliver.site`：声明即构建静态站点（index 导航 + 产物页）；
  4. renderer `html` 格式：纯标准库后端，流水线里声明 formats: ["html"] 即用。

E2E 走真件：真 Scheduler → DAGExecutor → Agent → 落盘，全离线、无外部 IO 罐头。
"""
import json
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def orch():
    """真编排器：注册内置 Agent（含 renderer html 后端）。"""
    from loom_core.pipeline import PipelineOrchestrator

    o = PipelineOrchestrator(
        agents_dir=str(PROJECT / "agents"),
        checkpoint_dir=str(PROJECT / "checkpoints"),
    )
    o.register_agents()
    return o


def _plan(yaml_text: str, tmp_path: Path, name: str):
    """写临时流水线 YAML 并解析（临时流水线无 lock，verify_lock=False）。"""
    from loom_core.scheduler import Scheduler

    out = tmp_path / name
    out.mkdir(parents=True, exist_ok=True)
    path = out / (name + ".yaml")
    path.write_text(yaml_text, encoding="utf-8")
    sched = Scheduler(agents_dir=str(PROJECT / "agents"))
    return sched.parse_file(str(path), verify_lock=False), out


DEMO_YAML = """_name: deliver-demo
_version: "1.0"
description: 交付形态判据流水线（全离线）

pipeline:
  output: {out}/result.md
  deliver:
    json: {out}/result.json
    site: {out}/site

defaults:
  parallelism:
    mode: single
    max_workers: 1

agents:
  - name: transform
    version: "1.0"
    timeout: 60
    dependencies: []
    config:
      items: ""
      template: |
        # 交付判据

        正文标记 DELIVERY-OK。

  - name: safe_writer
    version: "2.0"
    timeout: 60
    dependencies: ["transform"]

  - name: renderer
    version: "1.0"
    timeout: 60
    dependencies: ["safe_writer"]
    config:
      formats: ["html"]
      output_dir: {out}

topology:
  levels:
    - [transform]
    - [safe_writer]
    - [renderer]
"""


class TestDeliveryLedger:
    """账本三件事：写入→读回一致、缺记录返回 None、重复写覆盖（幂等收尾）。"""

    def test_record_get_roundtrip(self, tmp_path, monkeypatch):
        from loom_core.deliveries import DeliveryLedger

        monkeypatch.setenv("LOOM_STATE_DIR", str(tmp_path / "state"))
        led = DeliveryLedger()
        rec = {"run_id": "r-1", "pipeline": "p", "status": "done",
               "finished_at": 10.0, "duration_sec": 1.5,
               "output_path": "o.md", "nodes": [{"node": "n", "status": "success"}]}
        led.record(rec)
        got = led.get("r-1")
        assert got == rec, "读回的记录与写入不一致"
        assert led.get("no-such-run") is None
        assert [r["run_id"] for r in led.list()] == ["r-1"]

    def test_record_is_idempotent_replace(self, tmp_path, monkeypatch):
        from loom_core.deliveries import DeliveryLedger

        monkeypatch.setenv("LOOM_STATE_DIR", str(tmp_path / "state"))
        led = DeliveryLedger()
        led.record({"run_id": "r-1", "pipeline": "p", "status": "running"})
        led.record({"run_id": "r-1", "pipeline": "p", "status": "done"})
        assert led.get("r-1")["status"] == "done"
        assert len(led.list()) == 1, "重复收尾不该多行"

    def test_record_requires_run_id(self, tmp_path, monkeypatch):
        from loom_core.deliveries import DeliveryLedger

        monkeypatch.setenv("LOOM_STATE_DIR", str(tmp_path / "state"))
        with pytest.raises(ValueError):
            DeliveryLedger().record({"pipeline": "p"})


class TestHtmlExport:
    """引擎交付原语：单页转换 + 站点构建（纯标准库、离线）。"""

    def test_markdown_to_html_string_basics(self):
        from loom_core.html_export import markdown_to_html_string

        page = markdown_to_html_string(
            "# 标题\n\n正文\n\n```sql\nSELECT 1;\n```\n\n| A | B |\n|---|---|\n| 1 | 2 |",
            title="测试页")
        assert "<title>测试页</title>" in page
        assert "<h1>标题</h1>" in page
        assert "<pre>" in page and "SELECT 1;" in page
        assert "<table>" in page and "<td>1</td>" in page

    def test_build_site_index_links_and_backlinks(self, tmp_path):
        from loom_core.html_export import build_site

        info = build_site(
            [{"title": "报告 甲", "markdown": "# 甲\n内容甲"},
             {"title": "Report B", "markdown": "B body"}],
            tmp_path / "site", site_title="交付站点")
        assert info["page_count"] == 2
        index = (tmp_path / "site" / "index.html").read_text(encoding="utf-8")
        assert "交付站点" in index
        for item in info["pages"]:
            assert 'href="pages/' + item["slug"] + '.html"' in index, "导航链接缺失"
            page = Path(item["path"]).read_text(encoding="utf-8")
            assert "../index.html" in page, "页面缺返回导航"
        # slug 必须是 ASCII 文件名（中文标题不落文件名）
        assert all(s["slug"].isascii() for s in info["pages"])


class TestRendererHtml:
    """renderer 的 html 后端：纯标准库、永远可用。"""

    def test_render_html_writes_page(self, tmp_path):
        from loom_doc import renderer

        target = tmp_path / "out" / "x.html"
        res = renderer.render("# T\n\n正文 here", target, fmt="html", title="页面")
        assert res["status"] == "ok" and res["format"] == "html"
        content = target.read_text(encoding="utf-8")
        assert "<h1>T</h1>" in content and "正文 here" in content

    def test_agent_declares_html_format(self):
        """流水线声明 formats: ["html"] 时 agent 真用 html 后端（不被静默过滤）。"""
        from agents.renderer_agent import RendererAgent
        from loom_core.registry import AgentMeta

        class _Bus:
            def publish(self, *a):
                pass

        agent = RendererAgent("renderer", AgentMeta(name="renderer"),
                              {"formats": ["html"]}, _Bus(), object())
        assert agent._formats == ["html"]


class TestDeliverEndToEnd:
    """真 E2E：声明 deliver.json / deliver.site / formats:["html"] 的一次真跑。

    断言三种交付物同时真实存在且互相一致：
      - renderer 的 html 页面（含正文标记）；
      - deliver.json 的结构化记录（schema + 节点级状态）；
      - deliveries.db 的账本行（与 JSON 同形状——同一记录的两个出口）；
      - 静态站点（index 导航 + 产物页 + 返回链接）。
    """

    def test_full_delivery_pipeline(self, orch, tmp_path):
        plan, out = _plan(DEMO_YAML.format(out=str(tmp_path / "deliver-demo")),
                          tmp_path, "deliver-demo")
        (tmp_path / "in.md").write_text("# 输入\n交付判据输入。", encoding="utf-8")
        task_id = "deliver-e2e-1"
        task = orch.run_plan(plan, input_file=str(tmp_path / "in.md"),
                             task_id=task_id, wait=True)
        assert task.status.value == "done", task.error

        # 1) renderer html
        html_path = out / "result.html"
        assert html_path.exists(), "renderer 没产出 html"
        page = html_path.read_text(encoding="utf-8")
        assert "DELIVERY-OK" in page and "<h1>" in page

        # 2) deliver.json：结构化记录（schema 判据）
        record = json.loads((out / "result.json").read_text(encoding="utf-8"))
        assert record["run_id"] == task_id
        assert record["pipeline"] == "deliver-demo"
        assert record["status"] == "done"
        assert record["output_path"].endswith("result.md")
        node_map = {n["node"]: n for n in record["nodes"]}
        assert node_map["safe_writer"]["status"] == "success"
        assert node_map["renderer"]["status"] == "success"
        assert node_map["transform"]["agent"] == "transform"

        # 2b) A5: 指纹字段——topology_hash 与 config_hashes
        #     复用 Scheduler 静态方法，保证与 lockfile 生成/校验同一实现。
        #     两处各算一遍必然漂，锁就失去了意义。
        assert "topology_hash" in record, "交付记录缺 topology_hash"
        assert isinstance(record["topology_hash"], str)
        assert len(record["topology_hash"]) == 12, "topology_hash 应为 12 位 hex"
        int(record["topology_hash"], 16)  # 必须可解析为 hex

        assert "config_hashes" in record, "交付记录缺 config_hashes"
        assert isinstance(record["config_hashes"], dict)
        # 每个 agent 都该有指纹（transform/safe_writer/renderer 三个节点）
        assert len(record["config_hashes"]) >= 3, \
            f"config_hashes 应覆盖所有节点，实际 {list(record['config_hashes'])}"
        for name, h in record["config_hashes"].items():
            assert isinstance(h, str) and len(h) == 12, \
                f"{name} 的 config_hash 应为 12 位 hex，实际 {h!r}"
            int(h, 16)

        # 2c) A2: output_sha256——产物文件的内容哈希。
        #     复用与锁指纹相同的实现路径（同一方法内计算），保证一致性。
        assert "output_sha256" in record, "交付记录缺 output_sha256"
        assert isinstance(record["output_sha256"], str)
        # result.md 已产出（pipeline.output 指向它），哈希应为 64 位 hex
        assert len(record["output_sha256"]) == 64, "output_sha256 应为 64 位 hex"
        int(record["output_sha256"], 16)
        # 哈希应与实际文件内容一致
        import hashlib
        expected = hashlib.sha256(
            (out / "result.md").read_bytes()).hexdigest()
        assert record["output_sha256"] == expected, \
            "output_sha256 应等于实际产物文件内容的 sha256"

        # 3) 账本与 JSON 同形状（同一记录的两个出口）
        from loom_core.deliveries import DeliveryLedger
        row = DeliveryLedger().get(task_id)
        assert row is not None, "账本没有这轮运行的记录"
        assert row["nodes"] == record["nodes"]
        # 指纹字段也要一致（同一记录的两个出口，字段一个不能少）
        assert row["topology_hash"] == record["topology_hash"]
        assert row["config_hashes"] == record["config_hashes"]
        assert row["output_sha256"] == record["output_sha256"]

        # 4) 站点：index 导航 + 产物页 + 返回链接
        index = (out / "site" / "index.html").read_text(encoding="utf-8")
        assert "deliver-demo" in index
        page_file = out / "site" / "pages" / (task_id + ".html")
        assert page_file.exists(), "站点产物页缺失"
        site_page = page_file.read_text(encoding="utf-8")
        assert "DELIVERY-OK" in site_page
        assert "../index.html" in site_page

    def test_failed_run_is_ledgered_but_writes_no_files(self, orch, tmp_path):
        """失败运行：账本必有一行（status=failed），但不写 deliver.json/site。"""
        fail_yaml = DEMO_YAML.format(out=str(tmp_path / "fout")).replace(
            '      items: ""',
            '      items: "artifacts.no_such_thing"')
        plan, out = _plan(fail_yaml, tmp_path, "deliver-demo")
        (tmp_path / "in.md").write_text("# 输入\n失败路径判据。", encoding="utf-8")
        task_id = "deliver-e2e-fail"
        task = orch.run_plan(plan, input_file=str(tmp_path / "in.md"),
                             task_id=task_id, wait=True)
        assert task.status.value == "failed", "前提：这条流水线应当失败"

        from loom_core.deliveries import DeliveryLedger
        row = DeliveryLedger().get(task_id)
        assert row is not None, "失败运行也必须进账本（账本的意义就是失败可查）"
        assert row["status"] == "failed"
        assert not (out / "result.json").exists(), "失败运行不该写交付 JSON"
        assert not (out / "site").exists(), "失败运行不该建站点"
        # 失败运行也带指纹（topology_hash 照样算——拓扑是声明层的，与成败无关）
        assert len(row["topology_hash"]) == 12
        assert len(row["config_hashes"]) >= 3
        # 没有产物文件 → output_sha256 为空串（与"产出了但丢了"可区分）
        assert row["output_sha256"] == ""

    def test_fingerprint_matches_scheduler_static_methods(self, orch, tmp_path):
        """指纹字段必须与 Scheduler 静态方法直接计算的结果一致。

        交付记录里的 topology_hash / config_hashes 是通过
        Scheduler._topology_hash(plan) / Scheduler._node_config_hash(ac)
        计算的——测试直接调用同一方法，确保两处实现不漂。
        """
        plan, out = _plan(DEMO_YAML.format(out=str(tmp_path / "fp-demo")),
                          tmp_path, "fp-demo")
        (tmp_path / "in.md").write_text("# 输入\n指纹一致性判据。", encoding="utf-8")
        task_id = "fp-e2e-1"
        task = orch.run_plan(plan, input_file=str(tmp_path / "in.md"),
                             task_id=task_id, wait=True)
        assert task.status.value == "done", task.error

        from loom_core.deliveries import DeliveryLedger
        from loom_core.scheduler import Scheduler

        row = DeliveryLedger().get(task_id)
        assert row is not None

        # topology_hash 与 Scheduler._topology_hash 直接一致
        expected_topo = Scheduler._topology_hash(plan)
        assert row["topology_hash"] == expected_topo, \
            "交付记录的 topology_hash 与 Scheduler._topology_hash 不一致"

        # config_hashes 与 Scheduler._node_config_hash 逐节点一致
        for level in plan.levels:
            for node in level:
                ac = getattr(node, "agent_config", None)
                if ac is not None:
                    expected = Scheduler._node_config_hash(ac)
                    actual = row["config_hashes"].get(node.agent_name)
                    assert actual == expected, \
                        f"{node.agent_name} 的 config_hash 不一致: " \
                        f"交付={actual} 直接计算={expected}"

    def test_fingerprint_stable_across_runs(self, orch, tmp_path):
        """同一条流水线跑两次，指纹必须一致（声明层不变 → 指纹不变）。

        这是锁机制的前提：如果同一条流水线两次运行的指纹不同，
        锁文件就没有意义了（每次跑都会报漂移）。

        注意：用同一个 out 路径——renderer 的 output_dir 是 agent config 的
        一部分，路径不同则 config_hash 不同（这是正确的：输出路径确实改了
        配置，锁应当察觉）。
        """
        out = tmp_path / "stab-out"
        yaml1 = DEMO_YAML.format(out=str(out))
        plan1, _ = _plan(yaml1, tmp_path, "stab-a")
        (tmp_path / "in.md").write_text("# 输入\n稳定性判据。", encoding="utf-8")
        task1 = orch.run_plan(plan1, input_file=str(tmp_path / "in.md"),
                              task_id="stab-run-1", wait=True)
        assert task1.status.value == "done", task1.error

        # 重新解析同一份 YAML（plan 对象是一次性的，重新 parse）
        plan2, _ = _plan(yaml1, tmp_path, "stab-b")
        task2 = orch.run_plan(plan2, input_file=str(tmp_path / "in.md"),
                              task_id="stab-run-2", wait=True)
        assert task2.status.value == "done", task2.error

        from loom_core.deliveries import DeliveryLedger
        led = DeliveryLedger()
        r1 = led.get("stab-run-1")
        r2 = led.get("stab-run-2")
        assert r1 is not None and r2 is not None
        # 拓扑指纹一致（同一份 YAML → 同一条 DAG）
        assert r1["topology_hash"] == r2["topology_hash"], \
            "同一条流水线两次运行的 topology_hash 必须一致"
        # 逐 agent 配置指纹一致
        assert r1["config_hashes"] == r2["config_hashes"], \
            "同一条流水线两次运行的 config_hashes 必须一致"
