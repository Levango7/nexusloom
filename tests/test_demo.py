"""`loom demo` 60 秒离线体验（设计复核 2026-10-20 §3.1 A4）的判据。

A4 原文：干净 venv 里 `pip install nexusloom && loom demo` 产出一份 docx +
质量报告 + 账本记录，全程无网络无 Key。本文件覆盖三件事：
  1. demo 跑真流水线（Scheduler → DAGExecutor → Agent → 落盘），全离线——
     交付物、质量分、账本记录都是真算出来的，不是罐头断言；
  2. 沙盒隔离与如实呈现——状态目录整个搬进沙盒、缺后端的格式报跳过原因、
     失败运行不被粉饰成成功；
  3. 入口——`loom demo` 动词存在，子进程端到端能跑通。

安装态的真 venv 判据在 `tests/test_installable_product.py`（A4 验收线的
另一半：wheel 装出来照样能 demo）。
"""
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

PROJECT = Path(__file__).resolve().parent.parent


@pytest.fixture()
def demo_dir(tmp_path, monkeypatch):
    """在临时目录里跑 demo（沙盒建在 cwd 下，测试不污染真实环境）。"""
    monkeypatch.chdir(tmp_path)
    return tmp_path


class TestDemoRunner:
    def test_runs_offline_end_to_end(self, demo_dir):
        from loom_core.demo import run_demo

        r = run_demo(log=lambda _m: None)
        assert r.status == "done", f"demo 运行失败: {r.error}"
        assert r.task_id
        assert r.duration_sec > 0
        # 交付物：md 永远在，html 恒可用（纯标准库后端）
        names = sorted(d.name for d in r.deliverables)
        assert "demo-report.md" in names, names
        assert "demo-report.html" in names, names
        # docx：环境装了 python-docx 就必须有（A4 验收线）；没装则如实跳过
        from loom_doc import renderer as _r

        if "docx" in _r.supported_formats():
            assert "demo-report.docx" in names, names

    def test_deliverables_have_real_content(self, demo_dir):
        from loom_core.demo import run_demo

        r = run_demo(log=lambda _m: None)
        by_name = {d.name: d for d in r.deliverables}
        md = by_name.get("demo-report.md")
        assert md is not None and md.stat().st_size > 200
        text = md.read_text(encoding="utf-8")
        assert text.startswith("# "), "md 交付物必须是渲染后的正文（真产出，不是占位）"
        assert "Loom" in text

    def test_quality_score_is_real_and_passes(self, demo_dir):
        """质量分是 quality_gate 真算的：必须过阈值（模板写对了才配被展示）。"""
        from loom_core.demo import run_demo

        r = run_demo(log=lambda _m: None)
        assert r.quality, "demo 必须跑 quality_gate 节点并取到结果"
        assert r.quality.get("overall_score") is not None
        assert r.quality["overall_score"] >= r.quality.get("threshold", 70), (
            f"质量门未达标: {r.quality.get('overall_score')} < "
            f"{r.quality.get('threshold')}（模板需要调整——这是真评分）")
        assert r.quality.get("scores"), "维度分缺失"

    def test_ledger_records_the_run(self, demo_dir):
        from loom_core.demo import run_demo

        r = run_demo(log=lambda _m: None)
        assert r.ledger is not None, "交付账本必须记录这次运行"
        assert r.ledger.get("run_id") == r.task_id
        assert r.ledger.get("status") == "done"
        assert r.ledger.get("pipeline") == "demo"

    def test_sandbox_state_isolated(self, demo_dir):
        from loom_core.demo import run_demo

        before = os.environ.get("LOOM_STATE_DIR")
        r = run_demo(log=lambda _m: None)
        assert (r.sandbox / "input.md").exists()
        assert (r.sandbox / "state" / "deliveries.db").exists()
        assert os.environ.get("LOOM_STATE_DIR") == before

    def test_skipped_format_reported_honestly(self, demo_dir, monkeypatch):
        """缺后端的格式如实报跳过原因——不许假装成功（能力包的诚实边界）。"""
        from loom_core import demo
        from loom_doc import renderer as _r

        real_render = _r.render

        def fake_render(content, target, fmt=None, title=""):
            if fmt == "docx":
                return {"status": "skip", "message": "python-docx 未安装（pip install nexusloom[render]）"}
            return real_render(content, target, fmt=fmt, title=title)

        monkeypatch.setattr(_r, "render", fake_render)
        r = demo.run_demo(log=lambda _m: None)
        assert "docx" in r.skipped_formats
        assert "python-docx" in r.skipped_formats["docx"]

    def test_failed_run_not_masked(self, demo_dir, monkeypatch):
        """运行失败必须照实报（状态 + 错误文本），walkthrough 不许粉饰。"""
        from loom_core import demo
        from loom_core.pipeline import PipelineOrchestrator

        def boom(self, plan, input_file="", task_id=None, wait=True,
                 resume=False, owner=""):
            return SimpleNamespace(id="t1", status="failed", error="boom",
                                   result={}, output_path="")

        monkeypatch.setattr(PipelineOrchestrator, "run_plan", boom)
        logs: list = []
        r = demo.run_demo(log=logs.append)
        assert r.status == "failed"
        assert "boom" in r.error
        assert any("✗" in m and "failed" in m for m in logs), (
            "失败状态必须在 walkthrough 里如实呈现")

    def test_walkthrough_reports_deliverables(self, demo_dir):
        from loom_core.demo import run_demo

        logs: list = []
        run_demo(log=logs.append)
        joined = "\n".join(logs)
        assert "沙盒目录" in joined
        assert "交付物" in joined
        assert "质量门" in joined
        assert "交付账本" in joined
        assert "下一步" in joined


class TestDemoPipeline:
    def test_demo_pipeline_is_offline_quality_tail(self):
        """demo 复用 pack 的质量尾：有质量门，无 LLM/网络节点（writer/researcher/
        fetcher 一律不许出现——离线承诺的判据）。"""
        from loom_core.scheduler import Scheduler

        plan = Scheduler().parse("demo")
        agents = [n.agent_name for level in plan.levels for n in level]
        assert "quality_gate" in agents
        assert "renderer" in agents
        for banned in ("researcher", "fetcher", "writer"):
            assert banned not in agents, f"demo 流水线不得包含 {banned}（离线承诺）"

    def test_demo_lock_present_and_matches(self):
        from loom_core.scheduler import Scheduler

        plan = Scheduler().parse("demo")  # verify_lock=True：漂移即抛
        assert plan.node_count >= 4


class TestDemoVerb:
    def test_verb_registered(self):
        import run as run_mod

        verb, action, rest = run_mod._split_verb(["demo"])
        assert verb == "demo" and action == "demo" and rest == []

    def test_loom_demo_cli_end_to_end(self, tmp_path):
        """子进程真跑 `loom demo`：动词路径 + 沙盒 + walkthrough 全链路。"""
        r = subprocess.run(
            [sys.executable, str(PROJECT / "run.py"), "demo"],
            capture_output=True, text=True, timeout=300, cwd=str(tmp_path))
        assert r.returncode == 0, r.stdout[-2000:] + r.stderr[-2000:]
        assert "沙盒目录" in r.stdout
        assert "交付物" in r.stdout
        assert "质量门" in r.stdout
        assert "交付账本" in r.stdout
        assert "下一步" in r.stdout
        assert tmp_path.glob("loom-demo-*"), "沙盒目录必须建在 cwd 下"
