"""`loom demo` —— 60 秒离线体验（设计复核 2026-10-20 §3.1 A4）。

治的是什么：本产品的"快速开始"此前要装 requirements + 配 LLM Key + 有网络，
新用户在第一步就流失。demo 把"一次真实交付"压进一条命令：跑产品自带的离线
流水线（transform → quality_gate → checker → layout → safe_writer → renderer），
在**沙盒目录**里产出交付物、质量报告和交付账本记录——全程无网络、无 Key、
不碰用户既有状态。

三条边界（都是刻意选择，改之前先想清楚）：

- **沙盒即隔离**：`LOOM_STATE_DIR` / `LOOM_VERSIONS_DIR` 指进 `<沙盒>/state`，
  检查点也落在沙盒里——demo 不污染真实状态目录，也不被它污染（幂等键历史、
  队列、账本全是新的）。状态目录是调用期解析的（state_paths 模块注释），
  所以本模块把环境变量设在**构造编排器之前**。
- **如实呈现**：渲染格式缺后端（如 python-docx 未装）不假装成功——跳过原因
  取自 renderer 自己的 `skipped` 字段；质量分是 quality_gate 真算出来的，
  不及格就照实报——那本身就是产品行为的一部分，替它化妆等于再造一个 FP-1。
- **demo 是 docgen pack 的展示窗**：它复用 pack 的质量尾（quality_gate/
  checker/layout/safe_writer），流水线定义在 `packs/docgen/pipelines/demo.yaml`
  ——核心不放领域流水线（设计复核 §3.3 第 6 条），删掉 packs/ 时引擎与其余
  流水线照常工作，demo 跟着 pack 一起消失，口径一致。
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

DEMO_PIPELINE_NAME = "demo"

#: 输入文档：`## 键` 输入契约的最小样本（docgen 系流水线的输入约定）。
#: 首行是主题（质量门的主题相关度按它判），正文是资料路径占位——demo 的
#: transform 不检索，模板静态渲染。
DEMO_INPUT_DOC = """\
Loom 60 秒离线体验：一次完整的交付演示

## 说明

这是 `loom demo` 的输入文档。真实流水线的输入可以是检索词、资料路径或
结构化数据——由流水线的第一个 Agent 决定怎么消费它。
"""


def _resolve_demo_pipeline() -> Path:
    """定位 demo.yaml（核心 pipelines/ 优先，物理 pack 回退）。"""
    from .paths import pipelines_dir
    from .pack_manifest import pipeline_dirs

    candidates = [Path(pipelines_dir()) / f"{DEMO_PIPELINE_NAME}.yaml"]
    candidates.extend(d / f"{DEMO_PIPELINE_NAME}.yaml" for d in pipeline_dirs())
    for c in candidates:
        if c.is_file():
            return c
    raise FileNotFoundError(
        f"找不到 {DEMO_PIPELINE_NAME} 流水线（候选: {[str(c) for c in candidates]}）")


@dataclass
class DemoResult:
    """一次 demo 运行的全部事实——CLI 的退出码与 walkthrough 都从它出发。"""

    task_id: str
    status: str
    sandbox: Path
    #: 实际落盘且字节 > 0 的交付物（md 永远在；html 恒可用；docx 随 render extra）
    deliverables: list[Path] = field(default_factory=list)
    #: 请求了但没产出的格式 → renderer 自己给的跳过原因（不编原因）
    skipped_formats: dict[str, str] = field(default_factory=dict)
    #: quality_gate 节点结果（overall_score / scores / threshold；没跑到 = {}）
    quality: dict[str, Any] = field(default_factory=dict)
    #: 交付账本里这次运行的记录（读不到 = None，不造一条假的）
    ledger: dict[str, Any] | None = None
    duration_sec: float = 0.0
    error: str = ""


def run_demo(log: Callable[[str], None] = print) -> DemoResult:
    """跑一次完整 demo。构造编排器之前先落沙盒，异常原样向上抛（CLI 兜底）。"""
    sandbox = _make_sandbox(Path.cwd())
    _isolate_state(sandbox)
    input_file = sandbox / "input.md"
    input_file.write_text(DEMO_INPUT_DOC, encoding="utf-8")
    deliver_dir = sandbox / "deliverables"
    deliver_dir.mkdir(parents=True, exist_ok=True)

    from .pipeline import PipelineOrchestrator
    from .paths import agents_dir, pipelines_dir
    from .pack_manifest import pipeline_dirs
    from .scheduler import Scheduler

    log(f"[demo] 沙盒目录: {sandbox}")
    log(f"[demo] 输入文档: {input_file.name}（'## 键' 输入契约的最小样本）")
    log(f"[demo] 运行 {DEMO_PIPELINE_NAME} 流水线"
        "（transform → quality_gate → checker → layout → safe_writer → renderer）…")

    orch = PipelineOrchestrator(
        agents_dir=str(agents_dir()),
        checkpoint_dir=str(sandbox / "checkpoints"),
    )
    started = time.time()
    try:
        orch.register_agents()
        plan_path = _resolve_demo_pipeline()
        # parse_file（而非 parse）：锁校验跟着 YAML 所在目录走——pack 流水线的
        # 锁在 packs/docgen/pipelines/ 里，按名 parse 会去 pipelines/ 找然后
        # 静默跳过校验（这就是"绿而未验"的形状）。
        plan = Scheduler().parse_file(str(plan_path), verify_lock=True)
        plan.raw.setdefault("pipeline", {})["output"] = str(
            deliver_dir / "demo-report.md")
        plan.raw.setdefault("pipeline", {}).setdefault("deliver", {})[
            "json"] = str(deliver_dir / "demo-report.json")
        # 节点级 config 指进沙盒（DAG 路径读 plan 里的 AgentConfig.config）：
        # renderer 的 output_dir 与 safe_writer 的 backup_dir 默认值是相对路径，
        # 从用户 cwd 跑会写进用户目录。
        for level in plan.levels:
            for node in level:
                cfg = getattr(node, "agent_config", None)
                cfg = getattr(cfg, "config", None) if cfg is not None else None
                if isinstance(cfg, dict):
                    if cfg.get("formats") is not None:
                        cfg["output_dir"] = str(deliver_dir)
                    if "backup_dir" in cfg:
                        cfg["backup_dir"] = str(sandbox / "backups")
        task = orch.run_plan(plan, input_file=str(input_file), wait=True)
        duration = time.time() - started
        result = _collect(task, sandbox, deliver_dir, duration)
        if result.quality and "threshold" not in result.quality:
            result.quality["threshold"] = _quality_threshold(plan)
    finally:
        orch.shutdown()

    _walkthrough(result, log)
    return result


def _quality_threshold(plan: Any) -> float:
    """demo 流水线质量门的阈值（节点 config 里的 threshold；取不到给 70）。"""
    for level in getattr(plan, "levels", []):
        for node in level:
            cfg = getattr(node, "agent_config", None)
            cfg = getattr(cfg, "config", None) if cfg is not None else None
            if isinstance(cfg, dict) and "threshold" in cfg:
                return float(cfg["threshold"])
    return 70.0


def _make_sandbox(base: Path) -> Path:
    """沙盒目录：`loom-demo-<时间戳>`，重跑不覆盖上一次（上一次也是证据）。"""
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    sandbox = base / f"loom-demo-{stamp}"
    sandbox.mkdir(parents=True, exist_ok=False)
    return sandbox


def _isolate_state(sandbox: Path) -> None:
    """状态目录整个搬进沙盒——必须在任何 store/编排器构造之前调用。"""
    os.environ["LOOM_STATE_DIR"] = str(sandbox / "state")
    os.environ["LOOM_VERSIONS_DIR"] = str(sandbox / "versions")


def _collect(task: Any, sandbox: Path, deliver_dir: Path,
             duration: float) -> DemoResult:
    """从任务对象收集 DemoResult。缺什么记什么，不猜、不编。"""
    from .deliveries import DeliveryLedger

    status = str(getattr(getattr(task, "status", None), "value",
                         getattr(task, "status", "")))
    results = getattr(task, "result", None) or {}
    quality = results.get("quality_gate") if isinstance(results, dict) else None
    render = results.get("renderer") if isinstance(results, dict) else None

    deliverables: list[Path] = []
    skipped: dict[str, str] = {}
    md_path = getattr(task, "output_path", "") or str(
        deliver_dir / "demo-report.md")
    p = Path(md_path)
    if p.exists() and p.stat().st_size > 0:
        deliverables.append(p)
    if isinstance(render, dict):
        for fmt, path in (render.get("outputs") or {}).items():
            fp = Path(str(path))
            if fp.exists() and fp.stat().st_size > 0:
                deliverables.append(fp)
        skipped.update({str(k): str(v)
                        for k, v in (render.get("skipped") or {}).items()})

    ledger = None
    try:
        ledger = DeliveryLedger().get(str(getattr(task, "id", "")))
    except Exception:                        # noqa: BLE001 — 账本读不到就如实为 None
        ledger = None

    return DemoResult(
        task_id=str(getattr(task, "id", "")),
        status=status,
        sandbox=sandbox,
        deliverables=deliverables,
        skipped_formats=skipped,
        quality=dict(quality) if isinstance(quality, dict) else {},
        ledger=dict(ledger) if isinstance(ledger, dict) else None,
        duration_sec=duration,
        error=str(getattr(task, "error", "") or ""),
    )


def _walkthrough(r: DemoResult, log: Callable[[str], None]) -> None:
    """把结果讲成人话。失败的 demo 也是一次真实运行——照实讲，不粉饰。"""
    ok = r.status == "done"
    log(f"[demo] {'✓' if ok else '✗'} 状态: {r.status}"
        f"（task {r.task_id}，{r.duration_sec:.1f}s）"
        + (f" — {r.error}" if r.error else ""))
    if r.deliverables:
        log("[demo] 交付物:")
        for d in sorted(r.deliverables):
            log(f"    {d.name:24s} {d.stat().st_size / 1024:8.1f} KB  ({d.parent})")
    for fmt, reason in sorted(r.skipped_formats.items()):
        log(f"[demo] {fmt} 未渲染: {reason}")
    if r.quality:
        overall = r.quality.get("overall_score")
        threshold = r.quality.get("threshold", "?")
        scores = r.quality.get("scores") or {}
        dims = "  ".join(f"{k} {v:g}" for k, v in scores.items())
        log(f"[demo] 质量门: {overall} / 100（阈值 {threshold}）{dims}")
        log("[demo]   ↑ 真实评分：改沙盒里的 input.md 或流水线再跑，分数会变")
    if r.ledger is not None:
        log(f"[demo] 交付账本: deliveries.db 已记录（status={r.ledger.get('status')}，"
            f"pipeline={r.ledger.get('pipeline')}）")
    if ok:
        log("[demo] 下一步:")
        log("         loom plan demo        # 看执行计划与锁文件指纹")
        log("         loom --admin          # 起 API + 控制台，看 DAG/质量明细/审批")
        log("         loom --help           # 全部动词（run/plan/lock/pack/plugin/demo）")
