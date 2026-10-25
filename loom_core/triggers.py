"""定时触发：triggers.yaml → 到点把流水线提交成一次普通 run。

设计与边界（都是刻意选择，改之前先想清楚）：
- **与手动 run 同一条路径**：提交走 `orch.run_plan(plan, input_file=..., wait=False)`，
  与 CLI / MCP / Admin API 完全一致——定时触发的任务因此天然出现在同一任务
  队列与观测面（product-spec §5.3 的验收判据）。
- **错过不补跑**：进程没运行的时间窗直接跳过，跳过几个如实打印；常驻进程
  重启后从"现在"重新对表（last_check 初始化在启动时刻）。补跑策略属于产品
  决策，这里不偷偷选一个。
- **输入文档**：`inputs` 是字符串（原样写入；首行写主题、其余行写资料路径，
  与 kb-docgen 的输入约定一致），落到 `state_root()/trigger_inputs/<name>.md`。
  固定路径且内容随配置固定——不存在"任务读到半个文件"的中间态。
- **校验前置**：加载时把名字唯一 / 流水线存在 / cron 合法一次验完，错一条
  拒绝启动——配置写错不该等到触发那一刻才发现。
- **多进程互斥（B4）**：共享一个状态目录的多个 `--triggers` 进程只有持租约者
  提交（租约挂 RunStore 的 `schedules` 表，owner=`主机名:pid`，每轮续租）。
  RunStore 不可用时降级为单进程行为并警告——降级是事实，必须说出口。
"""
from __future__ import annotations

import contextlib
import os
import socket
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .cron import CronError, CronSpec, describe, next_fire, parse_cron
from .ids import new_task_id
from .state_paths import state_root

DEFAULT_CONFIG_NAME = "triggers.yaml"


class TriggerConfigError(ValueError):
    """triggers.yaml 配置非法（信息里带上出错的条目）。"""


@dataclass
class Trigger:
    name: str
    pipeline: str
    cron: str
    spec: CronSpec
    inputs: str = ""
    output: str = ""
    enabled: bool = True


def load_triggers(path: str | Path,
                  available: list[str] | None = None) -> list[Trigger]:
    """加载并校验 triggers.yaml；`available` 缺省取 scheduler.installed_pipelines()。"""
    p = Path(path)
    if not p.exists():
        raise TriggerConfigError(
            f"配置文件不存在: {p}（可复制根目录的 triggers.example.yaml 起手）")
    try:
        import yaml

        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception as e:                       # yaml 的多种异常统一转配置错误
        raise TriggerConfigError(f"配置文件解析失败: {e}") from e
    if not isinstance(data, dict) or "triggers" not in data:
        raise TriggerConfigError("缺少顶层 `triggers:` 列表")
    items = data.get("triggers")
    if not isinstance(items, list) or not items:
        raise TriggerConfigError("`triggers:` 必须是非空列表")

    if available is None:
        from .scheduler import installed_pipelines

        available = installed_pipelines()

    triggers: list[Trigger] = []
    seen: set[str] = set()
    yaml_bool_hint = "（注意 YAML 会把 off/on/yes/no 解析成布尔值，写名字时请加引号）"
    for idx, item in enumerate(items, start=1):
        where = f"triggers[{idx}]"
        if not isinstance(item, dict):
            raise TriggerConfigError(f"{where} 必须是映射（含 name/pipeline/cron）")
        raw_name = item.get("name")
        if not isinstance(raw_name, str):
            raise TriggerConfigError(f"{where} name 必须是字符串{yaml_bool_hint}")
        name = raw_name.strip()
        if not name:
            raise TriggerConfigError(f"{where} 缺少 name")
        if name in seen:
            raise TriggerConfigError(f"{where} name 重复: {name!r}")
        seen.add(name)
        raw_pipeline = item.get("pipeline")
        if not isinstance(raw_pipeline, str):
            raise TriggerConfigError(f"{where}（{name}）pipeline 必须是字符串{yaml_bool_hint}")
        pipeline = raw_pipeline.strip()
        if not pipeline:
            raise TriggerConfigError(f"{where}（{name}）缺少 pipeline")
        if pipeline not in available:
            raise TriggerConfigError(
                f"{where}（{name}）pipeline {pipeline!r} 不存在"
                f"（可用: {', '.join(available) or '无'}）")
        cron_raw = str(item.get("cron", "")).strip()
        if not cron_raw:
            raise TriggerConfigError(f"{where}（{name}）缺少 cron")
        try:
            spec = parse_cron(cron_raw)
        except CronError as e:
            raise TriggerConfigError(f"{where}（{name}）cron 非法: {e}") from e
        inputs = item.get("inputs", "") or ""
        if not isinstance(inputs, str):
            raise TriggerConfigError(
                f"{where}（{name}）inputs 只接受字符串（会原样写入输入文档）")
        output = item.get("output", "") or ""
        if not isinstance(output, str):
            raise TriggerConfigError(f"{where}（{name}）output 必须是路径字符串")
        enabled = item.get("enabled", True)
        if not isinstance(enabled, bool):
            raise TriggerConfigError(f"{where}（{name}）enabled 必须是布尔值")
        triggers.append(Trigger(name=name, pipeline=pipeline, cron=cron_raw,
                                spec=spec, inputs=inputs, output=output,
                                enabled=enabled))
    return triggers


def render_trigger_input(trigger: Trigger) -> str:
    """inputs → 输入文档内容；空 inputs 给标题行（文件非空，标题不会被当查询词）。"""
    body = (trigger.inputs or "").strip("\n")
    if not body:
        return f"# {trigger.name}\n"
    if body.lstrip().startswith("#"):
        return body if body.endswith("\n") else body + "\n"
    return f"# {trigger.name}\n\n{body}\n"


def input_file_for(trigger: Trigger) -> Path:
    """输入文件的固定存放路径（状态目录可被 LOOM_STATE_DIR 重定向）。"""
    d = state_root() / "trigger_inputs"
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{trigger.name}.md"
    p.write_text(render_trigger_input(trigger), encoding="utf-8")
    return p


def submit_trigger_run(orch: Any, trigger: Trigger, fire_time: datetime) -> str:
    """到点提交一次普通 run，返回 task_id（供日志与测试断言）。

    task_id 每次新生成：复用同一 task_id 会命中幂等键历史、把节点静默跳过
    而整体记 success——这条坑在 state_paths 的模块注释里记着。
    """
    from .scheduler import Scheduler

    plan = Scheduler().parse(trigger.pipeline)
    if trigger.output:
        plan.raw.setdefault("pipeline", {})["output"] = trigger.output
    input_file = input_file_for(trigger)
    task = orch.run_plan(plan, input_file=str(input_file),
                         task_id=new_task_id(), wait=False,
                         owner=f"trigger:{trigger.name}")
    return str(task.id)


@dataclass
class _Runtime:
    trigger: Trigger
    last_check: datetime


def tick(runtimes: list[_Runtime], now: datetime,
         submit: Callable[[Trigger, datetime], str],
         log: Callable[[str], None],
         lease: Callable[[Trigger], bool] | None = None,
         on_fire: Callable[[Trigger, datetime], None] | None = None) -> list[str]:
    """推进一次调度：到期提交、清点错过窗口。循环体本身无副作用，便于测试。

    `lease`（B4）在窗口计算**之前**判定"这个触发归不归本进程"。被拒时
    `last_check` 仍要前移——窗口记账与提交归属是两回事；不前移的话，持约
    进程一死，接管进程会把它已经提交过的旧窗口再算成"到期"而重复提交。
    `on_fire` 在提交成功后记账（schedules 表的 last_fire/next_fire），
    失败不影响本次提交——它是观测面，不是闸门。
    """
    submitted: list[str] = []
    for rt in runtimes:
        if not rt.trigger.enabled:
            continue
        if lease is not None:
            try:
                allowed = lease(rt.trigger)
            except Exception as e:               # 租约面故障不拖垮整个循环
                log(f"[triggers] {rt.trigger.name} 租约判定失败: {e}")
                rt.last_check = now
                continue
            if not allowed:
                rt.last_check = now
                continue
        nf = next_fire(rt.trigger.spec, rt.last_check)
        if nf > now:
            rt.last_check = now
            continue
        # 积压多个窗口（进程睡过/被挂起）：只跑最近一个，缺的计数如实打印
        missed = 0
        while True:
            nxt = next_fire(rt.trigger.spec, nf)
            if nxt <= now:
                missed += 1
                nf = nxt
                continue
            break
        rt.last_check = now
        try:
            task_id = submit(rt.trigger, nf)
        except Exception as e:               # 单次提交失败不拖垮整个循环
            log(f"[triggers] {rt.trigger.name} 提交失败: {e}")
            continue
        log(f"[triggers] {rt.trigger.name} → {rt.trigger.pipeline} 已提交"
            f"（task {task_id}，触发时刻 {nf:%Y-%m-%d %H:%M}）")
        if missed:
            log(f"[triggers] {rt.trigger.name} 跳过 {missed} 个错过窗口（进程未运行）")
        if on_fire is not None:
            with contextlib.suppress(Exception):
                on_fire(rt.trigger, nf)
        submitted.append(task_id)
    return submitted


def describe_triggers(triggers: list[Trigger], after: datetime,
                      count: int = 3) -> list[str]:
    """每个触发的最近 count 次时刻（--triggers-dry-run 的输出行）。"""
    lines: list[str] = []
    for t in triggers:
        if not t.enabled:
            lines.append(f"  {t.name:20s} {t.cron:16s} （停用）→ {t.pipeline}")
            continue
        fires = ", ".join(f"{d:%Y-%m-%d %H:%M}"
                          for d in describe(t.spec, after, count))
        lines.append(f"  {t.name:20s} {t.cron:16s} → {t.pipeline}；最近 {count} 次: {fires}")
    return lines


def run_trigger_loop(orch: Any, triggers: list[Trigger], *,
                     poll_seconds: float = 20.0,
                     now_fn: Callable[[], datetime] = datetime.now,
                     sleep_fn: Callable[[float], None] | None = None,
                     max_ticks: int | None = None,
                     log: Callable[[str], None] = print,
                     lease_ttl: float | None = None) -> None:
    """前台常驻循环（Ctrl+C 退出）。轮询间隔决定触发精度（秒级误差）。

    B4 起：多个进程共享一个状态目录时只有**持租约的进程**提交——租约经
    RunStore 的 `schedules` 表原子裁决，owner = `主机名:pid`，TTL 默认三倍
    轮询间隔（下限 60s），每轮续租；优雅退出时放掉本人租约，另一进程立即
    接管。RunStore 建不起时降级为单进程模式（B4 之前的行为），但降级必须
    说出口（警告一行），不能静默装没事。
    """
    active = [t for t in triggers if t.enabled]
    if not active:
        log("[triggers] 没有启用的触发（全部 enabled: false），无事可做")
        return
    ttl = lease_ttl if lease_ttl is not None else max(poll_seconds * 3.0, 60.0)
    owner = f"{socket.gethostname()}:{os.getpid()}"
    store, lease_cb, on_fire_cb = _wire_schedule_lease(
        orch, triggers, owner, ttl, log)
    started = now_fn()
    runtimes = [_Runtime(t, started) for t in triggers]
    for t in active:
        nf = next_fire(t.spec, started)
        log(f"[triggers] {t.name:20s} {t.cron:16s}"
            f" 下次触发 {nf:%Y-%m-%d %H:%M} → {t.pipeline}")
    log(f"[triggers] 常驻调度启动（{len(active)} 个启用触发，"
        f"轮询 {poll_seconds:g}s；Ctrl+C 退出）")
    if lease_cb is not None:
        log(f"[triggers] 租约已接 RunStore（owner={owner}，TTL {ttl:g}s）"
            "——共享同一状态目录的多进程只有持约者提交")
    sleep = sleep_fn or time.sleep

    def _submit(trigger: Trigger, fire_time: datetime) -> str:
        """把 orch 绑进提交闭包——tick 的契约只认 (trigger, fire_time)。"""
        return submit_trigger_run(orch, trigger, fire_time)

    ticks = 0
    try:
        while True:
            tick(runtimes, now_fn(), _submit, log,
                 lease=lease_cb, on_fire=on_fire_cb)
            ticks += 1
            if max_ticks is not None and ticks >= max_ticks:
                break
            sleep(poll_seconds)
    except KeyboardInterrupt:
        log("\n[triggers] 收到中断，退出调度")
    finally:
        if store is not None:
            for t in active:
                with contextlib.suppress(Exception):
                    store.release_schedule_lease(t.name, owner=owner)


def _wire_schedule_lease(orch: Any, triggers: list[Trigger], owner: str,
                         ttl: float, log: Callable[[str], None],
                         ) -> tuple[Any, Callable[[Trigger], bool] | None,
                                    Callable[[Trigger, datetime], None] | None]:
    """把 RunStore 的 `schedules` 表接成 tick 的租约/记账回调。

    返回 `(store, lease, on_fire)`；任一环节建不起来就返回 `(None, None, None)`
    ——降级成 B4 之前的单进程行为。登记一次做完（配置列幂等覆盖，租约列不动），
    之后每个 tick 只做条件 UPDATE 的抢/续约。
    """
    store = None
    getter = getattr(orch, "_runstore", None)
    if callable(getter):
        try:
            store = getter()
        except Exception as e:               # noqa: BLE001 — _runstore 本应 fail-soft
            log(f"[triggers] RunStore 访问失败，租约停用: {e}")
            return None, None, None
    if store is None:
        log("[triggers] RunStore 不可用，租约停用——同库多进程可能重复触发")
        return None, None, None
    try:
        for t in triggers:
            store.register_schedule(t.name, pipeline=t.pipeline,
                                    cron=t.cron, enabled=t.enabled)
    except Exception as e:                   # noqa: BLE001
        log(f"[triggers] 触发登记失败，租约停用: {e}")
        return None, None, None

    def _lease(trigger: Trigger) -> bool:
        return bool(store.acquire_schedule_lease(trigger.name, owner=owner,
                                                 ttl=ttl))

    def _on_fire(trigger: Trigger, fired_at: datetime) -> None:
        nxt = next_fire(trigger.spec, fired_at)
        store.record_schedule_fire(trigger.name, fired_at.timestamp(),
                                   nxt.timestamp())

    return store, _lease, _on_fire
