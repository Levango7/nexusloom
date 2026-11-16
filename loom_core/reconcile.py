"""A7 只读对账报告：任务队列 × 运行投影双源的 stale-running 漂移列示。

双源模型（板面 §三十四 A7 规格）：
- tasks.db 的 task_queue 行是**执行侧真源**（status/started_at/finished_at/owner_pid）；
- runs.db 的 runs 行 + events 表是**交付投影**（open_run 盖 running、close_run 盖终态章）。
正常收尾链是 队列终态 → 终态事件 → 投影盖章 → 交付账本；任何一环中断都会留下
双源不一致。本模块把不一致**列出来**——默认只报告、不修复：不自动杀进程、不写
任何库（人在圈内看了报告再决定动什么）。A7-C 起另提供**显式回收**入口
`reap_stale_running`（`loom reconcile --reap` / 启动 env 钩子 `LOOM_REAP_STALE_SECONDS`）：
仅对分类①「确证死亡」（owner_pid 已死/缺失且过期）的行做终态收尾（cancelled）＋
投影盖章＋`run.reaped` 审计事件；②③类不归它管（marker=incomplete 走 backfill 补账、
其余留人判断）。与 task_queue.recover() 的「翻回 pending 重试」语义刻意互补：recover
给任务第二次机会，reap 宣告它死亡（终态 cancelled，不再重试），互不替代。

三类漂移（互斥划分，每行恰好归一类）：
1. stale-owner-dead：队列仍 running，但 started_at 缺失/过期（超过 stale_seconds）
   且 owner_pid 缺失/已死（新鲜 running 或存活 pid 视为仍在执行，不报）；
2. projection-running：队列已终态（done/failed/cancelled）而投影仍 running；
3. projection-unstamped：队列已终态而投影行缺失，或投影状态非 running 也非终态
   （盖过头寸/异常态，即「未盖章」）。
一致行跳过；投影有行而队列无对应行（孤儿投影）不在本报告三类内，不报。
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

from .run_store import RUN_REAPED, TERMINAL_STATUSES, RunStore
from .task_queue import TaskQueue, _pid_alive

_logger = logging.getLogger(__name__)

#: 三类漂移的 category 取值（判据钉死：CLI 人类输出 / JSON / 测试同源）。
CAT_STALE_OWNER_DEAD = "stale-owner-dead"
CAT_PROJECTION_RUNNING = "projection-running"
CAT_PROJECTION_UNSTAMPED = "projection-unstamped"

#: 每行输出的键集合（JSON 键序即插入序，不作契约）。
ROW_KEYS = (
    "task_id", "category", "reason", "queue_status", "queue_started_at",
    "queue_finished_at", "owner_pid", "run_status", "last_event_ts",
)


def _last_event_ts(run_store: RunStore, run_id: str) -> float | None:
    """该 run 最后一条事件的 ts；无事件 / 无该 run → None。

    events() 按 seq ASC 返回，取 max 即最后事件时间。limit 给足（事件量由保留期
    GC 有界），避免截断把「最后事件」读成「第 1000 条」。
    """
    events = run_store.events(run_id, after_seq=0, limit=1_000_000)
    if not events:
        return None
    return float(max(e["ts"] for e in events))


def _row(task_id: str, category: str, reason: str, q: dict, owner_pid: int,
         run_status: str | None, last_event_ts: float | None) -> dict[str, Any]:
    """按 ROW_KEYS 顺序组装一行（键序稳定，人类输出与 JSON 同源）。"""
    return {
        "task_id": task_id,
        "category": category,
        "reason": reason,
        "queue_status": q["status"],
        "queue_started_at": q["started_at"],
        "queue_finished_at": q["finished_at"],
        "owner_pid": owner_pid,
        "run_status": run_status,
        "last_event_ts": last_event_ts,
    }


def stale_running_report(task_queue: TaskQueue, run_store: RunStore, *,
                         now: float | None = None,
                         stale_seconds: float = 900.0) -> list[dict[str, Any]]:
    """对账队列与投影，返回 stale-running 漂移行（只读，零写库）。

    Args:
        task_queue: 任务队列（tasks.db）读取侧。
        run_store: 运行投影（runs.db）读取侧。
        now: 当前时间（秒）；None = time.time()。测试注入以钉死判据。
        stale_seconds: running 行超过该阈值算过期（与 --lease-stale 同口径默认值）。

    Returns:
        漂移行列表，按 task_id 排序（稳定输出）；每行键见 ROW_KEYS。
    """
    if now is None:
        now = time.time()
    cutoff = now - stale_seconds

    # owner_pid 全量映射：list_all 的行形状（_ROW_COLS）不含 owner_pid，
    # 由 TaskQueue.owner_pids() 单独读（不改全量行形状，免牵连所有消费者）。
    owner_pids = task_queue.owner_pids()

    rows: list[dict[str, Any]] = []
    for q in sorted(task_queue.list_all(limit=100_000),
                    key=lambda r: r["task_id"]):
        task_id = q["task_id"]
        q_status = q["status"]
        owner_pid = owner_pids.get(task_id, 0)
        run = run_store.get_run(task_id)
        run_status = run["status"] if run else None

        if q_status == "running":
            # ① 陈旧且无人认领：started_at 缺失/过期 + owner_pid 缺失/已死。
            #    owner_pid 先判 falsy 再调 _pid_alive：posix 上 os.kill(0, 0)
            #    命中进程组，会把「无主」误判成「存活」。
            stale_started = not q["started_at"] or q["started_at"] < cutoff
            pid_dead = not owner_pid or not _pid_alive(owner_pid)
            if stale_started and pid_dead:
                rows.append(_row(
                    task_id, CAT_STALE_OWNER_DEAD,
                    "queue running past stale_seconds with dead/absent owner_pid",
                    q, owner_pid, run_status, _last_event_ts(run_store, task_id)))
            continue
        if q_status not in TERMINAL_STATUSES:
            # pending / claimed 等中间态：不在本报告口径内，跳过。
            continue
        # 队列已终态：投影必须跟着盖章，否则就是漂移。
        last_ts = _last_event_ts(run_store, task_id)
        if run is None:
            rows.append(_row(task_id, CAT_PROJECTION_UNSTAMPED,
                             "projection row missing", q, owner_pid,
                             run_status, last_ts))
        elif run_status == "running":
            rows.append(_row(task_id, CAT_PROJECTION_RUNNING,
                             "projection running while queue terminal", q,
                             owner_pid, run_status, last_ts))
        elif run_status not in TERMINAL_STATUSES:
            rows.append(_row(task_id, CAT_PROJECTION_UNSTAMPED,
                             f"projection not stamped terminal (status={run_status})",
                             q, owner_pid, run_status, last_ts))
        # 队列终态 + 投影终态 = 一致行，跳过。
    return rows


# ── A7-C：僵尸运行回收（显式动作，默认不跑）────────────────────────

#: 回收动作写进队列 `error` 列的固定前缀（判据钉死；CLI 人类输出与 JSON 同源）。
REAP_ERROR_PREFIX = "reaped: "


def reap_stale_running(task_queue: TaskQueue, run_store: RunStore, *,
                       now: float | None = None,
                       stale_seconds: float = 900.0,
                       dry_run: bool = False,
                       source: str = "loom reconcile --reap") -> dict[str, Any]:
    """回收分类①（stale-owner-dead）的僵尸行：终态收尾为 cancelled（显式写库）。

    只动**确证死亡**的行——队列 running 且 started_at 过期且 owner_pid 死/缺
    （判据与 `stale_running_report` 同源，不存在第二套分类）。慢任务防护：
    owner_pid 存活的行一律不碰，写前还复检一次（pid 复用导致「复活」也跳过）。

    写序（每行）：
    1. `task_queue.finish(..., "cancelled", allow_states=("running",))`——条件 UPDATE，
       竞态下读到新状态就 0 行，计入 skipped 不再往下写；
    2. `run_store.record(RUN_REAPED, ...)`——审计事件先行（`_append` 的
       INSERT OR IGNORE 顺带补出缺失的投影行，杜绝「有事件无投影」的半条事实）；
    3. `run_store.close_run("cancelled", ...)`——投影盖章 + 终态事件，条件 UPDATE
       保证「至多一条终态事件」，已终态的行不被覆写（`projection_closed=False` 披露）；
    4. 队列健康位升格 `ok`（先悲观后升格，同 finish 的 projection 参数口径）。

    投影写入失败的行停在 marker=incomplete（回到 ② 类漂移，reconcile 报告可见，
    可人工处置）——单行失败不拖垮整批，如实计入 failed。

    Args:
        task_queue: 任务队列（tasks.db）。
        run_store: 运行投影（runs.db）。
        now: 当前时间（秒）；None = time.time()。测试注入以钉死判据。
        stale_seconds: running 行算过期的阈值（与报告同口径默认值）。
        dry_run: True 只预测不写（返回的 reaped 行 action="dry-run"）。
        source: 审计事件 payload 里的动作来源标注。

    Returns:
        dict：`stale_seconds` / `dry_run` / `checked`（报告行总数）/
        `reaped` / `skipped` / `failed` / `other_categories`（②③计数，未动）。
    """
    if now is None:
        now = time.time()
    report = stale_running_report(task_queue, run_store, now=now,
                                  stale_seconds=stale_seconds)
    targets = [r for r in report if r["category"] == CAT_STALE_OWNER_DEAD]
    result: dict[str, Any] = {
        "stale_seconds": stale_seconds,
        "dry_run": dry_run,
        "checked": len(report),
        "reaped": [],
        "skipped": [],
        "failed": [],
        "other_categories": len(report) - len(targets),
    }

    for row in targets:
        task_id = row["task_id"]
        owner_pid = int(row["owner_pid"] or 0)
        # 慢任务防护最后一闸：写前复检。pid 万一「复活」（复用）就不碰。
        if owner_pid and _pid_alive(owner_pid):
            result["skipped"].append({
                "task_id": task_id,
                "reason": "owner_pid 写前复检存活（pid 复用？），跳过"})
            continue
        reason = (f"stale-owner-dead：队列 running 超过 {stale_seconds:g}s 且 "
                  f"owner_pid={owner_pid or '缺'} 已死/缺失（来源：{source}）")
        if dry_run:
            result["reaped"].append({
                "task_id": task_id, "action": "dry-run",
                "owner_pid": owner_pid, "reason": reason})
            continue

        # 1) 队列行终态（条件 UPDATE：状态已变则 0 行）。
        if not task_queue.finish(task_id, "cancelled",
                                 error=REAP_ERROR_PREFIX + reason,
                                 allow_states=("running",),
                                 projection="incomplete"):
            result["skipped"].append({
                "task_id": task_id,
                "reason": "写前队列状态已变（已收尾/已重排），跳过"})
            continue
        try:
            # 2) 审计事件先行：不是执行事实，但必须可区分、可追溯。
            run_store.record(task_id, RUN_REAPED, {
                "category": row["category"],
                "owner_pid": owner_pid,
                "stale_seconds": stale_seconds,
                "source": source,
                "queue_started_at": row["queue_started_at"],
                "queue_status_before": "running",
                "times": "queue-facts-only",
            }, ts=now)
            # 3) 投影盖章（已终态则 0 行，不覆写、不二发终态事件）。
            seq = run_store.close_run(task_id, "cancelled", finished_at=now,
                                      error=REAP_ERROR_PREFIX + reason)
            # 4) 健康位升格（失败则停在 incomplete，见 docstring）。
            upgraded = task_queue.set_projection(task_id, "ok")
        except Exception as e:  # noqa: BLE001 单行失败不拖垮整批，如实计入
            result["failed"].append({
                "task_id": task_id, "reason": f"投影写入失败：{e}"})
            continue
        result["reaped"].append({
            "task_id": task_id,
            "action": "reaped",
            "owner_pid": owner_pid,
            "projection_closed": seq is not None,
            "marker_upgraded": upgraded,
            "reason": reason,
        })
    return result


def reap_from_env(task_queue: TaskQueue, run_store: RunStore, env=None, *,
                  source: str = "startup auto-reap") -> dict[str, Any] | None:
    """服务启动钩子：读 `LOOM_REAP_STALE_SECONDS`，显式设置才跑回收。

    未设 / 空 / 非数字 / ≤0 ⇒ 返回 None（不跑、不炸；非数字记一条 warning）。
    其余情况等价 `reap_stale_running(..., stale_seconds=<env 值>)`。
    """
    env_map = os.environ if env is None else env
    raw = str(env_map.get("LOOM_REAP_STALE_SECONDS", "") or "").strip()
    if not raw:
        return None
    try:
        stale = float(raw)
    except ValueError:
        _logger.warning("LOOM_REAP_STALE_SECONDS=%r 不是数字，启动回收跳过", raw)
        return None
    if stale <= 0:
        return None
    if run_store is None:
        _logger.warning("启动回收跳过：RunStore 建口失败，分类判据不齐")
        return None
    return reap_stale_running(task_queue, run_store, stale_seconds=stale,
                              source=source)
