"""检查点管理器 —— 负责断点续传、检查点保存/加载/清理"""
from __future__ import annotations

import contextlib
import json
import os
import re
import tempfile
import time
import warnings
from pathlib import Path

# 安全修复 (P0): task_id 路径遍历防护 —— 仅允许字母/数字/下划线/连字符
_TASK_ID_RE = re.compile(r"^[A-Za-z0-9_\-]+$")


def _validate_task_id(task_id: str) -> bool:
    """校验 task_id 仅含字母/数字/下划线/连字符（防止路径遍历）。"""
    if not task_id or len(task_id) > 128:
        return False
    return bool(_TASK_ID_RE.match(task_id))


class CheckpointManager:
    """断点续传管理"""

    def __init__(self, checkpoint_dir: str = "checkpoints", logger=None):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self._logger = logger
        self.save_failure_count = 0

    def _log(self, level: str, msg: str, **kw):
        """结构化日志记录"""
        if self._logger:
            self._logger.log(level, msg, **kw)

    def save(self, task, full_state: bool = False, agent_snapshots: dict = None):
        """保存断点（原子写：同目录 tempfile + os.replace；失败上浮 RuntimeWarning 并计数）"""
        # 安全修复 (P0): task_id 路径遍历防护
        if not _validate_task_id(task.id):
            raise ValueError(f"无效的 task_id: {task.id!r}")
        data = task.to_dict()
        if full_state:
            data["_result"] = {
                k: v for k, v in task.result.items()
            }
            data["_steps"] = [
                {
                    "step_name": s.step_name,
                    "agent_name": s.agent_name,
                    "status": s.status,
                    "started_at": s.started_at,
                    "finished_at": s.finished_at,
                    "error": s.error,
                }
                for s in task.steps
            ]
            data["_dag_nodes"] = {
                name: {
                    "status": n.status,
                    "error": n.error,
                    "attempts": n.attempts,
                    "started_at": n.started_at,
                    "finished_at": n.finished_at,
                    "result": n.result,
                }
                for name, n in (task.dag_nodes or {}).items()
            }
        if agent_snapshots:
            data["agent_snapshots"] = agent_snapshots
        # 落点必须与 load()/remove() 同源（本 manager 的目录）。
        # 此前 save 用 task.checkpoint_file（orchestrator 侧字段，会被
        # config 的 checkpoint.dir 覆盖成另一个位置），而 load 用本目录——
        # 两处不同源时"保存成功但 resume 读不到"，且随 cwd 漂移（相对路径）。
        # 保存成功后把 task.checkpoint_file 回写成真实落点，展示字段不再说谎。
        final_path = self.checkpoint_dir / f"{task.id}.json"
        final_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            dir=str(final_path.parent), prefix=final_path.name + ".", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, str(final_path))
            task.checkpoint_file = str(final_path)
        except Exception as e:
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            self.save_failure_count += 1
            self._log("error", "保存断点失败", task_id=task.id,
                      error=str(e), failures=self.save_failure_count)
            warnings.warn(
                f"checkpoint save failed for task {task.id}: {e}",
                RuntimeWarning,
                stacklevel=2,
            )

    def load(self, task_id: str):
        """加载断点（基础信息 + DAG 节点状态），返回 (task, agent_snapshots) 元组"""
        # 安全修复 (P0): task_id 路径遍历防护
        if not _validate_task_id(task_id):
            raise ValueError(f"无效的 task_id: {task_id!r}")
        from .pipeline import PipelineTask, TaskNode, TaskStatus

        checkpoint_file = self.checkpoint_dir / f"{task_id}.json"
        if not checkpoint_file.exists():
            return None, None

        try:
            with open(checkpoint_file, encoding="utf-8") as f:
                data = json.load(f)

            # 断点完整性校验
            required = ["id", "pipeline", "input"]
            missing = [k for k in required if k not in data]
            if missing:
                self._log("warning", "断点损坏", missing=missing)
                return None, None

            # 验证关键字段不为空
            if not data.get("id") or not data.get("pipeline"):
                self._log("warning", "断点无效")
                return None, None

            task = PipelineTask(
                id=data["id"],
                pipeline_name=data["pipeline"],
                input_file=data.get("input", ""),
                config=data.get("config", {}),
                status=TaskStatus.PAUSED,
                current_step=len(data.get("steps", [])),
                # token 身份：断点读回不丢归属（旧断点无此键 → 空串=无身份世界）
                owner=str(data.get("owner", "") or ""),
            )
            task.result = data.get("_result", {})
            # 旧断点没有这两个键时保持空串；新断点必须恢复审批前已落盘的交付事实，
            # 否则跨进程 approve → resume 会在交付契约处误报“没有交付物”。
            task.output_path = str(data.get("output_path", "") or "")
            task.output_content = str(data.get("output_content", "") or "")

            restored_nodes = {}
            for name, snap in (data.get("_dag_nodes") or {}).items():
                if not isinstance(snap, dict):
                    continue
                restored_nodes[name] = {
                    "status": snap.get("status", "pending"),
                    "attempts": int(snap.get("attempts", 0) or 0),
                    "error": snap.get("error", "") or "",
                    "result": snap.get("result") or {},
                    "finished_at": snap.get("finished_at", 0) or 0,
                }
            if "_dag_nodes" in data:
                task.dag_nodes = {}
                for name, snap in restored_nodes.items():
                    node = TaskNode(name=name, agent_name=name)
                    node.status = snap["status"]
                    node.attempts = snap["attempts"]
                    node.error = snap["error"]
                    node.result = dict(snap["result"])
                    node.finished_at = float(snap["finished_at"] or 0)
                    task.dag_nodes[name] = node
                task._resumed_node_snapshots = restored_nodes

            agent_snapshots = data.get("agent_snapshots", {})
            return task, agent_snapshots
        except Exception as e:
            self._log("error", "加载断点失败", error=str(e))
            return None, None

    def remove(self, task_id: str):
        """移除断点文件"""
        # 安全修复 (P0): task_id 路径遍历防护
        if not _validate_task_id(task_id):
            raise ValueError(f"无效的 task_id: {task_id!r}")
        checkpoint_file = self.checkpoint_dir / f"{task_id}.json"
        try:
            if checkpoint_file.exists():
                checkpoint_file.unlink()
        except Exception as e:
            self._log("warning", "移除断点文件失败", error=str(e))

    def cleanup_old(self, max_age_days: int = 7):
        """清理过期的 checkpoint 和报告文件"""
        cutoff = time.time() - max_age_days * 86400
        for f in self.checkpoint_dir.iterdir():
            if f.is_file() and f.stat().st_mtime < cutoff:
                try:
                    f.unlink()
                except OSError as e:
                    self._log("warning", "清理过期 checkpoint 失败", file=str(f), error=str(e))
