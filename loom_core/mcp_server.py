"""MCP Server — Model Context Protocol 标准接口

让 AI agent（Claude / GPT / GLM 等）通过 MCP 协议调度 Loom。

协议: JSON-RPC 2.0 over stdio（每行一个 JSON 消息）
启动: loom --mcp（等价 python run.py --mcp）

暴露的 MCP Tools:
  - generate_document: 提交文档生成任务
  - get_task:          查询任务状态和结果
  - list_tasks:        列出所有任务
  - list_pipelines:    列出可用流水线
  - get_pipeline_info: 获取流水线详情

示例（AI agent 侧）:
  → {"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{}}}
  ← {"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"loom","version":"3.2.0"}}}

  → {"jsonrpc":"2.0","id":2,"method":"tools/list"}
  ← {"jsonrpc":"2.0","id":2,"result":{"tools":[...]}}

  → {"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"generate_document","arguments":{"query":"Python asyncio"}}}
  ← {"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"..."}]}}
"""
from __future__ import annotations

import contextlib
import os
import re
import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path
from typing import Any

from artesian.fast_json import dumps as _fast_dumps
from artesian.fast_json import loads as _fast_loads

from . import __version__ as SERVER_VERSION
from . import scheduler as _scheduler_mod
from .artifacts import task_output
from .ids import new_task_id
from .input_schema import InputValidationError, gate_freeform, prepare_inputs
from .scheduler import resolve_pipeline_name

PROTOCOL_VERSION = "2024-11-05"
#: serverInfo.name（spec §7.1「MCP serverInfo.name → loom」）
SERVER_NAME = "loom"

# 项目根锚（与 generate_document / list_pipelines 共用，消除同文件内分叉）
def _default_pipeline_from(orch) -> str:
    """运行期配置里的 default_pipeline——只认真字符串。"""
    cfg = getattr(orch, "config", None)
    value = cfg.get("default_pipeline", "") if cfg is not None and hasattr(cfg, "get") else ""
    return value if isinstance(value, str) else ""


PROJECT_ROOT = Path(__file__).resolve().parent.parent

# task_id 仅允许字母、数字、下划线、连字符（对齐 admin_api._validate_task_id）
_TASK_ID_RE = re.compile(r"^[A-Za-z0-9_\-]+$")


def _validate_task_id(task_id: str) -> bool:
    """校验 task_id 仅含字母/数字/下划线/连字符（防止路径遍历）。"""
    if not task_id or len(task_id) > 128:
        return False
    return bool(_TASK_ID_RE.match(task_id))


def _validate_output_path(path_str: str, base_dir: str | None = None) -> tuple[bool, str]:
    """校验输出路径必须落在 base_dir 范围内（白名单校验逻辑拷贝自 admin_api）。

    Returns:
        (ok, reason): ok=True 表示通过；ok=False 时 reason 为拒绝原因。
    """
    if not path_str:
        return True, ""  # 空路径不校验（交由默认逻辑）
    base = Path(base_dir or os.getcwd()).resolve()
    try:
        target = Path(path_str).resolve()
    except (OSError, ValueError) as e:
        return False, f"路径解析失败: {e}"
    try:
        target.relative_to(base)
    except ValueError:
        return False, f"路径 {path_str!r} 不在允许目录 {base!s} 内"
    return True, ""


TOOLS = [
    {
        "name": "generate_document",
        "description": "提交文档生成任务。输入一个主题，自动完成检索→抓取→写作→质量门控→检查→排版→安全落盘，输出结构化 Markdown 文档。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "文档主题/查询关键词（必填）",
                },
                "title": {
                    "type": "string",
                    "description": "文档标题（可选，默认同 query）",
                },
                "pipeline": {
                    "type": "string",
                    "description": "流水线名（可选，缺省用 config.default_pipeline；"
                                   "未配置且存在多条时会报错并列出可用值）",
                },
                "wait": {
                    "type": "boolean",
                    "description": "是否同步等待完成（可选，默认 false，建议 false 后用 get_task 轮询）",
                    "default": False,
                },
                "output": {
                    "type": "string",
                    "description": "输出文件相对路径（可选，须落在项目根目录白名单内；wait=true 时同步落盘）",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "run_workflow",
        "description": "运行任意已安装流水线（通用入口，不限于文档场景）。inputs 会作为本次运行的输入文档写入输入文件——对象按「## 键」分节序列化、字符串原样写入，流水线首层 Agent 经由输入文档/查询词读取它们。流水线在 YAML 里声明了 pipeline.inputs（类型化输入）时，inputs 先按声明逐字段校验，不合法会以带字段名的错误表返回（可用 get_pipeline_info 读声明）。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "流水线名（必填，见 list_pipelines）",
                },
                "inputs": {
                    "type": ["object", "string"],
                    "description": "本次运行的输入（可选）：对象按「## 键」分节序列化为"
                                   "输入文档；字符串原样写入。键值里的列表成 `- ` 行、"
                                   "嵌套对象序列化为 JSON",
                },
                "wait": {
                    "type": "boolean",
                    "description": "是否同步等待完成（可选，默认 false，建议 false 后用 get_task 轮询）",
                    "default": False,
                },
                "output": {
                    "type": "string",
                    "description": "输出文件相对路径（可选，须落在项目根目录白名单内；wait=true 时同步落盘）",
                },
            },
            "required": ["name"],
        },
    },
    {
        "name": "get_task",
        "description": "查询任务状态和结果（含输出文档内容）。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_id": {
                    "type": "string",
                    "description": "任务 ID",
                },
            },
            "required": ["task_id"],
        },
    },
    {
        "name": "list_tasks",
        "description": "列出所有任务（含 id/状态/流水线/进度）。",
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "list_pipelines",
        "description": "列出可用的流水线定义。",
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_pipeline_info",
        "description": "获取指定流水线的 Agent 拓扑和配置详情。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "流水线名（缺省用 config.default_pipeline；"
                                   "未配置且存在多条时会报错并列出可用值）",
                },
            },
        },
    },
]


class MCPServer:
    """MCP Server — JSON-RPC 2.0 over stdio"""

    def __init__(self, orch=None):
        self.orch = orch
        self._initialized = False

    def serve(self):
        """主循环：从 stdin 读 JSON-RPC，写响应到 stdout"""
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                request = _fast_loads(line)
            except Exception:
                self._send_error(None, -32700, "Parse error")
                continue

            response = self._handle_request(request)
            if response is not None:
                sys.stdout.write(_fast_dumps(response) + "\n")
                sys.stdout.flush()

    def _handle_request(self, request: dict) -> dict | None:
        method = request.get("method", "")
        req_id = request.get("id")
        params = request.get("params", {})

        try:
            if method == "initialize":
                requested = str(params.get("protocolVersion") or PROTOCOL_VERSION)
                return self._ok(req_id, {
                    "protocolVersion": requested,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                })
            elif method == "initialized":
                self._initialized = True
                return None
            elif method == "tools/list":
                return self._ok(req_id, {"tools": TOOLS})
            elif method == "tools/call":
                return self._handle_tool_call(req_id, params)
            elif method == "ping":
                return self._ok(req_id, {})
            else:
                return self._error(req_id, -32601, f"Method not found: {method}")
        except Exception as e:
            traceback.print_exc(file=sys.stderr)
            return self._error(req_id, -32603, f"Internal error: {e}")

    def _handle_tool_call(self, req_id: Any, params: dict) -> dict:
        name = params.get("name", "")
        args = params.get("arguments", {})

        if name == "generate_document":
            return self._tool_generate_document(req_id, args)
        elif name == "run_workflow":
            return self._tool_run_workflow(req_id, args)
        elif name == "get_task":
            return self._tool_get_task(req_id, args)
        elif name == "list_tasks":
            return self._tool_list_tasks(req_id, args)
        elif name == "list_pipelines":
            return self._tool_list_pipelines(req_id, args)
        elif name == "get_pipeline_info":
            return self._tool_get_pipeline_info(req_id, args)
        else:
            return self._error(req_id, -32602, f"Unknown tool: {name}")

    def _tool_generate_document(self, req_id: Any, args: dict) -> dict:
        query = args.get("query", "").strip()
        if not query:
            return self._tool_error(req_id, "Missing 'query' argument")

        title = args.get("title", query)
        pipeline_name = args.get("pipeline", "")
        wait = bool(args.get("wait", False))
        output_arg = str(args.get("output", "") or "")

        ok, reason = _validate_output_path(output_arg, base_dir=str(PROJECT_ROOT))
        if not ok:
            return self._tool_error(req_id, f"Invalid output path: {reason}")

        if not self.orch:
            return self._error(req_id, -32603, "Orchestrator not initialized")

        resolved, err = resolve_pipeline_name(
            pipeline_name, _scheduler_mod.installed_pipelines(),
            _default_pipeline_from(self.orch))
        if err:
            return self._tool_error(req_id, err)

        input_text = f"# {title}\n\n## 查询\n\n{query}\n"
        return self._submit_plan_run(
            req_id, resolved, wait,
            self._resolve_target_path(output_arg),
            {"query": query, "title": title},
            freeform=input_text, surface="MCP generate_document")

    def _tool_run_workflow(self, req_id: Any, args: dict) -> dict:
        """通用入口：跑任意已安装流水线，inputs 渲染成输入文档。

        与 generate_document 的差异只在"输入怎么给"：这里给的是**键→值对象**，
        所以它会过 `prepare_inputs` 这道类型化闸门（流水线声明了 `pipeline.inputs`
        时逐字段校验，错误带可定位字段）；generate_document 给的是自由文本。
        """
        name = str(args.get("name", "")).strip()
        if not name:
            return self._tool_error(req_id, "Missing 'name' argument")

        inputs = args.get("inputs", {})
        wait = bool(args.get("wait", False))
        output_arg = str(args.get("output", "") or "")

        ok, reason = _validate_output_path(output_arg, base_dir=str(PROJECT_ROOT))
        if not ok:
            return self._tool_error(req_id, f"Invalid output path: {reason}")

        if not self.orch:
            return self._error(req_id, -32603, "Orchestrator not initialized")

        resolved, err = resolve_pipeline_name(
            name, _scheduler_mod.installed_pipelines())
        if err:
            return self._tool_error(req_id, err)

        return self._submit_plan_run(
            req_id, resolved, wait,
            self._resolve_target_path(output_arg),
            {"inputs": inputs},
            inputs=inputs)

    @staticmethod
    def _resolve_target_path(output_arg: str) -> Path | None:
        if not output_arg:
            return None
        candidate = Path(output_arg)
        return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate

    def _submit_plan_run(self, req_id: Any, pipeline_name: str,
                         wait: bool, target_path: Path | None,
                         extra: dict, *,
                         inputs: Any = None, freeform: str | None = None,
                         surface: str = "") -> dict:
        """共享提交尾：解析 plan → 过输入闸门 → 临时输入文件 → run_plan → 收尾。

        generate_document 与 run_workflow 共用这一段——wait 语义、watcher 与
        临时文件清理只在单处维护，不在两条工具里各写一份。

        **先 parse 再渲染**是刻意的：类型化校验要读 `plan.inputs_schema`，而载荷一旦
        渲染成文档，键→值结构就没了（见 `input_schema` 模块注释）。`inputs` 与
        `freeform` 二选一，对应两种入口形状；自由文本在流水线声明了 inputs 时会被
        `gate_freeform` 直接拒——不留能绕过闸门的门。
        """
        task_id = new_task_id()

        try:
            from .scheduler import Scheduler
            sched = Scheduler()
            plan = sched.parse(pipeline_name)
        except Exception as e:
            return self._tool_error(req_id, f"Failed to parse pipeline: {e}")

        try:
            if inputs is not None:
                input_text = prepare_inputs(plan, inputs, title=pipeline_name)
            else:
                gate_freeform(plan, surface=surface or "MCP")
                input_text = freeform or ""
        except InputValidationError as e:
            return self._tool_error(req_id, str(e))
        except Exception as e:
            return self._tool_error(req_id, f"Failed to prepare inputs: {e}")

        input_file = Path(tempfile.gettempdir()) / f"mcp_{task_id}.md"
        input_file.write_text(input_text, encoding="utf-8")

        try:
            task = self.orch.run_plan(plan, input_file=str(input_file),
                                      task_id=task_id, wait=wait, owner="mcp")
        except Exception as e:
            self._cleanup_temp_input(input_file)
            return self._tool_error(req_id, f"Pipeline failed: {e}")

        result = {
            "task_id": task.id,
            "status": task.status.value if hasattr(task.status, "value") else str(task.status),
            "pipeline": task.pipeline_name,
            **extra,
        }

        if wait:
            result["result_keys"] = list(getattr(task, "result", {}).keys())
            result["error"] = getattr(task, "error", None)
            path, content = self._extract_output_content(task)
            if path is not None:
                result["output_path"] = str(path)
            if content is not None:
                result["output_content"] = content
            self._write_output_target(result, content, target_path)
            self._cleanup_temp_input(input_file)
        else:
            result["message"] = "task started, poll get_task for status"
            watcher = threading.Thread(
                target=self._watch_and_finalize,
                args=(str(task.id), input_file, target_path),
                daemon=True,
            )
            watcher.start()

        return self._tool_result(req_id, result)

    @staticmethod
    def _extract_output_content(task) -> tuple[Path | None, str | None]:
        """交付物取自引擎记录的 task.output_*（与 Admin API 同一份实现）。"""
        path, content = task_output(task)
        return (Path(path) if path else None), content

    @staticmethod
    def _write_output_target(result: dict, content: str | None,
                             target_path: Path | None) -> None:
        if target_path is None or not content:
            return
        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_text(content, encoding="utf-8")
            result["output_written"] = str(target_path)
        except OSError as e:
            result["output_write_error"] = str(e)

    def _cleanup_temp_input(self, input_file: Path) -> None:
        with contextlib.suppress(OSError):
            input_file.unlink()

    def _watch_and_finalize(self, task_id: str, input_file: Path,
                            target_path: Path | None,
                            max_wait_seconds: int = 3600) -> None:
        """wait=false：后台等待任务终态 → 落盘 output（若有）→ 清理临时输入文件"""
        deadline = time.time() + max_wait_seconds
        task = None
        while time.time() < deadline:
            with contextlib.suppress(Exception):
                task = self.orch.get_task(task_id) if self.orch else None
            if task is None:
                break
            status = str(getattr(task.status, "value", task.status))
            if status not in ("pending", "running", "paused"):
                break
            time.sleep(2.0)
        self._cleanup_temp_input(input_file)
        if target_path is not None and task is not None \
                and str(getattr(task.status, "value", task.status)) == "done":
            _, content = self._extract_output_content(task)
            if content:
                empty: dict = {}
                self._write_output_target(empty, content, target_path)

    def _tool_get_task(self, req_id: Any, args: dict) -> dict:
        task_id = args.get("task_id", "")
        if not _validate_task_id(task_id):
            return self._tool_error(req_id, f"Invalid task_id: {task_id!r}")
        if not self.orch:
            return self._error(req_id, -32603, "Orchestrator not initialized")

        # 存在性改由句柄回答（内存 → RunStore 投影 → 持久队列）：内存淘汰不再
        # 等于"查不到"，这是 Stage 2 要拆掉的那条旧前提。
        handle = self.orch.run_handle(task_id)
        if not handle.exists():
            return self._tool_error(req_id, f"Task not found: {task_id}")

        payload = handle.to_dict()
        # 用 .get 而不是下标：_trim_task_history 会淘汰超窗口的已完成任务，
        # 存在性检查与 to_dict() 之间被清掉时，载荷会退化成"只有身份与状态"，
        # 那种情况下也不能抛 KeyError 把整条 JSON-RPC 帧打飞。
        result = {
            "id": payload["id"],
            "status": payload["status"],
            "pipeline": payload.get("pipeline", ""),
            "result": payload.get("result", {}),
            "error": payload.get("error"),
            # 产物清单来自 RunHandle：接口消费者不必再从 result 的键里猜哪个是产出
            # （MAOP 侧此前是按节点名造伪 ID，那份猜测就属于这里该收掉的东西）
            "artifacts": payload.get("artifacts", []),
        }
        return self._tool_result(req_id, result)

    def _tool_list_tasks(self, req_id: Any, args: dict) -> dict:
        if not self.orch:
            return self._error(req_id, -32603, "Orchestrator not initialized")

        # 与 Admin /tasks 同一条读口径：内存缓存 ∪ RunStore 投影（淘汰后仍列得出）
        runs = self.orch.list_runs()
        result = {
            "tasks": [
                {
                    "id": r["id"],
                    "status": r["status"],
                    "pipeline": r["pipeline"],
                    "progress": r.get("progress", 0),
                }
                for r in runs
            ],
            "count": len(runs),
        }
        return self._tool_result(req_id, result)

    def _tool_list_pipelines(self, req_id: Any, args: dict) -> dict:
        from .pack_manifest import pipeline_dirs
        pipelines = []
        seen: set[str] = set()
        for d in pipeline_dirs():
            if not d.is_dir():
                continue
            for f in sorted(d.glob("*.yaml")):
                if f.name.startswith("test_") or f.stem in seen:
                    continue
                seen.add(f.stem)
                pipelines.append({"name": f.stem, "file": str(f)})
        return self._tool_result(req_id, {"pipelines": pipelines})

    def _tool_get_pipeline_info(self, req_id: Any, args: dict) -> dict:
        name = args.get("name", "")
        name, err = resolve_pipeline_name(name, _scheduler_mod.installed_pipelines(),
                                          _default_pipeline_from(self.orch))
        if err:
            return self._tool_error(req_id, err)
        try:
            from .scheduler import Scheduler
            sched = Scheduler()
            plan = sched.parse(name)
            nodes = [n for level in plan.levels for n in level]
            result = {
                "name": plan.pipeline_name,
                "node_count": plan.node_count,
                "levels": [[n.agent_name for n in level] for level in plan.levels],
                "agents": [
                    {
                        "name": n.agent_name,
                        "dependencies": list(n.dependencies),
                        "timeout": n.timeout,
                        "max_retries": n.max_retries,
                    }
                    for n in nodes
                ],
                # 声明出来的类型化输入：客户端据此生成表单，不用再去猜有哪些键
                "inputs": plan.inputs_schema,
            }
            return self._tool_result(req_id, result)
        except Exception as e:
            return self._tool_error(req_id, f"Failed to parse pipeline: {e}")

    def _ok(self, req_id: Any, result: dict) -> dict:
        return {"jsonrpc": "2.0", "id": req_id, "result": result}

    def _error(self, req_id: Any, code: int, message: str) -> dict:
        return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}

    def _tool_error(self, req_id: Any, message: str) -> dict:
        """工具执行失败：按 MCP 规范返回 result.content + isError:true（业务失败
        不再误用 JSON-RPC error，协议层错误仍走 _error）。"""
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "content": [{"type": "text", "text": f"Error: {message}"}],
                "isError": True,
            },
        }

    def _tool_result(self, req_id: Any, data: dict) -> dict:
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "content": [{"type": "text", "text": _fast_dumps(data, ensure_ascii=False)}],
            },
        }

    def _send_error(self, req_id: Any, code: int, message: str):
        response = self._error(req_id, code, message)
        sys.stdout.write(_fast_dumps(response) + "\n")
        sys.stdout.flush()


def run_mcp_server(orch=None):
    """启动 MCP server（stdio 模式）"""
    if orch is None:
        from . import PipelineOrchestrator
        orch = PipelineOrchestrator(
            agents_dir=str(PROJECT_ROOT / "agents"),
            checkpoint_dir=str(PROJECT_ROOT / "checkpoints"),
        )
        orch.register_agents()

    server = MCPServer(orch=orch)
    server.serve()
