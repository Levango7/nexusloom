# 将 nexusloom 嵌入你的项目

nexusloom 可以作为其他项目的 **AI 文档交付运行时**：调用方负责登录、权限聚合与业务上下文，nexusloom 负责流水线执行、质量门、审批、恢复、产物与审计。

提供两种接入方式：

| 方式 | 适用 | 入口 |
|---|---|---|
| 应用函数调用 | Python、Node.js、浏览器/TypeScript | 零依赖 SDK＋`generate()` |
| Agent 工具调用 | 支持 MCP 的 Agent/桌面系统 | stdio 或 admin-only `POST /mcp` |

## 1. 启动运行时

```bash
export ADMIN_API_KEY='<本机管理凭据>'
python run.py --dashboard
```

默认地址 `http://127.0.0.1:8910`。嵌入方只需统一配置：

```text
LOOM_RUNTIME_URL=http://127.0.0.1:8910
LOOM_RUNTIME_API_KEY=<本机管理凭据>
```

`ADMIN_API_KEY` 也可作回退名。密钥只经环境变量或密钥服务注入，不写进源码、URL 或日志。

## 2. Python：一次创建、一次生成

```python
from loom_core import LoomClient

client = LoomClient.from_env()
result = client.generate(
    query="写一份本周工作摘要",
    pipeline="office-daily-report",
)
print(result["output_content"])
```

`from_env()` 读取 `LOOM_RUNTIME_URL` 与 `LOOM_RUNTIME_API_KEY`。也可显式创建：

```python
client = LoomClient("http://127.0.0.1:8910", token="...")
```

## 3. JavaScript / TypeScript：同一名词、同一语义

```js
import { createLoomClientFromEnv } from "./sdk/js/nexusloom-client.mjs";

const client = createLoomClientFromEnv(process.env);
const result = await client.generate({
  query: "写一份本周工作摘要",
  pipeline: "office-daily-report",
});
console.log(result.output_content);
```

`sdk/js/` 包含 `package.json` 与 `.d.ts`，可复制到你的依赖树；**当前尚未发布 npm 包**，不要写成已从 registry 安装。

## 4. 返回值与异常

`generate()` 在 `done` 时返回最后一次任务快照，含 `id`、`status`、`output_content`、`artifacts`、`approvals` 等字段。

| 情况 | Python / JavaScript 行为 |
|---|---|
| 正常完成 | 返回快照 |
| 失败/取消 | 抛 `LoomTaskError`，含任务 id、task_status 与最后快照 |
| 轮询超时 | 抛 `LoomTimeoutError` |
| HTTP/鉴权/输入错误 | 抛 `LoomError`，含 HTTP status 与响应体 |
| 人工审批等待且未给回调 | 立即抛 `LoomApprovalRequired`，不替用户批准 |

原有 `submit()`、`run_workflow()`、`status()`、`wait()`、`cancel()`、`pause()`、`resume()`、`approve()`、`reject()` 保持可用。

## 5. 人工审批：必须显式决定

```python
result = client.generate(
    inputs="内部已鉴权材料",
    pipeline="approval-pipeline",
    approval_handler=lambda snapshot: "approve",
)
```

```js
const result = await client.generate({
  inputs: "内部已鉴权材料",
  pipeline: "approval-pipeline",
  approvalHandler: async (snapshot) => "reject",
});
```

回调只能返回 `approve` 或 `reject`。驳回后任务进入 cancelled，随后按结构化任务失败抛出。**不要在无人值守场景传一个永远 approve 的回调。**

## 6. 长任务与轮询

需要自行显示进度时，使用低层接口：

```python
submitted = client.submit(query="写一份长报告", pipeline="docgen", wait=False)
snapshot = client.wait(submitted["task_id"], timeout=600)
```

`paused + awaiting_approval` 表示人工审批门，不能用 resume 绕过；普通用户暂停才使用 resume。

## 7. Agent 系统继续使用 MCP

Modulo 等已有 MCP 工具面的系统无需改成函数 SDK：

- stdio：`python run.py --mcp`
- Streamable HTTP：`POST /mcp`

MCP 同样提供 9 个工具：generate_document、run_workflow、get_task、list_tasks、list_pipelines、get_pipeline_info、approve_task、reject_task、resume_task。查询工具带 `readOnlyHint=true`，写/控制工具仍由宿主执行危险确认。

## 边界

本 SDK 解决**统一调用契约**。运行时的按需下载、安装与自动启动属于 β1「可选组件、按需下载运行时包」阶段，不在本文承诺范围内。
