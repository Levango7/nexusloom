# Python SDK（`LoomClient`）

`loom_core.LoomClient` 是 Admin API 的官方 Python 客户端：**零第三方依赖**（只用标准库
`urllib`），装完 `nexusloom` 就能用，不需要再装任何东西。

```bash
pip install nexusloom
```

它与 [HTTP API](api.md) **同源**：每个公开方法在 `loom_core/client.py` 的 `_ROUTES` 里登记
`(method, path)`，`tests/test_client_sdk.py` 把整张表逐条对照 `generate_spec()["paths"]`
（规范化路径参数），本文档的方法表又被 `tests/test_doc_consistency.py` **双向**对照
`_ROUTES`——文档、实现、OpenAPI 三处只改一边都会红。

## 快速开始

```python
from loom_core import LoomClient, LoomError, LoomTimeoutError

client = LoomClient("http://127.0.0.1:8910", token="alice-2026")   # 或 api_key=...

resp = client.submit(query="写一篇 Git rebase 指南", pipeline="docgen")
task = client.wait(resp["task_id"], timeout=300)      # 轮询到 done/failed/cancelled
print(task["status"])
for art in client.artifacts(resp["task_id"]):
    print(art.get("kind"), art.get("path") or art.get("keys"))
```

声明了 `inputs` 的流水线走通用入口（载荷按 `pipeline.inputs` 校验，非法载荷 400 且带字段名）：

```python
client.run_workflow("tldr", {"max_sentences": 2}, wait=True)
```

## 认证

三种形态，`token` 优先于 `api_key`：

| 参数 | 含义 |
|------|------|
| `token="..."` | `tokens.yaml` 里的具名凭证（Bearer），带 principal，按租户隔离 |
| `api_key="..."` | `ADMIN_API_KEY` 全局口令（Bearer），等同 admin |
| 都不给 | 走服务端「本机信任」：**只有回环地址免鉴权**，远程一律 401 |

**信任模型（刻意不做 URL 校验）**：`base_url` 与 Bearer 凭证同属调用方的部署配置，调用者
就是请求发起者——SDK 不做协议/主机/IP 白名单，否则默认的 `127.0.0.1:8910` 本机形态与局域网
部署都会被误伤。若上层应用要把**用户输入**拼进 `base_url`，那是上层自己的输入校验责任
（拼出来的 URL 会把凭证一起带过去）。同理，SDK 不做重定向跟随之外的任何网络安全策略。

## 方法表

`状态`/`wait`/`artifacts` 打的是**同一个**端点，差别只在语义（见下方「语义边界」）。

| 方法 | HTTP | 路由 | 用途 |
|------|------|------|------|
| `health` | GET | `/health` | 服务健康（免鉴权，监控探针同款） |
| `submit` | POST | `/api/tasks` | 提交任务：`query`（自由文本）或 `inputs`（类型化载荷）至少给一个 |
| `run_workflow` | POST | `/api/workflows/{name}/runs` | 通用提交口：按 `pipeline.inputs` 声明校验载荷 |
| `status` | GET | `/tasks/{task_id}` | 单任务详情（结果、输出、DAG 视图、审批等待态、产物清单） |
| `wait` | GET | `/tasks/{task_id}` | 轮询到终态并返回最后一次快照（超时抛 `LoomTimeoutError`） |
| `artifacts` | GET | `/tasks/{task_id}` | 产物清单（`kind=artifact` 带 node+keys，`kind=deliverable` 带 path+bytes） |
| `cancel` | POST | `/tasks/{task_id}/cancel` | 取消运行 |
| `pause` | POST | `/tasks/{task_id}/pause` | 暂停运行（在层边界生效） |
| `resume` | POST | `/tasks/{task_id}/resume` | 恢复被暂停/被重启打断的运行 |
| `rerun` | POST | `/tasks/{task_id}/rerun` | 复用上一次的 plan 与输入重跑 |
| `approve` | POST | `/tasks/{task_id}/approve` | 审批放行（只受理「审批等待中」的任务，否则 409） |
| `reject` | POST | `/tasks/{task_id}/reject` | 审批驳回（驳回 = 终止；原因进任务 error 与审计） |
| `approvals` | GET | `/api/approvals` | 待批收件箱（跨运行、跨重启；非 admin 只看自己的） |
| `deliveries` | GET | `/api/deliveries` | 最近交付（`limit` 默认 50、上限 200，按 `finished_at` 降序） |
| `delivery` | GET | `/api/deliveries/{run_id}` | 单次交付的整条结构化载荷（逐节点历史 + 健康位 + 锁指纹 + 逐产物摘要） |
| `locks` | GET | `/api/locks` | 锁漂移巡检：**当前**流水线 YAML 与**当前**锁文件对不对得上 |

## 错误模型

| 异常 | 何时抛 | 带什么 |
|------|--------|--------|
| `LoomError` | 服务端返回 4xx/5xx | `status`（HTTP 码）、`body`（响应 JSON；解不动给 `{"raw": ...}`） |
| `LoomTimeoutError` | `wait()` 轮询超时 | `status=0`（**不是** HTTP 码）、`body={"task_id", "status"}` |
| `ValueError` | `submit()` 本地前置校验：`query` 与 `inputs` 都没给 | 不发 HTTP，省一次往返 |
| `urllib.error.URLError` | 连不上/网络层失败 | 原样上抛，**不**包装成 `LoomError`——"服务没回话"和"服务回了错误"是两件事 |

`LoomTimeoutError` 继承 `LoomError`，所以 `except LoomError` 能一把兜住，需要区分时再先捕子类。

## 语义边界（这几条不写清最容易误用）

- **`paused` 不是终态**：`wait()` 只认 `done`/`failed`/`cancelled`。等人审批的任务会等到超时
  并抛 `LoomTimeoutError`——"还在等人审"与"已经结束"是两件事，合并成一个返回值会让调用方
  以为跑完了。
- **`approve()` 只受理等待中的任务**：不在闸门上时服务端 409，审计里不会出现无中生有的决策。
  收件箱用 `approvals()` 取，别靠轮询 `status()` 猜。
- **`deliveries(limit)` 没有翻页**：只有"最近 N 条"（上限 200）。要更早的历史请按 `run_id`
  用 `delivery(run_id)` 直取，或走 CLI/账本。
- **`locks()` 回答的是「现在」**：当前 YAML vs 当前锁文件。某次运行**当时**用的锁指纹可以从
  `delivery(run_id)` 的 `topology_hash` / `config_hashes` 与逐节点 `artifacts` 里读（v3.15.0 起
  落库）——两者不要混。
- **产物内容不在 SDK 里下载**：`artifacts()` 给清单（路径/字节数/摘要），取文件本身请按
  `path` 读服务端文件系统或用运维侧通道；SDK 目前没有产物下载方法（见下）。

## 还没做（明确写清，避免当成"应该有"）

- **异步客户端**：没有 `AsyncLoomClient`。异步调用方请自己在线程池里包一层。
- **自动重试 / 退避**：没有。提交类请求（POST）重放可能产生第二次运行，SDK 不在传输层替
  调用方做这个决定。
- **分页遍历 / 流式**：没有，见上面 `deliveries(limit)` 与 `/stream`（SSE 目前只有 HTTP 面）。
- **产物下载端点**：没有（要先定"路径越界"的口径）。

## 与其它入口的关系

同一批操作在 CLI（`loom ...`）、MCP（`run_workflow` 等工具）、HTTP 三个面上语义一致；
SDK 只是 HTTP 面的薄封装，不引入第四条实现路径。选型建议：脚本/CI 用 CLI，Python 服务用
SDK，模型侧集成用 MCP。
