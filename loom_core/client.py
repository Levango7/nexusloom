"""LoomClient — nexusloom Admin API 的 Python 客户端 SDK（产品评审 A7）。

零第三方依赖（stdlib urllib），装完即用：

    from loom_core import LoomClient

    client = LoomClient("http://127.0.0.1:8910", token="...")   # 或 api_key=...
    resp = client.submit(query="写一篇 Git rebase 指南", pipeline="docgen")
    client.wait(resp["task_id"], timeout=300)                    # 轮询到终态
    client.approve(resp["task_id"], note="放行")

信任模型（刻意不做 URL 校验的原因）：`base_url` 与 Bearer 凭证同属调用方的
部署配置，调用者即请求发起者——SDK 不做协议/主机/IP 白名单，否则默认的
`127.0.0.1:8910` 本机形态与局域网部署都被误伤。若上层应用要把**用户输入**
拼进 base_url，那是上层自己的输入校验责任（拼接出的 URL 会把凭证带过去）。

与 OpenAPI spec 同源：本模块 `_ROUTES` 登记每个公开方法的 (method, path)，
测试 `tests/test_client_sdk.py` 把它们逐一对照 `generate_spec()["paths"]`——
spec 与实现只改一边会红，这是"同源"承诺的机器判据而不是文档宣称。
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from urllib.parse import urlencode

__all__ = ["LoomClient", "LoomError", "LoomTimeoutError"]

#: `wait()` 认的终态三件套。`paused` 不在其中——它等人审批，不是运行结束；
#: 把它算成终态会让"等审批的任务"在 wait 里静默提前返回。
TERMINAL_STATUSES = frozenset({"done", "failed", "cancelled"})


class LoomError(RuntimeError):
    """HTTP 层错误。`status` 是服务端状态码；`body` 是响应 JSON（解不动时给 {"raw": ...}）。"""

    def __init__(self, status: int, body, message: str | None = None):
        self.status = status
        self.body = body
        super().__init__(message or f"HTTP {status}: {body}")


class LoomTimeoutError(LoomError):
    """`wait()` 轮询超时：任务还没进终态。status=0 表示"没拿到终态"，不是 HTTP 错误码。"""


#: 公开方法 → (method, path template)。测试对照 generate_spec()["paths"]；
#: 新增方法忘了登记 spec（或 spec 加了端点没在 SDK 露面）都会被门禁抓住。
_ROUTES: dict[str, tuple[str, str]] = {
    "health": ("GET", "/health"),
    "submit": ("POST", "/api/tasks"),
    "run_workflow": ("POST", "/api/workflows/{name}/runs"),
    "status": ("GET", "/tasks/{task_id}"),
    "wait": ("GET", "/tasks/{task_id}"),
    "artifacts": ("GET", "/tasks/{task_id}"),
    "cancel": ("POST", "/tasks/{task_id}/cancel"),
    "pause": ("POST", "/tasks/{task_id}/pause"),
    "resume": ("POST", "/tasks/{task_id}/resume"),
    "rerun": ("POST", "/tasks/{task_id}/rerun"),
    "approve": ("POST", "/tasks/{task_id}/approve"),
    "reject": ("POST", "/tasks/{task_id}/reject"),
    "approvals": ("GET", "/api/approvals"),
    "deliveries": ("GET", "/api/deliveries"),
    "delivery": ("GET", "/api/deliveries/{run_id}"),
    "locks": ("GET", "/api/locks"),
}


def routes() -> dict[str, tuple[str, str]]:
    """SDK 用到的路由表（method, path template）——供测试与文档工具对照 spec。"""
    return dict(_ROUTES)


class LoomClient:
    """nexusloom Admin API 客户端。

    凭证二选一（都给时 token 优先）：
    - `token`：tokens.yaml 里的具名凭证（Bearer）；
    - `api_key`：ADMIN_API_KEY 全局口令（Bearer）。
    两者都不给时走服务端"本机信任"——只有回环地址免鉴权，远程一律 401。
    """

    def __init__(self, base_url: str = "http://127.0.0.1:8910",
                 api_key: str = "", token: str | None = None,
                 timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        headers = {"Accept": "application/json"}
        bearer = token or api_key
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        self._headers = headers

    # ── 传输层 ──────────────────────────────────────────────────────

    def _request(self, method: str, path: str, *, body=None, params=None) -> dict:
        url = self.base_url + path
        if params:
            clean = {k: v for k, v in params.items() if v is not None}
            if clean:
                url = f"{url}?{urlencode(clean)}"
        data = json.dumps(body).encode("utf-8") if body is not None else None
        headers = dict(self._headers)
        if data is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
                if not raw:
                    return {}
                try:
                    parsed: dict = json.loads(raw.decode("utf-8"))
                    return parsed
                except ValueError:
                    return {"raw": raw.decode("utf-8", errors="replace")}
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                payload = json.loads(raw.decode("utf-8"))
            except ValueError:
                payload = {"raw": raw.decode("utf-8", errors="replace")}
            raise LoomError(e.code, payload) from None

    def _get(self, path: str, params=None) -> dict:
        return self._request("GET", path, params=params)

    def _post(self, path: str, body=None) -> dict:
        return self._request("POST", path, body=body if body is not None else {})

    # ── 健康 ────────────────────────────────────────────────────────

    def health(self) -> dict:
        """服务健康（免鉴权端点，监控探针同款）。"""
        return self._get("/health")

    # ── 提交 ────────────────────────────────────────────────────────

    def submit(self, query: str | None = None, inputs: dict | None = None,
               pipeline: str | None = None, title: str | None = None,
               wait: bool = False, output: str | None = None) -> dict:
        """提交任务。`query`（自由文本）与 `inputs`（类型化载荷）至少给一个。

        本地先判再发——缺载荷直接 ValueError，不浪费一次 HTTP 往返；
        服务端的 anyOf 语义与 OpenAPI 的 TaskSubmit schema 同一条口径。
        """
        if query is None and inputs is None:
            raise ValueError("query 或 inputs 至少给一个")
        body: dict = {}
        if query is not None:
            body["query"] = query
        if inputs is not None:
            body["inputs"] = inputs
        if pipeline is not None:
            body["pipeline"] = pipeline
        if title is not None:
            body["title"] = title
        if wait:
            body["wait"] = True
        if output is not None:
            body["output"] = output
        return self._post("/api/tasks", body)

    def run_workflow(self, pipeline: str, inputs: dict,
                     wait: bool = False, output: str | None = None) -> dict:
        """通用提交口：按流水线 `pipeline.inputs` 声明校验载荷后开一次运行。

        与 `submit` 的差别只在载荷形状——没有 query 语义，声明了 inputs 的
        流水线共用同一份契约；非法载荷 400 且错误带字段名。
        """
        body: dict = {"inputs": inputs}
        if wait:
            body["wait"] = True
        if output is not None:
            body["output"] = output
        return self._post(f"/api/workflows/{pipeline}/runs", body)

    # ── 状态 ────────────────────────────────────────────────────────

    def status(self, task_id: str) -> dict:
        """单任务详情（含结果、输出内容、DAG 视图、审批等待态、产物清单）。"""
        return self._get(f"/tasks/{task_id}")

    def wait(self, task_id: str, timeout: float = 300.0,
             poll_interval: float = 1.0) -> dict:
        """轮询到终态（done/failed/cancelled）并返回最后一次 status 快照。

        `paused` 不算终态——等审批的任务会等到超时并抛 LoomTimeoutError，
        这正是调用方该看到的行为（"还在等人审"和"已经结束"是两件事）。
        """
        deadline = time.monotonic() + timeout
        while True:
            data = self.status(task_id)
            status = str(data.get("status") or "")
            if status in TERMINAL_STATUSES:
                return data
            if time.monotonic() >= deadline:
                raise LoomTimeoutError(
                    0, {"task_id": task_id, "status": status},
                    f"等待 {task_id} 终态超时（{timeout:.0f}s，当前 {status or '未知'}）")
            time.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))

    def artifacts(self, task_id: str) -> list:
        """产物清单（RunHandle.artifacts 单源：kind=artifact 带 node+keys，
        kind=deliverable 带 path+bytes）。"""
        return self.status(task_id).get("artifacts") or []

    # ── 生命周期 ────────────────────────────────────────────────────

    def cancel(self, task_id: str) -> dict:
        return self._post(f"/tasks/{task_id}/cancel")

    def pause(self, task_id: str) -> dict:
        return self._post(f"/tasks/{task_id}/pause")

    def resume(self, task_id: str) -> dict:
        return self._post(f"/tasks/{task_id}/resume")

    def rerun(self, task_id: str) -> dict:
        return self._post(f"/tasks/{task_id}/rerun")

    # ── 审批 ────────────────────────────────────────────────────────

    def approve(self, task_id: str, note: str = "") -> dict:
        """审批放行。只受理"审批等待中"的任务——不在等待中时服务端 409，
        审计里不会出现无中生有的决策。"""
        return self._post(f"/tasks/{task_id}/approve", {"note": note})

    def reject(self, task_id: str, reason: str = "") -> dict:
        """审批驳回（驳回=终止；原因进任务 error 与审计）。"""
        return self._post(f"/tasks/{task_id}/reject", {"reason": reason})

    def approvals(self) -> list:
        """待批收件箱（跨运行、跨重启）。非 admin 只看自己的待办。"""
        return self._get("/api/approvals").get("approvals") or []

    # ── 交付账本 / 锁巡检（只读）─────────────────────────────────────

    def deliveries(self, limit: int = 50) -> list:
        """最近交付（finished_at 降序；上限 200）。"""
        return self._get("/api/deliveries", {"limit": limit}).get("deliveries") or []

    def delivery(self, run_id: str) -> dict:
        """单次交付的整条结构化载荷（含逐节点历史与健康位）。"""
        payload: dict = self._get(f"/api/deliveries/{run_id}")
        delivery: dict = payload["delivery"]
        return delivery

    def locks(self, pipeline: str | None = None) -> list:
        """锁漂移巡检：当前流水线 YAML vs 当前锁文件对不对得上。

        口径是"现在这两份文件"——不是"某次运行时用的锁对不对"（运行时的
        config_hash/topology_hash 不落库，后者今天无法回答）。
        """
        return self._get("/api/locks", {"pipeline": pipeline} if pipeline else None
                         ).get("pipelines") or []
