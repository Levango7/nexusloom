/**
 * Zero-dependency nexusloom embedding SDK for Node.js and browsers.
 * The caller owns baseUrl and credentials; secrets are never logged or persisted.
 */
export class LoomError extends Error {
  constructor(status, body, message) {
    super(message || `HTTP ${status}: ${JSON.stringify(body)}`);
    this.name = "LoomError";
    this.status = status;
    this.body = body;
  }
}

export class LoomTimeoutError extends LoomError {
  constructor(status, body, message) {
    super(status, body, message);
    this.name = "LoomTimeoutError";
  }
}

export class LoomTaskError extends LoomError {
  constructor(taskId, taskStatus, body) {
    super(0, body, `任务 ${taskId} 以 ${taskStatus || "未知"} 状态结束`);
    this.name = "LoomTaskError";
    this.taskId = taskId;
    this.taskStatus = taskStatus;
  }
}

export class LoomApprovalRequired extends LoomTaskError {
  constructor(taskId, taskStatus, body) {
    super(taskId, taskStatus, body);
    this.name = "LoomApprovalRequired";
  }
}

const TERMINAL_STATUSES = new Set(["failed", "cancelled"]);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export class LoomClient {
  constructor(options = {}) {
    this.baseUrl = String(options.baseUrl || "http://127.0.0.1:8910").replace(/\/+$/, "");
    this.token = options.token || "";
    this.timeoutMs = options.timeoutMs || 30000;
    this.fetchImpl = options.fetchImpl || globalThis.fetch;
    if (typeof this.fetchImpl !== "function") throw new TypeError("fetch implementation required");
  }

  async _request(method, path, body) {
    const headers = { Accept: "application/json" };
    if (this.token) headers.Authorization = `Bearer ${this.token}`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const init = { method, headers, signal: controller.signal };
      if (body !== undefined) {
        headers["Content-Type"] = "application/json";
        init.body = JSON.stringify(body);
      }
      const response = await this.fetchImpl(this.baseUrl + path, init);
      const text = await response.text();
      let payload = {};
      if (text) {
        try { payload = JSON.parse(text); } catch { payload = { raw: text }; }
      }
      if (!response.ok) throw new LoomError(response.status, payload);
      return payload;
    } catch (error) {
      if (error instanceof LoomError) throw error;
      throw new LoomError(0, { message: String(error && error.message || error) });
    } finally {
      clearTimeout(timer);
    }
  }

  health() {
    return this._request("GET", "/health");
  }

  submit({ query, inputs, pipeline, title, wait = false, output } = {}) {
    if (query === undefined && inputs === undefined) {
      throw new TypeError("query 或 inputs 至少给一个");
    }
    const body = {};
    if (query !== undefined) body.query = query;
    if (inputs !== undefined) body.inputs = inputs;
    if (pipeline !== undefined) body.pipeline = pipeline;
    if (title !== undefined) body.title = title;
    if (wait) body.wait = true;
    if (output !== undefined) body.output = output;
    return this._request("POST", "/api/tasks", body);
  }

  runWorkflow(pipeline, inputs, { wait = false, output } = {}) {
    const body = { inputs };
    if (wait) body.wait = true;
    if (output !== undefined) body.output = output;
    return this._request("POST", `/api/workflows/${pipeline}/runs`, body);
  }

  status(taskId) {
    return this._request("GET", `/tasks/${taskId}`);
  }

  async wait(taskId, { timeout = 300000, pollInterval = 1000 } = {}) {
    const deadline = Date.now() + timeout;
    for (;;) {
      const data = await this.status(taskId);
      const status = String(data.status || "");
      if (status === "done" || TERMINAL_STATUSES.has(status)) return data;
      if (Date.now() >= deadline) {
        throw new LoomTimeoutError(0, { task_id: taskId, status },
          `等待 ${taskId} 终态超时（当前 ${status || "未知"}）`);
      }
      await sleep(Math.min(pollInterval, Math.max(0, deadline - Date.now())));
    }
  }

  async generate({ query, inputs, pipeline, title, output, timeout = 300000,
    pollInterval = 1000, approvalHandler } = {}) {
    const submitted = await this.submit({ query, inputs, pipeline, title, output });
    const taskId = String(submitted.task_id || "");
    if (!taskId) throw new LoomError(0, submitted, "提交响应缺少 task_id");
    const deadline = Date.now() + timeout;
    for (;;) {
      const data = await this.status(taskId);
      const status = String(data.status || "");
      if (status === "done") return data;
      if (TERMINAL_STATUSES.has(status)) throw new LoomTaskError(taskId, status, data);
      if (status === "paused" && data.awaiting_approval) {
        if (!approvalHandler) throw new LoomApprovalRequired(taskId, status, data);
        const action = await approvalHandler(data);
        if (action === "approve") await this.approve(taskId);
        else if (action === "reject") await this.reject(taskId);
        else throw new TypeError("approvalHandler 必须返回 'approve' 或 'reject'");
      }
      if (Date.now() >= deadline) {
        throw new LoomTimeoutError(0, { task_id: taskId, status },
          `等待 ${taskId} 终态超时（当前 ${status || "未知"}）`);
      }
      await sleep(Math.min(pollInterval, Math.max(0, deadline - Date.now())));
    }
  }

  artifacts(taskId) {
    return this.status(taskId).then((data) => data.artifacts || []);
  }

  cancel(taskId) {
    return this._request("POST", `/tasks/${taskId}/cancel`, {});
  }

  pause(taskId) {
    return this._request("POST", `/tasks/${taskId}/pause`, {});
  }

  resume(taskId) {
    return this._request("POST", `/tasks/${taskId}/resume`, {});
  }

  approve(taskId, note = "") {
    return this._request("POST", `/tasks/${taskId}/approve`, { note });
  }

  reject(taskId, reason = "") {
    return this._request("POST", `/tasks/${taskId}/reject`, { reason });
  }
}

export function createLoomClient(options = {}) {
  return new LoomClient(options);
}

export function createLoomClientFromEnv(env = {}, options = {}) {
  return new LoomClient({
    ...options,
    baseUrl: env.LOOM_RUNTIME_URL || options.baseUrl || "http://127.0.0.1:8910",
    token: env.LOOM_RUNTIME_API_KEY || env.ADMIN_API_KEY || options.token || "",
  });
}
