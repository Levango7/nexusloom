import test from "node:test";
import assert from "node:assert/strict";
import {
  createLoomClient,
  createLoomClientFromEnv,
  LoomApprovalRequired,
  LoomTaskError,
} from "../nexusloom-client.mjs";

function response(status, body) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => "application/json" },
    text: async () => JSON.stringify(body),
  };
}

test("fromEnv reads the standard runtime configuration", async () => {
  const seen = [];
  const client = createLoomClientFromEnv({
    LOOM_RUNTIME_URL: "http://127.0.0.1:9999/",
    LOOM_RUNTIME_API_KEY: "fake-test-token",
  }, {
    fetchImpl: async (url, init) => {
      seen.push({ url, init });
      return response(200, { status: "ok" });
    },
  });
  await client.health();
  assert.equal(seen[0].url, "http://127.0.0.1:9999/health");
  assert.equal(seen[0].init.headers.Authorization, "Bearer fake-test-token");
});

test("generate returns the done output with one submit", async () => {
  const calls = [];
  const client = createLoomClient({
    baseUrl: "http://127.0.0.1:8910",
    token: "fake-test-token",
    fetchImpl: async (url, init) => {
      calls.push({ url, init });
      if (url.endsWith("/api/tasks")) {
        return response(200, { task_id: "task-1", status: "running" });
      }
      return response(200, { id: "task-1", status: "done", output_content: "成品" });
    },
  });
  const result = await client.generate({ query: "SDK 一次调用验收", pipeline: "demo" });
  assert.equal(result.status, "done");
  assert.equal(result.output_content, "成品");
  assert.equal(calls.filter((c) => c.url.endsWith("/api/tasks")).length, 1);
});

test("generate raises a structured error for a failed task", async () => {
  const client = createLoomClient({
    fetchImpl: async (url) => url.endsWith("/api/tasks")
      ? response(200, { task_id: "task-fail", status: "running" })
      : response(200, { id: "task-fail", status: "failed", error: "HARD_FLOOR" }),
  });
  await assert.rejects(client.generate({ query: "失败样例" }), (error) => {
    assert.ok(error instanceof LoomTaskError);
    assert.equal(error.taskId, "task-fail");
    assert.equal(error.taskStatus, "failed");
    return true;
  });
});

test("approval is explicit, never silently approved", async () => {
  const makeClient = () => createLoomClient({
    fetchImpl: async (url) => {
      if (url.endsWith("/api/tasks")) return response(200, { task_id: "task-gate" });
      if (url.endsWith("/approve")) return response(200, { approved: true, task_id: "task-gate" });
      return response(200, {
        id: "task-gate",
        status: "paused",
        awaiting_approval: { nodes: ["release_gate"] },
      });
    },
  });
  await assert.rejects(makeClient().generate({ inputs: "审批材料" }), (error) => {
    assert.ok(error instanceof LoomApprovalRequired);
    assert.equal(error.taskStatus, "paused");
    return true;
  });

  const client = createLoomClient({
    fetchImpl: async (url) => {
      if (url.endsWith("/api/tasks")) return response(200, { task_id: "task-gate" });
      if (url.endsWith("/approve")) return response(200, { approved: true, task_id: "task-gate" });
      return response(200, {
        id: "task-gate",
        status: "done",
        approvals: [{ decision: "approved", by: "tester" }],
      });
    },
  });
  const result = await client.generate({ inputs: "审批材料", approvalHandler: () => "approve" });
  assert.equal(result.status, "done");
  assert.equal(result.approvals[0].decision, "approved");
});
