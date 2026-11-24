# @nexusloom/client

零依赖的 nexusloom 嵌入 SDK，支持 Node.js 与浏览器，含 TypeScript 声明。

```js
import { createLoomClientFromEnv } from "@nexusloom/client";

const client = createLoomClientFromEnv(process.env);
const result = await client.generate({
  query: "写一份本周工作摘要",
  pipeline: "office-daily-report",
});
console.log(result.output_content);
```

环境变量：`LOOM_RUNTIME_URL`、`LOOM_RUNTIME_API_KEY`（兼容 `ADMIN_API_KEY`）。

- `done`：返回完整任务快照。
- 失败/取消：抛 `LoomTaskError`。
- 审批等待：默认抛 `LoomApprovalRequired`；只有传入 `approvalHandler` 才会继续。
- 审批回调返回 `approve` 或 `reject`，也可用 async 函数。

```js
const result = await client.generate({
  inputs: "内部已鉴权材料",
  pipeline: "approval-pipeline",
  approvalHandler: () => "approve",
});
```

`wait()` 等低层方法保持可用；长任务推荐 `submit()` 后轮询 `status()`，或直接使用 `generate()`。
