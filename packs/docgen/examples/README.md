# docgen pack — 示例库

本目录提供 docgen pack 的输入/输出样例，帮助你快速理解如何使用流水线。

---

## 1. 离线体验：`loom demo`（无需任何 API Key）

```bash
loom demo
```

这是最快的体验方式：一条命令、60 秒内完成一次完整的交付链演示。
输出样例见 [demo_output.md](./demo_output.md)。

`loom demo` 在沙盒目录中运行，不写真实状态目录，交付物落在沙盒的 `deliverables/` 下。

---

## 2. 完整文档生成：`docgen` 流水线（需要搜索 / LLM Key）

```bash
# 准备输入文件（样例见 demo_input.md）
cp demo_input.md input.md

# 运行完整流水线
python run.py input.md -c config.production.json -o output/report.md
```

输入文件格式：Markdown 或纯文本，首行为文档标题，后续为章节大纲或查询意图。

完整运行需要：
- 至少一个搜索引擎 Key（Bocha / Tavily / Serper / Metaso）
- 可选：LLM Key（OpenAI / DeepSeek 等），用于 writer 润色

未配置任何 Key 时，流水线会降级为 HTML 兜底引擎 + 规则模式，仍可完成但质量较低。

---

## 3. 文件说明

| 文件 | 说明 |
|------|------|
| `demo_input.md` | 完整 docgen 流水线的输入样例 |
| `demo_output.md` | `loom demo` 离线体验的输出样例 |
