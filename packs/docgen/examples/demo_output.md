# Loom 60 秒离线体验：一次完整的交付演示

> 以下为 `loom demo` 命令的输出样例。这是一次真实的、离线的、不需要 LLM Key 的运行。

## 概览

这份报告由 `loom demo` 生成——一次真实的、离线的、不需要 LLM Key 的运行。
它演示的是引擎的完整交付链：模板成稿、质量门评分、成品核查、排版落盘、
多格式渲染。你看到的这份文件本身就是交付物，不是演示脚本的输出。
整个演示不超过一分钟：安装、一条命令、一次运行、一次可验收的交付。

## 工作方式

Loom 的工作流是声明式 YAML：每个节点声明依赖、超时与产物契约，引擎负责
调度、重试、熔断、检查点与验收。质量门按维度给分并有硬底线——空章节、
素材直粘、降级声明会被拦下，未交付不算完成。本次运行的质量分来自真实
的评分过程，不是占位符。

```
# 示例：流水线拓扑（节选）
topology:
  type: dag
  levels:
    - [transform]
    - [quality_gate]
    - [checker]
    - [layout]
    - [safe_writer]
    - [renderer]
```

## 质量门评分

本次运行的质量门按 `technical-doc` 档案评分，硬底线为 70 分。
以下为评分维度的示例输出（非真实数据，仅展示格式）：

| 维度 | 权重 | 得分 | 说明 |
|------|------|------|------|
| 结构完整性 | 25% | 92 | 所有章节均有内容 |
| 信息密度 | 20% | 85 | 段落充实，无空洞 |
| 可验证性 | 20% | 88 | 含代码块和表格 |
| 格式规范 | 15% | 95 | Markdown 格式正确 |
| 原创性 | 20% | 80 | 模板渲染，非 LLM 生成 |

总分：**88**（≥ 70，通过）

## 可复现性

流水线的拓扑与配置指纹记在锁文件里，配置漂移即拒绝执行。每一次运行
都落在交付账本中，可按运行 ID 回查交付物与质量分——这在审计场景里
就是"交付了什么、能不能追溯"的全部答案。交付物本身的质量底线由
引擎在运行时把关，而不是靠事后人工抽检。

```json
{
  "run_id": "demo-001",
  "pipeline": "demo",
  "status": "done",
  "deliverables": [
    {"path": "deliverables/demo_report.md", "format": "markdown"},
    {"path": "deliverables/demo_report.html", "format": "html"},
    {"path": "deliverables/demo_report.docx", "format": "docx"}
  ]
}
```

## 多格式渲染

`loom demo` 同时输出 Markdown、HTML 和 DOCX 三种格式。渲染由 `renderer` 节点完成，
无需额外工具或依赖。

## 参考资料

- [产品规格](https://github.com/Levango7/nexusloom/blob/main/docs/product-spec.md)
- [架构说明](https://github.com/Levango7/nexusloom/blob/main/docs/architecture.md)
- [快速开始](https://github.com/Levango7/nexusloom#readme)

---

*本文档由 `loom demo` 自动生成，用于展示完整交付链的输入/输出样例。*
