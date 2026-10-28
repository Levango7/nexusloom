# 示例目录

两个参照实现，各自对应一类扩展：

- `plugin-hello/` —— **第三方插件**：最小的独立 pip 包，通过 `loom.agents`
  entry_points 提供 Agent，装好即被发现 / 注册 / 执行。
- `packs/greeting/` —— **领域件包（pack）**：`loom pack new greeting` 生成的
  同一形状（pack.yaml + agents/ + pipelines/ + prompts/ + quality/），展示
  五件套如何拼成一条可跑流水线。它**不是**产品流水线：放在 `examples/` 下，
  而 `pack_manifest` 只扫 `packs/`，所以 `--check` 与已安装流水线清单都不受
  它影响。真要加入产品，把它挪进 `packs/`。

---

# 外部插件作者模板（examples/plugin-hello）

这里是**第三方插件机制的参照实现**：一个最小的独立 pip 包，通过
`loom.agents` entry_points 提供 Agent——装好即被本仓零改动地
发现 / 注册 / 执行。

> 2026-10-11 单发行物整合：原与 hello 并列的五支实用插件
> （keyword_extract / readability / link_check / fact_coverage / tldr）
> 已并入产品为**内置件**（`agents/` 下，随 `pip install` 直接可用）。
> 本目录只留 hello 作外部作者的起点；真实第三方插件出现时，
> 其 pip 包将出现在这里描述的同一机制上。

## 契约（与内置 `agents/*.py` 同构）

- entry point 的**值必须是模块路径**（`包.模块`），group 固定
  `loom.agents`；
- 模块顶层声明 `AGENT_NAME`（`AGENT_VERSION` / `AGENT_DESC` / topics 可选），
  并至少定义一个 `BaseAgent` 子类；
- 加载器按 `__file__` 做与内置件同一套 AST 安全检查——不碰黑名单里的调用
  就**不需要**任何声明；`SANDBOX_TRUSTED = True` 只留给随产品发布的内置件；
- 同名冲突时**本仓 `agents/*.py` 优先**，插件顶不掉内置 Agent。

## 试一下

```bash
pip install ./examples/plugin-hello
python run.py --list-agents      # char_stats 带 [entry_point:char_stats] 标记
```

## 验证（与 CI 同款判据）

- `tests/test_plugin_example.py` —— 模板包契约 + **真 venv + 真 pip install**
  的端到端判据（空 agents 目录下仍被发现/注册/执行）；
- `tests/test_plugins_ecosystem.py` —— 内置化后五支的发现/注册/执行、
  模板形状、诚实计数（真实第三方 = 0）。
