"""文档口径与代码实态一致性护栏。

为什么需要这一族测试：本仓的文档一直领先或落后于代码而无人发现——
README 长期写"1854 passed"而实测早已 2200+；`docs/deployment.md` 教用户
`POST /tasks` 提交任务，但该路由只有 GET（`POST` 在 `/api/tasks`）；
`llm_router` 的模块注释写"10 个供应商"而定义表里是 16 条。
此前 `tests/` 里**没有任何一条**测试引用 README 或 docs，所以这些数字
只能靠人肉发现。本文件把三处最容易腐烂的口径改成从代码取数。
"""
from __future__ import annotations

import ast
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent


# ─── 1. README 的测试数必须等于实际收集数 ───────────────────

def _readme_counts() -> tuple[int, int] | None:
    """README 那一行里的一对数：声明通过数 + 声明跳过数。

    跳过数也读出来，是因为它才是"允许差多少"的依据——容差写死成 10 的时候，
    `requires_pg` 那批环境门涨到 21 条，护栏就把一个**正确**的数字判成了过期。
    容差改成「README 自报的 skipped + 平台差」，两个数同源就不会各说各话。
    """
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    m = re.search(r"\*\*(\d+)\s*个测试本机全绿\*\*（`\1 passed", text)
    if not m:
        return None
    passed = int(m.group(1))
    m2 = re.search(r"`\d+ passed, (\d+) skipped", text)
    if not m2:
        return None
    return passed, int(m2.group(1))


#: Linux CI 比 Windows 本机**多收集**的用例数（2026-10-29 实测：3208 vs 3200 = 8，
#: 多为 POSIX-only 的路径/权限类用例）。README 那个 passed 数是 Windows 实测值，
#: 所以在 Linux 上允许它和 collected 差出这 8 条；Windows 上不放宽（精确到 skipped）。
LINUX_COLLECT_DELTA = 8


@pytest.mark.slow
def test_readme_test_count_matches_collection():
    """README 声明的通过数 == 实际收集数（减去环境门 skipif 的那些）。

    数字来自 `pytest --co`，不是写死的常量——加测试时忘了改 README 会直接红。
    """
    counts = _readme_counts()
    assert counts is not None, "README 的测试数句式被改坏了，护栏读不到数字"
    declared, declared_skipped = counts

    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "--co", "-q",
         "-p", "no:cacheprovider", "--no-header"],
        cwd=str(ROOT), capture_output=True, text=True, timeout=300,
    )
    assert proc.returncode in (0, 5), f"收集失败：{proc.stdout[-800:]}{proc.stderr[-800:]}"
    tail = proc.stdout.strip().splitlines()[-1]
    m = re.search(r"(\d+)/(\d+)\s+tests? collected", tail)
    if m:
        collected = int(m.group(1))               # 已排除 deselected 的 e2e
    else:
        m2 = re.search(r"(\d+)\s+tests? collected", tail)
        assert m2, f"读不懂 --co 的汇总行：{tail!r}"
        collected = int(m2.group(1))

    # skipif 的用例被收集但不执行，所以 declared 允许比 collected 小的部分只能
    # 是这些 skip；上界 = README 自报的 skipped 数（Windows 侧精确），Linux 上再
    # 加上实测的"多收集 8 条"。不写死一个"宽到不会红"的常量：环境门或平台差变了，
    # 这里就该有人来解释一句，而不是悄悄放进一个更大的数里。
    import sys as _sys

    platform_delta = 0 if _sys.platform.startswith("win") else LINUX_COLLECT_DELTA
    budget = declared_skipped + platform_delta
    assert 0 <= collected - declared <= budget, (
        f"README 写 {declared} passed / {declared_skipped} skipped，而实际收集 "
        f"{collected} 条；差值 {collected - declared} 超出「仅环境门 skip」的上界 "
        f"{budget}，请更新 README 的测试一节")


# ─── 2. 文档里教的 METHOD+路径必须真存在 ────────────────────

#: 规范端点本身不出现在它自己生成的 paths 里，改查 admin_api 源码是否真的处理它
SPEC_SELF = {"/api/openapi.json"}


def _spec_paths() -> dict:
    from loom_core.openapi_spec import generate_spec
    return generate_spec().get("paths", {})


def _served_by_admin(route: str) -> bool:
    src = (ROOT / "loom_core" / "admin_api.py").read_text(encoding="utf-8")
    return f'"{route}"' in src or f"'{route}'" in src


@pytest.mark.parametrize("doc", ["docs/deployment.md", "docs/api.md", "README.md"])
def test_documented_endpoints_exist(doc):
    """文档里反引号包住的 `METHOD /路径` 必须在实码里存在且支持该动词。

    只匹配反引号形式，是为了不把散文里的斜杠误当成路由。曾经的事故是
    `docs/deployment.md` 教用户用 POST 打 `/tasks`，而那条路由只有 GET。
    """
    paths = _spec_paths()
    text = (ROOT / doc).read_text(encoding="utf-8")
    refs = re.findall(r"`(GET|POST|DELETE)\s+(/[A-Za-z0-9_/{}.-]*)`", text)
    assert refs, f"{doc} 里没抓到任何端点引用，护栏需要改写法"

    missing = []
    for method, route in refs:
        if route in SPEC_SELF:
            if not _served_by_admin(route):
                missing.append(f"{method} {route}（admin_api 不再处理它）")
            continue
        norm = re.sub(r"<[^>]+>", "{x}", route)
        hit = norm if norm in paths else None
        if hit is None:
            missing.append(f"{method} {route}（规范里没有这个路径）")
        elif method.lower() not in {m.lower() for m in paths[hit]}:
            missing.append(f"{method} {route}（该路径只支持 {sorted(paths[hit])}）")
    assert not missing, f"{doc} 教了代码里不存在的端点：{missing}"


def test_tasks_is_get_only_and_docs_do_not_teach_post_tasks():
    """`/tasks` 只有 GET —— 文档不得把它写成提交任务的入口。

    写法上刻意不在 deployment.md 里留下反引号形式的 `POST /tasks`（连"别这么用"
    的提醒都不行），因为关键字扫描会命中这种自我引用；文档改用散文表述。
    """
    paths = _spec_paths()
    assert "post" not in {m.lower() for m in paths.get("/tasks", {})}, \
        "/tasks 若新增了 POST，这条护栏与 deployment.md 的说明都要同步改"
    deploy = (ROOT / "docs" / "deployment.md").read_text(encoding="utf-8")
    assert "`POST /tasks`" not in deploy
    assert "POST /api/tasks" in deploy, "提交任务的正确入口必须在文档里写明"


# ─── 3. LLM 供应商条数从代码取，不许文档写死 ─────────────────

def _provider_defs_length() -> int:
    tree = ast.parse((ROOT / "loom_core" / "llm_router.py").read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for tgt in node.targets:
                if (isinstance(tgt, ast.Name) and tgt.id == "provider_defs"
                        and isinstance(node.value, ast.List)):
                    return len(node.value.elts)
    raise AssertionError("找不到 provider_defs，护栏口径需重写")


def test_llm_router_docstring_matches_provider_table():
    src = (ROOT / "loom_core" / "llm_router.py").read_text(encoding="utf-8")
    n = _provider_defs_length()
    docstring = ast.get_docstring(ast.parse(src)) or ""
    m = re.search(r"供应商定义表\s*(\d+)\s*家", docstring)
    assert m, "llm_router 模块注释里得有「供应商定义表 N 家」这句，护栏才有效"
    assert int(m.group(1)) == n, f"注释说 {m.group(1)} 家，定义表实际 {n} 条"

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert f"{n} 供应商定价表" in readme or f"{n} 供应商" in readme, \
        f"README 的供应商口径应与定义表一致（当前 {n}）"


# ─── 4. CI 的产出判据必须与门禁契约同源 ─────────────────────

def test_ci_step_carries_the_output_fidelity_checks():
    """`.github/workflows/ci.yml` 的 docgen 步骤必须自己带着两条产出判据。

    那份 21,926 字节的素材直粘文档就是从这一步以 `OUTPUT FIDELITY OK` 的名义过的，
    所以这一步不能只依赖 quality_gate：判据若从 ci.yml 里消失（或被注释掉），
    等于门禁单方面失效，这里要红。字符串取自 loom_doc.degradation，
    不在测试里重抄一份字面量。
    """
    from loom_doc import degradation

    text = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert degradation.EMPTY_RESULT_DOC in text, \
        "ci.yml 不再检查占位语，交付物是否空洞就没人兜底了"
    assert r"^下载时间: [0-9]{4}" in text, \
        "ci.yml 不再检查抓取层原始素材签名（degradation.FETCH_TIMESTAMP_RE 的等价式）"
    assert "OUTPUT FIDELITY OK" in text, "护栏与被检对象一并失效，先确认这一步还在"


# ─── 5. E2E Nightly 的触发实态与文档口径必须同步 ─────────────

def _nightly_triggers(text: str) -> set:
    """从 YAML 解析出真实触发集合。

    必须用解析器而不是 grep：定时块现在是被注释掉的形态留在文件里
    （留着是为了"重启只需取消注释"），文本搜索会把注释当成触发，
    于是这条护栏会永远自我安慰成"定时还在"。
    """
    import yaml

    doc = yaml.safe_load(text) or {}
    # YAML 1.1 把裸 `on` 解析成布尔 True，GitHub 的 `on:` 因此有两种键形态。
    on = doc.get("on", doc.get(True, {}))
    if isinstance(on, str):
        return {on}
    return set(on or {})


def test_nightly_trigger_reader_sees_both_shapes():
    """先证明读取函数本身活着：注释掉的 schedule 不算触发，真写的算。

    双向对照——若哪天有人只改了注释而没改实触发，读取函数必须给出不同结论。
    """
    scheduled = _nightly_triggers("on:\n  schedule:\n    - cron: '0 18 * * *'\n")
    assert scheduled == {"schedule"}, f"读取函数漏掉了真定时：{scheduled}"
    dispatch_only = _nightly_triggers("on:\n  workflow_dispatch:\n")
    assert dispatch_only == {"workflow_dispatch"}, dispatch_only
    commented = _nightly_triggers(
        "on:\n  workflow_dispatch:\n  # schedule:\n  #   - cron: '0 18 * * *'\n")
    assert "schedule" not in commented, f"注释被当成了触发：{commented}"


def test_nightly_disabled_state_is_disclosed_in_docs():
    """E2E Nightly 有没有定时，README 与 CONTRIBUTING 的说法必须跟文件一致。

    2026-10-08 停用的那晚，问题不在判据而在文档：README 只写了"未配 Secret 会全部
    skip"，读者据此以为夜间回归还在跑。反向也成立——真要恢复定时，
    把 schedule 加回来的同一天这两处文档就得改，否则这里红。
    """
    text = (ROOT / ".github" / "workflows" / "e2e-nightly.yml").read_text(encoding="utf-8")
    triggers = _nightly_triggers(text)
    assert "workflow_dispatch" in triggers, \
        "e2e-nightly.yml 连手动触发都没了，Secret 配好后无法先手动验证再恢复定时"

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    contributing = (ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8")
    discloses_disabled = "已于 2026-10-08 停用" in readme

    if "schedule" in triggers:
        assert not discloses_disabled, \
            "定时触发已恢复，README 却仍说 E2E Nightly 处于停用状态"
    else:
        assert discloses_disabled, \
            "E2E Nightly 已无定时触发，README「测试」一节必须写明停用日期与重启条件"
        assert "重启条件" in readme, "停用要连同怎么恢复一起写，否则等于永久放弃"
        row = [line for line in contributing.splitlines() if "e2e-nightly.yml" in line]
        assert row, "CONTRIBUTING 的工作流表里读不到 e2e-nightly.yml 这一行，护栏需重写"
        assert "停用" in row[0], \
            f"CONTRIBUTING 仍按「定时照跑」介绍 e2e-nightly：{row[0]}"

