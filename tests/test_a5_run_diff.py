"""A5 运行间差异视图判据（2026-11-26 认领）。

治的是什么：Run 详情此前只显示本次的 `topology_hash` / 逐节点
`config_hash`，回答"这一跑用的是哪把锁"；但**没有"本次 vs 上次"**
的差异视图——同一流水线两次跑结果不同时，用户看不出是哪把锁变了。
`loom plan --diff` 只回答"当前文件 vs 锁文件"，不回答运行间差异。

本文件钉三件事：
1. 同流水线两次运行，指纹变化能在明细口读到（拓扑变 / 逐节点配置变）；
2. 首次运行（无上次）如实回空，不编造差异；
3. 上次运行同样按 principal 裁决可见性——越权当不存在处理。

判据一律走**真 HTTP**（`AdminAPI` + `urllib`），与
`tests/test_deliveries_api.py` 同一口径。
"""
from __future__ import annotations

import hashlib
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from loom_core.admin_api import AdminAPI, AdminHandler
from loom_core.pipeline import PipelineOrchestrator
from loom_core.scheduler import Scheduler

PROJECT = Path(__file__).resolve().parent.parent

INPUT = "# 标题\n\n这是运行间差异判据的测试输入。\n"

TOK_ALICE = "tok-a5-alice-secret"
TOK_BOB = "tok-a5-bob-secret"
TOK_OPS = "tok-a5-ops-secret"


def _yaml(max_sentences: int) -> str:
    return f"""
name: da_diff
agents:
  - name: tldr
    version: "0.1"
    dependencies: []
    config: {{max_sentences: {max_sentences}}}
topology:
  levels: [["tldr"]]
  edges: []
"""


def _state_dir(monkeypatch, tmp_path: Path) -> Path:
    d = tmp_path / "state"
    monkeypatch.setenv("LOOM_STATE_DIR", str(d))
    return d


def _plan(tmp_path: Path, max_sentences: int):
    d = tmp_path / "pipe_diff"
    d.mkdir(parents=True, exist_ok=True)
    f = d / "diff.yaml"
    f.write_text(_yaml(max_sentences), encoding="utf-8")
    return Scheduler(agents_dir=str(PROJECT / "agents"),
                     pipeline_dir=str(d)).parse_file(str(f), verify_lock=False)


def _orch(tmp_path: Path) -> PipelineOrchestrator:
    orch = PipelineOrchestrator(agents_dir=str(PROJECT / "agents"),
                                checkpoint_dir=str(tmp_path / "ckpt"))
    orch.register_agents()
    return orch


def _run(orch, tmp_path, task_id: str, max_sentences: int,
         owner: str = ""):
    inp = tmp_path / f"{task_id}.md"
    inp.write_text(INPUT, encoding="utf-8")
    return orch.run_plan(_plan(tmp_path, max_sentences),
                         input_file=str(inp), task_id=task_id,
                         wait=True, owner=owner)


def _wait_projection(orch, task_id: str, want: str, timeout: float = 10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = orch.task_queue.get(task_id)
        if isinstance(row, dict) and row.get("projection") == want:
            return
        time.sleep(0.05)
    raise AssertionError(f"投影健康位没到 {want!r}")


def _tokens_yaml(tmp_path: Path) -> Path:
    f = tmp_path / "tokens.yaml"
    f.write_text(f"""
tokens:
  - id: alice
    token_sha256: {hashlib.sha256(TOK_ALICE.encode()).hexdigest()}
  - id: bob
    token_sha256: {hashlib.sha256(TOK_BOB.encode()).hexdigest()}
  - id: ops
    admin: true
    token_sha256: {hashlib.sha256(TOK_OPS.encode()).hexdigest()}
""", encoding="utf-8")
    return f


def _start(orch, tokens_file: str):
    api = AdminAPI(host="127.0.0.1", port=0, api_key="",
                   tokens_file=tokens_file)
    assert api.start(orch) is True
    port = api._server.server_address[1]
    return f"http://127.0.0.1:{port}", api


def _req(method: str, url: str, token: str | None = None,
         timeout: float = 20.0):
    req = urllib.request.Request(url, method=method)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, {"raw": raw}


@pytest.fixture
def served(tmp_path, monkeypatch):
    """alice 同流水线跑两次（配置不同），bob 跑一次。"""
    _state_dir(monkeypatch, tmp_path)
    orch = _orch(tmp_path)
    _run(orch, tmp_path, "a5-alice-1", 2, owner="alice")
    time.sleep(0.05)
    _run(orch, tmp_path, "a5-alice-2", 3, owner="alice")
    time.sleep(0.05)
    _run(orch, tmp_path, "a5-bob-1", 2, owner="bob")
    for tid in ("a5-alice-1", "a5-alice-2", "a5-bob-1"):
        _wait_projection(orch, tid, "ok")
    orch._running_tasks.clear()
    base, api = _start(orch, str(_tokens_yaml(tmp_path)))
    yield base, orch
    api.stop()
    orch.shutdown()
    AdminHandler.tokens = None
    AdminHandler.orch = None
    AdminHandler.principal = None


def _detail(base: str, run_id: str, token: str):
    code, body = _req("GET", f"{base}/api/deliveries/{run_id}", token)
    assert code == 200, body
    return body["delivery"]


class TestRunToRunDiff:
    def test_config_change_is_reported(self, served):
        base, _orch = served
        d = _detail(base, "a5-alice-2", TOK_ALICE)
        assert d["previous_run_id"] == "a5-alice-1", d
        diff = d["run_diff"]
        assert diff is not None
        assert diff["topology_changed"] is False, diff
        assert diff["config_changed"] == ["tldr"], diff
        assert diff["config_added"] == [] and diff["config_removed"] == [], diff

    def test_first_run_has_no_previous(self, served):
        base, _orch = served
        d = _detail(base, "a5-alice-1", TOK_ALICE)
        assert d["previous_run_id"] == "", d
        assert d["run_diff"] is None, d

    def test_previous_run_invisible_to_other_principal(self, served):
        """bob 的上次是 alice 的运行——越权当不存在，不报差异。"""
        base, _orch = served
        d = _detail(base, "a5-bob-1", TOK_BOB)
        assert d["previous_run_id"] == "", d
        assert d["run_diff"] is None, d

    def test_admin_sees_previous_across_principals(self, served):
        base, _orch = served
        d = _detail(base, "a5-bob-1", TOK_OPS)
        assert d["previous_run_id"] == "a5-alice-2", d
        assert d["run_diff"]["topology_changed"] is False
        assert d["run_diff"]["config_changed"] == ["tldr"], d
