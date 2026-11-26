"""A4 安装证据债判据（2026-11-26 认领）。

A4 原文验收线：干净 venv 里 `pip install nexusloom && loom demo` 产出一份
docx + 质量报告 + 账本记录，全程无网络无 Key。

当前事实边界（2026-11-26 复核）：
- 基础安装 `pip install nexusloom` 不含 `render` extra，因此 docx 不会产出；
  demo 对缺后端的格式如实回报跳过（`skipped_formats`），不假装成功。
- docx 产出需要 `pip install nexusloom[render]`。

本文件补的是"干净 venv 独立解析依赖"这一证据缺口：
- `TestIsolatedBaseInstall`：真 venv（--no-system-site-packages）+ 独立依赖
  解析，基础安装跑 demo → md/html 真产出，docx 如实跳过；
- `TestIsolatedRenderInstall`：真 venv + `nexusloom[render]`，跑 demo →
  docx/md/html 全产出。

判据缺口（docx 是否进基础包、验收线是否改为 `nexusloom[render]`）属产品
决策，留给 owner；本批不改变打包口径，只把当前行为如实钉成可复现判据。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent


def _venv_python(venv_dir: Path) -> Path:
    return venv_dir / ("Scripts" if os.name == "nt" else "bin") / "python"


def _build_wheel(tmp_path: Path) -> Path:
    out = tmp_path / "wheelhouse"
    out.mkdir()
    r = subprocess.run(
        [sys.executable, "-m", "pip", "wheel", ".", "--no-deps",
         "--disable-pip-version-check", "-w", str(out)],
        cwd=str(PROJECT), capture_output=True, text=True, timeout=900)
    assert r.returncode == 0, (
        f"wheel build failed: {r.stdout[-800:]} {r.stderr[-800:]}")
    wheels = sorted(out.glob("*.whl"))
    assert wheels, "pip wheel produced no wheel"
    return wheels[-1]


def _make_venv(venv_dir: Path) -> Path:
    r = subprocess.run(
        [sys.executable, "-m", "venv", str(venv_dir)],
        capture_output=True, text=True, timeout=300)
    assert r.returncode == 0, r.stderr[-800:]
    return _venv_python(venv_dir)


def _install_resolved(venv_python: Path, wheel: Path, *, render: bool) -> None:
    """独立依赖解析：不借宿主机 site-packages，不跳过依赖。"""
    target = f"{wheel}[render]" if render else str(wheel)
    r = subprocess.run(
        [str(venv_python), "-m", "pip", "install",
         "--disable-pip-version-check", target],
        capture_output=True, text=True, timeout=1800)
    assert r.returncode == 0, (
        f"pip install failed: {r.stdout[-800:]} {r.stderr[-800:]}")


def _run_demo_probe(venv_python: Path, cwd: Path) -> dict:
    probe = (
        "import json\n"
        "from loom_core.demo import run_demo\n"
        "from loom_doc.renderer import supported_formats\n"
        "r = run_demo(log=lambda _m: None)\n"
        "print('PROBE ' + json.dumps({\n"
        "    'status': r.status,\n"
        "    'error': r.error,\n"
        "    'deliverables': sorted(d.name for d in r.deliverables),\n"
        "    'skipped': r.skipped_formats,\n"
        "    'ledger_status': (r.ledger or {}).get('status'),\n"
        "    'supported': supported_formats(),\n"
        "    'overall_score': (r.quality or {}).get('overall_score'),\n"
        "}, ensure_ascii=False))\n"
    )
    r = subprocess.run([str(venv_python), "-c", probe], cwd=str(cwd),
                       capture_output=True, text=True, timeout=600)
    assert r.returncode == 0, (
        f"demo probe failed: {r.stdout[-1500:]} {r.stderr[-1500:]}")
    line = next(ln for ln in r.stdout.splitlines() if ln.startswith("PROBE "))
    return json.loads(line[len("PROBE "):])


class TestIsolatedBaseInstall:
    """基础安装：md/html 真产出，docx 如实跳过。"""

    def test_base_install_demo_produces_md_html_and_skips_docx(
            self, tmp_path):
        wheel = _build_wheel(tmp_path)
        venv_python = _make_venv(tmp_path / "venv-base")
        _install_resolved(venv_python, wheel, render=False)

        neutral = tmp_path / "neutral-base"
        neutral.mkdir()
        data = _run_demo_probe(venv_python, neutral)

        assert data["status"] == "done", data
        assert data["ledger_status"] == "done", data
        assert data["overall_score"] is not None, data
        assert "demo-report.md" in data["deliverables"], data
        assert "demo-report.html" in data["deliverables"], data
        assert "demo-report.docx" not in data["deliverables"], data
        assert "docx" not in data["supported"], data
        # renderer 对不支持的格式不请求、不假装产出；skipped 可能为空
        # （renderer 过滤掉不支持格式），所以这里不要求 docx 进 skipped。
        # 关键边界是：docx 既不在交付物，也不在 supported。


class TestIsolatedRenderInstall:
    """装 render extra 后：docx 与 md/html 全产出，不跳过。"""

    def test_render_extra_install_demo_produces_docx(self, tmp_path):
        wheel = _build_wheel(tmp_path)
        venv_python = _make_venv(tmp_path / "venv-render")
        _install_resolved(venv_python, wheel, render=True)

        neutral = tmp_path / "neutral-render"
        neutral.mkdir()
        data = _run_demo_probe(venv_python, neutral)

        assert data["status"] == "done", data
        assert data["ledger_status"] == "done", data
        assert data["overall_score"] is not None, data
        assert "demo-report.md" in data["deliverables"], data
        assert "demo-report.html" in data["deliverables"], data
        assert "demo-report.docx" in data["deliverables"], data
        assert "docx" in data["supported"], data
        assert "docx" not in data["skipped"], data
