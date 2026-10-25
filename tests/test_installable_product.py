"""安装态判据：pip install 出来的产品必须自洽 —— product-spec §3 P0 判据 1。

原文：**干净 venv 装完后 `--check` 不红**。此前这一条被两件事同时破坏：
  1. `bootstrap.py` 把 `tests/` 写进 required_dirs、把 `agents/writer.py`、
     `pipelines/docgen.yaml` 写进必需 key_files —— 装完的 wheel 没有这两样，     自检必红（实测：26 OK / 1 WARN / 1 ERROR，红的就是 docgen.yaml）；
  2. `pipelines/*.yaml`、`quality/`、`prompts/` 不在任何 wheel 数据里 ——
     `pip install .` 装出一个"没有流水线的引擎"，装完态无解。

本文件把这两件事钉成可复现判据：
  - `TestPackagingDeclaration`：快判据，断言 pyproject 打包声明在位
    （防止修好又被删），且 bootstrap 不再点名任何 pack 的文件；
  - `TestInstalledSelfCheck`：真判据，真打 wheel → 真 venv → 真装 →
    跑 `run_startup_check()`，断言零 ERROR 且 pack 清册 > 0。

真判据的 venv 用 `--system-site-packages`（借宿主机的 artesian / PyYAML 等
运行时依赖，否则离线环境装不动），安装用 `--no-deps`：本判据验的是
"packaging 声明对不对"，不是依赖能不能拉下来。
"""
import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent


class TestPackagingDeclaration:
    """快判据：打包声明在位（不真装，只读配置）。"""

    def test_pyproject_ships_pipeline_data(self):
        data = tomllib.loads((PROJECT / "pyproject.toml").read_text(encoding="utf-8"))
        entries = data["tool"]["setuptools"].get("data-files", {})
        pipelines = entries.get("pipelines")
        assert pipelines, (
            "pyproject 的 [tool.setuptools.data-files] 缺 pipelines 条目："
            "wheel 装出来将不含任何流水线（安装态曾如此，实测 0 条可跑）")

    def test_declared_patterns_cover_lock_quality_and_prompts(self):
        """docgen pack 的数据必须全量声明进 `packs.docgen` package-data。

        拆出后它们不在顶层 pipelines/ 下，data-files 的 `pipelines` 键只剩
        散装核心流水线；quality/prompts 若漏声明 = 安装态静默缺件
        （glob 零命中不报错，2026-10-12 拆分后口径）。
        """
        data = tomllib.loads((PROJECT / "pyproject.toml").read_text(encoding="utf-8"))
        entries = data["tool"]["setuptools"]["data-files"]
        assert "pipelines" in entries
        assert any(p.endswith("*.lock") for p in entries["pipelines"]), (
            "pipelines 键缺 *.lock：装出来的流水线过不了锁校验")
        pkg = data["tool"]["setuptools"]["package-data"]["packs.docgen"]
        for pat in ("pipelines/*.yaml", "pipelines/*.lock",
                    "quality/*.yaml", "prompts/*.yaml", "pack.yaml"):
            assert pat in pkg, f"packs.docgen package-data 缺 {pat}"

    def test_bootstrap_does_not_name_pack_specific_files(self):
        """最核心的解绑判据：自检不许再点名某个 pack 的文件。"""
        src = (PROJECT / "loom_core" / "bootstrap.py").read_text(encoding="utf-8")
        for lineno, line in enumerate(src.splitlines(), 1):
            for forbidden in ("agents/writer.py", "agents/researcher.py",
                              "pipelines/docgen.yaml"):
                assert forbidden not in line, (
                    f"bootstrap.py:{lineno} 又写死了 {forbidden}："
                    f"没有 docgen 的安装态会被它判红")
        assert '"tests"' not in src, (
            "required_dirs 里又出现 tests/：安装态没有这个目录，装完必红")


class TestInstalledSelfCheck:
    """真判据：打 wheel → 装 venv → 自检零 ERROR、pack 清册非空。

    venv 用 `--system-site-packages` 借宿主机依赖（artesian / PyYAML …），
    安装用 `--no-deps`：这里验 packaging 声明，不验依赖解析。
    """

    @staticmethod
    def _venv_python(venv_dir: Path) -> Path:
        return venv_dir / ("Scripts" if os.name == "nt" else "bin") / "python"

    def _build_wheel(self, tmp_path: Path) -> Path:
        """从当前源码树打 wheel（不带依赖）。"""
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

    def _install_into_venv(self, venv_python: Path, wheel: Path) -> None:
        """装 wheel 进 venv。宿主机无 setuptools 可借时回退隔离构建。"""
        r = subprocess.run(
            [str(venv_python), "-m", "pip", "install", "--no-deps",
             "--no-build-isolation", "--disable-pip-version-check", str(wheel)],
            capture_output=True, text=True, timeout=900)
        if r.returncode != 0 and "setuptools.build_meta" in (r.stderr or ""):
            r = subprocess.run(
                [str(venv_python), "-m", "pip", "install", "--no-deps",
                 "--disable-pip-version-check", str(wheel)],
                capture_output=True, text=True, timeout=900)
        assert r.returncode == 0, (
            f"pip install failed: {r.stdout[-800:]} {r.stderr[-800:]}")

    def test_clean_venv_install_passes_startup_check(self, tmp_path):
        """P0 判据 1 原文：干净 venv 装完后 --check 不红。"""
        wheel = self._build_wheel(tmp_path)
        venv_dir = tmp_path / "venv"
        r = subprocess.run(
            [sys.executable, "-m", "venv", "--system-site-packages", str(venv_dir)],
            capture_output=True, text=True, timeout=300)
        assert r.returncode == 0, r.stderr[-800:]

        venv_python = self._venv_python(venv_dir)
        self._install_into_venv(venv_python, wheel)

        # 从既不是仓库也不是源码树的目录跑，确保导入的是安装态那份
        neutral = tmp_path / "neutral-cwd"
        neutral.mkdir()
        probe = (
            "import json\n"
            "from loom_core.bootstrap import run_startup_check\n"
            "from loom_core.pack_manifest import stats\n"
            "report = run_startup_check()\n"
            "print('PROBE ' + json.dumps(\n"
            "    {'has_errors': report.has_errors,\n"
            "     'packs': stats()['packs'],\n"
            "     'missing': stats()['agents_missing']}, ensure_ascii=False))\n"
        )
        r = subprocess.run([str(venv_python), "-c", probe], cwd=str(neutral),
                           capture_output=True, text=True, timeout=300)
        assert r.returncode == 0, r.stderr[-1500:]
        line = next(ln for ln in r.stdout.splitlines() if ln.startswith("PROBE "))
        data = json.loads(line[len("PROBE "):])
        assert data["has_errors"] is False, "安装态自检有 ERROR（P0 判据 1 未过）"
        assert data["packs"] > 0, (
            "安装态 pack 清册为空：wheel 没带上 pipelines 数据")
        assert data["missing"] == [], (
            f"安装态有引用不到的 agent: {data['missing']}")

    def test_clean_venv_demo_runs_offline(self, tmp_path):
        """A4 验收线：干净 venv 装完 `loom demo` 跑通一次完整交付。

        全程离线（demo 流水线没有检索/抓取/LLM 节点）；交付物、质量门、
        账本全部真产出——这是 P0 欠账的最后一条判据。
        """
        wheel = self._build_wheel(tmp_path)
        venv_dir = tmp_path / "venv-demo"
        r = subprocess.run(
            [sys.executable, "-m", "venv", "--system-site-packages", str(venv_dir)],
            capture_output=True, text=True, timeout=300)
        assert r.returncode == 0, r.stderr[-800:]
        venv_python = self._venv_python(venv_dir)
        self._install_into_venv(venv_python, wheel)

        neutral = tmp_path / "neutral-cwd"
        neutral.mkdir()
        loom_exe = venv_dir / ("Scripts" if os.name == "nt" else "bin") / "loom"
        r = subprocess.run([str(loom_exe), "demo"], cwd=str(neutral),
                           capture_output=True, text=True, timeout=600)
        assert r.returncode == 0, r.stdout[-1500:] + r.stderr[-1500:]
        assert "沙盒目录" in r.stdout
        assert "交付物" in r.stdout
        assert "质量门" in r.stdout, "质量门分数必须出现在 walkthrough（真评分）"
        assert "交付账本" in r.stdout
        assert neutral.glob("loom-demo-*"), "沙盒必须建在 cwd 下"
        sandbox = next(neutral.glob("loom-demo-*"))
        assert (sandbox / "deliverables" / "demo-report.md").exists()
        assert (sandbox / "deliverables" / "demo-report.html").exists()
