"""CLI 动词收敛判据 —— spec §5.3 CLI 行 / §7.3 第 7 步。

动词层：`run / plan / lock / pack / plugin`（prefix 翻译，缺省动词 = run）。
旧 flag（--plan/--dry-run/--write-lock/--list-agents/--enhance 一族）已摘除，
回归到"散装 flag"就意味着收敛被回滚。

四层判据：
  1. 翻译层：纯函数 `_split_verb` 的映射与用法报错（快，定位精确）；
  2. 摘除判据：旧 flag 经 argparse 必被拒（unrecognized；in-process + 一条真 CLI）；
  3. 门面：--help 用户可见面列出五个动词；
  4. 真 CLI 冒烟：pack list / plugin list / plan 各跑一次（含 lock 的沙盒实证）。
"""
import importlib
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parent.parent


def _run_module():
    """取当前 sys.modules 中的 run 模块实例（test_run.py 会 del 后重导，随取随用）。"""
    return importlib.import_module("run")


# ─── 1. 翻译层（纯函数）────────────────────────────

class TestSplitVerb:
    def test_no_verb_is_implicit_run(self):
        """裸输入 = 隐式 run：既有 `run.py input.md …` 形态必须照旧。"""
        verb, action, rest = _run_module()._split_verb(["input.md", "-o", "x.md"])
        assert verb is None and action is None
        assert rest == ["input.md", "-o", "x.md"]

    def test_empty_argv(self):
        verb, action, rest = _run_module()._split_verb([])
        assert (verb, action, rest) == (None, None, [])

    def test_run_verb_strips_prefix(self):
        verb, action, rest = _run_module()._split_verb(["run", "input.md", "-q", "a"])
        assert verb == "run" and action is None
        assert rest == ["input.md", "-q", "a"]

    def test_plan_verb_takes_pipeline_name(self):
        """`loom plan docgen` 的裸词 = 流水线名（翻译为 --pipeline）。"""
        verb, action, rest = _run_module()._split_verb(["plan", "docgen"])
        assert (verb, action) == ("plan", "plan")
        assert rest == ["--pipeline", "docgen"]

    def test_plan_verb_without_name(self):
        verb, action, rest = _run_module()._split_verb(["plan"])
        assert (verb, action, rest) == ("plan", "plan", [])

    def test_plan_verb_flag_forms_untouched(self):
        verb, action, rest = _run_module()._split_verb(["plan", "--pipeline-file", "x.yaml"])
        assert (verb, action) == ("plan", "plan")
        assert rest == ["--pipeline-file", "x.yaml"]

    def test_lock_verb(self):
        verb, action, rest = _run_module()._split_verb(["lock", "docgen"])
        assert (verb, action) == ("lock", "lock")
        assert rest == ["--pipeline", "docgen"]

    @pytest.mark.parametrize(("argv", "action"), [
        (["pack", "list"], "pack_list"),
        (["pack", "check"], "pack_check"),
    ])
    def test_pack_subcommands(self, argv, action):
        verb, got, rest = _run_module()._split_verb(argv)
        assert verb == "pack" and got == action and rest == []

    def test_pack_requires_known_subcommand(self, capsys):
        with pytest.raises(SystemExit) as exc:
            _run_module()._split_verb(["pack"])
        assert exc.value.code == 2
        assert "loom pack list|check" in capsys.readouterr().err

    def test_plugin_list_only(self, capsys):
        verb, action, rest = _run_module()._split_verb(["plugin", "list"])
        assert (verb, action, rest) == ("plugin", "plugin_list", [])
        with pytest.raises(SystemExit) as exc:
            _run_module()._split_verb(["plugin", "install"])
        assert exc.value.code == 2
        assert "loom plugin list" in capsys.readouterr().err


# ─── 2. 摘除判据：旧 flag 必被拒 ──────────────────

REMOVED_FLAGS = ["--plan", "--dry-run", "--write-lock", "--list-agents",
                 "--enhance", "--enhance-output", "--no-search"]


class TestRemovedFlags:
    @pytest.mark.parametrize("flag", REMOVED_FLAGS)
    def test_flag_is_unrecognized(self, flag, capsys):
        """动词收敛 = 旧散装 flag 摘除；任一回魂都要在这转红。"""
        parser = _run_module().build_arg_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args([flag])
        assert exc.value.code == 2
        assert f"unrecognized arguments: {flag}" in capsys.readouterr().err

    def test_real_cli_rejects_legacy_flag(self):
        """真 CLI 端到端一条：--plan 走真实 argv 也被拒（rc=2）。"""
        proc = subprocess.run(
            [sys.executable, str(PROJECT / "run.py"), "x.md", "--plan"],
            capture_output=True, text=True, timeout=120)
        assert proc.returncode == 2
        assert "unrecognized arguments: --plan" in proc.stderr

    def test_internal_defaults_still_exist(self):
        """内部开关仍在（动词注入用），只是不再暴露为 flag。"""
        parser = _run_module().build_arg_parser()
        args = parser.parse_args(["x.md"])
        assert args.plan is False
        assert args.write_lock is False
        assert args.list_agents is False


# ─── 3. 门面：--help 列出动词 ─────────────────────

class TestHelpSurface:
    def test_help_lists_all_verbs(self):
        proc = subprocess.run(
            [sys.executable, str(PROJECT / "run.py"), "--help"],
            capture_output=True, text=True, timeout=120)
        assert proc.returncode == 0
        visible = proc.stdout + proc.stderr
        for verb in ("loom run", "loom plan", "loom lock",
                     "loom pack list", "loom plugin list"):
            assert verb in visible, f"--help 缺动词: {verb}"


# ─── 4. 真 CLI 冒烟 ───────────────────────────────

class TestVerbSmoke:
    def _cli(self, argv):
        return subprocess.run(
            [sys.executable, str(PROJECT / "run.py"), *argv],
            capture_output=True, text=True, timeout=180, cwd=PROJECT)

    def test_pack_list_reports_physical_pack(self):
        proc = self._cli(["pack", "list"])
        assert proc.returncode == 0, proc.stderr[-400:]
        assert "docgen" in proc.stdout
        assert "[pack]" in proc.stdout

    def test_plugin_list_includes_pack_agents(self):
        """plugin list 列的是全清册（含 packs/docgen/agents 的文档件）。"""
        proc = self._cli(["plugin", "list"])
        assert proc.returncode == 0, proc.stderr[-400:]
        assert "writer" in proc.stdout and "quality_gate" in proc.stdout

    def test_plan_verb_previews_without_executing(self):
        proc = self._cli(["plan", "three_pass"])
        assert proc.returncode == 0, proc.stderr[-400:]
        assert "Level" in proc.stdout
        assert "总节点数" in proc.stdout

    def test_lock_verb_writes_lock_and_exits_without_running(self, tmp_path):
        """lock = 纯维护动词：只写锁文件，不执行流水线（沙盒里验证落点）。"""
        src = PROJECT / "pipelines" / "three_pass.yaml"
        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()
        target = sandbox / "three_pass.yaml"
        target.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        proc = self._cli(["lock", "--pipeline-file", str(target)])
        assert proc.returncode == 0, proc.stderr[-400:]
        lock = sandbox / "three_pass.lock"
        assert lock.exists(), "lock 动词没有在 YAML 同目录写出锁文件"
        assert "config_hash" in lock.read_text(encoding="utf-8")

    def test_run_verb_equals_bare_input(self):
        """两种形态进入同一个执行路径：参数解析层面断言等价。"""
        parser = _run_module().build_arg_parser()
        args_bare = parser.parse_args(["x.md", "-q", "a b"])
        _, _, rest = _run_module()._split_verb(["run", "x.md", "-q", "a b"])
        args_verb = parser.parse_args(rest)
        assert args_bare.input == args_verb.input == "x.md"
        assert args_bare.queries == args_verb.queries == ["a b"]


class TestPlanDiffFlag:
    """A5: `loom plan --diff` CLI 判据。

    `--diff` 是锁指纹比对的运维入口：比对当前流水线与 .lock 文件，
    输出人话差异项后退出（不执行）。覆盖三种场景：
      - 无锁文件 → 如实报告"无锁文件"并退出 0；
      - 锁一致 → 如实报告"锁一致，无漂移"并退出 0；
      - 锁漂移 → 逐条列出差异项并退出 0（不抛、不执行）。
    """

    def test_diff_flag_registered(self):
        """--diff 必须在 arg parser 里注册（门面判据）。"""
        parser = _run_module().build_arg_parser()
        args = parser.parse_args(["x.md", "--diff"])
        assert args.diff is True
        args2 = parser.parse_args(["x.md"])
        assert args2.diff is False

    def test_plan_diff_no_lock_file(self, tmp_path):
        """无锁文件时：如实报告，退出 0（不报错——这是运维巡检，不是执行）。"""
        src = PROJECT / "pipelines" / "three_pass.yaml"
        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()
        target = sandbox / "three_pass.yaml"
        target.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        proc = subprocess.run(
            [sys.executable, str(PROJECT / "run.py"),
             "plan", "--pipeline-file", str(target), "--diff"],
            capture_output=True, text=True, timeout=120, cwd=str(tmp_path))
        assert proc.returncode == 0, proc.stderr[-800:]
        assert "无锁文件" in proc.stdout
        assert "loom lock" in proc.stdout, "应提示用 loom lock 生成锁文件"

    def test_plan_diff_lock_consistent(self, tmp_path):
        """锁一致时：如实报告"锁一致，无漂移"，退出 0。

        先 lock 再 diff——用仓库里的 three_pass 流水线。
        """
        src = PROJECT / "pipelines" / "three_pass.yaml"
        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()
        target = sandbox / "three_pass.yaml"
        target.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        lock_proc = subprocess.run(
            [sys.executable, str(PROJECT / "run.py"),
             "lock", "--pipeline-file", str(target)],
            capture_output=True, text=True, timeout=120, cwd=str(tmp_path))
        assert lock_proc.returncode == 0, lock_proc.stderr[-800:]
        assert (sandbox / "three_pass.lock").exists(), "前提：锁文件没写出来"
        diff_proc = subprocess.run(
            [sys.executable, str(PROJECT / "run.py"),
             "plan", "--pipeline-file", str(target), "--diff"],
            capture_output=True, text=True, timeout=120, cwd=str(tmp_path))
        assert diff_proc.returncode == 0, diff_proc.stderr[-800:]
        assert "锁一致" in diff_proc.stdout
        assert "无漂移" in diff_proc.stdout

    def test_plan_diff_detects_drift(self, tmp_path):
        """锁漂移时：逐条列出差异项，退出 0（不抛、不执行）。

        锁刚写完时一致，改了 YAML 的 agent config 后锁应当报漂移。
        """
        src = PROJECT / "pipelines" / "three_pass.yaml"
        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()
        target = sandbox / "three_pass.yaml"
        target.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        lock_proc = subprocess.run(
            [sys.executable, str(PROJECT / "run.py"),
             "lock", "--pipeline-file", str(target)],
            capture_output=True, text=True, timeout=120, cwd=str(tmp_path))
        assert lock_proc.returncode == 0, lock_proc.stderr[-800:]
        # 改 YAML：改 researcher 的 max_results（改了 config → config_hash 漂）
        original = src.read_text(encoding="utf-8")
        drifted = original.replace("max_results: 20", "max_results: 99", 1)
        target.write_text(drifted, encoding="utf-8")
        diff_proc = subprocess.run(
            [sys.executable, str(PROJECT / "run.py"),
             "plan", "--pipeline-file", str(target), "--diff"],
            capture_output=True, text=True, timeout=120, cwd=str(tmp_path))
        assert diff_proc.returncode == 0, diff_proc.stderr[-800:]
        assert "漂移" in diff_proc.stdout, \
            f"改了 agent config 后 --diff 应当报漂移，stdout={diff_proc.stdout[-400:]}"
        assert "检测到" in diff_proc.stdout, "应当报告检测到 N 项漂移"
