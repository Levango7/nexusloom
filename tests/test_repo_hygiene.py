"""仓库卫生：冲突标记不许进库（2026-10-26 实测事故）。

事故形态：`docs/product-design-review-2026-10-20.md` 在 main 上带着未解决的
`<<<<<<<` / `=======` / `>>>>>>>` 三行标记被合了进去，而 CI 六条 required 全绿——
因为没有任何门禁扫这个。冲突标记的意思是"两种说法并存、还没裁决"，被提交进库后
读文档的人只能自己猜哪一段算数；比内容写错更难发现，因为它长得像内容。

判据分两半，缺一半都不成立：
  1. 对**真实仓库**（`git ls-files` 全量，不只 `.md`）断言零命中；
  2. 对**构造样本**断言扫描器能命中——否则扫描器写错成永远返回空，第 1 条会一直绿。

只认行首（允许缩进）的连续 7 个 `<` 或 `>`；**不认 `=======`**：markdown 的 setext
标题（下划线 `=====`）会撞车，误报会让人学会忽略这条门禁。两个标记成对出现才是冲突，
所以扫出任一方向就足够判红。
"""
from __future__ import annotations

import re
import subprocess
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent

#: 不能用字面量写标记：扫描器会扫到自己（本仓栽过"关键字扫描器命中自己"）。
_OPEN = re.compile(r"^\s*" + re.escape("<" * 7) + r"(?: |$)")
_CLOSE = re.compile(r"^\s*" + re.escape(">" * 7) + r"(?: |$)")

_BINARY_SUFFIXES = (".png", ".jpg", ".jpeg", ".gif", ".ico", ".pdf", ".woff",
                    ".woff2", ".zip", ".db", ".pyc", ".xlsx", ".pptx", ".docx")


def find_conflict_markers(files) -> list[str]:
    """返回 `文件:行号: 该行` 列表（空 = 干净）。"""
    hits: list[str] = []
    for f in files:
        path = Path(f)
        if path.suffix.lower() in _BINARY_SUFFIXES or not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for i, line in enumerate(text.splitlines(), 1):
            if _OPEN.match(line) or _CLOSE.match(line):
                hits.append(f"{f}:{i}: {line[:80]}")
    return hits


def _tracked_files() -> list[str]:
    out = subprocess.run(["git", "ls-files", "-z"], cwd=str(PROJECT),
                         capture_output=True, check=True)
    return [p for p in out.stdout.decode("utf-8").split("\0") if p]


def test_no_conflict_markers_in_tracked_files():
    hits = find_conflict_markers(_tracked_files())
    assert hits == [], "冲突标记进了库（合并冲突没解完就提交）：\n" + "\n".join(hits)


def test_scanner_actually_fires(tmp_path):
    """射程自证：扫描器命中构造样本，第 1 条才有意义。"""
    bad = tmp_path / "bad.md"
    bad.write_text(f"前一行\n{'<' * 7} HEAD\n左\n{'=' * 7}\n右\n{'>' * 7} abc123\n",
                   encoding="utf-8")
    clean = tmp_path / "clean.md"
    # setext 下划线（全是 =）与正常的 <= 运算符都不该被当成冲突
    clean.write_text("标题\n" + "=" * 30 + "\n\nif a <= b and c >= d:\n    pass\n",
                     encoding="utf-8")
    hits = find_conflict_markers([str(bad), str(clean)])
    assert len(hits) == 2, hits
    assert "bad.md:2" in hits[0] and "bad.md:6" in hits[1], hits
