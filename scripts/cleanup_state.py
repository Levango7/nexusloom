"""运行态卫生清理（默认 dry-run，`--apply` 才动盘）。

背景（实测）：在分层隔离（`loom_core/state_paths.py`）落地之前，测试直接
把运行态写进 checkout 的 `bus_data/` 与 `versions/`。留下的后果是可测的：

  - `bus_data/message_bus.db` 21k 条消息 / 1.3k 条幂等键，绝大多数属于
    `test_*`、`resume_e2e`、`parity_*`、`kb-e2e*` 这类夹具任务；
  - `versions/` 281 个目录里有 243 个的源文件已不存在（236 个指向已删除的
    `.pytest_tmp/`），`/api/versions/stats` 要扫完整个目录树；
  - 仪表盘上的质量分与任务历史因此混进夹具数据。

判据刻意保守，宁可不删：
  1. 能识别为夹具命名空间的行 → 删；
  2. 版本目录里记录的源文件已不存在 → 删目录（源文件没了，历史无从对照）；
  3. 其余一律保留（含 uuid 形式的 API 任务，无法静态判定来源）；
  4. `cost.db` 不动——那是花钱记录，不属于本工具的职责范围。

用法：
    python scripts/cleanup_state.py            # 只看报告
    python scripts/cleanup_state.py --apply    # 执行，并先把将被改写的库复制到
                                               # bus_data/archive/<UTC 时间戳>/
"""
from __future__ import annotations

import argparse
import contextlib
import json
import re
import shutil
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from loom_core import state_paths  # noqa: E402

# 夹具任务命名空间：测试与本地冒烟里出现过的 task_id 前缀/形态。
TEST_TASK_RE = re.compile(
    r"^("
    r"test[_-]"              # test_full_run / test-ckpt ...
    r"|parity[_-]"           # parity_yaml / parity_legacy2
    r"|resume_e2e"
    r"|kb-(e2e|dbg)"
    r"|real-[0-9a-f]{6,}"
    r"|redact-test"
    r"|demo[_-]"
    r"|smoke[_-]"
    r")"
)
# 单元夹具里的极短 id：'t' / 'a' / 'c-0' 这种，不可能是人提交的任务名。
TINY_TASK_RE = re.compile(r"^[a-z0-9]{1,2}(-\d+)?$")

STATE_DIR = state_paths.state_root()
VERSIONS_DIR = Path(state_paths.versions_root())
STORES = {
    "message_bus": "message_bus.db",
    "tasks": "tasks.db",
    "quality": "quality.db",
}
KEY_TABLES = {
    "message_bus": [
        ("processed_keys", "key", "key"),          # (表, 取 task_id 的列, 删除依据列)
        ("messages", "payload_json", "msg_id"),
    ],
    "tasks": [("task_queue", "task_id", "task_id")],
    "quality": [("quality_history", "task_id", "id")],
}


def _connect(db: Path) -> sqlite3.Connection:
    """本模块唯一的连接点：connect 后即设 busy_timeout=3000。

    为什么要它：这些库可能正被运行中的 loom（worker/dashboard）持写锁——裸
    connect 无 busy_timeout 时 SQLite 遇竞争立即抛 "database is locked"，
    扫描或清理会在半途炸掉（备份与提交状态对不上）。3000ms 与 approvals/
    message_store/db_backend 同口径（#129 起本仓的连接卫生线）。
    """
    con = sqlite3.connect(db)
    con.execute("PRAGMA busy_timeout=3000")
    return con


def _task_id_of(store: str, table: str, raw: str) -> str:
    """从一行的键里取出 task_id。

    processed_keys 有两种历史格式：`{task}:{node}:{attempt}` 与
    `regenerate_{task}_{node}_g{n}`；messages 则要把 payload JSON 解开。
    """
    if table == "messages":
        with contextlib.suppress(Exception):
            return str(json.loads(raw).get("task_id", ""))
        return ""
    if table == "processed_keys":
        if raw.startswith("regenerate_"):
            body = raw[len("regenerate_"):]
            for sep in ("_writer_", "_quality_gate_", "_layout_"):
                if sep in body:
                    return body.split(sep)[0]
            return body.split("_g")[0]
        return raw.split(":")[0]
    return raw


def _is_test_task(task_id: str) -> bool:
    if not task_id:
        return False
    return bool(TEST_TASK_RE.match(task_id) or TINY_TASK_RE.match(task_id))


def _scan_stores() -> tuple[dict, int, int]:
    """返回 {文件: [(表, 命中谓词的行数, 该表总行数)]}，以及命中总数与保留数。"""
    report = {}
    hits = kept = 0
    for store, fname in STORES.items():
        db = STATE_DIR / fname
        if not db.exists():
            report[fname] = []
            continue
        con = _connect(db)
        try:
            per_table = []
            for table, id_col, _pk in KEY_TABLES[store]:
                try:
                    rows = con.execute(f"select {id_col} from {table}").fetchall()  # noqa: S608
                except sqlite3.Error:
                    continue
                total = len(rows)
                matched = sum(1 for (raw,) in rows if _is_test_task(_task_id_of(store, table, str(raw))))
                per_table.append((table, matched, total))
                hits += matched
                kept += total - matched
            report[fname] = per_table
        finally:
            con.close()
    return report, hits, kept


def _task_id_samples(limit: int = 12) -> tuple[list[str], list[str]]:
    """给出将被删与被留的 task_id 样本，让判据可以被肉眼复核。"""
    raw_ids: list[str] = []
    bus = STATE_DIR / "message_bus.db"
    if bus.exists():
        con = _connect(bus)
        with contextlib.suppress(sqlite3.Error):
            raw_ids += [k for (k,) in con.execute("select key from processed_keys")]
        con.close()
    tdb = STATE_DIR / "tasks.db"
    if tdb.exists():
        con = _connect(tdb)
        with contextlib.suppress(sqlite3.Error):
            raw_ids += [t for (t,) in con.execute("select task_id from task_queue")]
        con.close()

    dropped: set[str] = set()
    kept: set[str] = set()
    for raw in raw_ids:
        tid = _task_id_of("message_bus", "processed_keys", raw)
        if not tid:
            continue
        (dropped if _is_test_task(tid) else kept).add(tid)
    return sorted(dropped)[:limit], sorted(kept)[:limit]


def _scan_versions() -> tuple[list[Path], list[Path]]:
    dead: list[Path] = []
    alive: list[Path] = []
    if not VERSIONS_DIR.exists():
        return dead, alive
    for d in sorted(VERSIONS_DIR.iterdir()):
        if not d.is_dir():
            continue
        idx = d / "index.json"
        try:
            entries = json.loads(idx.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            entries = []
        paths = [e.get("file_path", "") for e in entries if isinstance(e, dict)]
        if any(p and Path(p).exists() for p in paths):
            alive.append(d)
        else:
            dead.append(d)
    return dead, alive


def _dir_bytes(paths) -> int:
    total = 0
    for p in paths:
        if p.is_dir():
            total += sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
        elif p.exists():
            total += p.stat().st_size
    return total


def _backup(stores_touched: list[Path]) -> Path:
    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    dest = STATE_DIR / "archive" / ts
    dest.mkdir(parents=True, exist_ok=True)
    for db in stores_touched:
        if db.exists():
            shutil.copy2(db, dest / db.name)
    return dest


def _apply_store_purge(store: str, fname: str) -> int:
    db = STATE_DIR / fname
    if not db.exists():
        return 0
    con = _connect(db)
    removed = 0
    try:
        for table, id_col, _pk in KEY_TABLES[store]:
            try:
                rows = con.execute(f"select rowid, {id_col} from {table}").fetchall()
            except sqlite3.Error:
                continue
            doomed = [rid for rid, raw in rows
                      if _is_test_task(_task_id_of(store, table, str(raw)))]
            for chunk in (doomed[i:i + 500] for i in range(0, len(doomed), 500)):
                marks = ",".join("?" * len(chunk))
                con.execute(f"delete from {table} where rowid in ({marks})", chunk)
            removed += len(doomed)
        con.commit()
        con.execute("vacuum")           # 回收空间，否则文件尺寸不降
        con.execute("pragma wal_checkpoint(truncate)")
    finally:
        con.close()
    return removed


def main() -> int:
    ap = argparse.ArgumentParser(description="清理 checkout 里的测试态运行数据（默认 dry-run）")
    ap.add_argument("--apply", action="store_true", help="真正删除（删除前先备份将被改写的库）")
    ap.add_argument("--keep-versions", action="store_true", help="只清 SQLite，不动 versions/")
    args = ap.parse_args()

    print(f"状态目录: {STATE_DIR}")
    print(f"版本目录: {VERSIONS_DIR}")

    report, hits, kept = _scan_stores()
    print("\n== SQLite 运行态 ==")
    for fname, tables in report.items():
        if not tables:
            print(f"  {fname}: 不存在或无待清表")
            continue
        parts = [f"{t} {m}/{tot}" for t, m, tot in tables]
        print(f"  {fname}: " + ", ".join(parts))
    size_before = _dir_bytes([STATE_DIR / f for f in STORES.values()])
    print(f"  命中夹具命名空间 {hits} 行，保留 {kept} 行；当前占用 {size_before / 1024:.0f} KiB")
    print("  cost.db 不在本工具范围内（花钱记录），保持原样")
    dropped_s, kept_s = _task_id_samples()
    print(f"  判据样本 · 会删: {dropped_s}")
    print(f"  判据样本 · 会留: {kept_s}")

    dead_v, alive_v = ([], []) if args.keep_versions else _scan_versions()
    if not args.keep_versions:
        vsize = _dir_bytes(dead_v)
        print("\n== versions/ ==")
        print(f"  共 {len(dead_v) + len(alive_v)} 个条目；源文件已消失的 {len(dead_v)} 个"
              f"（可回收 {vsize / 1024:.0f} KiB），源文件仍在的 {len(alive_v)} 个保留")

    if not args.apply:
        print("\ndry-run 结束，未改动任何文件。加 --apply 执行。")
        return 0

    dest = _backup([STATE_DIR / f for f in STORES.values()])
    print(f"\n已备份待清库到 {dest}")
    removed = 0
    for store, fname in STORES.items():
        n = _apply_store_purge(store, fname)
        removed += n
        print(f"  {fname}: 删除 {n} 行")
    if not args.keep_versions:
        for d in dead_v:
            shutil.copytree(d, dest / "versions" / d.name, dirs_exist_ok=True)
            shutil.rmtree(d)
        print(f"  versions/: 移除 {len(dead_v)} 个死条目（副本在备份里）")
    print(f"\n完成：删除 {removed} 行；新的占用 "
          f"{_dir_bytes([STATE_DIR / f for f in STORES.values()]) / 1024:.0f} KiB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
