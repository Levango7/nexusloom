"""核对设计文档里引用的 file:line 是否真的指向声称的东西。

用法：python scripts/check_design_anchors.py  （在仓库根跑）
每条 = (文件, 行号, 该行必须出现的子串)。任一条不符即退出 1 并打印实际行内容。

**不进 CI**：ANCHORS 钉的是 `docs/runstore-eventlog-design-2026-10-21.md` 引用当时的行号，
而那份设计正是要把这些行陆续改掉/删掉的——接进 CI 就变成"写死断言不随迁移增长"的反例。
规矩：Stage 0/1/2/3 每落地一批，手工更新对应条目或删掉该行；某 Stage 全部落地后本脚本可退役。
"""
import pathlib
import sys

ANCHORS = [
    ("loom_core/event_hook.py", 285, "def emit"),
    ("loom_core/message_bus_v3.py", 128, "save_message"),
    ("loom_core/message_store.py", 171, "sqlite3.connect"),
    ("loom_core/audit.py", 31, "def write_audit"),
    ("loom_core/deliveries.py", 61, "def record"),
    ("loom_core/approvals.py", 84, "def request"),
    ("loom_core/approvals.py", 107, "def decide"),
    ("loom_core/approvals.py", 121, "def consume"),
    ("loom_core/task_queue.py", 186, "def complete"),
    ("loom_core/task_queue.py", 110, "add_column_if_missing"),
    ("loom_core/task_queue.py", 113, "add_column_if_missing"),
    ("loom_core/task_queue.py", 117, "add_column_if_missing"),
    ("loom_core/checkpoint_manager.py", 38, "def save"),
    ("loom_core/checkpoint_manager.py", 87, "os.replace"),
    ("loom_core/run_handle.py", 52, "def status"),
    ("loom_core/run_handle.py", 103, "def artifacts"),
    ("loom_core/run_handle.py", 133, "def _ledger_record"),
    ("loom_core/run_handle.py", 178, "def to_dict"),
    ("loom_core/db_backend.py", 81, "add_column_if_missing"),
    ("loom_core/dag_executor.py", 821, "def _merge_resumed_nodes"),
    ("loom_core/dag_executor.py", 952, "def _reuse_completed_node"),
    ("loom_core/streaming.py", 154, "_history"),
    # pipeline.py 这一批随 Stage 0（异步并轨）+ Stage 1（双写口）整体下移：Stage 0 插了
    # 4 行字段 + 61 行 helper，Stage 1 又插 7 行 init + 108 行写口。**偏移不是常数**
    # （+4 / +65 / +47 / +95 / +106 并存），所以 remap 只能按整行文本对齐，不能推算。
    ("loom_core/pipeline.py", 308, "_max_task_history"),
    ("loom_core/pipeline.py", 763, "def cancel"),
    ("loom_core/pipeline.py", 776, "def resume"),
    # 原 1290「不更新 task_queue」那句 docstring 已被 Stage 0 改掉——退役它，改钉新增的
    # 共用写口：§2 那条裂缝的"已并轨"结论必须指向一个真实存在的函数。
    ("loom_core/pipeline.py", 506, "def _record_plan_terminal_state"),
    ("loom_core/pipeline.py", 696, "pipeline.started"),
    ("loom_core/pipeline.py", 1043, "pipeline.finished"),
    ("loom_core/pipeline.py", 1479, "pipeline.finished"),
    ("loom_core/pipeline.py", 1712, "record"),
    ("loom_core/pipeline.py", 1810, "pipeline.audit"),
    ("loom_core/pipeline.py", 1922, ".done"),
    ("loom_core/admin_api.py", 650, "get_task"),
    ("loom_core/admin_api.py", 1014, "list_tasks"),
    ("loom_core/admin_api.py", 1029, "list_tasks"),
    ("loom_core/admin_api.py", 1278, "def _task_checkpoint_view"),
    ("loom_core/admin_api.py", 1583, "list_tasks"),
    ("loom_core/admin_api.py", 1896, "list_tasks"),
    ("loom_core/admin_api.py", 2159, "run_plan_async"),
    ("loom_core/admin_api.py", 2229, "get_callback"),
]

root = pathlib.Path(__file__).resolve().parent.parent
bad = []
for rel, lineno, needle in ANCHORS:
    path = root / rel
    if not path.is_file():
        bad.append((rel, lineno, needle, "<文件不存在>"))
        continue
    lines = path.read_text(encoding="utf-8").splitlines()
    actual = lines[lineno - 1].strip() if 0 < lineno <= len(lines) else "<行号越界>"
    if needle not in actual:
        bad.append((rel, lineno, needle, actual))

print(f"checked={len(ANCHORS)} mismatched={len(bad)}")
for rel, lineno, needle, actual in bad:
    print(f"  {rel}:{lineno} 期望含 {needle!r} 实际 {actual[:110]!r}")
sys.exit(1 if bad else 0)
