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
    # Stage 3a：审批的三个写点从 approvals.py 搬进了 RunStore（投影 + 同事务事件）。
    # §1 那一行"审批本身就是一个写面"的结论必须指向新位置，而不是退役成无处可指。
    # Stage 3c 补账把这三口整体下推 12 行（`close_run` 长成条件裁决版）。
    ("loom_core/run_store.py", 300, "def open_approval"),
    ("loom_core/run_store.py", 321, "def decide_approval"),
    ("loom_core/run_store.py", 362, "def consume_approval"),
    # Stage 3c（补账）的两处新落点：设计文档 §4 末"至多一条终态事件已下沉到存储层"
    # 这句话必须指向一个真实存在的函数，而不是只写在文档里。
    ("loom_core/run_store.py", 258, "def close_run"),
    ("loom_core/backfill.py", 35, "class RunBackfill"),
    ("loom_core/backfill.py", 59, "def replay"),
    # Stage 3b：`complete()` 摘掉 result 参数，整体下移到 203。
    ("loom_core/task_queue.py", 203, "def complete"),
    # Stage 3b 给 task_queue 加了第 4 列（projection 健康位），迁移调用也跟着变成 4 处。
    ("loom_core/task_queue.py", 120, "add_column_if_missing"),
    ("loom_core/task_queue.py", 123, "add_column_if_missing"),
    ("loom_core/task_queue.py", 127, "add_column_if_missing"),
    ("loom_core/task_queue.py", 134, "add_column_if_missing"),
    ("loom_core/checkpoint_manager.py", 38, "def save"),
    ("loom_core/checkpoint_manager.py", 87, "os.replace"),
    ("loom_core/run_handle.py", 124, "def status"),
    # Stage 3b：`projection()` 新读维插在 artifacts 之前，三个方法一起下移 25 行。
    ("loom_core/run_handle.py", 203, "def artifacts"),
    ("loom_core/run_handle.py", 261, "def _ledger_record"),
    ("loom_core/run_handle.py", 306, "def to_dict"),
    ("loom_core/db_backend.py", 81, "add_column_if_missing"),
    ("loom_core/dag_executor.py", 821, "def _merge_resumed_nodes"),
    ("loom_core/dag_executor.py", 952, "def _reuse_completed_node"),
    ("loom_core/streaming.py", 154, "_history"),
    # pipeline.py 这一批随 Stage 0（异步并轨）+ Stage 1（双写口）整体下移：Stage 0 插了
    # 4 行字段 + 61 行 helper，Stage 1 又插 7 行 init + 108 行写口。**偏移不是常数**
    # （+4 / +65 / +47 / +95 / +106 并存），所以 remap 只能按整行文本对齐，不能推算。
    # Stage 3b 再钉一次：`_record_plan_terminal_state` 里的"先悲观后升格"与
    # `_runstore_close` 让下面的调用点又各自下移（同样不是常数偏移）。
    # Stage 3c 再钉一次：cancel() 结清待批 + 补账两个入口把 pipeline.py 又推了 18~43 行。
    ("loom_core/pipeline.py", 315, "_max_task_history"),
    ("loom_core/pipeline.py", 837, "def cancel"),
    ("loom_core/pipeline.py", 875, "def resume"),
    # 原 1290「不更新 task_queue」那句 docstring 已被 Stage 0 改掉——退役它，改钉新增的
    # 共用写口：§2 那条裂缝的"已并轨"结论必须指向一个真实存在的函数。
    ("loom_core/pipeline.py", 513, "def _record_plan_terminal_state"),
    ("loom_core/pipeline.py", 770, "pipeline.started"),      # legacy run() 内
    ("loom_core/pipeline.py", 1137, "pipeline.finished"),    # _finalize_plan_task 内
    ("loom_core/pipeline.py", 1590, "pipeline.finished"),    # _finalize_plan_task_async 内
    ("loom_core/pipeline.py", 1824, "record"),               # _deliver 内 DeliveryLedger().record
    ("loom_core/pipeline.py", 1922, "pipeline.audit"),
    ("loom_core/pipeline.py", 2089, ".done"),
    ("loom_core/admin_api.py", 654, "get_task"),
    ("loom_core/admin_api.py", 1038, "list_tasks"),
    # Stage 2：/tasks、dashboard、cost 三处读口从 `list_tasks`（内存）换成
    # `list_runs`（内存缓存 ∪ 投影）——锚点跟着换而不是删，否则"读侧已并轨"
    # 这句话在表里就没有对应物了。
    ("loom_core/admin_api.py", 1056, "list_runs"),
    ("loom_core/admin_api.py", 1355, "def _task_checkpoint_view"),
    ("loom_core/admin_api.py", 1660, "list_runs"),
    ("loom_core/admin_api.py", 1988, "list_runs"),
    ("loom_core/admin_api.py", 2352, "run_plan_async"),
    ("loom_core/admin_api.py", 2421, "get_callback"),
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
