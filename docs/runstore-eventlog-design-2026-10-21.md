# RunStore + EventLog 设计（2026-10-21）

> 承接 `docs/product-design-review-2026-10-20.md` §5。本文只解决一件事：**把"一次运行的事实"从 5 套写入 + 20 处直读，收敛成一个写入口、一个读接口。**
> 行号基线：main @ `0f6090f`（PR #50 合并后），本仓 `fix/console-runtime-validation` 分支上取证的落点已逐条复核。**本文所有 `file:line` 是"当时那一行的原状"**，Stage 每落地一批就会让一批引用失效——`scripts/check_design_anchors.py` 按 `(文件, 行号, 锚文本)` 逐条核对，改了代码就要按新现实更新或退役对应条目。
> 落地进度：§6 三件事已拍板；**Stage 0 / Stage 1 已合并**（PR #54 / #55），**Stage 2 读侧已交付**（PR #57），SSE 重放拆到 Stage 2b（PR #58，见 §4 末），**Stage 3a 审批摘写已交付**（PR #60，见 §4 末），**Stage 3b 队列收口已交付**（PR #62，见 §4 末），**Stage 3c 补账工具已交付**（PR：本批，见 §4 末）。Stage 3 的两条退出判据里，"每个源有且只有一个写入者"达成；"RunHandle 五原语全部只读 RunStore"**只在观测面达成**——`cancel/resume` 仍是内存动作（Stage 2 就写了这条边界：动作只在内存态有意义），不把它算成已闭环。

---

## 1. 现状：五套事件写入 + 两类事实写

| 源 | 载体 | 写点 | 语义 | 持久 |
|---|---|---|---|---|
| `emit_event` | `event_hook.py:285` | 16 处（`pipeline.py:465/468/471/531/532/567/607/682/742/882/885/888/1257/1259` 等） | 进程内分发 | **否**（无 seq、无落盘；匹配不到钩子 `return 0`；webhook 队列满丢事件） |
| `bus.publish` | `message_bus_v3.py:128` → `message_store.py:171` | `pipeline.py:534/555/899/1266/1302/1615/1727` | 每节点 + 每运行的平行轨迹 | 是（SQLite-only） |
| `write_audit` | `audit.py:31` | `pipeline.py:752`、`admin_api.py:1717`、`webhooks.py:142` | 三个 channel 的 JSONL 追加 | 是（**文件，不是表**） |
| `deliveries` | `deliveries.py:61` | `pipeline.py:1517` | 每 run 一行终态载荷 | 是（`INSERT OR REPLACE`，PG 无翻译规则） |
| `approvals` | `approvals.py:84/107/121` | `pipeline.py:1014/669/731/954/933/1062` | 同一行 status 覆写 | 是（SQLite-only，依赖 `sqlite3.Row`） |

另两类"非事件形"的运行事实：`task_queue` 原地 UPDATE 状态列（`task_queue.py:186-292`）、checkpoint JSON 整文件覆盖只留最新（`checkpoint_manager.py:38` `save()`，原子写 `os.replace` 在 87）。

**读侧**：`RunHandle` 是唯一的"号称统一"读原语，但它按 `内存 → task_queue → deliveries` 逐维回落（`run_handle.py:52/103/133/178`），五原语里 `cancel/resume` **只读内存**（`pipeline.py:600/611`）。Admin 侧 20 处绕过它直读（`admin_api.py:604/968/983/1006/1097/1232/1537/1619/1850/2183`），其中三处后果最重：

- `_scoped_task`（`admin_api.py:604`）用 `orch.get_task` 判存在性 ⇒ **内存淘汰即 404**；淘汰线是硬编码 100 条（`pipeline.py:297`，无 config/env 入口）；
- `/api/cost`（`admin_api.py:1850`）用内存任务集合当作用域集 ⇒ 成本归因随淘汰丢；
- `/stream`（`admin_api.py:2183`）读 `streaming.py:154` 的进程内 `_history`（上限 500）⇒ 跨进程/重启不可重放。

## 2. 必须先修的裂缝：两条引擎不同形

`run_plan_async`（唯一生产调用者 = SSE 的 `admin_api.py:2113`）相对同步版**少写两样**，且这是代码自己承认的：

> `pipeline.py:1290`：「async 任务收尾：与 `_finalize_plan_task` 的差异是不更新 task_queue、不发事件钩子」
>
> ——这句 docstring 已被 Stage 0 改掉。并轨后的形状是同步与异步收尾共用同一个写口
> `pipeline.py:506 _record_plan_terminal_state`（队列 `finish` + 终态事件一次做完），锚点表已从"钉那句裂缝"
> 改成"钉这个共用写口"——**"裂缝已补"这件事本身也要可核对**。

后果实测口径：**从控制台流式发起的 run，在持久队列里没有行、在事件面里一条都不发**，只留下 checkpoint、deliveries 与总线消息。这解释了此前一类反复出现的现象——"UI 上看得见的运行，接口查不到"。

**Stage 0（已实现，独立 PR）**：并轨。`run_plan_async` 补 `task_queue.submit` + `claim_direct` + 终态 `finish`，收尾补 `emit_event`；`run()` 与 `run_plan()` 的终态事件去重（同一 run 同一终态现在可发两次：`pipeline.py:465` 与 `882` 双路径 + `567`/`607` 的旁路）。判据：同步与异步各起一次同流水线，断言队列行形状、事件序列、账本行三者逐字段相等（parity 断言必须能变红——把 async 的 submit 注掉就该红）。

实测落地形状与本文的差一处：暂停等待那半段本文原先没写，但它同属"两条引擎不同形"——异步循环原先只 `await asyncio.sleep` 观察不到他进程写下的审批决定（同步版有、异步版没有），挂在闸门前等一个永远不会来的信号。Stage 0 一并把暂停边界与外部决定观察抽成两条循环共用的 `_pause_boundary_snapshot` / `_observe_external_decision`。

**Stage 0 实际交付的判据范围（对着上文那句"三者逐字段相等"要打的折扣）**：`tests/test_async_run_parity.py` 钉住的是**事件序列 + 队列行形状 + 暂停边界检查点形状**三项，`账本行（deliveries）比对挪到 Stage 1` —— 一则这批用例的流水线没声明 sink，两侧都没有 `deliveries` 行可比（比"都为空"是零射程断言）；二则 §4 给 Stage 1 定的门禁本来就是"每 run 跑完自动比对 `runs`/`run_nodes` 与 `task_queue`/`deliveries`/内存三方"，账本比对照那道门禁做才有承载体。

**Stage 0 期间查实、但不属于并轨的一条写侧缺口**：`task_queue.submit` 的三个调用点（`pipeline.py` 里 legacy / `run_plan` / `run_plan_async` 各一处——**这里刻意写符号不写行号**：行号会随每次引擎改动漂移，而"当前状态"类断言该按符号可核对，行号只留给 §1 那张"当时原状"基线表）**都没传 `principal`**，`admin_api` 里只有 `:1393` 一处传 `owner`，SSE 的 `:2113` 不传 ⇒ 队列行与内存任务的归属全是空串，`/tasks` 与 `/api/dashboard` 对具名 token 按"owner 空串不展示"处理（`admin_api.py:986` 的注释自己写了这条口径）。这不是异步并轨引入的（同步直跑同样为空），但 **§3.1 的 `runs.principal` 列不能假设上游已经填过**——列是先有的，身份是还没有的；Stage 1 要么让 `RunRecorder` 从调用方接住 Principal，要么就如实承认新表也是无主行。〔**Stage 1 落地时选了后者并写明**：`open_run` 如实带上引擎此刻知道的归属（`task.owner`，今天多为空串），身份贯穿属 Stage 2 读侧切换的活，不在双写这批偷偷改调用方语义。〕

## 3. 设计核心：一个写入口，事件与投影同事务

```
                RunRecorder.record(run_id, type, payload)   ← 引擎/worker/审批的唯一写口
                          │  同一事务
              ┌───────────┴────────────┐
              ▼                        ▼
      events（追加，seq 单调）    runs / run_nodes（投影，原地 upsert）
              │                        │
              └──────► RunStore 读接口 ◄┘   ← principal 必带
                          │
        RunHandle / /tasks / /api/dashboard / /api/cost / MCP / SSE 重放
```

为什么投影不从事件"事后重建"：那样恢复与审计会分属两个时间坐标系，且 rebuild 一旦落后，读侧就看到"事件说完成、投影说运行中"。**同事务双写**（一次 `commit`）保证两套永不分叉；事件用于重放/审计/SSE 游标，投影用于列表与详情。

边界（明确不做）：
- **不 backfill** 历史运行。新运行起才进 RunStore；文档写"运行历史自 vX 起"。老运行仍按现状经 `deliveries` 回落读得到终态。
- checkpoint **保留**"恢复快照"职责，不改成事件源（`_merge_resumed_nodes`/`_reuse_completed_node` 读快照，`dag_executor.py:821/952`）。它的磁盘存在性不再对外报（`admin_api.py:1232` `_task_checkpoint_view` 的 `exists`/`resumed` 改由事件/投影回答"是否续跑过"）。
- 本批只落 4 张表：`schema_version` / `events` / `runs` / `run_nodes`。`artifacts`、`schedules`、`cost_entries` 属后续批。
- `RunStore` 走 `db_backend.resolve_backend` 的**已翻译子集**（`?`、`INSERT OR IGNORE`、`REAL`），因此本批 SQL 禁用 `INSERT OR REPLACE`；PG 实现仍是下一步，判据按 `LOOM_TEST_PG_URL` 门控（与 `tests/test_state_backend.py` 同口径）。

### 3.1 表结构（本批）

| 表 | 列 | 说明 |
|---|---|---|
| `schema_version` | version INTEGER PK, applied_at REAL | 取代 `add_column_if_missing` 的临时迁移——**范围只到 `runs.db`**：`task_queue` 所在的 `tasks.db` 仍走加列（Stage 3b 给它加了第 4 列 `projection`，调用点在 `task_queue._init_db`，baseline 时是 3 处）。所以"现仅 3 处生产调用"这句按符号核对即可，别把行号当契约 |
| `events` | run_id TEXT, seq INTEGER, type TEXT, payload_json TEXT, ts REAL；PK(run_id, seq) | seq 由 recorder 在事务内取 `MAX(seq)+1`，**不用自增 id**（跨进程共享库时 per-run 单调才可用） |
| `runs` | run_id PK, workflow, principal, status, trigger, inputs_json, started_at, finished_at, error, cost_usd, quality_score, last_seq, artifacts_json | 列表页与作用域判定的唯一来源；`last_seq` 让 SSE 游标能跨进程续传 |
| `run_nodes` | run_id, node, status, attempts, started_at, finished_at, error, result_json, artifact_keys_json；PK(run_id, node) | S2/S3 直读；把现在藏在 `deliveries.payload_json` 里的逐节点事实升到一等行 |
| `approvals`（结构版本 2 起，Stage 3a） | request_id PK, run_id, pipeline, principal, nodes_json, messages_json, status, requested_at, decided_at, decided_by, note, reason, consumed | 审批是可变状态（pending→approved/rejected→consumed），收件箱要按 status 查，所以有投影；每次变更同事务追加 `approval.*` 事件 |

`Deliveries` 不动，作为"交付账本"继续写；`runs.artifacts_json` 由 recorder 在 `_deliver` 处同事务写入，避免两个来源（迁移到视图是后续批）。

## 4. 分期与退出条件

| Stage | 交付 | 旧写入点处置 | 退出判据 |
|---|---|---|---|
| 0 | 两条引擎并轨（§2）**已交付** | 终态事件去重 | 同/异步 parity 断言逐字段相等（**实交付=事件序列+队列行+检查点形状；账本行比对挪 Stage 1**，见 §2 末）；parity 变异可红（最终头实测：还原 `pipeline.py` ⇒ 4 条全红，只摘"已发过"闸门 ⇒ 恰好 2 红 2 绿，红的是两条取消判据） |
| 1 | `RunStore`（落地时的类名，本文原写作 `RunRecorder`）+ 4 表 + **双写** 已交付 | 不删任何旧写；写口全部 fail-soft | 三方对账：`runs`/`run_nodes` vs `task_queue`/`deliveries`/内存，不一致当场红（`tests/test_runstore_parity.py` 8 条，随 CI 全量跑）。**两处与本文原设想不同，照实记**：① 门禁是**显式用例**覆盖两引擎 × done/cancelled，不是"每条用例跑完自动后置检查"——后者现在挂上去只会把每个既有用例都拖一遍库盘，等 Stage 2 读侧切完再谈；② `runs.cost_usd`/`quality_score` **本批不填**（来源未定，不拿猜测当事实），`principal` 沿用调用方归属（今天 SSE/直跑都传不出 owner ⇒ 列有、值多空串，见 §2 末） |
| 2 | 读侧切换：`/tasks`、`/tasks/<id>`、`/api/dashboard`、`/api/cost` 作用域、MCP `list_tasks`/`get_task`、`_scoped_task` 存在性 **已交付**（PR #57）；SSE 重放改读 `events` **已交付（Stage 2b，独立 PR）** | 内存退化为缓存（仅加速，不再作为存在性判据）**已交付**：`RunHandle` 与 `list_runs` 依次回落到投影 | 双 token 真 HTTP：淘汰 100 条后仍查得到、越权仍 404 不泄露存在性（`tests/test_runstore_readside.py` 18 条，五组窄变异各只红预期用例）；`/stream` 断线重连跨进程可重放（`tests/test_sse_replay.py` 11 条，三组窄变异；重放游标 `r<seq>`、重连不再起第二条流水线） |
| 3 | 摘写：**3a `approvals` 已改为 `runs.db` 的投影 + `approval.*` 事件（PR #60）**；**3b 队列已收口（本批）**——停写 `result_json`（实测无读者的第三份拷贝），保留 `status/started_at/owner_pid` 这些**租约**列，并新增 `projection` 健康位 | 老台账 `approvals.db` 停止新增行，只保留对既有行的就地决策/消费；`queue.finish` **不删**（它是 `recover()` 的判据，不是投影的镜像） | 每个源有且只有一个写入者（**观测面达成**：内存=缓存、审批=store、结果正文=投影/账本、队列=租约）；`RunHandle` 五原语只读 RunStore（`status/artifacts` 达成，`cancel/resume` 仍按 Stage 2 的边界只认内存） |

双写窗口的长度用门禁约束：Stage 1 起，"两套口径漂移"不是靠人盯，而是被 parity 断言钉在 CI 里；Stage 3 才允许删写。

**Stage 2 实际交付的三条边界（照实记）**：

- **`/stream` 重放拆成 Stage 2b 并已交付**：它要先把"RunStore 事件 → SSE 帧"的映射定成契约
  （现有 SSE 事件是 `progress/section/complete`，RunStore 事件是 `run.opened/node.finished/run.completed`）。
  2b 的定稿：重放帧类型 `run_event`、`id: r<seq>` 是运行级游标（进程内计数器纯数字、
  跨进程无意义），终态收流；**重连不再起第二条流水线**。逐 chunk 正文流从不落盘，
  不在重放范围内。
- **淘汰后的详情是降级但诚实的**：`/tasks/<id>` 从投影回答状态/流水线/时间/错误/产物/逐节点摘要；
  节点 `result` 正文本从不落盘（Stage 1 只存摘要），所以那些键不出现、不编空值。
  `cancel/resume/审批` 仍只读内存——**动作**只在内存态才有意义，淘汰后它们照旧 404。
- **身份贯穿只覆盖新运行**：三处 `task_queue.submit` 与 SSE 调用的 `owner` 已带上；Stage 1 之前
  没有投影的老运行不在 `runs` 里（不 backfill），仍按原路径从 `deliveries` 读终态。

**Stage 3a 实际交付的三条边界（照实记，2026-10-23）**：

- **"一个源一个写入者"在这里按行集成立，不是按表**：`approvals.db` 不再有新增行，
  但仍保留对本批之前那些行的**就地决策/消费**。不这么留的代价是活的：升级那一刻停在
  闸门上的运行此后永远批不掉（`decide` 在新投影里查不到行 → fail-closed → 任务卡死）。
  老行耗尽后该表只剩历史，回落读也就成了死代码——那时再摘第二个写点。
- **老台账是数据边界，不是开关**：读侧"store 里没有这个运行的审批行 → 回看老台账"，
  与 §5 里删掉 `LOOM_RUNSTORE` 时讲的同一条规矩一致；不 backfill，不抄行、不造伪事件。
  文件不存在就当没有（读连接带 `query_only`，绝不因为"查一下"建库）。
- **审批的整体可用性挂在 RunStore 上**（这是本批换来的一条真实代价）：`runs.db` 打不开时
  连内存路径的 `approve()` 也会被拒——写不进台账却放行等于执行一个没有持久授权的决定。
  失败面由 `/api/health/deep` 的 `runstore` 组件与引擎首错的 error 日志承担。
- **决定只有一个时刻**：`nodes/by/at/decision` 由 `_decision_from_record` 从台账行单点映射，
  审计（`audit/approvals.jsonl`）、内存 `task.approvals`、`approval.decided` 事件与投影行
  四方同值（`tests/test_runstore_approvals.py::TestLedgerIsAuthoritative`）。此前调用方与
  台账各取一次 `time.time()`，同一个决定有两个时刻。

**Stage 3b 交付时的两条更正（照实记，2026-10-24）**：

- ~~前置①"`recover()` 改读投影"~~ —— **这条前提是错的**，本批按实测改掉。`recover()`
  读 `status='running'` + `owner_pid` 存活 + 新鲜度阈值，这套判据本身就是**租约**，
  不是"运行事实的第二份拷贝"：投影里没有 lease 概念，改读投影等于把抢占语义搬进
  观测面。所以 `queue.finish` 保留——它是恢复路径的判据，删了会让跑完的运行在
  下次 worker 启动时被整批重跑。真正重复的是 `result_json` 那一列，摘的是它。
- 前置②"投影写失败时队列怎么办"是**产品口径**，已由用户 2026-10-24 拍板：
  **标 incomplete 后继续**（观测面故障不放大成可用性故障，但缺口必须可发现）。
  落成的形状：终态写入**先悲观后升格**（先按 `projection='incomplete'` 写队列行，
  投影确认盖章才升 `ok`）；`run_store_health()` 报 `incomplete_runs`（待补账）与
  `legacy_runs`（按设计没有投影）；`RunHandle.projection()` 把标记排在"投影里查得到行"
  **之前**——盖章失败时 `runs` 那行会永远停在 `running`，只看有无行会把它读成 complete
  （这条顺序是第一轮实测撞出来的，也留了变异 M5 钉住）。

**为什么 `legacy` 与 `incomplete` 必须是两个值**：`_runstore_open` 只在 `run_plan` /
`run_plan_async` 里调用（实测），legacy 路径 `run()` 连一行投影都不写——那是
"deprecated 兜底，按设计缺观测面"。混成一个数，健康检查就把兜底路径报成数据丢失，
而**误报的运维代价高于不报**（人会开始忽略这个指标）。

**上一批没做的、本批做完了**：补账工具——把一个 `incomplete` 运行重新投影进 `runs` 表
需要的就是"幂等的终态重放入口"（2026-10-25 交付，见下"Stage 3c"）。`deliveries.db` 与 checkpoint
按 §3 的边界原样保留（一个仍是交付账本，一个仍是恢复快照）。

**Stage 3c（补账工具，2026-10-25 交付）**：`loom_core/backfill.py` + `loom --replay-run
<id>` / `--replay-incomplete`（`--replay-dry-run` 先看不动笔）。三条设计决定值得留在文档里：

- **事实来源只有一个**：交付账本。补账不读 checkpoint（那是恢复快照，§3 已划出去）、
  不读内存（补账常发生在原进程已死之后）。因此**账本没有的东西就承认没有**：节点绝对时间
  从未落账 ⇒ 补写的 `run_nodes` 行时间列留 0，`run.replayed` 事件里标
  `"times": "unavailable"`，而不是拿运行结束时刻倒推出一个假的每节点时间。
- **幂等靠"只补缺 + 原子终态"两条**：已有的 `run_nodes` 行不覆盖；`close_run` 从"调用方
  保证至多发一次"（`_claim_terminal_event` 是**进程内**的，跨进程重放绕过它）改成事务内
  `UPDATE … WHERE status NOT IN (done, failed, cancelled)`，影响 0 行就不再追加终态事件。
  这条改动同时把 §5 里"seq 必须由单一写入者持有"的新锁点收在同一个 `BEGIN IMMEDIATE` 内。
- **拒绝而不是猜**：队列 / 账本 / 投影三方状态不一致、行未终态、健康位不是 `incomplete`、
  账本里根本没这条运行——一律拒绝并给出可读理由。补账工具一旦开始"顺手纠正"口径冲突，
  它自己就成了第二个事实源。

## 5. 风险清单（动共享状态前）

- **写放大**：每 run 新增约 `节点数 + 3` 次写（events 逐节点 + runs 起终 + run_nodes 逐节点）。缓解：recorder 合批（同一层节点一次事务）+ WAL（现有 `sqlite_util.enable_wal`）。判据要含"50 节点流水线跑完的耗时不与基线差一个量级"（perf-trend 腿已有基线口径）。
- **锁竞争**：历史上 `database is locked` 在并行 worker 下出现过（CHANGELOG 3.9.2 修过四个 store 的首建竞态）。新增 store 必须复用同一套 `enable_wal + busy_timeout=3000` 与幂等加列。
- **seq 分配**：`MAX(seq)+1` 在同一 run 上必须由单一写入者持有；async/worker/审批三方都往同一 run 写 ⇒ 需要在事务内用 `BEGIN IMMEDIATE`（PG 侧对应行锁），否则并发下 seq 撞 PK。这是本设计唯一的新锁点，必须自带变异判据。
- **淘汰语义变更**：`_max_task_history=100` 现在是"存在性"的事实源；Stage 2 后它只影响内存缓存。**这是行为变化**：老部署里"重启即查不到"会变成"查得到"——要写进 CHANGELOG 与 README，不是静默改。
- **回退**：~~Stage 1 的双写用一个内部开关~~〔**2026-10-21 移除**：开关不承载读语义（回落是数据边界），留着只会在运行期制造两套形状；写侧改为恒开 + `run_store_health()`与 `/api/health/deep` 的 `runstore` 组件把失败报出来。回退手段=回滚版本〕

## 6. 三件事的结论（2026-10-21 已拍板）

1. **顺序**：Stage 0 独立成 PR 先合——它本身是缺陷修复且不依赖新表。
2. **PG 范围**：本批只立接口 + SQLite 实现，`resolve_backend` 接进 RunStore 与 `LOOM_TEST_PG_URL` 腿留下一批。
3. **不 backfill**：接受"运行历史自本版本起"，并把这条边界写进对外文档（不是默默少数据）；老运行仍经 `deliveries` 回落读得到终态。
