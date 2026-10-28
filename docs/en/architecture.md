# Architecture

This document describes the layered structure, thread model, and complete data flow of a task in Loom.
For a quick module reference, see the README "Core Modules" section.

---

## 1. Layer Overview

```
┌─────────────────────────── Entry Layer ───────────────────────────┐
│  run.py (CLI)   --mcp→ mcp_server.py (JSON-RPC/stdio)            │
│  --admin/--dashboard→ admin_api.py (ThreadingHTTPServer)         │
│  External systems → POST /api/tasks / SSE /stream                │
└──────────────────────────┬────────────────────────────────────────┘
                           ▼
┌─────────────────────────── Orchestration Layer ───────────────────┐
│  scheduler.py     pipeline YAML → ExecutionPlan (Schema + lock)  │
│  pipeline.py      PipelineOrchestrator: run_plan level loop,     │
│                   checkpoint, recover_tasks, pooled result merge   │
│  dag_executor.py  DAGExecutor: node scheduling (sync thread pool /│
│                   async), retry backoff, circuit breaker, rate    │
│                   limiter, step audit                            │
│  executor_factory.py  thread/process executor factory             │
└──────────────────────────┬────────────────────────────────────────┘
                           ▼
┌─────────────────────────── Transport Layer ───────────────────────┐
│  message_bus_v3.py  MessageBus: pub/sub + request/response +     │
│                   backpressure                                   │
│  message_store.py   PersistentStore: SQLite(WAL) persistence +   │
│                   DLQ                                              │
│  task_queue.py      SQLite task queue (interrupt recovery)       │
└──────────────────────────┬────────────────────────────────────────┘
                           ▼
┌──────────────────────────── Agent Layer ─────────────────────────┐
│  registry.py       Registry: metadata / health check / respawn / │
│                   hot-plug                                         │
│  agent_loader.py   discovery + AST sandbox loading                │
│  base_agent.py     BaseAgent contract (handle + lifecycle hooks)  │
│  agents/*.py       researcher → fetcher → writer →                │
│                   quality_gate → checker → layout → safe_writer   │
└──────────────────────────────────────────────────────────────────┘

Cross-cutting: circuit_breaker.py / rate_limiter.py / cache_manager.py /
         observability.py (logging + Metrics) / event_hook.py (webhook) /
         llm_router.py / cost_tracker.py /
         alert_manager.py / quality_feedback.py / version_manager.py
```

### 1.1 Package Layering and Dependency Direction

Runtime layers above are mirrored by source packages with **unidirectional** dependencies:

```
    agents/            Plugin layer: concrete capabilities, discovered by
                       agent_loader per directory
        │  can import ↓, neither side may hardcode-import back
    loom_doc/       Document domain layer: renderer / ingest /
                    document_enhancer
        │  can import ↓
    loom_core/     Engine layer: DAG, bus, retry/circuit-break/
                   rate-limit, checkpoint, Schema
```

Constraints and rationale:

| Rule | Rationale |
|------|-----------|
| `loom_core` must not import `loom_doc` | The engine is domain-agnostic. Reverse references force non-document workflows to modify the engine |
| `loom_core` must not import `agents` | Agents are discovered + AST-sandboxed; hardcoding forces new agents to modify the engine |
| `loom_doc` must not import `agents` | Plugins calling domain layer is forward; reverse creates a cycle |
| `loom_doc/__init__.py` does not re-export | Prevents dual stub entry points (`loom_doc.render` vs `loom_doc.renderer.render`) causing false greens |
| `loom_doc` external deps must be registered | New non-stdlib imports must be claimed in `tests/test_layering.py` registry; `loom_doc → scripts` is a pinned historical coupling, only `document_enhancer` allowed |

These are enforced by `tests/test_layering.py` via AST static scanning — covering
lazy imports inside function bodies and `importlib.import_module("...")` literals,
not just file-header imports. All four rules were verified with injection-based
positive tests (inserting `from loom_doc import renderer` into core turns red).

> `loom_doc` only depends on `loom_core`'s `llm_router` (used by `document_enhancer`),
> which remains an engine-level cross-cutting component. The former `search_engines`
> has been moved to the independent library `artesian`: it is now a third-party
> dependency, no longer listed in this "core does not know domains" registry.

### 1.2 Two Kinds of "skipped" — Do Not Confuse

Node status `skipped` has two completely different origins, distinguished by
`TaskNode.skip_reason`:

| Origin | `skip_reason` | Downstream | Meaning |
|--------|---------------|------------|---------|
| `when` condition false | `"condition"` | **Execute normally** | Author declares this branch not needed this round |
| Dependency not successful, cascade-skipped | `""` | Continue skipping | Upstream failed, no input available |

Implementation: `_submit_level_futures` (both thread and async versions — changing
one requires changing both) evaluates `node.when` before submission: if false, marks
skipped and records a `status="skipped"` step (visible in reports as empty); if true,
submits normally. **If evaluation raises `ConditionError`, it propagates and fails the
entire run** — downgrading it to "condition false" would silently skip a branch while
still reporting done.

Evaluation is implemented in `loom_core/conditions.py` (pure functions, no exec/eval).
Context `upstream.*` covers the **entire upstream closure**, not just direct
dependencies: when the author writes `upstream.quality_gate.overall_score`,
quality_gate is often an ancestor node. Context table and operator list are in the
README "Conditional Nodes" section.

Relationship with delivery contract (§1.3): if **all** WRITES_OUTPUT nodes are
skipped by condition → treat as declared no-delivery this round, run still done with
warning; if skipped by dependency failure → still failed.

### 1.3 Delivery Contract

`_execute_plan` no longer unconditionally marks DONE: when the plan contains nodes
declaring `WRITES_OUTPUT`, the file at `task.output_path` must actually exist (or
have non-empty inline content) for done to be valid. Origin: kb-docgen was observed
reporting `done` + exit 0 with no output file.

### 1.4 Node Identity: One Name = "Who in the Graph" + "Who Executes"

Node name format `agent[_pool_i][__alias…]` — two resolutions **must be used
separately** (`loom_core/naming.py`):

| Function | Purpose | Result for `writer_pool_0__review` |
|----------|---------|-----------------------------------|
| `agent_of()` | Registry metadata, RPC target, circuit-breaker key | `writer` |
| `family_of()` | Pool sibling merging, upstream closure grouping, result readback | `writer__review` |

Mixing them was observed in practice, not just theoretical: after inline expansion,
the closure grouping was once written as `agent_of`, causing `layout__review` to
drop out of `layout__audit`'s upstream closure — the third node received content
from two hops back, with no error, just silently changed output. Similarly,
`_merge_pooled_results` merging by Agent mixes pool results from two different
sub-flows. Criterion: grouping always uses `family_of`, agent-lookup always uses
`agent_of`, each side has tests (`TestRuntimeIdentity` / `TestPoolMergeIsAliasAware`),
both verified with mutation-based regression.

The same rule governs **Agent-declared capabilities**. `registry.get_meta(name)` is
exact-match; querying with an alias name returns `None` — `None` does not throw, the
capability simply disappears. docgen hit two cases when switching to quality-tail
fragments:

| Location | Alias-name meta query consequence |
|----------|-----------------------------------|
| `_record_task_output` | `WRITES_OUTPUT` fails → `task.output_path` not recorded → delivery contract says "no delivery" despite file existing |
| Quality redo branch | `SUPPORTS_REGENERATION` fails → inlined quality_gate no longer triggers redo, quality gate degrades to one-shot scoring |

Criterion: `TestInlinedNodesKeepAgentCapabilities` runs both cases with a real
executor, and changing `get_meta(base_agent)` back to `get_meta(node.agent_name)`
turns both red. The recheck target (`REGENERATION_RECHECK` defaulting to the node
name) must also be restored to the base name — topics are addressed by Agent,
`quality_gate__tail.input` has no subscribers.

### 1.5 Sub-Pipeline Fragments and Parameter Scope

`pipelines/_quality-tail.yaml` is the shared quality tail for six pipelines
(docgen / docgen-render / docgen-verified / docgen-lean / docreq / kb-docgen).
Before extraction, the tails were byte-compared: `quality_gate/checker/layout/safe_writer`
had identical version/timeout/config, differing only in whether fact_checker is
present and under what condition. The differences converge into two parameters
(`fact_check` / `min_score`), passed by the caller in the `call` node's `inputs:`.

The criterion is not "looks the same": after re-locking, node-by-node comparison of
`config_hash` / `version` / `pool_size` between pre- and post-extraction locks showed
0 inconsistencies across 26 groups.

`three_pass` deliberately does not reference the fragment — its tail is a different
source of truth (threshold 65, max_regenerations 2, checker/layout timeout 120,
safe_writer timeout 60). Merging it would either change its behavior or require a
suite of "per-node override" parameters on the fragment — creating more complex
duplication to eliminate duplication.

Three parameter scope rules:

1. **Fragment nodes can write `inputs` as defaults**, provided its own `when` reads
   `inputs.*` — otherwise it is dead config ("configured but not read") and is
   rejected at parse time. With defaults, the fragment can be parsed/linted standalone.
2. **Only keys the node actually reads are written onto the node**. Carrying every
   node's full tail parameters on each node would flood the lockfile with meaningless
   entries and leak outer parameters into inner scopes.
3. **Nested `call` uses its own arguments**: inner `call` node parameters take effect
   in its own sub-plan; outer same-name keys do not override them
   (`test_outer_args_do_not_leak_into_inner_scope`, red on the old "merge outer" approach).

Fragments are not in user-visible lists: `installed_pipelines` / `Scheduler.list_pipelines`
/ run.py candidate names all filter `_` prefix, but `load()` still resolves by name
(needed by `call`). Fragments and their referencing YAML must be in the same directory —
`parse_file` passes the referencing directory to the inline logic, so `--pipeline-file
/tmp/x.yaml` finds its fragment in `/tmp`, not the process cwd.

### 1.6 Per-Item Expansion (`foreach`): Expansion Inside One Node Execution

`naming.py` once cited "supports foreach (expand same node per item)" as the reason
for centralized node identity resolution — this section is the form in which it
pays that debt.

**Why not create a new node identity per item** (`transform__0` / `transform__1` …):
that would make levels, upstream closure, and lockfile node set all vary with
**runtime data**, while the lockfile's purpose is precisely "should not change if
declaration doesn't change". So expansion happens inside a single
`execute_node_from_scheduler` call: the DAG sees 1 node, `node_count`, checkpoint
granularity, retry granularity, and report steps all remain the same. The cost is
that breakpoint resumption is per-node — resuming after an interrupted foreach node
re-runs every item. This is a deliberate trade-off, not an omission.

Three mechanisms to remember:

1. **Idempotency keys split per item** (suffix `#{i}`). The bus records history by
   `task_id:node:attempts`; shared prefixes cause item 2+ to hit item 1's cache —
   "expanded N times, actually executed 1 time", output shape completely normal.
   Criterion: `test_idempotency_keys_split_per_item`, red without the suffix.
2. **`item`/`index`/`count` are engine-owned keys** (in `ENGINE_OWNED_KEYS`). If an
   upstream declares same-name outputs, it overrides them — consequence is not an
   error but N items rendering identical content; `merge_artifact` rejects this.
3. **Any item failure = node failure**, including subscriber-returned `None` timeout
   items; `hard_floor` accumulates per-item and still takes the hard-fail prefix.
   Rationale: same family as every hole this project has closed — "skip bad items"
   during aggregation lets deliverables silently shrink, and a short document still
   looks complete.

Rejected combinations: `foreach` + `call` ("this node" doesn't exist after expansion),
`foreach` + `pool_size>1` (two "split into multiple executions" mechanisms have no
unique allocation semantics), `foreach` + `REGENERATION_*` and `foreach` + `WRITES_OUTPUT`
(the latter two operate on "the entire output"; writes must come after foreach, letting
the sink consume aggregated output). First two are parse-time errors, last two are
errors **before any delivery occurs**.

`topology_hash` captures the **expansion contract** (`over` and `max_items`), not the
runtime-resolved item count: item count comes from data, writing it into the lock makes
the lock depend on data files, turning `loom lock` into daily re-locking.

Version-lock coverage: fragment nodes, edges, config hashes, and **caller-passed
arguments** all enter the parent pipeline's `topology_hash`/`config_hash`, so changing
the fragment or `inputs` causes parent lock drift; the fragment's own lock records
its standalone expansion shape.

## 2. Core Concepts

| Concept | Definition Location | Description |
|---------|-------------------|-------------|
| `ExecutionPlan` | `scheduler.py` | YAML parse product: `levels` (topology levels), `raw` original config, `checkpoint` strategy |
| `PipelineTask` | `pipeline.py` | Runtime task state: status/dag_nodes/steps/result/stop_event |
| `TaskNode` | `pipeline.py` | Unified node model: static config (timeout/retry/backoff) + runtime state (attempts/status/result) |
| `AgentMeta` | `registry.py` | Agent registration metadata |
| Circuit Breaker | `circuit_breaker.py` | CLOSED/OPEN/HALF_OPEN three states, per-agent isolation, fast-fail when open |
| Regeneration Loop | `agents/quality_gate.py` | Score below threshold → trigger `REGENERATION_TARGET` (writer) redo → recheck |

## 3. Thread Model

| Thread | Created At | Responsibility | Closed By |
|--------|-----------|---------------|-----------|
| Bus worker | `MessageBus.__init__` | Consume async queue, batch-deliver subscribers | `bus.shutdown()` |
| Webhook event loop | `event_hook.py` | aiohttp async webhook delivery | Notified on orchestrator shutdown |
| Admin HTTP | `ThreadingHTTPServer` (daemon_threads=True) | One thread per connection for REST/SSE | Process exit |
| Executor pool | `executor_factory.create_executor` | Node execution worker threads | Context-manager auto-close |

Shutdown entry is unified to `orch.shutdown()` (called at end of each `run.py` branch),
responsible for stopping the bus and cleaning up agents (including `cleanup_stale_temp`).
SIGTERM is converted to KeyboardInterrupt via `_sigterm_handler` through the same
cleanup path.

## 4. Data Flow of One Task

Using the default `docgen` pipeline as example:

1. **Submit**: CLI / `POST /api/tasks` → `orch.run_plan(plan, input_file, task_id)`
2. **Create Task**: Create `PipelineTask` (RUNNING), register in `_running_tasks`,
   write to SQLite `task_queue`, emit `task.created/started` events and `pipeline.started`
3. **Level Loop**: Execute per `plan.levels`; before each level check cancel/pause, snapshot
   state; `DAGExecutor.execute_level` submits level nodes to thread pool for concurrent execution
4. **Node Execution**: `execute_node_from_scheduler` → assemble Message → `agent.handle(msg)`,
   result written to task output; failures go through exponential backoff retry (interruptible
   by stop_event), final failure triggers circuit-breaker count or fail_fast soft-abort; each
   step records StepResult + audit + Metrics
5. **Quality Loop**: quality_gate score below threshold triggers regeneration per config
6. **Pooled Merge**: In-level multi-instance (`*_pool_*`) results aggregated by `results_merge`
7. **Finalize**: `_finalize_plan_task` updates progress/timestamps → task_queue status → event
   hooks (completed/failed/cancelled) → report generation → temp file cleanup → checkpoint handling
8. **Write**: safe_writer atomically writes output document with backup

Breakpoint resumption: after task interruption, checkpoint retains each agent's
`on_snapshot()` state; `--resume` or `--recover` restores from checkpoint/task_queue.

## 5. Design Trade-off Notes

- **Process Mode** (`executor_type: process`): Nodes execute in ProcessPoolExecutor
  child processes. When DAGExecutor is pickled into child processes, all thread-lock-containing
  components are stripped; the worker entry (`dag_executor._execute_node_worker`) rebuilds
  Registry and non-persistent MessageBus once per worker process using `child_context`
  (agents_dir / agent_names / config, written by `register_agents()`), then loads Agents
  via AgentLoader. **Known limitations**: ① Bus events in child processes do not return to
  parent (node results return via pickle, unaffected); ② Circuit breaker and rate-limit counts
  are per-process isolated, not aggregated cross-process; ③ Each worker's first execution has
  Agent cold-start overhead. I/O-intensive scenarios should still use the default thread mode.
- `MessageBus` starts a worker thread at construction — every instance must be paired with
  `shutdown()` (orchestrator and test fixtures already ensure this).
