# Agent Development Guide

This document explains how to write custom Agents for Loom. All content is based on current code:
contract definition in `loom_core/base_agent.py`, loading and sandbox in `loom_core/agent_loader.py`.

**Where to put new code** (dependency direction is unidirectional, enforced by `tests/test_layering.py`):

| Location | What | Constraint |
|----------|------|------------|
| `agents/` | A capability that pipelines can subscribe to (Agent) | Discovered by loader per directory, must not be hardcode-imported by `loom_core` |
| `loom_doc/` | Document-domain pure functions/classes: rendering, ingestion, enhancement | Can only depend on `loom_core`; Agent thin shell calls it |
| `loom_core/` | Engine primitives that hold for any task type | Must not import `loom_doc` / `agents`, must not contain domain terms |

Criterion: if removing the "document" scenario still makes the code useful, it belongs in `loom_core`;
otherwise it is domain layer or Agent.

---

## 1. Agent Module Contract

Each Agent is a `.py` file in `agents/` directory (filename = Agent name).
The loader reads module-level constants as registration metadata:

| Constant | Type | Description |
|----------|------|-------------|
| `AGENT_NAME` | str | Agent name (must match filename) |
| `AGENT_VERSION` | str | Version |
| `AGENT_DESC` | str | One-sentence description |
| `AGENT_AUTHOR` | str | Author |
| `AGENT_PRIORITY` | int | Subscription priority (lower = earlier) |
| `INPUT_TOPICS` | list[str] | Subscribed message topics (auto-attached to MessageBus) |
| `OUTPUT_TOPICS` | list[str] | Output topics (for documentation) |
| `DEPENDENCIES` | list[str] | Dependent Agents |
| `CACHE_TTL` | int | Cache TTL seconds |
| `RESPAWN` | bool | Auto-respawn on abnormal exit |
| `SUPPORTS_REGENERATION` / `REGENERATION_TARGET` / `REGENERATION_RECHECK` | — | Quality redo loop: if declaring support, must provide `REGENERATION_TARGET`, engine no longer guesses default |
| `PRODUCES` | dict[str, str] | Output artifacts and merge strategy: `{"content": "last"}` (mechanism in `loom_core/artifacts.py`) |
| `CONSUMES` | list[str] | Expected upstream artifact names (declares intent, for validation and reading) |
| `CONFIG_SCHEMA` | dict[str, tuple] | Config contract: `{"threshold": (["int", "float"], 70)}`, type names are **strings** |
| `LEGACY_AUTO` | bool | Default `True`: participate in `--legacy` auto-graph. Legacy by design does not read YAML, has no per-node config; anything "cannot produce output without config" (network needs `url`, transform needs `template`) must set `False`, otherwise it is pulled into every legacy run and inevitably fails |
| `SANDBOX_TRUSTED` | bool | Built-in Agents shipped with the product explicitly declare `True`; loader skips AST sandbox check accordingly |

### 1.1 Artifact Contract (How the Engine Passes Upstream Results to You)

The engine assembles downstream payloads **by declaration only**: only keys you wrote in `PRODUCES`
are merged into the `payload` top level (also retained in `payload["dependencies_results"][node_name]`
for raw access).

| Strategy | Semantics | Typical |
|----------|-----------|---------|
| `"list"` | Concatenate across all upstream producers (including same-level pooled instances) | `researcher.results`, `fetcher.articles` |
| `"last"` | Topologically **closest** producer wins | Body `content` (writer → layout progressively overwritten) |
| `"first"` | Earliest producer frozen | `requirements_analyzer.spec` |

Order comes from DAG levels, not any hardcoded Agent list; undeclared keys do not leak as top-level keys,
`task_id` / `config` / `queries` etc. engine-owned keys are never overwritten by upstream.

### 1.2 How Config Contract Takes Effect

`Scheduler` uses **AST** to read your `CONFIG_SCHEMA` literal (does not execute Agent code);
during pipeline parsing: missing items fill defaults, type mismatch raises `TypeError` directly.
This way misconfigured values are not silently swallowed by `config.get(key, hardcoded_default)` at runtime.
Type names only accept `str/int/float/bool/list/dict`; anything else errors at parse time.

## 2. BaseAgent Required Methods

Inherit `loom_core.base_agent.BaseAgent` and implement the sole abstract method:

```python
def handle(self, msg: Message) -> dict | None:
    """Process one message, return result dict (written to task output) or None"""
```

`BaseAgent.__init__(name, meta, config, message_bus=None, registry=None)` is called by the loader,
subclasses generally do not need to override; if overriding, call `super().__init__(...)` first
(base class handles cache directory, logging, auto-subscribing `INPUT_TOPICS`).

## 3. Lifecycle Hooks (All Optional Overrides)

| Hook | Trigger |
|------|---------|
| `on_start()` / `on_stop()` | Agent start / stop (`on_stop` should release resources) |
| `on_pause()` / `on_resume()` | Pipeline pause / resume (breakpoint resumption) |
| `on_config_update(changed_keys)` | `POST /api/config/reload` config hot-update |
| `on_snapshot()` / `on_restore(state)` | Checkpoint create / restore (breakpoint resumption state) |
| `cleanup_task_temp(task_id)` / `cleanup_stale_temp(max_age_hours)` | Temp file cleanup at task end / startup (returns cleaned count) |
| `is_healthy()` | Registry health check |

## 4. Messages and Utility Methods

Base class helper methods available inside `handle()` (see `loom_core/base_agent.py`):

- `self.publish(topic, payload)` — Publish message to bus
- `self.send_to(to_agent, topic, payload)` — Direct send
- `self.reply(original_msg, payload)` — Reply to request
- `self.cache_get(key)` / `self.cache_set(key, data)` — File-backend cross-process cache
- `self.log_debug/info/warning/error(msg)` — Structured logging
- `self.report(status, info)` — Report status to Registry

## 5. Security Sandbox

Third-party Agents undergo AST static check at load time (`loom_core/agent_loader.py:_check_safety`),
built-in whitelist Agents skip the check. Default `strict_safety=True`, hit = block loading.

**Dangerous call blacklist** (`_DANGEROUS_CALLS`, excerpt):
`os.system`, `os.popen`, `os.remove/unlink/rmdir/rename/chmod/kill/fork`,
`subprocess.Popen/run/call/check_call/check_output`, `eval`, `exec`, `compile`,
`__import__`, `shutil.rmtree/move/copy2`, `open`, `socket.socket/connect/bind`,
`ctypes.CDLL/PyDLL/WinDLL/pythonapi`, `pickle.loads/load`, `marshal.loads`.

**Import interception**:
- `from subprocess import Popen` etc. `from <dangerous module> import <dangerous name>` directly intercepted;
- `from os import system/remove/...` intercepts dangerous function names;
- `import ctypes / pickle / marshal` directly intercepted.

In other words: custom Agents must **not use `open()`** — for file read/write, use base class cache
or path wrappers provided by config.

## 6. Module Identity: One File = One Object in Process

`AgentLoader.register()` defaults to **reusing** the module object already loaded from the same file
in `sys.modules["agents.<name>"]`; to force new code, explicitly pass `reload=True`.

This is not an optimization, it breaks a class of false green: previously every registration did
`module_from_spec + exec_module` and overwrote `sys.modules`, so the same Agent had two class objects —
in tests `patch("agents.writer.WriterAgent.handle")` patched the first-imported copy, while the
registrar's instances used the other copy, the patch ran empty the whole time, and the test still passed
(this repo has twice retracted "green" conclusions because of this). Repeated registration also
discards old modules continuously, module-level state doubling.

Three boundaries:

| Situation | Behavior |
|-----------|----------|
| Cached module's `__file__` is the same file as `agents_dir/<name>.py` | Reuse, do not re-execute |
| Same name but from **another directory** (common with test fixtures, plugin dirs coexisting) | Do not reuse, reload from this directory's file |
| `reload=True` | Force re-execute (hot-plug new code entry) |

Reuse or not, AST safety scan still runs: the cached copy may have been brought in by ordinary
`import`, never gone through this check; checking only on "new load" equals giving imported modules
a free pass.

Criterion: `tests/test_agent_loader.py::TestModuleIdentity` (including "patch before registration
must take effect", "same name different directory must not reuse", "reload really changes object",
"still scanned on reuse"), changing the reuse condition back to "always new" turns three red.

## 7. Minimal Example

```python
"""agents/echo_agent.py — minimal custom Agent example"""
from loom_core.base_agent import AgentStatus, BaseAgent, Message

AGENT_NAME = "echo"
AGENT_VERSION = "1.0"
AGENT_DESC = "Demo Agent that echoes input"
AGENT_AUTHOR = "your-name"
AGENT_PRIORITY = 90
INPUT_TOPICS = ["echo.input"]
OUTPUT_TOPICS = ["echo.done"]
DEPENDENCIES = []
CACHE_TTL = 0
RESPAWN = False


class EchoAgent(BaseAgent):
    def handle(self, msg: Message) -> dict | None:
        payload = msg.payload if hasattr(msg, "payload") else {}
        text = payload.get("text", "")
        self.log_info(f"echo received: {text[:50]}")
        self.publish("echo.done", {"text": text})
        return {"status": "ok", "text": text}
```

Place in `agents/` directory, then `python run.py input.md --agent echo` or declare in pipeline YAML's
`agents:` list to be loaded. If Agent is not referenced by pipeline DAG, use `loom plugin list`
to confirm registration status.

## 8. Node-Level Config Comes from `payload["config"]`

YAML's `- name: xxx` under `config:` is **not guaranteed** to appear in `self.config`: the DAG executor
merges node config into the payload's `config` key for delivery, instance config is only a fallback.
So reading config must merge both:

```python
cfg = {**(self.config or {}), **(msg.payload or {}).get("config", {})}
url = str(cfg.get("url") or "")
```

Agents that only read `self.config` will be all-green in unit tests but fail or silently use defaults
on real pipelines — `http_request`'s first version was exactly this (unit tests directly constructed
instances with config, but production `api-report` runs immediately reported "no url given"). Similarly,
`CONFIG_SCHEMA` defaults only take effect when keys are missing, do not treat them as "will definitely
be read at runtime".

Artifact keys must be declared: `PRODUCES = {"response": "last"}`, `CONSUMES = [...]`. The engine
assembles downstream payloads by declaration; undeclared return values do not become artifacts;
file-writing Agents must also hit the delivery contract (`WRITES_OUTPUT`), otherwise "ran successfully
but no deliverable" goes undetected.

## 9. General Capabilities Must Have Production-Line Consumers

Adding a non-domain-semantic Agent (`agents/http_request_agent.py`, `agents/transform_agent.py`)
must simultaneously deliver a **pipeline that actually references it** (`pipelines/api-report.yaml`)
and an **offline E2E that runs the full chain**. Reason: this repo has repeatedly verified that
"implemented ≠ wired"; capability not entering any pipeline equals zero, and unit tests only prove
the function can be called, not that Scheduler → DAGExecutor → disk-write chain recognizes it.

E2E assertion targets are **the deliverables themselves**, not task.status: file exists, template has
no residual `{{`, rendered result is in the body. The other test case that breaks the interface
requires the pipeline to truthfully `failed` and carry the error out — "failure also truthful" and
"success has artifacts" are two sides of the same coin.
