# CLAUDE.md

Project guidance for the **"Chaos Engineered: How to Build Invincible Agents and
MCP Systems"** demo — AgntCon / MCPCon North America, October 2026.

## What this demo is

The audience sabotages a live taco order from their phones while Temporal keeps
resuming it. A taco-ordering agent talks to Richard's MCP server; the agent, the
MCP server, and the audience voting app are all Temporal workflows.

Richard owns the shop **and** is the agent — the robot from Replay 2026, now
working the counter.

The talk's claim, which every change should serve: **an LLM that retries is not
the same as a system that resumes. Persistence re-does the work; durability
doesn't.** The demo fails if it only shows "it came back." It works if the
audience sees work *not* being redone, measured.

## Ownership

Two people, two repos. Don't build across the line.

| Piece | Owner |
|---|---|
| Agent workflow (`agent/`) | this repo |
| Stage screen — the projector view (`stage-ui/`) | this repo |
| API services, MCP server, audience "circuit breaker" site | colleague's repo |

The stage screen is a **consumer** of the colleague's component-health feed, not
a second source of truth. Don't add a `ChaosWorkflow` here without confirming
who owns that state.

## Commands

```bash
cd agent
uv pip install -e ".[dev]"        # after a fresh clone; uv venv --python 3.12 first
uv run pytest tests/ -q           # 11 tests, no API key needed
```

Full chaos run — three terminals, no API key needed (the model turn is scripted):

```bash
temporal server start-dev
cd agent && uv run uvicorn fake_mcp.server:app --port 8931
cd agent && uv run python scripts/e2e_chaos.py
```

Real model run needs `ANTHROPIC_API_KEY`:

```bash
cd agent && uv run python -m richards_agent.worker      # terminal A
cd agent && uv run python -m richards_agent.starter     # terminal B
```

## The invariant that matters most

`agent/src/richards_agent/mcp_client.py::_classify()` draws one line:

- **Infrastructure failure** (503, 429, connection refused, timeout, JSON-RPC
  error) → **raise**. Temporal retries with backoff. The model is never
  re-prompted, so no tokens are re-spent.
- **Business outcome** (out of stock, card declined, bad address) → **return**
  with `is_error=True`. The activity *succeeds*; the result reaches the model,
  which re-plans.

Why it matters: the agent runs unlimited retries. If a business outcome is
raised instead of returned, `execute_activity` never returns, the result never
lands in `messages`, and the model is never asked again. The workflow sits there
hammering a tool that is working correctly. On stage that reads as "durable
execution is stuck," which is the opposite of the point.

`tests/test_agent.py::test_out_of_stock_is_not_retried` guards this. If you
change error handling and that test starts failing with a retry storm, the fix
is the classification, not the test.

## Conventions in this repo

- **Never hardcode tool names.** Tools are discovered at runtime via
  `tools/list` because the MCP server's tool names come from *its* env vars. The
  agent doesn't know what Richard sells until it asks — which is also a line in
  the talk.
- **No retry logic in workflow or activity code.** Retrying is a `RetryPolicy`
  argument. `FOREVER` in `workflow.py` uses `maximum_attempts=0`, which means
  unlimited — that's deliberate, and it's what lets you dare the room to stop
  the order.
- **`max_retries=0` on the Anthropic client.** The SDK retries 429s and 5xxs
  internally by default, which would bury a rate limit inside the activity where
  Temporal can't see it, back off from it, or show it on the timeline. Temporal
  is the retry layer. Don't "fix" this.
- **Idempotency keys are derived from workflow state only** —
  `f"{workflow_id}-t{turn}-c{index}"`. Never `uuid4()`, not even
  `workflow.uuid4()`: the key must be byte-identical across replays, or a
  retried `take_payment` looks like a new charge.
- **Don't cache business errors against an idempotency key.** The agent restocks
  and retries the same logical call; serving it a stale out-of-stock result
  wedges the order permanently. See `fake_mcp/server.py` — only successful
  results are remembered.
- **`os._exit(1)` in the worker watchdog is intentional.** A graceful drain
  would finish in-flight activities and prove nothing. The audience needs a real
  crash.

## Model configuration

`AGENT_MODEL` defaults to `claude-opus-5`. Stage latency is **unmeasured** — no
real model run has happened yet. Time a full order end to end before deciding
whether to drop to `claude-haiku-4-5`; that's a stopwatch decision, not a
guess. `AGENT_EFFORT` defaults to `low` because this is a simple, well-specified
tool loop and dead air on stage is expensive.

## `fake_mcp/` is scaffolding

It exists so the agent could be built before the real MCP server was ready. It
is **not** the real thing: no Temporal, no workflow per tool call, dedup is a
dict. It does implement the classification contract correctly, which is the
point. Delete it once the real endpoint is up and stable.

## Open integration questions

Unresolved with the colleague — check before assuming any of them:

- Does the real MCP server return out-of-stock as a **result** or an **error**?
- Will tool results carry `_meta.deduplicated` when a retried call attaches to
  an existing execution? Dedup happens inside their MCP server, so the agent can
  only count it if they report it — and it's the number behind the demo's best
  moment ("charged exactly once").
- Does the server read the idempotency key from the `Idempotency-Key` header or
  MCP `_meta.idempotencyKey`? The agent currently sends both.
- Who owns component-health state?
