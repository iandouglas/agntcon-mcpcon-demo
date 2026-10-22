# Integration notes

For Ian, and for any coding agent working in this repo. Written at handoff,
2026-09-15. Everything here is verified against the code, not remembered.

`README.md` is the run-book — how to start the thing. This file is the contract:
what we consume from your side, what we guarantee back, and which lines are
load-bearing enough that changing them breaks the talk.

---

## 1. What this repo owns

| Path | What |
|---|---|
| `agent/src/richards_agent/` | The agent as a Temporal workflow, its activities, the worker |
| `agent/src/stage/` | The projector view — FastAPI backend + one page, SSE snapshots |
| `agent/src/fake_mcp/` | Offline stand-in for **both** your MCP server and the audience board |
| `agent/scripts/` | Probe, order stream, audience simulator, scripted end-to-end run |

Not ours: the MCP server, the mock Go services, the audience board, the admin
console. Your repos are the source of truth for every contract below — if this
file and your repos disagree, yours win and this one is stale.

**Setup and verification, start to finish:**

```bash
cd agent
uv venv --python 3.12 && uv pip install -e ".[dev]"
uv run pytest tests/ -q          # 38 tests, ~11s, no API key and no network
```

If those 38 pass, the protocol handling and the failure classification are
intact. That suite is the guardrail — run it after any change in here.

---

## 2. The one thing to load into your head first

> **A tool call never fails because a service is down. It just takes longer.**

Your MCP server absorbs downstream failures and retries internally, so a broken
service is a *slow call*, never a failed one. Every call eventually returns
`completed: true`.

Three consequences that shape all of our code:

- **There are no "service down" error states on our side, by design.** Don't add
  any. We will never observe one.
- `attempts` in the response is effectively **seconds of outage** — Temporal's
  retry timers floor at 1s, so a configured 250ms interval measures as ~1/sec.
- **We cannot detect switch state from the call path.** The stage screen reads it
  from the board, never by inference.

Narration split, so we don't tell the same story twice: *activity retries with
backoff* is yours. Ours is *resumed without redoing the work* — worker killed,
workflow replayed, tokens not re-spent.

---

## 3. What we consume from your MCP server

Implemented in [`agent/src/richards_agent/mcp_client.py`](agent/src/richards_agent/mcp_client.py).
Verified against the live server. Each of these four is silent in rehearsal and
fatal once the room holds a service down — i.e. you'd discover it on stage.

1. **`MCP-Protocol-Version: 2026-07-28`** on every request. Omit it and we get
   routed to the legacy transport with no SSE keepalive; the call dies at ~5 min
   behind Railway's proxy.
2. **`params._meta` carries both `io.modelcontextprotocol/*` envelope keys** —
   `protocolVersion` and `clientCapabilities`, on *every* method including
   `tools/list`. Without them: instant `-32602`, no workflow starts.
3. **Responses are JSON *or* SSE.** Calls running silent past ~15s upgrade to
   `text/event-stream` with `: ping` comments. Our parser handles both and
   accumulates multi-line `data:` fields.
4. **Timeouts above 15 minutes.** Your wait cap is 840s, Railway's ceiling 900s.
   Our client reads for **960s**; the activity's `start_to_close` is **17 min**.

**Requests we send:**

| Where | What |
|---|---|
| Header | `MCP-Protocol-Version`, `Authorization: Bearer $MCP_AUTH_TOKEN`, `Mcp-Method`, `Mcp-Name` (tool calls only), `Accept: application/json, text/event-stream` |
| `params._meta` | the two envelope keys, plus `idempotencyKey` on `tools/call` |

**Note the idempotency key travels in `params._meta.idempotencyKey` only** — we
do *not* send an `Idempotency-Key` header. If your server reads the header
instead, dedup silently stops working and we double-order on a retry. Worth
confirming once.

**Response fields we actually read:**

| Field | Used for |
|---|---|
| `result.structuredContent.completed` | whether the call finished |
| `result.structuredContent.response` | what the model is told |
| `result.structuredContent.attempts` | the outage-seconds number on the screen |
| `result.structuredContent.workflow_id` | shown on screen; `probe_live.py` asserts on it |
| `result.isError` | returned to the model as a business outcome (see §5) |
| `result._meta.deduplicated` | honoured if present, not required — see §6 |
| `/healthz` (public, no auth) | liveness, and the documented place tool names appear |

Tool names are **discovered at runtime** via `tools/list`, never hardcoded — they
come from your Railway env vars and have already changed once
(`order_coffee` → `order_churro`).

---

## 4. What we consume from the audience board

Implemented in [`agent/src/stage/board.py`](agent/src/stage/board.py). We
**poll** `GET {BOARD_URL}/api/admin/state` with `Authorization: Bearer
$BOARD_ADMIN_TOKEN`. We do not hold a websocket.

Fields read: `names`, `state`, `taps`, `services[].kind`, `armed`, `conns`,
`bus.healthy`.

`state` values are accepted as **booleans or 0–100 numbers** (threshold: `>=50`
is up, so a 50/50 resting state reads UP), or a `{"up":…, "percent":…}` dict.
Both shapes are tested. Your move to the tug-of-war is a non-event on our side —
please don't feel you need to coordinate the deploy.

`board.fetch()` **never raises**. An unreachable board greys out one panel
instead of taking the projector down mid-talk. Unrecognised shapes fail *open*
(assume healthy), because a false "everything is broken" is the worse failure on
a projector.

---

## 5. Failure taxonomy — the line that matters

Two exception types in `mcp_client.py`:

| Condition | Type | Temporal behaviour |
|---|---|---|
| MCP server unreachable, stream severed, 5xx, 429 | `McpInfrastructureError` | **Raised** → retried forever with backoff |
| 401, malformed params, JSON-RPC `error`, non-JSON body | `McpProtocolError` | **Raised non-retryable** → fails fast and loudly |
| `result.isError: true` | *not* an exception | **Returned** to the model as a business outcome to re-plan around |

That third path **currently cannot happen**: your services return only
`200 {"<name>": true}` or a bare `500`, so there are no business outcomes. The
path costs nothing and is where out-of-stock would land if you add a third mode.
`tests/test_agent.py::test_out_of_stock_is_not_retried` guards it either way.

**Why the line matters:** the agent retries unlimited. A business outcome that
*raises* instead of *returning* means `execute_activity` never returns, the
result never reaches the model, and the workflow hammers a tool that is working
correctly. On stage that reads as "durable execution is stuck", which is the
opposite of the point. If that test ever fails with a retry storm, fix the
classification, not the test.

---

## 6. What we guarantee back to you

- **Idempotency keys are `f"{workflow_id}-t{turn}-c{index}"`** — derived from
  workflow state only, never a UUID, not even `workflow.uuid4()`. They are
  byte-identical across replays, so a re-issued call after a worker death
  carries the *same* key and must attach to your existing execution rather than
  read as a new order. This is the mechanism behind "charged exactly once".
- **Dedup is counted on our side**, from `activity.info().attempt > 1` — not from
  your response. Attempt 2+ means the first attempt already reached you and the
  key is the only reason we didn't order twice. **This needs nothing from you.**
  We also honour `_meta.deduplicated` if you ever set it.
- **One workflow type, one task queue**: `TacoOrderAgent` on `richards-agent`.
- **A `status` query** returns `AgentState` (see
  [`shared.py`](agent/src/richards_agent/shared.py)) — order id, step, turns,
  tool calls, `tokens_spent`, `cache_read_tokens`, `dedup_hits`,
  `downstream_attempts`. That's the read surface if you ever want our numbers.

---

## 7. Naming — `richards_*` is deliberate, please keep it

The package is `richards_agent`, the activities are `list_richards_tools` and
`call_richards_tool`, the task queue is `richards-agent`, and the system prompt
opens with "You are the ordering agent for Richard's Taquería."

**This is the theme, not a leftover.** Richard is the robot from Replay 2026 —
he owns the shop *and* is the agent, working the counter. It carries through the
talk, and it's the naming convention to match if you add anything on your side
that wants to look like part of the same system. A coding agent reading this
repo cold will be tempted to "clean up" the name. Don't.

---

## 8. Two clusters, two UIs

Our agent workflow runs on **local Temporal** (`localhost:7233`, UI on `:8233`).
Your tool-call workflows run on **your own self-hosted Temporal on Railway** —
with Postgres and a temporal-ui service, *not* Temporal Cloud, whatever the MCP
README and `.env.example` say.

So there are two clusters and two UIs right now. When something looks missing,
check you're looking at the right one. Pointing our worker at your Railway
Temporal is a deliberate later step and an easy switch (`TEMPORAL_ADDRESS`).

---

## 9. Rules for a coding agent working in this repo

Each of these is load-bearing for the demo, and each looks like a bug or an
oversight if you don't know why:

- **No retry logic in workflow or activity code.** Retrying is a `RetryPolicy`
  argument. `FOREVER` uses `maximum_attempts=0` — unlimited, deliberately. It's
  what lets Melissa dare the room to stop the order.
- **`max_retries=0` on the Anthropic client.** The SDK retries 429s internally by
  default, which would bury a rate limit inside the activity where Temporal
  can't see it, back off from it, or show it on the timeline. Temporal is the
  retry layer. Don't "fix" this.
- **Heartbeats are not optional.** Activities heartbeat every 2s against a 5s
  `heartbeat_timeout`. Without them Temporal can't tell a busy worker from a dead
  one and waits out `start_to_close` — 17 minutes of stalled demo after the
  audience kills the worker. With them, seconds.
- **`os._exit(1)` in the worker watchdog is intentional.** A graceful drain would
  finish in-flight activities and prove nothing. The room needs a real crash.
- **Never hardcode tool names.** Runtime discovery is also a line in the talk.
- **Don't collapse `board.py` to one switch shape** until the threshold version
  has been live a while.
- **Don't make `fake_mcp/` lie.** It mirrors your observable contract; an earlier
  version returned `503` for a downed service and trained the agent for a failure
  mode that doesn't exist. Change it *toward* what the real server does.
- **"Tokens not re-spent" is measured, not modelled** — at each recovery we bank
  the tokens already in history, because completed `llm_turn`s are served from
  history on replay. The number describes something that happened. Keep it that
  way; a modelled "naive agent would have spent X" is not defensible on stage.

**To verify a change:** `uv run pytest tests/ -q` (38, offline), then
`uv run python scripts/probe_live.py` against the live server. The probe does
`tools/list`, one real call, then **re-issues the same idempotency key** and
asserts it comes back with the same `workflow_id` — that's the dedup guarantee
checked end to end. Run it after either side deploys, and again before the talk.

---

## 10. Open items

**Needs you:**

- **Arm the board for a rehearsal window.** `armed: false` today, so audience
  taps don't register and we can't break your services. Until then the only
  chaos we can drive is killing our own worker.
- **Is the `mcp` switch wired to dispatch?** The board reports `mcp: false` while
  tool calls succeed in 0.4s, and there's no `dispatch` record in its state.
- **Confirm the idempotency key is read from `_meta`, not a header** (§3).
- **Zero-argument tools, no business outcomes.** The agent calls four tools in
  order with no branch — a `for` loop would look identical, and an AI-conference
  audience will notice. A third return mode is *moderate* work in the Go
  service. This is the biggest open product question, not a bug.
- **Health: booleans or 0–100?** You recommend binary services with HP in the
  board; the stage prototype assumed 0–100. We handle both, so this is a
  narration decision, not a blocker.
- **Who owns the snapshot translation** — your board's shape or ours.
- **Where does the stage screen run?** Inside your Railway project it gets
  `/admin/live` for free. Outside, you'd have to expose port 8081 publicly, which
  puts the admin control plane on the internet behind one token.

**Ours, still to do:**

- **LLM chaos.** `llm_api` appears in the health feed but breaking it does
  nothing — the Anthropic proxy was never written. It's the one chaos target only
  we can build, and the slide build-up promises it.
- **Stage latency has never been timed.** A real model run works. Time a full
  order with a stopwatch before deciding between `claude-opus-5` and
  `claude-haiku-4-5`; `AGENT_THINKING=off` is the biggest single lever.
- **`e2e_chaos.py` still points at the local fake.** Every piece is verified
  separately, but *your live server plus a worker kill* has not run in one go.
  This is the demo's central moment and the highest-value thing left to rehearse.

---

## 11. Reference

- Your dev notes — https://github.com/iandouglas/temporal-agntcon-devnotes
  (how the repos fit together, and `RAILWAY.md` for deploying them)
- `dejavu-tacos`, the pattern this borrows from —
  https://github.com/temporal-sa/demo-dejavu-tacos
- Failure taxonomy, protocol traps, and stage-screen internals in more depth:
  `CLAUDE.md` in this repo.
