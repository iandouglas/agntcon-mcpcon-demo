# CLAUDE.md

Project guidance for the **"Chaos Engineered: How to Build Invincible Agents and
MCP Systems"** demo — AgntCon / MCPCon North America, October 2026.

## What this demo is

The audience breaks services from their phones while Temporal keeps an order
moving. An ordering agent talks to a remote MCP server whose every tool call is
itself a Temporal workflow.

The talk's claim, which every change should serve: **an LLM that retries is not
the same as a system that resumes. Persistence re-does the work; durability
doesn't.** The demo fails if it only shows "it came back." It works if the
audience sees work *not* being redone, measured.

## Ownership

Two people, five repos. Don't build across the line.

| Piece | Owner |
|---|---|
| Agent workflow (`agent/`) | this repo |
| Stage screen — the projector view (`agent/src/stage/`) | this repo |
| MCP server, mock API services, audience board, admin panel | Ian |

Ian's repos are the source of truth for every contract. Read them rather than
guessing, and prefer his dev notes over anything written here:

- `iandouglas/temporal-agntcon-devnotes` — **start here**, `notes-for-melissa.md`
  is the call sheet and `INTEGRATION-NOTES.md` the detail
- `iandouglas/temporal-agntcon-mcp` — the MCP server (Python). `DESIGN.md` is good
- `iandouglas/temporal-agntcon-service` — the mock services (Go)
- `iandouglas/temporal-agntcon-audience` — the audience board (TypeScript)
- `iandouglas/temporal-agntcon-testdemo` — admin panel

Everything is deployed on Railway, including **his own self-hosted Temporal**
(with Postgres and a temporal-ui service), not Temporal Cloud, whatever his MCP
README and `.env.example` say. **Our worker and stage screen now run there too**,
on his cluster, on our own task queue `richards-agent`. See `DEPLOY.md`.

Locally you still get a second cluster and a second UI from
`temporal server start-dev`, so when something looks missing, check you are
looking at the right one.

His services all run 2 replicas and his chaos is purely Redis flags, so **no
process of his ever dies**. Our agent worker is the only component in the demo
that actually crashes, and it is now a real switch on his board, with a public
`/chaos` feed the watchdog and `run_worker.sh` read. The room can kill us.

## Commands

```bash
cd agent
uv venv --python 3.12 && uv pip install -e ".[dev]"
uv run pytest tests/ -q                  # 38 tests, no API key or network needed
```

Secrets live in `agent/.env` (gitignored; see `.env.example`). Load per terminal:

```bash
cd agent && set -a && source .env && set +a
```

| What | Command |
|---|---|
| Probe Ian's live server | `uv run python scripts/probe_live.py` |
| Stream orders (what the talk runs on) | `uv run python scripts/order_stream.py` |
| A single order | `uv run python -m richards_agent.starter` |
| Supervised worker (survives being killed) | `./scripts/run_worker.sh` |
| Be the audience | `uv run python scripts/audience.py kill agent_worker` |
| Local chaos run, scripted model | `uv run python scripts/e2e_chaos.py` |

`MCP_ENDPOINT` defaults to Ian's live server. Point it at
`http://localhost:8931/mcp` to use the local fake instead.

## The four protocol traps

From Ian's notes, all verified against the live server. Each is silent in
rehearsal and fatal once the audience holds a service down — which is exactly
when you'd be discovering it. All four are handled in `mcp_client.py`; don't
undo them.

1. **`MCP-Protocol-Version: 2026-07-28` on every request.** Omit it and the
   server routes you to the legacy transport with no SSE keepalive; the call
   dies at ~5 minutes behind Railway's proxy.
2. **`params._meta` must carry both `io.modelcontextprotocol/*` envelope keys** —
   on *every* method, `tools/list` included. Without them: instant `-32602`.
3. **Responses are JSON *or* SSE.** Calls running silent past ~15s upgrade to
   `text/event-stream` with `: ping` comments. A JSON-only parser passes every
   test until a service is held down.
4. **Timeouts above 15 minutes.** His wait cap is 840s, Railway's ceiling 900s.
   Our client reads for 960s and the activity's `start_to_close` is 17 min.

## The mental model — the thing most likely to be got wrong

> **A tool call never fails because a service is down. It just takes longer.**

Ian's server absorbs downstream failures and retries internally, so a broken
service is a *slow call*, never a failed one. Every call eventually returns
`completed: true`.

Consequences:

- **Don't build error states for "service down."** You will never see one.
- `attempts` in the response is effectively **seconds of outage** — Temporal's
  retry timers floor at 1s, so his configured 250ms interval measures as ~1/sec.
  Narrate it that way; don't hardcode a rate in a label.
- **You cannot detect switch state from the call path.** Get it from the
  audience board, never by inference.
- **"Activity retries with backoff" is Ian's story now**, not ours. Ours is
  *resumed without redoing the work* — worker kill, replay, tokens not re-spent.
  Worth keeping straight so the two of you don't narrate the same beat.

## Failure taxonomy in `mcp_client.py`

Two exception types draw the line:

- `McpInfrastructureError` → **raised**, Temporal retries. The MCP server itself
  unreachable, or our stream severed. Retrying is safe because the idempotency
  key attaches it to the original workflow.
- `McpProtocolError` → **raised non-retryable**. We got the request wrong (bad
  token, malformed params). Retrying can't fix it, so fail fast and loudly.
- A tool result with `isError: true` → **returned** to the model as a business
  outcome it can re-plan around.

That third path currently **cannot happen**: Ian's services return only
`200 {"<name>": true}` or a bare `500`, so there are no business outcomes. The
path costs nothing and is where out-of-stock would land if he adds a third mode.
`tests/test_agent.py::test_out_of_stock_is_not_retried` guards it either way —
if it starts failing with a retry storm, fix the classification, not the test.

## Conventions in this repo

- **Never hardcode tool names.** Discovered at runtime; they come from Ian's
  Railway env vars and have already changed once (`order_coffee` →
  `order_churro`). `/healthz` is public and lists them without auth.
- **No retry logic in workflow or activity code.** Retrying is a `RetryPolicy`
  argument. `FOREVER` uses `maximum_attempts=0` — unlimited, deliberately, and
  it's what lets you dare the room to stop the order.
- **`max_retries=0` on the Anthropic client.** The SDK retries 429s internally
  by default, which would bury a rate limit inside the activity where Temporal
  can't see it, back off from it, or show it on the timeline. Don't "fix" this.
- **Idempotency keys derive from workflow state only** —
  `f"{workflow_id}-t{turn}-c{index}"`. Never a UUID, not even
  `workflow.uuid4()`: the key must be byte-identical across replays, or a
  re-issued call reads as a brand new order.
- **Dedup is counted from `activity.info().attempt`**, not from the server.
  Attempt 2+ means the first attempt already reached the MCP server and the key
  is the only reason we didn't order twice. Needs nothing from Ian.
- **`os._exit(1)` in the worker watchdog is intentional.** A graceful drain
  would finish in-flight activities and prove nothing.

## `fake_mcp/` is scaffolding

A local stand-in for Ian's server *and* the audience board, so the agent can be
developed and rehearsed offline. It mirrors his observable contract — envelope
enforcement, `structuredContent`, blocking-and-SSE on a broken service, dedup by
key — but it is not Temporal-backed, and dedup is a dict.

It is easy to make this file lie. An earlier version returned `503` for a
downed service, which trained the agent for a failure mode that does not exist.
If you change it, change it toward what the real server does.

## Model configuration

`AGENT_MODEL` defaults to `claude-opus-5`; `AGENT_EFFORT` to `low`, because this
is a simple well-specified tool loop and dead air on stage is expensive.
`AGENT_THINKING=off` disables thinking entirely.

A real model run works, but **stage latency has never been timed.** Do that with
a stopwatch before deciding whether to drop to `claude-haiku-4-5`. Keep
`AGENT_MAX_TOKENS` well above the thinking budget — a truncated turn used to
read as "order finished" until that was made a loud failure.

## Open decisions with Ian

From `notes-for-melissa.md` §7 — these need a conversation, not a code change:

- **Tools take zero arguments** and can't report business outcomes, so the agent
  calls four tools in order with no branch. A `for` loop would look identical,
  and an AI-conference audience will notice. Adding a third return mode is
  *moderate* work in his Go service.
- **Health: booleans or 0–100?** He recommends services stay binary with HP held
  in the board. The stage-screen prototype assumed 0–100.
- **Who owns the snapshot translation** — his board's shape, or ours.
- **Where does the stage screen run?** Inside his Railway project it gets
  `/admin/live` for free. Outside, he'd have to expose port 8081 publicly, which
  puts the admin control plane on the internet behind one token.

## The stage screen

`agent/src/stage/` — FastAPI backend that merges our workflow state with Ian's
board and pushes full snapshots over SSE to a single page. Run it with
`uvicorn stage.server:app --port 8900`.

Two things in it are load-bearing:

- **`board.py` accepts booleans *or* percentages.** The deployed board reports
  booleans; Ian is moving to a 0-100 tug-of-war with a threshold at 50. Both
  shapes are tested. Don't collapse it to one until the new one has been live a
  while.
- **"Tokens not re-spent" is measured, not modelled.** When the worker dies the
  workflow query stops answering; when it answers again the workflow has
  replayed and `tokens_spent` comes back *unchanged*, because completed
  `llm_turn`s were served from history. At each recovery we bank the tokens
  already in history. That number is defensible because it describes something
  that happened, not a hypothetical naive agent.
  `collector.py` also distinguishes a closed workflow from a dead worker, so a
  clean finish doesn't log a fake death at the end of every run.
- **`orders.py` submits a batch in the background, never inline.** A thousand
  orders is a thousand RPCs; holding the HTTP response open for them reads as
  a hung button. It also only ever runs one batch at a time, and `stop()`
  cancels the *remaining* submissions — orders already started are left alone,
  because they are durable and killing them to tidy up undoes the claim.
  Relatedly, `_auto_follow` adopts a new order only when the current one is
  closed: with hundreds in flight it used to re-target twice a second.

## Two different "start the demo" buttons

They are unrelated and both are needed:

- **Ian's rehearsal console** opens the floor — his switches go from inert to
  live. Nothing the audience taps registers before that.
- **The stage screen's order control** starts orders flowing: a count and a
  button in the header. The count is that many *separate* orders, not one
  order of that size, and the big numbers are meant to outrun the worker so
  the queued-vs-fulfilled gap is visible from the back of the room.
  `scripts/order_stream.py` does the same job from a terminal, paced and
  capped, which is still the better tool for a rehearsal you leave running.
  Both are reachable to Ian now; we share his Temporal cluster, so anything
  that can talk to it can start a `TacoOrderAgent` on the `richards-agent`
  task queue.

## Not built yet

- **LLM chaos.** `llm_api` appears in the health feed but breaking it does
  nothing — the Anthropic proxy was never written. This is the one chaos target
  only we can build, and it's the one the slide build-up promises.
