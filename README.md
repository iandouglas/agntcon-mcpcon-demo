# agntcon-mcpcon-demo

Demo for **"Chaos Engineered: How to Build Invincible Agents and MCP Systems"** —
AgntCon / MCPCon North America, October 2026.

The audience breaks services from their phones while Temporal keeps a stream of
orders moving. An ordering agent talks to a remote MCP server whose every tool
call is itself a Temporal workflow.

One component in the whole demo genuinely dies — our agent worker. Everything of
Ian's degrades instead: a Redis flag makes calls 503 and Temporal absorbs them.

> **Picking this up from the other side of the integration?** Start with
> [`INTEGRATION.md`](INTEGRATION.md) — the contracts we consume, what we
> guarantee back, and which lines are load-bearing. This README is the run-book.

## Layout

| Path | Owner | What it is |
|---|---|---|
| `agent/src/richards_agent/` | this repo | The agent workflow, its activities, the worker |
| `agent/src/stage/` | this repo | The projector view — backend + single page |
| `agent/src/fake_mcp/` | this repo | Local stand-in for Ian's server and board |
| — | Ian's five repos | MCP server, mock API services, audience board, admin console |

## Setup, once

```bash
cd agent
uv venv --python 3.12
uv pip install -e ".[dev]"
uv run pytest tests/ -q          # 38 tests, no API key or network needed
cp .env.example .env             # then fill it in
```

`.env` needs `ANTHROPIC_API_KEY`, `MCP_AUTH_TOKEN` and `BOARD_ADMIN_TOKEN`.
Current values live in `iandouglas/temporal-agntcon-devnotes/notes-for-melissa.md`.

## Running the demo

**Source `.env` in every terminal** — that's the one rule:

```bash
cd agent && set -a && source .env && set +a
```

Four long-running terminals, plus one you type into.

| # | What | Command |
|---|---|---|
| 1 | Temporal (UI on :8233) | `temporal server start-dev` |
| 2 | Chaos feed for our worker | `cd agent && uv run uvicorn fake_mcp.server:app --port 8931` |
| 3 | The worker, supervised | `cd agent && ./scripts/run_worker.sh` |
| 4 | Stage screen (open :8900) | `cd agent && uv run uvicorn stage.server:app --port 8900` |
| 5 | Stream orders | `cd agent && uv run python scripts/order_stream.py` |
| 5 | Or just one order | `cd agent && uv run python -m richards_agent.starter` |
| 5 | Be the audience | `cd agent && uv run python scripts/audience.py kill agent_worker` |

`order_stream.py` keeps orders arriving — one every 6s, capped at 5 in flight.
It deliberately keeps starting them while the worker is dead: they queue as
RUNNING workflows, and when a fixer revives the worker it chews through the
backlog. That backlog is the demo's argument, and it's what makes "orders
completed" a number worth projecting.

Terminal 2 is only there because Ian's board tracks *his* services, not our
agent worker. Its MCP side is unused while `MCP_ENDPOINT` points at Ian's
server. Terminal 3 is supervised so a killed worker comes back — that restart
is where replay happens, which is the whole demo.

### Shorter variants

- **Just check the agent runs** — terminals 1, 3, 5.
- **No API key, no network** — terminals 1 and 2, then
  `uv run python scripts/e2e_chaos.py` (the model turn is scripted).
- **Ignore Ian's server entirely** — set `MCP_ENDPOINT=http://localhost:8931/mcp`.
- **Check the live integration** — `uv run python scripts/probe_live.py`.

### Two clusters, two UIs

Our agent workflow runs on **local Temporal** (:8233). Ian's tool-call workflows
run on **his own Temporal deployed on Railway** — self-hosted with Postgres, not
Temporal Cloud, whatever his MCP README says. When something looks missing,
check you're looking at the right one.

Eventually we point our worker at his Railway Temporal and it all becomes one
cluster with one UI. Deliberately deferred — it's an easy switch later.

## The thing to understand before changing anything

> **A tool call never fails because a service is down. It just takes longer.**

Ian's MCP server absorbs downstream failures and retries internally, so every
call eventually returns `completed: true`. Don't build error states for "service
down" — you will never see one. See `CLAUDE.md` for the failure taxonomy, the
four protocol traps, and why each one is silent until the audience starts
breaking things.

### Configuration

| Env var | Default | Notes |
|---|---|---|
| `MCP_ENDPOINT` | Ian's Railway server | Set to `http://localhost:8931/mcp` for the local fake |
| `MCP_AUTH_TOKEN` | unset | **Required** by Ian's server; 401 without it |
| `BOARD_URL` | Ian's audience board | Where the stage screen reads switch state |
| `BOARD_ADMIN_TOKEN` | unset | Without it the stage screen's left pane greys out |
| `AGENT_MODEL` | `claude-opus-5` | Latency untimed; switch to `claude-haiku-4-5` only after a stopwatch run |
| `AGENT_THINKING` | `on` | `off` disables thinking — the biggest latency lever |
| `TEMPORAL_ADDRESS` | `localhost:7233` | |
| `TEMPORAL_API_KEY` | unset | Set for Temporal Cloud (enables TLS) |
| `CHAOS_URL` | `localhost:8931/chaos` | Where the worker watchdog reads its own health |

Tools are discovered at runtime via `tools/list` — never hardcoded, because the
MCP server's tool names come from its own env vars.

## Open items

**Needs Ian:**

- **Arm the board for a rehearsal window.** `armed: false` today, so audience
  taps don't register and we can't break his services. Until then the only
  chaos we can drive is killing our own worker.
- **Is the `mcp` switch wired to dispatch?** The board reports `mcp: false`
  while tool calls succeed in 0.4s, and there's no `dispatch` record in its
  state.
- **Zero-argument tools, no business outcomes.** Services return only
  `200 {"<name>": true}` or a bare `500`, so the agent calls four tools in
  order with no branch. A `for` loop would look identical. A third return mode
  is *moderate* work in his Go service.

**Ours:**

- **LLM chaos.** `llm_api` shows in the health feed but breaking it does
  nothing — the Anthropic proxy was never written. It's the one chaos target
  only we can build, and the slide build-up promises it.
- **Stage latency.** A real model run works but has never been timed. Do it
  with a stopwatch before deciding between `claude-opus-5` and
  `claude-haiku-4-5`.
- **`e2e_chaos.py` still points at the fake MCP server.** Every piece is
  verified separately; the combination of Ian's live server *plus* a worker
  kill has not run in one go.

## Design references

- Ian's dev notes — https://github.com/iandouglas/temporal-agntcon-devnotes
- Demo spec — https://claude.ai/code/artifact/3b1e83d6-24e1-478e-9c02-54116a7af236
- Stage screen prototype (simulated, predates the real integration) — https://claude.ai/code/artifact/8d903f38-6e5c-4841-aeac-9f04fd8c0654
- `dejavu-tacos`, the pattern this borrows from — https://github.com/temporal-sa/demo-dejavu-tacos
