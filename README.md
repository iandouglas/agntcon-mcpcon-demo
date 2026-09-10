# agntcon-mcpcon-demo

Demo for **"Chaos Engineered: How to Build Invincible Agents and MCP Systems"** —
AgntCon / MCPCon North America, October 2026.

The audience sabotages a live taco order while Temporal keeps resuming it from
where it left off. A taco-ordering agent talks to Richard's MCP server; both are
Temporal workflows, and so is the app the room votes in.

## Layout

| Path | Owner | What it is |
|---|---|---|
| `agent/` | this repo | The Temporal agent workflow, its activities, and a fake MCP server to develop against |
| `stage-ui/` | this repo | The projector view (see prototype link below) |
| — | colleague's repo | API services, MCP server, audience "circuit breaker" site |

## agent/

```bash
cd agent
uv venv --python 3.12
uv pip install -e ".[dev]"
uv run pytest tests/ -q
```

### Run it end to end

Three terminals, no API key needed for the chaos script (the model turn is scripted):

```bash
temporal server start-dev
```

```bash
cd agent && uv run uvicorn fake_mcp.server:app --port 8931
```

```bash
cd agent && uv run python scripts/e2e_chaos.py
```

That starts an order, kills the MCP server mid-flight, leaves it down through
several retries, brings it back, and then checks the customer was charged once.

For a real model run, set `ANTHROPIC_API_KEY` and use the worker + starter:

```bash
cd agent && uv run python -m richards_agent.worker      # terminal A
cd agent && uv run python -m richards_agent.starter     # terminal B
```

### The one thing to understand before changing anything

`agent/src/richards_agent/mcp_client.py` draws a single line, in `_classify()`:

- **infrastructure failure** (503, 429, connection refused, timeout) → **raises** →
  Temporal retries with backoff, and the model is never re-prompted.
- **business outcome** (out of stock, card declined) → **returns** with
  `is_error=True` → the model sees it and re-plans.

Get that backwards and the agent retries "out of stock" forever while the token
counter climbs — the exact opposite of the talk's point. `tests/test_agent.py::
test_out_of_stock_is_not_retried` is the guard.

### Configuration

| Env var | Default | Notes |
|---|---|---|
| `MCP_ENDPOINT` | `http://localhost:8931/mcp` | Point at the real MCP server when it's up |
| `AGENT_MODEL` | `claude-opus-5` | Stage latency is unmeasured; switch to `claude-haiku-4-5` only after timing a real run |
| `TEMPORAL_ADDRESS` | `localhost:7233` | |
| `TEMPORAL_API_KEY` | unset | Set for Temporal Cloud (enables TLS) |
| `CHAOS_URL` | unset | When set, the worker watchdog hard-exits on a down vote |

Tools are discovered at runtime via `tools/list` — never hardcoded, because the
MCP server's tool names come from its own env vars.

## Open items

- **Real MCP endpoint.** Still pointed at `fake_mcp`. Swapping is one env var.
- **Idempotency key.** The agent sends a stable key per tool call as both an
  `Idempotency-Key` header and MCP `_meta.idempotencyKey`. Needs confirming
  against the real server.
- **`deduplicated` flag.** Dedup happens inside the MCP server, so the agent
  can only count it if the tool result reports it. Not yet agreed.
- **Chaos state ownership.** If the audience site is the source of truth, the
  stage screen subscribes to it rather than owning a `ChaosWorkflow`.
- **Latency.** No real model run yet.

## Design references

- Demo spec — https://claude.ai/code/artifact/3b1e83d6-24e1-478e-9c02-54116a7af236
- Stage screen prototype (interactive, simulated) — https://claude.ai/code/artifact/8d903f38-6e5c-4841-aeac-9f04fd8c0654
- `dejavu-tacos`, the pattern this borrows from — https://github.com/temporal-sa/demo-dejavu-tacos
