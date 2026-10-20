# Deploying to Railway

Both of our pieces run in Ian's `agentcon-demo` Railway project, alongside his
MCP server, mock services, audience board and self-hosted Temporal. One image,
two services, different start commands. Same shape his MCP server uses, so
there is one thing to debug at 9am rather than two.

| Service | Start command | Public domain |
|---|---|---|
| `agent-worker` | `./scripts/run_worker.sh` | none, outbound only |
| `stage-screen` | `./scripts/run_stage.sh` | yes, the projector view |

Both build from `agent/Dockerfile` with **Root Directory `agent`**.

## Why these two scripts and not a bare command

**`run_worker.sh`, not `python -m richards_agent.worker`.** The supervisor loop
is the point: when the room votes the worker below 50% the watchdog calls
`os._exit(1)`, and the loop then *refuses to restart it* until the chaos feed
says a fixer has pushed it back over the line. Railway would restart a bare
worker process immediately, which makes the kill cosmetic and the fixers
decorative.

**`run_stage.sh`, not `uvicorn stage.server:app`.** uvicorn binds `127.0.0.1`
by default. Behind Railway's proxy that produces a green deploy, healthy logs,
and a 502 on every single request. The script passes `--host 0.0.0.0` and
Railway's `$PORT`.

## Environment

Everything is reachable over Railway's private network, so none of this traffic
leaves the project. Private addresses need an explicit port; `.railway.internal`
has no default.

### Shared with Ian's services

Use `${{shared.<name>}}` rather than copying values; he rotates the tokens after
the talk and there should be one place to do it.

| Variable | Value |
|---|---|
| `TEMPORAL_ADDRESS` | `${{shared.temporal_address}}` |
| `TEMPORAL_NAMESPACE` | `${{shared.temporal_namespace}}` |

We run on his Temporal cluster but on **our own task queue**, `richards-agent`
(hardcoded in `shared.py`). His `temporal_task_queue` shared variable is
`durable-mcp` and is not ours; do not set it here.

### `agent-worker`

| Variable | Value | Why |
|---|---|---|
| `ANTHROPIC_API_KEY` | *(secret)* | `llm_turn` 401s without it |
| `MCP_ENDPOINT` | `http://mcp.railway.internal:8080/mcp` | his MCP server |
| `MCP_AUTH_TOKEN` | *(secret)* | the token half of his `mcp_agent_tokens` |
| `CHAOS_URL` | `http://audience-site.railway.internal:8080/chaos` | how the room kills us |
| `CHAOS_COMPONENT` | `agent_worker` | the switch on his board |
| `CHAOS_THRESHOLD` | `50` | matches his board's tipping point |

Going over the private network to the MCP server also drops Railway's 15-minute
edge cap, which is the one hard ceiling above our 960s read timeout. The four
protocol traps in `mcp_client.py` still apply; they are about his server, not
about the proxy.

`WORKER_FORCE_REVIVE_SECONDS` is a rehearsal escape hatch that revives the
worker whether or not anyone fixed it. Leave it unset on stage: it takes the
decision away from the room.

### `stage-screen`

| Variable | Value | Why |
|---|---|---|
| `BOARD_URL` | `http://audience-site.railway.internal:8080` | origin, not a websocket URL |
| `BOARD_ADMIN_TOKEN` | `${{shared.admin_token}}` | the board rejects an unauthenticated read |
| `STAGE_COMPONENTS` | `all` | without this the screen shows only `agent_worker` |
| `STAGE_ADMIN_TOKEN` | `${{shared.admin_token}}` | guards `POST /api/orders/terminate`; unset disables it |
| `PORT` | *(Railway sets it)* |; |

`STAGE_ADMIN_TOKEN` is the only authenticated thing on the stage screen. The
rest of its API is open, which is fine for a screen you point at a projector,
but `POST /api/orders/terminate` ends every running order and the service
answers on a public hostname. Leave the variable unset and the endpoint returns
503 rather than running: a teardown switch reachable by anyone who finds the URL
is worse than one that is off. The control panel's STOP ALL ORDERS button is
what calls it, so it needs the same value Ian's board uses.

`STAGE_COMPONENTS` defaults to `agent_worker` alone, which was right while his
five switches were permanently UP and therefore noise. They are live now, so the
screen should show all six.

## What the room can actually break, once this is up

`agent_worker` is a real switch on his board now, and this is the only component
in the whole demo where a process genuinely dies. Verified end to end against a
live board and a real Temporal:

```
--- agent_worker at 0% · waiting for a fixer to cross 50% ---
--- revived after 10s down ---
--- worker generation 1 starting ---
agent_worker at 33%; below 50%, the breakers won. exiting hard
--- worker died (exit 1) ---
--- agent_worker at 33% · waiting for a fixer to cross 50% ---
```

## Checks before the talk

- `stage-screen`'s public URL renders and the left pane lists six components.
- Its scoreboard counts orders. That needs Temporal **visibility**, which works
  on his Postgres-backed cluster; if `orders completed` sticks at 0 while orders
  are plainly finishing, look for `order count query failed` in the logs rather
  than assuming the demo is broken.
- `agent-worker`'s logs say `watchdog armed`. If they say
  `CHAOS_URL unset; watchdog DISABLED`, the room cannot kill it.
- Start an order and watch it land on the stage screen.
