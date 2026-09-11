"""MCP client for Ian's durable MCP server.

Written against the contract in `temporal-agntcon-devnotes/notes-for-melissa.md`
and verified against the live server. Four things here are load-bearing; each
one is silent in rehearsal and fatal the moment the audience breaks something.

1. `MCP-Protocol-Version: 2026-07-28`. Omit it and the server routes us to the
   legacy transport, which has no SSE keepalive — the call then dies at ~5 min
   behind Railway's proxy, mid-outage.
2. `params._meta` carrying the two `io.modelcontextprotocol/*` envelope keys.
   Required on EVERY request, tools/list included. Without them: instant -32602
   and no workflow starts.
3. The response is JSON *or* SSE. Any call running silent past ~15s upgrades to
   `text/event-stream` with `: ping` comments. A JSON-only parser works right up
   until a service is held down, which is the entire demo.
4. Timeouts above 15 minutes. The server's own wait cap is 840s.

One thing NOT to build: error states for "service is down". Ian's server absorbs
those and retries internally, so a broken service is a slow call, never a failed
one. Every call eventually returns `completed: true`.
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from .shared import ToolResult, ToolSpec

PROTOCOL_VERSION = "2026-07-28"

# The envelope every request must carry inside params._meta.
_ENVELOPE: dict[str, Any] = {
    "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
    "io.modelcontextprotocol/clientCapabilities": {},
}


class McpInfrastructureError(Exception):
    """Transport-level breakage. Retryable — Temporal will try again.

    Note this is NOT "a downstream service is down"; the server hides that from
    us. This is the MCP server itself being unreachable, or our stream severed.
    """


class McpProtocolError(Exception):
    """The server answered, but we got the request wrong. Retrying won't help."""


def _headers(token: str, method: str, tool_name: str | None = None) -> dict[str, str]:
    headers = {
        "MCP-Protocol-Version": PROTOCOL_VERSION,
        "Mcp-Method": method,
        "Authorization": f"Bearer {token}",
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
    }
    if tool_name:
        headers["Mcp-Name"] = tool_name
    return headers


def _timeout(read_seconds: float) -> httpx.Timeout:
    # Generous read, tight connect: a server we can't reach should fail fast so
    # Temporal retries, while a server that's working hard gets all the time it
    # needs.
    return httpx.Timeout(connect=10.0, read=read_seconds, write=30.0, pool=10.0)


async def _parse_sse(response: httpx.Response) -> dict:
    """Pull the first JSON-RPC message out of an SSE stream.

    Skips `: ping` keepalive comments and accumulates multi-line `data:` fields
    per the SSE spec rather than assuming one line per event.
    """
    buffer: list[str] = []

    async for line in response.aiter_lines():
        line = line.rstrip("\r")

        if line.startswith(":"):          # keepalive comment — the whole point
            continue
        if line.startswith("data:"):
            buffer.append(line[5:].lstrip())
            continue
        if line == "" and buffer:         # blank line ends the event
            break
        # event:, id:, retry: — irrelevant to us

    if not buffer:
        raise McpInfrastructureError("SSE stream ended before any data arrived")

    try:
        return json.loads("\n".join(buffer))
    except json.JSONDecodeError as exc:
        raise McpProtocolError(f"SSE data was not JSON: {exc}") from exc


async def _rpc(
    endpoint: str,
    token: str,
    method: str,
    params: dict[str, Any],
    read_seconds: float,
    tool_name: str | None = None,
) -> dict:
    """One JSON-RPC round trip, handling both response encodings."""
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        # Merge rather than overwrite: callers add idempotencyKey alongside.
        "params": {**params, "_meta": {**_ENVELOPE, **(params.get("_meta") or {})}},
    }

    try:
        async with httpx.AsyncClient(timeout=_timeout(read_seconds)) as client:
            async with client.stream(
                "POST", endpoint, json=body, headers=_headers(token, method, tool_name)
            ) as response:
                if response.status_code >= 500 or response.status_code == 429:
                    await response.aread()
                    raise McpInfrastructureError(
                        f"MCP server returned {response.status_code} — retryable"
                    )
                if response.status_code == 401:
                    await response.aread()
                    raise McpProtocolError(
                        "MCP server rejected our agent token (401) — check MCP_AUTH_TOKEN"
                    )
                if response.status_code >= 400:
                    await response.aread()
                    raise McpProtocolError(
                        f"MCP server returned {response.status_code}: {response.text[:300]}"
                    )

                content_type = response.headers.get("content-type", "")
                if "text/event-stream" in content_type:
                    payload = await _parse_sse(response)
                else:
                    raw = await response.aread()
                    try:
                        payload = json.loads(raw)
                    except json.JSONDecodeError as exc:
                        raise McpProtocolError(f"non-JSON response: {exc}") from exc

    except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
        raise McpInfrastructureError(f"cannot reach MCP server: {exc}") from exc
    except (httpx.ReadTimeout, httpx.ReadError, httpx.RemoteProtocolError) as exc:
        # A severed stream mid-call. Retrying is correct AND safe, because the
        # idempotency key attaches the retry to the original workflow instead of
        # placing a second order.
        raise McpInfrastructureError(f"stream to MCP server failed: {exc}") from exc
    except httpx.HTTPError as exc:
        raise McpInfrastructureError(f"transport failure: {exc}") from exc

    if "error" in payload:
        err = payload["error"] or {}
        raise McpProtocolError(f"JSON-RPC {err.get('code')}: {err.get('message')}")
    if "result" not in payload:
        raise McpProtocolError("response carried neither result nor error")

    return payload["result"]


async def list_tools(endpoint: str, token: str, read_seconds: float = 30.0) -> list[ToolSpec]:
    """Discover what the server exposes.

    Never hardcode the tool list: the names come from Ian's Railway env vars and
    have already changed once (order_coffee -> order_churro).
    """
    result = await _rpc(endpoint, token, "tools/list", {}, read_seconds)

    tools = result.get("tools")
    if not isinstance(tools, list):
        raise McpProtocolError("tools/list did not return a list of tools")

    return [
        ToolSpec(
            name=tool["name"],
            description=tool.get("description", ""),
            input_schema=tool.get("inputSchema") or {"type": "object", "properties": {}},
        )
        for tool in tools
    ]


async def call_tool(
    endpoint: str,
    token: str,
    name: str,
    arguments: dict[str, Any],
    idempotency_key: str,
    read_seconds: float = 960.0,
) -> ToolResult:
    """Call one tool and block until the work is genuinely done.

    May take minutes. That is not a bug — it is the server retrying a broken
    downstream on our behalf, and `attempts` is roughly how many seconds the
    outage lasted.
    """
    result = await _rpc(
        endpoint,
        token,
        "tools/call",
        {
            "name": name,
            "arguments": arguments,
            "_meta": {"idempotencyKey": idempotency_key},
        },
        read_seconds,
        tool_name=name,
    )

    structured = result.get("structuredContent") or {}

    return ToolResult(
        content=_readable(name, structured, result),
        is_error=bool(result.get("isError", False)),
        attempts=int(structured.get("attempts", 0) or 0),
        workflow_id=str(structured.get("workflow_id", "") or ""),
        deduplicated=bool((result.get("_meta") or {}).get("deduplicated", False)),
    )


def _readable(name: str, structured: dict, result: dict) -> str:
    """What the model sees.

    Deliberately hides `attempts` and `workflow_id` — how hard the server had to
    work is the dashboard's business, not the agent's. Telling the model a call
    took 143 attempts invites it to "handle" a problem that is already solved.
    """
    if structured:
        if structured.get("completed"):
            payload = structured.get("response")
            detail = f" {json.dumps(payload)}" if payload else ""
            return f"{name} completed.{detail}"
        return f"{name} did not complete: {json.dumps(structured)[:400]}"

    blocks = result.get("content") or []
    parts: list[str] = []
    for block in blocks:
        if isinstance(block, dict) and block.get("type") == "text":
            parts.append(block.get("text", ""))
        elif isinstance(block, str):
            parts.append(block)
    return "\n".join(p for p in parts if p) or f"{name} returned no content."
