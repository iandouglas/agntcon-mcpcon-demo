"""Minimal MCP client over streamable HTTP.

Deliberately stateless: one HTTP round trip per call, no long-lived session.
Temporal activities are independent and retryable, so a session object that
had to survive across attempts would be a liability, not a feature.

THE IMPORTANT PART OF THIS FILE is `_classify`. Everything the demo claims
rests on drawing one line correctly:

    infrastructure failure  -> raise  -> Temporal retries, model never re-asked
    business outcome        -> return -> model sees it and re-plans

Get that backwards and the agent retries "out of stock" forever while the
token counter climbs, which is the exact opposite of the point.
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from .shared import ToolSpec


class McpInfrastructureError(Exception):
    """Something is broken. Retryable — Temporal will try again."""


class McpProtocolError(Exception):
    """The server answered, but nonsensically. Not worth retrying."""


JSONRPC = "2.0"


def _rpc(method: str, params: dict[str, Any] | None = None, req_id: int = 1) -> dict:
    body: dict[str, Any] = {"jsonrpc": JSONRPC, "id": req_id, "method": method}
    if params is not None:
        body["params"] = params
    return body


def _classify(response: httpx.Response) -> dict:
    """Turn an HTTP response into a JSON-RPC result, or raise the right error type."""
    # 5xx and 429 are the classic "try again in a moment" cases.
    if response.status_code >= 500 or response.status_code == 429:
        raise McpInfrastructureError(
            f"MCP server returned {response.status_code} — retryable"
        )
    # 4xx (bad tool name, malformed args) will not fix itself on retry.
    if response.status_code >= 400:
        raise McpProtocolError(
            f"MCP server returned {response.status_code}: {response.text[:200]}"
        )

    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        raise McpProtocolError(f"MCP server returned non-JSON: {exc}") from exc

    # A JSON-RPC *error* object is a protocol-level failure — the request never
    # reached the tool. Distinct from a tool that ran and reported bad news.
    if "error" in payload:
        err = payload["error"] or {}
        raise McpProtocolError(f"JSON-RPC error {err.get('code')}: {err.get('message')}")

    if "result" not in payload:
        raise McpProtocolError("JSON-RPC response had neither result nor error")

    return payload["result"]


async def _post(
    client: httpx.AsyncClient,
    endpoint: str,
    body: dict,
    headers: dict[str, str] | None = None,
) -> dict:
    try:
        response = await client.post(endpoint, json=body, headers=headers or {})
    except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
        # The audience unplugged the cable. Textbook retryable.
        raise McpInfrastructureError(f"cannot reach MCP server: {exc}") from exc
    except httpx.ReadTimeout as exc:
        raise McpInfrastructureError(f"MCP server timed out: {exc}") from exc
    except httpx.HTTPError as exc:
        raise McpInfrastructureError(f"transport failure: {exc}") from exc
    return _classify(response)


async def list_tools(endpoint: str, timeout: float = 20.0) -> list[ToolSpec]:
    """Ask the server what Richard sells today.

    Discovered at runtime on purpose — the tool names live in the MCP server's
    env vars, so hardcoding them here would break the moment they're retuned.
    """
    async with httpx.AsyncClient(timeout=timeout) as client:
        result = await _post(client, endpoint, _rpc("tools/list"))

    tools = result.get("tools")
    if not isinstance(tools, list):
        raise McpProtocolError("tools/list did not return a list of tools")

    specs: list[ToolSpec] = []
    for tool in tools:
        specs.append(
            ToolSpec(
                name=tool["name"],
                description=tool.get("description", ""),
                input_schema=tool.get("inputSchema") or tool.get("input_schema") or {},
            )
        )
    return specs


async def call_tool(
    endpoint: str,
    name: str,
    arguments: dict[str, Any],
    idempotency_key: str,
    timeout: float = 120.0,
) -> tuple[str, bool, bool]:
    """Call one tool. Returns (content, is_error, deduplicated).

    Returning rather than raising on `isError` is the whole contract: an MCP
    tool result that says "out of stock" is a successful activity carrying bad
    news, and the model is the one that should decide what to do about it.
    """
    body = _rpc(
        "tools/call",
        {
            "name": name,
            "arguments": arguments,
            # Idempotency key travels two ways so the server can read it
            # whichever way is convenient — as an MCP _meta field...
            "_meta": {"idempotencyKey": idempotency_key},
        },
    )
    # ...and as a plain header, which is easier to reach in a FastAPI handler.
    headers = {"Idempotency-Key": idempotency_key}

    async with httpx.AsyncClient(timeout=timeout) as client:
        result = await _post(client, endpoint, body, headers)

    content = _flatten_content(result.get("content", []))
    is_error = bool(result.get("isError", False))

    # Non-standard, agreed with the MCP server: tells us a retried call
    # attached to an existing workflow instead of running a second time.
    meta = result.get("_meta") or {}
    deduplicated = bool(meta.get("deduplicated", False))

    return content, is_error, deduplicated


def _flatten_content(blocks: Any) -> str:
    """MCP content is a list of typed blocks; the model wants a string."""
    if isinstance(blocks, str):
        return blocks
    if not isinstance(blocks, list):
        return json.dumps(blocks)

    parts: list[str] = []
    for block in blocks:
        if isinstance(block, str):
            parts.append(block)
        elif isinstance(block, dict):
            if block.get("type") == "text":
                parts.append(block.get("text", ""))
            else:
                parts.append(json.dumps(block))
    return "\n".join(p for p in parts if p) or "(empty result)"
