"""Probe Ian's live MCP server with the real client. No Temporal, no model.

Use this to re-verify the integration after either side deploys, and right
before the talk. It does a tools/list and one real tool call.

    export MCP_ENDPOINT=https://<your-mcp-server>/mcp
    export MCP_AUTH_TOKEN=...
    uv run python scripts/probe_live.py
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import httpx  # noqa: E402

from richards_agent import config, mcp_client  # noqa: E402


async def main() -> int:
    if not config.MCP_AUTH_TOKEN:
        print("set MCP_AUTH_TOKEN first", file=sys.stderr)
        return 1

    endpoint = config.MCP_ENDPOINT
    origin = endpoint.rsplit("/mcp", 1)[0]
    print(f"endpoint : {endpoint}\n")

    # /healthz is public — the cheapest liveness check, and the documented
    # place to read tool names rather than hardcoding them.
    try:
        health = httpx.get(f"{origin}/healthz", timeout=15).json()
        print(f"healthz  : ok={health.get('ok')} tools={health.get('tools')}")
    except Exception as exc:  # noqa: BLE001
        print(f"healthz  : FAILED {exc}")

    print()
    tools = await mcp_client.list_tools(endpoint, config.MCP_AUTH_TOKEN)
    print(f"tools/list returned {len(tools)}:")
    for t in tools:
        args = "zero-arg" if not t.input_schema.get("properties") else "has args"
        print(f"  {t.name:20} [{args}]  {t.description[:58]}")

    target = tools[0].name
    key = f"probe-{int(time.time())}"
    print(f"\ncalling {target} (idempotency key {key}) ...")

    started = time.monotonic()
    result = await mcp_client.call_tool(
        endpoint, config.MCP_AUTH_TOKEN, target, {}, key, read_seconds=120
    )
    elapsed = time.monotonic() - started

    print(f"  {elapsed:.2f}s · attempts={result.attempts} · dedup={result.deduplicated}")
    print(f"  workflow_id: {result.workflow_id}")
    print(f"  content    : {result.content}")

    # Same key again: must attach to the original, not order a second one.
    print(f"\nre-issuing the SAME key (simulating a severed stream) ...")
    again = await mcp_client.call_tool(
        endpoint, config.MCP_AUTH_TOKEN, target, {}, key, read_seconds=120
    )
    print(f"  attempts={again.attempts} · dedup={again.deduplicated}")
    print(f"  workflow_id: {again.workflow_id}")

    same = again.workflow_id == result.workflow_id
    print(f"\n  {'PASS' if same else 'FAIL'}  same workflow id — retry attached, no second order")
    return 0 if same else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
