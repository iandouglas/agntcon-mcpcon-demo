"""Tests for the stage screen's backend.

The board payload below is the real response from the deployed board on
2026-09-11, trimmed. When Ian's threshold change lands, add a second fixture
with numeric `state` values rather than editing this one — keeping both is how
we know the adapter still handles a rollback.
"""

from __future__ import annotations

from stage import board

LIVE_BOOLEAN_PAYLOAD = {
    "ok": True,
    "replica": "21407910",
    "conns": 5,
    "armed": False,
    "names": {
        "mcp": "AGENT ↔ MCP SERVER",
        "service_1": "Order Taco",
        "service_2": "Order Churro",
        "service_3": "Restock Inventory",
        "service_4": "Send Receipt",
    },
    "bus": {"kind": "redis", "healthy": True, "failures": 0, "lastError": None},
    "state": {"mcp": False, "service_1": True, "service_2": True,
              "service_3": True, "service_4": True},
    "pool": {"inFlight": 0, "queued": 0},
    "services": [
        {"id": "mcp", "name": "AGENT ↔ MCP SERVER", "kind": "mcp", "configured": True},
        {"id": "service_1", "name": "Order Taco", "kind": "api", "configured": True},
        {"id": "service_2", "name": "Order Churro", "kind": "api", "configured": True},
        {"id": "service_3", "name": "Restock Inventory", "kind": "api", "configured": True},
        {"id": "service_4", "name": "Send Receipt", "kind": "api", "configured": True},
    ],
}

# What we expect once the tug-of-war ships: percentages instead of booleans.
THRESHOLD_PAYLOAD = {
    **LIVE_BOOLEAN_PAYLOAD,
    "armed": True,
    "state": {"mcp": 44, "service_1": 50, "service_2": 72, "service_3": 12, "service_4": 100},
    "taps": {"mcp": 36, "service_1": 0, "service_2": 4, "service_3": 19, "service_4": 0},
}


def test_parses_the_live_boolean_shape():
    state = board.parse(LIVE_BOOLEAN_PAYLOAD)

    assert state.reachable is True
    assert state.armed is False
    assert state.conns == 5
    assert state.bus_healthy is True
    assert len(state.components) == 5

    # mcp sorts first — the order the call path actually runs in.
    assert state.components[0].id == "mcp"
    assert state.components[0].name == "AGENT ↔ MCP SERVER"
    assert state.components[0].up is False
    # Booleans carry no percentage, so the screen draws a plain state pill.
    assert state.components[0].percent is None
    assert state.components[1].id == "service_1"
    assert state.components[1].up is True


def test_parses_the_threshold_shape_ians_change_is_a_non_event():
    state = board.parse(THRESHOLD_PAYLOAD)
    by_id = {c.id: c for c in state.components}

    assert state.armed is True
    assert by_id["mcp"].percent == 44 and by_id["mcp"].up is False
    assert by_id["service_2"].percent == 72 and by_id["service_2"].up is True
    assert by_id["service_3"].percent == 12 and by_id["service_3"].up is False
    assert by_id["mcp"].taps == 36


def test_fifty_reads_as_up():
    """50/50 is the resting state and the board shows UP, so the boundary is
    inclusive. Getting this backwards would show every service down at rest."""
    state = board.parse({**LIVE_BOOLEAN_PAYLOAD, "state": {"service_1": 50}})
    assert state.components[0].up is True
    assert state.components[0].percent == 50


def test_out_of_range_percentages_are_clamped():
    state = board.parse({**LIVE_BOOLEAN_PAYLOAD, "state": {"a": 140, "b": -20}})
    by_id = {c.id: c for c in state.components}
    assert by_id["a"].percent == 100
    assert by_id["b"].percent == 0


def test_unknown_shape_fails_open_to_healthy():
    """A parsing bug must not paint the screen red.

    A false 'everything is broken' mid-talk is worse than missing a real
    outage, which the timeline and the order itself will show anyway.
    """
    state = board.parse({**LIVE_BOOLEAN_PAYLOAD, "state": {"weird": {"nope": 1}}})
    assert state.components[0].up is True


def test_missing_names_falls_back_to_the_id():
    state = board.parse({"state": {"service_9": True}})
    assert state.components[0].name == "service_9"


def test_unreachable_board_is_not_fatal():
    """The screen degrades to a greyed panel; it never dies."""
    import asyncio

    import httpx

    async def go():
        async with httpx.AsyncClient() as client:
            return await board.fetch(client, "http://127.0.0.1:1", "token")

    state = asyncio.run(go())
    assert state.reachable is False
    assert "unreachable" in state.error
    assert state.components == []


def test_unconfigured_board_says_so_rather_than_pretending():
    import asyncio

    import httpx

    async def go():
        async with httpx.AsyncClient() as client:
            return await board.fetch(client, "", "")

    state = asyncio.run(go())
    assert state.reachable is False
    assert "not configured" in state.error
