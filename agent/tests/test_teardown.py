"""Tests for the after-the-talk stop.

This is the one piece of the demo whose job is to destroy durable work, so
what these pin down is mostly restraint: that it reaches only our own orders,
that a workflow finishing underneath it is not an error, and that it cannot
spin forever on a queue that is still being filled.
"""

from __future__ import annotations

import pytest
from temporalio.service import RPCError, RPCStatusCode

from richards_agent.shared import TASK_QUEUE
from stage.teardown import RUNNING_ORDERS, terminate_running_orders


class _Execution:
    def __init__(self, workflow_id: str) -> None:
        self.id = workflow_id


class _Handle:
    def __init__(self, client, workflow_id: str) -> None:
        self._client = client
        self._id = workflow_id

    async def terminate(self, reason: str | None = None) -> None:
        self._client.terminated.append(self._id)
        self._client.reasons.append(reason)
        error = self._client.raises.get(self._id)
        if error is not None:
            raise error


class _Client:
    """Hands out a fresh page of running ids per pass."""

    def __init__(self, pages: list[list[str]], raises: dict | None = None) -> None:
        self.pages = list(pages)
        self.raises = raises or {}
        self.queries: list[str] = []
        self.terminated: list[str] = []
        self.reasons: list[str | None] = []

    def list_workflows(self, query: str):
        self.queries.append(query)
        page = self.pages.pop(0) if self.pages else []

        async def gen():
            for workflow_id in page:
                yield _Execution(workflow_id)

        return gen()

    def get_workflow_handle(self, workflow_id: str) -> _Handle:
        return _Handle(self, workflow_id)


def _rpc(status: RPCStatusCode) -> RPCError:
    return RPCError("nope", status, b"")


@pytest.mark.asyncio
async def test_every_running_order_is_terminated():
    client = _Client([["a", "b", "c"], []])
    result = await terminate_running_orders(client, pause=0)

    assert sorted(client.terminated) == ["a", "b", "c"]
    assert result["terminated"] == 3
    assert result["failed"] == 0


@pytest.mark.asyncio
async def test_it_only_reaches_our_own_orders():
    """The MCP server runs its own workflows in this namespace. Ending those
    would be reaching into somebody else's service to tidy up after ourselves."""
    client = _Client([[]])
    await terminate_running_orders(client, pause=0)

    query = client.queries[0]
    assert 'WorkflowType = "TacoOrderAgent"' in query
    assert f'TaskQueue = "{TASK_QUEUE}"' in query
    assert 'ExecutionStatus = "Running"' in query
    assert RUNNING_ORDERS == query


@pytest.mark.asyncio
async def test_it_sweeps_again_because_visibility_lags():
    """One pass misses anything started just before the button: the list comes
    from Postgres and runs seconds behind reality."""
    client = _Client([["a"], ["b"], []])
    result = await terminate_running_orders(client, pause=0)

    assert sorted(client.terminated) == ["a", "b"]
    assert result["passes"] == 3


@pytest.mark.asyncio
async def test_it_gives_up_rather_than_chasing_a_queue_being_refilled():
    """If orders keep arriving, stop after the cap. Holding the request open
    until the queue runs dry could mean never."""
    client = _Client([["a"], ["b"], ["c"], ["d"], ["e"]])
    result = await terminate_running_orders(client, max_passes=3, pause=0)

    assert result["passes"] == 3
    assert len(client.terminated) == 3


@pytest.mark.asyncio
async def test_an_order_that_finishes_underneath_us_is_not_a_failure():
    """Visibility lag means the list is always slightly stale, so this is the
    ordinary case on a draining queue. Counting it as an error would make a
    clean sweep look broken."""
    client = _Client(
        [["a", "b"], []],
        raises={"a": _rpc(RPCStatusCode.NOT_FOUND), "b": _rpc(RPCStatusCode.FAILED_PRECONDITION)},
    )
    result = await terminate_running_orders(client, pause=0)

    assert result["failed"] == 0
    assert result["terminated"] == 0
    assert result["error"] == ""


@pytest.mark.asyncio
async def test_a_real_error_is_counted_and_reported():
    client = _Client([["a", "b"], []], raises={"a": _rpc(RPCStatusCode.PERMISSION_DENIED)})
    result = await terminate_running_orders(client, pause=0)

    assert result["failed"] == 1
    assert result["terminated"] == 1
    assert "PERMISSION_DENIED" in result["error"] or "RPCError" in result["error"]


@pytest.mark.asyncio
async def test_one_stubborn_order_does_not_abort_the_sweep():
    client = _Client([["a", "b", "c"], []], raises={"b": _rpc(RPCStatusCode.INTERNAL)})
    result = await terminate_running_orders(client, pause=0)

    assert sorted(client.terminated) == ["a", "b", "c"], "sweep stopped at the bad one"
    assert result["terminated"] == 2
    assert result["failed"] == 1


@pytest.mark.asyncio
async def test_visibility_being_down_does_not_raise():
    """The teardown is pressed when something has already gone wrong often
    enough that it must not add an exception of its own."""

    class _Broken(_Client):
        def list_workflows(self, query: str):
            raise RuntimeError("visibility is down")

    result = await terminate_running_orders(_Broken([]), pause=0)

    assert result["terminated"] == 0
    assert "visibility is down" in result["error"]


@pytest.mark.asyncio
async def test_nothing_running_is_a_clean_no_op():
    client = _Client([[]])
    result = await terminate_running_orders(client, pause=0)

    assert result == {"terminated": 0, "failed": 0, "passes": 1, "error": ""}
    assert client.terminated == []


@pytest.mark.asyncio
async def test_the_reason_is_recorded_on_the_workflow():
    """Whoever opens this in the Temporal UI next week should find out why it
    stopped without having to ask anyone."""
    client = _Client([["a"], []])
    await terminate_running_orders(client, reason="after the talk", pause=0)

    assert client.reasons == ["after the talk"]


# ------------------------------------------------------------------ the gate
#
# The rest of the stage API is unauthenticated, which is fine for a screen you
# point at a projector. This endpoint ends running work and the service answers
# on a public hostname, so these cover the gate rather than the sweep.

import json as _json  # noqa: E402

from stage import server as srv  # noqa: E402


class _Request:
    def __init__(self, headers=None, params=None) -> None:
        self.headers = headers or {}
        self.query_params = params or {}


def _body(response) -> dict:
    return _json.loads(response.body)


@pytest.fixture
def armed(monkeypatch):
    monkeypatch.setattr(srv, "ADMIN_TOKEN", "s3cret")
    monkeypatch.setattr(srv, "_client", _Client([[]]))
    monkeypatch.setattr(srv, "_launcher", None)


@pytest.mark.asyncio
async def test_without_a_token_configured_the_endpoint_is_off_not_open(monkeypatch):
    """Fail closed. A teardown switch that defaults to reachable by anyone who
    finds the URL is worse than one that is simply disabled."""
    monkeypatch.setattr(srv, "ADMIN_TOKEN", "")
    monkeypatch.setattr(srv, "_client", _Client([["a"], []]))

    response = await srv.terminate_orders(_Request())

    assert response.status_code == 503
    assert "STAGE_ADMIN_TOKEN" in _body(response)["error"]


@pytest.mark.asyncio
async def test_no_token_is_rejected(armed):
    response = await srv.terminate_orders(_Request())
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_a_wrong_token_is_rejected(armed):
    response = await srv.terminate_orders(
        _Request(headers={"authorization": "Bearer wrong"})
    )
    assert response.status_code == 401


def test_an_empty_token_never_authorises(monkeypatch):
    """`hmac.compare_digest("", "")` is True.

    So with ADMIN_TOKEN unset, a caller sending nothing at all would compare
    equal and be let through. The route refuses before it gets here, but this
    is the check that makes `_authorised` safe on its own rather than only in
    the one place it currently has a second guard in front of it.

    Asserted against `_authorised` directly and not through the route, because
    through the route the 503 fires first and the assertion passes whether or
    not the guard exists at all.
    """
    monkeypatch.setattr(srv, "ADMIN_TOKEN", "")

    assert srv._authorised(_Request()) is False
    assert srv._authorised(_Request(headers={"authorization": "Bearer "})) is False
    assert srv._authorised(_Request(params={"token": ""})) is False


@pytest.mark.asyncio
async def test_the_right_bearer_runs_the_sweep(armed):
    response = await srv.terminate_orders(
        _Request(headers={"authorization": "Bearer s3cret"})
    )
    assert response.status_code == 200
    assert _body(response)["terminated"] == 0


@pytest.mark.asyncio
async def test_a_query_token_also_works_for_hand_testing(armed):
    response = await srv.terminate_orders(_Request(params={"token": "s3cret"}))
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_it_says_when_a_batch_is_still_being_submitted(monkeypatch):
    """Terminating does not stop submission, so a sweep run mid-batch is about
    to be undone. Report that rather than let the number look final."""

    class _Busy:
        busy = True

    monkeypatch.setattr(srv, "ADMIN_TOKEN", "s3cret")
    monkeypatch.setattr(srv, "_client", _Client([[]]))
    monkeypatch.setattr(srv, "_launcher", _Busy())

    response = await srv.terminate_orders(_Request(params={"token": "s3cret"}))

    assert _body(response)["submitting"] is True


# ------------------------------------------------- placing and stopping orders
# Every order spends model tokens, so once a token is configured these need it
# too. Unset they stay open, which is what local work wants.


@pytest.mark.asyncio
async def test_placing_orders_needs_the_token_once_one_is_set(armed):
    response = await srv.place_orders(_Request())
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_stopping_orders_needs_the_token_once_one_is_set(armed):
    response = await srv.stop_orders(_Request())
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_with_no_token_set_placing_orders_is_not_gated(monkeypatch):
    monkeypatch.setattr(srv, "ADMIN_TOKEN", "")
    monkeypatch.setattr(srv, "_launcher", None)

    response = await srv.place_orders(_Request())

    # Past the auth check and on to "no launcher", rather than a 401.
    assert response.status_code == 503
