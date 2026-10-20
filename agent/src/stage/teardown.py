"""Stopping orders that are already running.

`orders.py` has `stop()`, which cancels submissions that have not happened yet
and deliberately leaves started orders alone: killing durable work to tidy up
would undo the exact claim the demo is making.

This is the other thing. It is the after-the-talk switch that does kill them,
on purpose, once the point has been made and nobody wants a few hundred
workflows still retrying against a taqueria that is no longer listening. It is
destructive and it is meant to be, which is why `server.py` puts it behind a
token and a confirmation rather than next to the order buttons.
"""

from __future__ import annotations

import asyncio
import logging

from temporalio.service import RPCError, RPCStatusCode

from richards_agent.shared import TASK_QUEUE

log = logging.getLogger(__name__)

RUNNING_ORDERS = (
    f'WorkflowType = "TacoOrderAgent" AND TaskQueue = "{TASK_QUEUE}" '
    'AND ExecutionStatus = "Running"'
)

# Bounded for the same reason starts are: a few hundred concurrent gRPC calls
# rate-limit themselves into being slower than doing it in waves.
CONCURRENCY = 25

# Visibility is Postgres-backed and runs a few seconds behind reality, so one
# pass reliably misses anything started just before the button was pressed.
# Sweep again until a pass finds nothing, capped so that a batch which is still
# being submitted cannot hold the request open indefinitely.
MAX_PASSES = 3
PASS_PAUSE_SECONDS = 2.0

DEFAULT_REASON = "stopped from the control panel"


def _already_finished(exc: BaseException) -> bool:
    """True when the workflow closed between the list and the terminate.

    Not a failure. Visibility lag guarantees the list is slightly stale, so on
    a draining queue this is the ordinary case rather than an edge one, and
    counting it as an error would make a clean sweep look broken.
    """
    if not isinstance(exc, RPCError):
        return False
    return exc.status in (RPCStatusCode.NOT_FOUND, RPCStatusCode.FAILED_PRECONDITION)


async def terminate_running_orders(
    client,
    *,
    reason: str = DEFAULT_REASON,
    max_passes: int = MAX_PASSES,
    pause: float = PASS_PAUSE_SECONDS,
) -> dict:
    """Terminate every running order. Returns a summary, never raises.

    Scoped to this demo's own workflow type and task queue on purpose: the MCP
    server runs its own workflows in the same namespace, and they are not ours
    to end.
    """
    terminated = 0
    failed = 0
    last_error = ""
    passes = 0
    gate = asyncio.Semaphore(CONCURRENCY)

    async def kill(workflow_id: str) -> None:
        nonlocal terminated, failed, last_error
        async with gate:
            try:
                await client.get_workflow_handle(workflow_id).terminate(reason)
            except Exception as exc:  # noqa: BLE001 - one stubborn order is not the sweep
                if _already_finished(exc):
                    return
                failed += 1
                last_error = f"{type(exc).__name__}: {exc}"
                log.warning("could not terminate %s: %s", workflow_id, exc)
            else:
                terminated += 1

    while passes < max_passes:
        passes += 1
        try:
            ids = [execution.id async for execution in client.list_workflows(RUNNING_ORDERS)]
        except Exception as exc:  # noqa: BLE001 - visibility may be down
            last_error = f"{type(exc).__name__}: {exc}"
            log.warning("could not list running orders: %s", exc)
            break

        if not ids:
            break

        log.info("terminating %d running order(s), pass %d", len(ids), passes)
        await asyncio.gather(*(kill(wid) for wid in ids))

        if passes < max_passes and pause:
            # Give visibility a moment to catch up before deciding we are done.
            await asyncio.sleep(pause)

    log.info("sweep done: %d terminated, %d failed, %d pass(es)", terminated, failed, passes)
    return {
        "terminated": terminated,
        "failed": failed,
        "passes": passes,
        "error": last_error,
    }


__all__ = ["RUNNING_ORDERS", "DEFAULT_REASON", "terminate_running_orders"]
