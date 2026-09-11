"""The agent worker. This is the process the audience gets to kill."""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import timedelta

from temporalio.client import Client
from temporalio.worker import Worker

from . import config
from .activities import call_richards_tool, list_richards_tools, llm_turn
from .shared import TASK_QUEUE
from .workflow import TacoOrderAgent


async def connect() -> Client:
    kwargs: dict = {"namespace": config.TEMPORAL_NAMESPACE}
    if config.TEMPORAL_API_KEY:
        kwargs["api_key"] = config.TEMPORAL_API_KEY
        kwargs["tls"] = True
    return await Client.connect(config.TEMPORAL_ADDRESS, **kwargs)


# Matches the audience board: >= 50 is up, below it the breakers have won.
# Keep in sync with stage/board.py::THRESHOLD.
HEALTH_THRESHOLD = int(os.getenv("CHAOS_THRESHOLD", "50"))


def read_health(payload: object, component: str) -> int | None:
    """Pull one component's health out of a chaos feed.

    Handles the local fake's flat shape, {"agent_worker": 40}, and Ian's board,
    {"state": {"agent_worker": 40}, ...} — whose values are booleans today and
    percentages once the tug-of-war ships.

    Returns None when the component is absent, which means "not a switch on
    this feed", not "down".
    """
    if not isinstance(payload, dict):
        return None
    source = payload.get("state") if isinstance(payload.get("state"), dict) else payload
    if component not in source:
        return None

    value = source[component]
    if isinstance(value, bool):
        return 100 if value else 0
    if isinstance(value, (int, float)):
        return max(0, min(100, int(round(value))))
    if isinstance(value, dict):
        for key in ("percent", "pct", "value", "v"):
            inner = value.get(key)
            if isinstance(inner, (int, float)) and not isinstance(inner, bool):
                return max(0, min(100, int(round(inner))))
        if isinstance(value.get("up"), bool):
            return 100 if value["up"] else 0
    return None


async def watchdog(poll_seconds: float = 1.0) -> None:
    """Hard-exit when the room drags this worker below the threshold.

    os._exit and not a graceful shutdown, on purpose: a clean drain would
    finish in-flight activities and prove nothing.

    It deliberately does NOT protect itself against starting up while still
    voted down — that is the supervisor's job (scripts/run_worker.sh), which
    refuses to launch a worker until the room has voted it back up. An earlier
    version guarded here instead, which made the worker immune after its first
    death and made fixers decorative.

    Disabled unless CHAOS_URL is set, so local development is never surprised
    by its own worker vanishing.
    """
    chaos_url = os.getenv("CHAOS_URL")
    log = logging.getLogger(__name__)
    if not chaos_url:
        log.warning("CHAOS_URL unset — watchdog DISABLED; the audience cannot kill this worker")
        return

    import httpx  # local import: the worker runs fine without this path

    component = os.getenv("CHAOS_COMPONENT", "agent_worker")
    log.info(
        "watchdog armed: exits when %s drops below %s%%, reading %s",
        component, HEALTH_THRESHOLD, chaos_url,
    )
    missing_warned = False

    async with httpx.AsyncClient(timeout=3.0) as client:
        while True:
            await asyncio.sleep(poll_seconds)
            try:
                payload = (await client.get(chaos_url)).json()
            except Exception:  # noqa: BLE001 - a chaos feed we can't reach is not fatal
                continue

            health = read_health(payload, component)
            if health is None:
                if not missing_warned:
                    missing_warned = True
                    log.warning(
                        "%s is not a switch on this chaos feed — the audience has no way "
                        "to kill this worker", component,
                    )
                continue

            if health < HEALTH_THRESHOLD:
                log.error(
                    "%s at %s%% — below %s%%, the breakers won. exiting hard",
                    component, health, HEALTH_THRESHOLD,
                )
                os._exit(1)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = await connect()

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[TacoOrderAgent],
        activities=[list_richards_tools, llm_turn, call_richards_tool],
        # Default is 10s. When a worker dies, the in-flight workflow task is
        # still assigned to its sticky queue, and Temporal waits out this
        # timeout before re-dispatching to the general queue. On stage that is
        # ten seconds of nothing happening after the audience lands its kill,
        # which reads as a hang rather than as recovery.
        sticky_queue_schedule_to_start_timeout=timedelta(seconds=2),
    )

    logging.info("agent worker polling %s", TASK_QUEUE)
    await asyncio.gather(worker.run(), watchdog())


if __name__ == "__main__":
    asyncio.run(main())
