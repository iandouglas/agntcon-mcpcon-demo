"""The agent worker. This is the process the audience gets to kill."""

from __future__ import annotations

import asyncio
import logging
import os

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


async def watchdog(poll_seconds: float = 1.0) -> None:
    """Hard-exit when the room votes this worker down.

    os._exit and not a graceful shutdown, on purpose: a clean drain would
    finish in-flight activities and prove nothing. This is a real crash, and
    the supervisor (docker restart policy / k8s) brings the process back.

    Disabled unless CHAOS_URL is set, so local development is never surprised
    by its own worker vanishing.
    """
    chaos_url = os.getenv("CHAOS_URL")
    if not chaos_url:
        return

    import httpx  # local import: the worker runs fine without this path

    component = os.getenv("CHAOS_COMPONENT", "agent_worker")
    log = logging.getLogger(__name__)

    # A worker that just restarted will still read health 0 for a moment. Exiting
    # on that would crash-loop, so refuse to act until we've seen ourselves
    # healthy once. Self-correcting — no grace period to keep in sync with the
    # supervisor's restart delay.
    seen_healthy = False

    async with httpx.AsyncClient(timeout=3.0) as client:
        while True:
            await asyncio.sleep(poll_seconds)
            try:
                health = (await client.get(chaos_url)).json()
            except Exception:  # noqa: BLE001 - a chaos feed we can't reach is not fatal
                continue

            if health.get(component, 100) > 0:
                if not seen_healthy:
                    log.info("watchdog armed (%s healthy)", component)
                seen_healthy = True
            elif seen_healthy:
                log.error("%s voted down — exiting hard", component)
                os._exit(1)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = await connect()

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[TacoOrderAgent],
        activities=[list_richards_tools, llm_turn, call_richards_tool],
    )

    logging.info("agent worker polling %s", TASK_QUEUE)
    await asyncio.gather(worker.run(), watchdog())


if __name__ == "__main__":
    asyncio.run(main())
