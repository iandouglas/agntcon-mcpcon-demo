"""Start an order and stream its progress. Stands in for the stage screen."""

from __future__ import annotations

import asyncio
import sys

from .shared import TASK_QUEUE, OrderRequest
from .worker import connect
from .workflow import TacoOrderAgent

DEFAULT_ORDER = "Two chicken tacos and whatever today's special is, please."


async def main() -> None:
    request = " ".join(sys.argv[1:]) or DEFAULT_ORDER
    client = await connect()

    order_id = f"order-{int(asyncio.get_event_loop().time() * 1000) % 100000}"
    handle = await client.start_workflow(
        TacoOrderAgent.run,
        OrderRequest(order_id=order_id, request=request),
        id=f"taco-{order_id}",
        task_queue=TASK_QUEUE,
    )
    print(f"started {handle.id}\n")

    async def poll() -> None:
        last = ""
        while True:
            await asyncio.sleep(0.5)
            try:
                state = await handle.query(TacoOrderAgent.status)
            except Exception:  # noqa: BLE001 - worker may be mid-restart; keep polling
                continue
            line = (
                f"[{state.status}] {state.step} · turns={state.turns} "
                f"tools={state.tool_calls} tokens={state.tokens_spent} "
                f"cached={state.cache_read_tokens} dedup={state.dedup_hits}"
            )
            if line != last:
                print(line)
                last = line
            if state.finished:
                return

    poller = asyncio.create_task(poll())
    result = await handle.result()
    poller.cancel()
    print(f"\n{result}")


if __name__ == "__main__":
    asyncio.run(main())
