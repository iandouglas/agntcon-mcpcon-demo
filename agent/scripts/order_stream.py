"""Keep a stream of orders flowing, so the room has something to break.

`starter.py` fires one order and exits, which is fine for a smoke test and
wrong for the talk: the scoreboard's "orders completed" tile only means
something if orders keep arriving, and killing the worker is far more
convincing when it takes out several in-flight orders at once and they all
resume.

Deliberately keeps starting orders while the worker is dead. They queue as
RUNNING workflows with nobody to run them, and when a fixer brings the worker
back it chews through the backlog — which is the whole argument, and it is also
what makes Ian's `ordered - fulfilled` gap move.

    uv run python scripts/order_stream.py
    uv run python scripts/order_stream.py --every 4 --max-in-flight 8
    uv run python scripts/order_stream.py --total 20        # then stop

Ctrl-C to stop. Orders already running are left alone — they are durable, and
cancelling them would undo the point.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from richards_agent.shared import TASK_QUEUE, OrderRequest  # noqa: E402
from richards_agent.worker import connect  # noqa: E402
from richards_agent.workflow import TacoOrderAgent  # noqa: E402

DEFAULT_REQUEST = (
    "Two chicken tacos and a churro, please, "
    "and send me a receipt when it is done."
)


async def counts(client, since: str) -> tuple[int, int]:
    """(running, completed) for orders started by this run."""
    base = (
        f'WorkflowType = "TacoOrderAgent" AND TaskQueue = "{TASK_QUEUE}" '
        f'AND StartTime > "{since}"'
    )
    try:
        running = await client.count_workflows(f'{base} AND ExecutionStatus = "Running"')
        done = await client.count_workflows(f'{base} AND ExecutionStatus = "Completed"')
        return running.count, done.count
    except Exception:  # noqa: BLE001 - visibility lags; the cap just waits a beat
        return -1, -1


async def main() -> int:
    parser = argparse.ArgumentParser(description="Stream orders into the agent.")
    parser.add_argument("--every", type=float, default=6.0,
                        help="seconds between orders (default 6)")
    parser.add_argument("--max-in-flight", type=int, default=5,
                        help="pause when this many are already running (default 5)")
    parser.add_argument("--total", type=int, default=0,
                        help="stop after this many; 0 means keep going")
    parser.add_argument("--request", default=DEFAULT_REQUEST)
    args = parser.parse_args()

    from datetime import datetime, timezone
    since = datetime.now(timezone.utc).isoformat()

    client = await connect()
    print(f"streaming orders · one every {args.every}s · cap {args.max_in_flight} in flight")
    print("ctrl-c to stop\n")

    started = 0
    held = False

    try:
        while True:
            running, done = await counts(client, since)

            # The cap matters: during an outage nothing completes, so without it
            # a two-minute break would leave hundreds of orders queued and the
            # recovery would take longer than the talk.
            if running >= args.max_in_flight:
                if not held:
                    held = True
                    print(f"  {running} in flight — holding at the cap "
                          f"(something is probably broken)")
                await asyncio.sleep(1.0)
                continue
            held = False

            started += 1
            order_id = f"{int(time.time() * 1000) % 1_000_000}-{started}"
            await client.start_workflow(
                TacoOrderAgent.run,
                OrderRequest(order_id=order_id, request=args.request),
                id=f"taco-order-{order_id}",
                task_queue=TASK_QUEUE,
            )
            flight = f"{running} in flight" if running >= 0 else "in flight unknown"
            print(f"  #{started:<4} started taco-order-{order_id}  "
                  f"· {flight} · {done} done")

            if args.total and started >= args.total:
                print(f"\nstarted {args.total}, stopping. running orders keep going.")
                return 0

            await asyncio.sleep(args.every)

    except KeyboardInterrupt:
        running, done = await counts(client, since)
        print(f"\nstopped after starting {started}. "
              f"{running} still running, {done} completed — left alone on purpose.")
        return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
