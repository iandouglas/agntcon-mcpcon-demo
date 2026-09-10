"""The audience, as a command line.

Stands in for the phone app your colleague is building, so the full break/fix
loop can be rehearsed before that site exists. It only ever changes numbers in
the shared chaos state — it never touches a process directly, which is exactly
how the real audience will work.

    uv run python scripts/audience.py watch
    uv run python scripts/audience.py kill agent_worker
    uv run python scripts/audience.py break payment_api
    uv run python scripts/audience.py fix payment_api
    uv run python scripts/audience.py tap payment_api --delta -25
    uv run python scripts/audience.py room --breakers 0.7 --seconds 45
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import sys

import httpx

CHAOS = os.getenv("CHAOS_URL", "http://localhost:8931/chaos")

# Only the workers actually die. Everything else refuses work and gets retried
# — which is why "kill" is the wrong verb for them, on stage and in here.
HARD = {"agent_worker", "mcp_worker"}


def bar(health: int, width: int = 24) -> str:
    filled = round(health / 100 * width)
    return "█" * filled + "·" * (width - filled)


async def get_health(client: httpx.AsyncClient) -> dict[str, int]:
    return (await client.get(CHAOS)).json()


async def cmd_watch(args) -> int:
    """Live view of what the room has done. Ctrl-C to stop."""
    async with httpx.AsyncClient(timeout=5) as client:
        last = ""
        while True:
            health = await get_health(client)
            lines = []
            for name, value in health.items():
                verb = "DEAD" if (value <= 0 and name in HARD) else "DOWN" if value <= 0 else ""
                lines.append(f"  {name:16} {bar(value)} {value:3}  {verb}")
            out = "\n".join(lines)
            if out != last:
                print(f"\n{out}", flush=True)
                last = out
            await asyncio.sleep(0.4)


async def _set(component: str, action: str) -> dict:
    async with httpx.AsyncClient(timeout=5) as client:
        r = await client.post(f"{CHAOS}/{component}/{action}")
        r.raise_for_status()
        return r.json()


async def cmd_kill(args) -> int:
    if args.component not in HARD:
        print(
            f"'{args.component}' does not die — it refuses work and Temporal retries it.\n"
            f"Use `break {args.component}` instead. Only {sorted(HARD)} actually exit.",
            file=sys.stderr,
        )
        return 2
    health = await _set(args.component, "down")
    print(f"{args.component} voted down → health {health[args.component]}")
    print("the worker's watchdog will hard-exit within ~1s; supervisor restarts it in ~5s")
    return 0


async def cmd_break(args) -> int:
    health = await _set(args.component, "down")
    print(f"{args.component} → health {health[args.component]} (will 503; activities retry)")
    return 0


async def cmd_fix(args) -> int:
    health = await _set(args.component, "up")
    print(f"{args.component} → health {health[args.component]}")
    return 0


async def cmd_tap(args) -> int:
    async with httpx.AsyncClient(timeout=5) as client:
        r = await client.post(f"{CHAOS}/{args.component}/tap", params={"delta": args.delta})
        r.raise_for_status()
        health = r.json()
    print(f"{args.component} → health {health[args.component]}")
    return 0


async def cmd_room(args) -> int:
    """A whole room of fixers and breakers, tapping away.

    `--breakers 0.7` means 70% of taps are damage. Crank it to 1.0 and the
    order should still complete — that's the dare.
    """
    per_tick = max(1, round(args.phones / 60))
    damage = max(1, round(100 / max(8, args.phones / 5)))
    print(
        f"{args.phones} phones · {args.breakers:.0%} breakers · "
        f"{damage} health/tap · {per_tick} taps per 400ms · {args.seconds}s\n"
    )

    async with httpx.AsyncClient(timeout=5) as client:
        components = list(await get_health(client))
        ticks = int(args.seconds / 0.4)
        for _ in range(ticks):
            for _ in range(per_tick):
                target = random.choice(components)
                delta = -damage if random.random() < args.breakers else damage
                try:
                    await client.post(f"{CHAOS}/{target}/tap", params={"delta": delta})
                except httpx.HTTPError:
                    pass
            await asyncio.sleep(0.4)

    health = await get_health(httpx.AsyncClient(timeout=5))
    print("room went quiet. final health:")
    for name, value in health.items():
        print(f"  {name:16} {bar(value)} {value:3}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Stand-in for the audience site.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("watch", help="live health view")

    p = sub.add_parser("kill", help="hard-break a worker (it actually exits)")
    p.add_argument("component")

    p = sub.add_parser("break", help="soft-break a service (it starts refusing work)")
    p.add_argument("component")

    p = sub.add_parser("fix", help="restore a component to full health")
    p.add_argument("component")

    p = sub.add_parser("tap", help="one audience tap")
    p.add_argument("component")
    p.add_argument("--delta", type=int, default=-20)

    p = sub.add_parser("room", help="simulate a whole crowd tapping")
    p.add_argument("--phones", type=int, default=200)
    p.add_argument("--breakers", type=float, default=0.7)
    p.add_argument("--seconds", type=float, default=30)

    args = parser.parse_args()
    handler = {
        "watch": cmd_watch, "kill": cmd_kill, "break": cmd_break,
        "fix": cmd_fix, "tap": cmd_tap, "room": cmd_room,
    }[args.cmd]

    try:
        return asyncio.run(handler(args))
    except KeyboardInterrupt:
        return 0
    except httpx.HTTPError as exc:
        print(f"cannot reach chaos state at {CHAOS}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
