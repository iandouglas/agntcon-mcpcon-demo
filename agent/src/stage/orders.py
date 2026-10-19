"""Starting orders from the stage screen.

`scripts/order_stream.py` paces orders and holds at a cap, which is what you
want from a rehearsal you leave running in a terminal. This is the opposite
tool. It dumps a whole batch in as fast as Temporal will accept them, because
on stage the backlog *is* the visual: the gap between ordered and fulfilled is
the argument, and a cap would close it.

Two things make that safe to drive from an HTTP handler:

- Submission runs as a background task. A thousand orders is a thousand RPCs,
  so the request returns the moment the batch is accepted rather than holding
  the connection open while it drains. Progress rides along in the snapshot.
- Starts are bounded by a semaphore. Unbounded `gather` over a thousand
  coroutines will open a thousand concurrent gRPC calls, which mostly
  rate-limits itself into a slower result than doing it in waves.

`stop()` cancels the *remaining* submissions only. Orders already started are
left alone on purpose -- they are durable, and killing them to tidy up would
undo the thing we are claiming.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time

from richards_agent.shared import TASK_QUEUE, OrderRequest
from richards_agent.workflow import TacoOrderAgent

log = logging.getLogger(__name__)

# Quantities are rolled per order rather than fixed.
#
# The MCP tools take no arguments at all: `order_taco` means exactly one taco.
# So N tacos is N separate tool calls, N separate MCP-side workflows and N
# separate downstream requests. A fixed "two tacos and a churro" therefore made
# every single order produce the identical four calls against the identical
# four tiles, which reads as a recording rather than a system.
#
# The text names only tools the server actually has. It used to ask for
# "whatever today's special is", but there is no tool to look a special up --
# the local fake has get_specials and production does not, so the gap only
# showed against the real server. Opus papered over it by guessing the special
# was the churro; Haiku refused to guess, asked the customer, and called
# nothing at all. Six of eight sampled orders made zero tool calls and still
# closed Completed, so the tiles counted orders the MCP server was never told
# about.
MIN_ITEMS = 1
MAX_ITEMS = 5

# Words, not digits: this text is read off the Temporal UI mid-talk, and
# "Three chicken tacos" scans where "3 chicken tacos" stutters. Falls back to
# the numeral if MAX_ITEMS is ever raised past the list.
_COUNT_WORDS = ("zero", "One", "Two", "Three", "Four", "Five")


def _count_word(n: int) -> str:
    return _COUNT_WORDS[n] if 0 <= n < len(_COUNT_WORDS) else str(n)


def compose_request(rng: random.Random) -> str:
    """One order's text, with the quantities rolled fresh."""
    tacos = rng.randint(MIN_ITEMS, MAX_ITEMS)
    churros = rng.randint(MIN_ITEMS, MAX_ITEMS)
    return (
        f"{_count_word(tacos)} chicken taco{'' if tacos == 1 else 's'} "
        f"and {_count_word(churros).lower()} churro{'' if churros == 1 else 's'}, "
        "please, and send me a receipt when it is done."
    )

# The dropdown's top entry, and the route's ceiling.
#
# Lower than it was, because the orders themselves got bigger: up to five tacos
# and five churros is up to eleven tool calls each, run one after another
# inside the turn (workflow.py executes a turn's calls serially). Twenty-five
# of those is already several hundred downstream requests.
MAX_BATCH = 25

# Orders go out in bursts of a random size in this range, one burst at a time.
# The ceiling is the old fixed concurrency and is what keeps a 1000-order batch
# landing inside a sentence without opening a socket per order. The floor being
# 1 is the point: a steady 25 drew a straight line down the "still in the
# queue" tile, which reads as a progress bar. Uneven bursts read as a rush.
MIN_BURST = 1
MAX_BURST = 25

# A short random pause between bursts. Without it the bursts land back to back
# and the unevenness averages out before anyone can see it -- the batch is
# submitted far faster than the workers drain it, so submission is the only
# part of this the room can watch arrive. Kept small: at ~77 bursts for a
# 1000-order batch this adds a handful of seconds, not a wait.
MAX_BURST_PAUSE_SECONDS = 0.2

# If the first handful all fail there is something wrong with Temporal, and
# grinding through the remaining 990 to produce 990 identical errors helps
# nobody. Bail and say so.
EARLY_BAIL_FAILURES = 10


class LauncherBusy(RuntimeError):
    """A batch is still being submitted."""


class OrderLauncher:
    """Starts batches of individual orders. One batch at a time."""

    def __init__(
        self,
        client,
        request: str | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self._client = client
        # None means roll quantities per order. An explicit string is used
        # verbatim for every order instead, which is what STAGE_ORDER_REQUEST
        # is for: pinning the text when you need two runs to be comparable.
        self._request = request
        # Injectable so a test can pin the burst sizes. Nothing here needs to
        # be cryptographically anything; it needs to not look like a metronome.
        self._rng = rng or random.Random()
        self._task: asyncio.Task | None = None
        self._target = 0
        self._started = 0
        self._failed = 0
        self._last_error = ""
        self._stopping = False

    # ------------------------------------------------------------------ api
    @property
    def busy(self) -> bool:
        return self._task is not None and not self._task.done()

    def progress(self) -> dict:
        """Snapshot-shaped. `submitting` is what the UI disables the button on."""
        return {
            "submitting": self.busy,
            "target": self._target,
            "started": self._started,
            "failed": self._failed,
            "stopping": self._stopping,
            "error": self._last_error,
        }

    def submit(self, count: int) -> dict:
        """Accept a batch and return immediately. Raises `LauncherBusy`."""
        if self.busy:
            raise LauncherBusy(
                f"still submitting {self._started}/{self._target}; stop it or wait"
            )

        self._target = count
        self._started = 0
        self._failed = 0
        self._last_error = ""
        self._stopping = False
        self._task = asyncio.create_task(self._run(count))
        return self.progress()

    def stop(self) -> dict:
        """Cancel the remaining submissions. Started orders keep running."""
        if self.busy:
            self._stopping = True
        return self.progress()

    async def aclose(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    # ------------------------------------------------------------ internals
    async def _run(self, count: int) -> None:
        # One stamp for the batch: unique across batches because a new batch
        # cannot begin while this one is in flight, and unique within the batch
        # because the index is appended.
        stamp = int(time.time() * 1000)
        log.info("submitting %d order(s), batch %s", count, stamp)

        async def start_one(index: int) -> None:
            if self._stopping or self._bailing():
                return
            order_id = f"{stamp}-{index}"
            request = self._request or compose_request(self._rng)
            try:
                await self._client.start_workflow(
                    TacoOrderAgent.run,
                    OrderRequest(order_id=order_id, request=request),
                    id=f"taco-order-{order_id}",
                    task_queue=TASK_QUEUE,
                )
            except Exception as exc:  # noqa: BLE001 - one bad start is not the batch
                self._failed += 1
                self._last_error = f"{type(exc).__name__}: {exc}"
                log.warning("order %s failed to start: %s", order_id, exc)
            else:
                self._started += 1

        try:
            # One burst at a time, each a random size. `stop()` and the early
            # bail are checked between bursts as well as inside start_one, so
            # a stop lands within one burst rather than after the whole batch.
            index = 1
            while index <= count:
                if self._stopping or self._bailing():
                    break
                burst = min(self._rng.randint(MIN_BURST, MAX_BURST), count - index + 1)
                await asyncio.gather(
                    *(start_one(i) for i in range(index, index + burst))
                )
                index += burst
                if index <= count and MAX_BURST_PAUSE_SECONDS:
                    await asyncio.sleep(
                        self._rng.uniform(0, MAX_BURST_PAUSE_SECONDS)
                    )
        except asyncio.CancelledError:
            raise
        finally:
            self._stopping = False
            if self._bailing():
                self._last_error = (
                    f"gave up after {self._failed} failed starts -- "
                    f"is Temporal reachable? ({self._last_error})"
                )
            log.info(
                "batch %s done: %d started, %d failed", stamp, self._started, self._failed
            )

    def _bailing(self) -> bool:
        """Nothing has gone right yet and plenty has gone wrong."""
        return self._started == 0 and self._failed >= EARLY_BAIL_FAILURES


__all__ = [
    "MAX_BATCH",
    "MAX_ITEMS",
    "MIN_ITEMS",
    "LauncherBusy",
    "OrderLauncher",
    "compose_request",
]
