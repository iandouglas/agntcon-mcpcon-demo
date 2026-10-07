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
import time

from richards_agent.shared import TASK_QUEUE, OrderRequest
from richards_agent.workflow import TacoOrderAgent

log = logging.getLogger(__name__)

DEFAULT_REQUEST = "Two chicken tacos and whatever today's special is, please."

# The dropdown's top entry. Past this the submission takes long enough that it
# stops reading as a button press and starts reading as a hang.
MAX_BATCH = 1000

# Concurrent start_workflow calls. Tuned for "fast enough that a 1000-order
# batch lands inside a sentence" without opening a socket per order.
CONCURRENCY = 25

# If the first handful all fail there is something wrong with Temporal, and
# grinding through the remaining 990 to produce 990 identical errors helps
# nobody. Bail and say so.
EARLY_BAIL_FAILURES = 10


class LauncherBusy(RuntimeError):
    """A batch is still being submitted."""


class OrderLauncher:
    """Starts batches of individual orders. One batch at a time."""

    def __init__(self, client, request: str = DEFAULT_REQUEST) -> None:
        self._client = client
        self._request = request
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
        semaphore = asyncio.Semaphore(CONCURRENCY)
        log.info("submitting %d order(s), batch %s", count, stamp)

        async def start_one(index: int) -> None:
            async with semaphore:
                if self._stopping or self._bailing():
                    return
                order_id = f"{stamp}-{index}"
                try:
                    await self._client.start_workflow(
                        TacoOrderAgent.run,
                        OrderRequest(order_id=order_id, request=self._request),
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
            await asyncio.gather(*(start_one(i + 1) for i in range(count)))
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


__all__ = ["DEFAULT_REQUEST", "MAX_BATCH", "LauncherBusy", "OrderLauncher"]
