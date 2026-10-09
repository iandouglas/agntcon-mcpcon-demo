"""Tests for placing orders from the stage screen.

The launcher is the part worth testing: it is the only thing in the stage
backend that *writes* anything, and it writes a thousand times when someone
picks the top of the dropdown.
"""

from __future__ import annotations

import asyncio
import random

import pytest

from stage.collector import Collector
from stage.orders import (
    EARLY_BAIL_FAILURES,
    MAX_BURST,
    MIN_BURST,
    LauncherBusy,
    OrderLauncher,
)


class FakeClient:
    """Records start_workflow calls. `fail_first` raises for the first N."""

    def __init__(self, fail_first: int = 0, fail_all: bool = False) -> None:
        self.started: list[str] = []
        self.requests: list[str] = []
        self._fail_first = fail_first
        self._fail_all = fail_all

    async def start_workflow(self, run, arg, *, id, task_queue):  # noqa: A002
        if self._fail_all or self._fail_first > 0:
            self._fail_first -= 1
            raise RuntimeError("temporal unreachable")
        self.started.append(id)
        self.requests.append(arg.request)
        return object()


async def drain(launcher: OrderLauncher, timeout: float = 5.0) -> None:
    """Wait for the background batch to finish submitting."""
    deadline = asyncio.get_running_loop().time() + timeout
    while launcher.busy:
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("launcher never finished submitting")
        await asyncio.sleep(0.001)


# ------------------------------------------------------------------ launcher
async def test_a_batch_starts_that_many_separate_orders():
    """Thirty means thirty workflows, not one order for thirty tacos."""
    client = FakeClient()
    launcher = OrderLauncher(client)

    launcher.submit(30)
    await drain(launcher)

    assert len(client.started) == 30
    # Every workflow id is distinct, or Temporal would reject the duplicates
    # and the batch would silently be smaller than the room was told.
    assert len(set(client.started)) == 30
    assert launcher.progress()["started"] == 30
    assert launcher.progress()["failed"] == 0


async def test_submit_returns_before_the_batch_is_done():
    """A thousand starts must not hold the HTTP response open."""
    client = FakeClient()
    launcher = OrderLauncher(client)

    progress = launcher.submit(500)

    assert progress["submitting"] is True
    assert progress["target"] == 500
    assert progress["started"] < 500  # it has barely begun
    await drain(launcher)
    assert len(client.started) == 500


async def test_a_second_batch_is_refused_while_one_is_running():
    client = FakeClient()
    launcher = OrderLauncher(client)
    launcher.submit(200)

    with pytest.raises(LauncherBusy):
        launcher.submit(1)

    await drain(launcher)
    launcher.submit(1)  # fine once the first one is done
    await drain(launcher)
    assert len(client.started) == 201


async def test_stop_halts_the_rest_but_keeps_what_started():
    client = FakeClient()
    launcher = OrderLauncher(client)

    launcher.submit(1000)
    launcher.stop()
    await drain(launcher)

    # The exact number depends on how far the first wave got; the point is that
    # it stopped early and kept what it had rather than unwinding.
    assert len(client.started) < 1000
    assert launcher.progress()["started"] == len(client.started)
    assert launcher.progress()["stopping"] is False  # cleared once it lands


async def test_one_bad_start_does_not_sink_the_batch():
    client = FakeClient(fail_first=3)
    launcher = OrderLauncher(client)

    launcher.submit(20)
    await drain(launcher)

    assert launcher.progress()["failed"] == 3
    assert launcher.progress()["started"] == 17
    assert "temporal unreachable" in launcher.progress()["error"]


async def test_it_gives_up_when_nothing_is_working():
    """A thousand identical errors helps nobody, so bail and say why."""
    client = FakeClient(fail_all=True)
    launcher = OrderLauncher(client)

    launcher.submit(1000)
    await drain(launcher)

    progress = launcher.progress()
    assert progress["started"] == 0
    # Bounded by the bail threshold plus whatever the first wave had already
    # dispatched concurrently -- not anywhere near 1000.
    assert progress["failed"] < 100
    assert progress["failed"] >= EARLY_BAIL_FAILURES
    assert "gave up" in progress["error"]


async def test_the_order_text_is_overridable():
    client = FakeClient()
    launcher = OrderLauncher(client, request="One churro, extra cinnamon.")

    launcher.submit(2)
    await drain(launcher)

    assert client.requests == ["One churro, extra cinnamon."] * 2


# ----------------------------------------------------------- following_live
# `_auto_follow` adopts a new order only when this is False. With hundreds in
# flight, getting it wrong means the screen re-targets twice a second.
def collector(workflow_id="taco-order-1"):
    return Collector(None, workflow_id, "", "")


def test_nothing_to_follow_yet():
    assert collector(workflow_id=None).following_live is False


def test_an_open_order_is_followed():
    c = collector()
    c._order = {"running": True, "finished": False}
    assert c.following_live is True


def test_a_finished_order_releases_the_screen():
    c = collector()
    c._order = {"running": False, "finished": True}
    assert c.following_live is False


def test_a_dead_worker_does_not_release_the_screen():
    """The kill is the moment worth watching; do not cut away from it."""
    c = collector()
    c._order = {"running": False, "status": "worker down"}
    assert c.following_live is True


def test_a_closed_but_unfinished_order_releases_the_screen():
    """Failed or terminated: the query never said `finished`, but it is over."""
    c = collector()
    c._order = {"running": False, "status": "worker down"}
    c._following_closed = True
    assert c.following_live is False


def test_following_a_new_order_clears_the_closed_flag():
    c = collector()
    c._following_closed = True
    c.set_workflow("taco-order-2")
    assert c.following_live is True


# ------------------------------------------------------------------- bursts
#
# Orders go out in bursts of a random size rather than at a steady 25. The
# steady rate drew a straight line down the "still in the queue" tile, which
# reads as a progress bar rather than a rush. What these pin is that making it
# uneven did not make it lossy.


class BurstRecordingClient(FakeClient):
    """Records how many starts were in flight at once, per burst."""

    def __init__(self) -> None:
        super().__init__()
        self.in_flight = 0
        self.peak = 0
        self.bursts: list[int] = []

    async def start_workflow(self, run, arg, *, id, task_queue):  # noqa: A002
        if self.in_flight == 0:
            self.bursts.append(0)
        self.in_flight += 1
        self.bursts[-1] += 1
        self.peak = max(self.peak, self.in_flight)
        try:
            await asyncio.sleep(0)  # let the rest of the burst start too
            return await super().start_workflow(run, arg, id=id, task_queue=task_queue)
        finally:
            self.in_flight -= 1


async def test_every_order_is_still_submitted_exactly_once():
    """The burst arithmetic is off-by-one bait: 1000 means 1000, no gaps."""
    client = FakeClient()
    launcher = OrderLauncher(client, rng=random.Random(7))

    launcher.submit(1000)
    await drain(launcher, timeout=30.0)

    assert len(client.started) == 1000
    assert len(set(client.started)) == 1000
    # Contiguous 1..1000, so no burst was skipped and none overlapped.
    suffixes = sorted(int(i.rsplit("-", 1)[1]) for i in client.started)
    assert suffixes == list(range(1, 1001))


async def test_a_burst_never_exceeds_the_ceiling():
    client = BurstRecordingClient()
    launcher = OrderLauncher(client, rng=random.Random(7))

    launcher.submit(300)
    await drain(launcher, timeout=30.0)

    assert client.peak <= MAX_BURST, f"burst of {client.peak} exceeds {MAX_BURST}"


async def test_burst_sizes_actually_vary():
    """The launcher must not come out as a metronome.

    Reads the bursts the launcher really produced, not numbers the test rolled
    for itself -- an earlier version of this did the latter and passed happily
    against a hardcoded burst of 25, which is the exact thing it exists to
    catch. Distinct sizes rather than a pinned sequence, so reorganising the
    arithmetic does not break it.
    """
    client = BurstRecordingClient()
    launcher = OrderLauncher(client, rng=random.Random(11))

    launcher.submit(400)
    await drain(launcher, timeout=30.0)

    assert sum(client.bursts) == 400
    assert all(MIN_BURST <= b <= MAX_BURST for b in client.bursts), client.bursts
    assert len(set(client.bursts)) > 5, f"only these burst sizes: {set(client.bursts)}"


async def test_the_final_burst_does_not_overshoot():
    """A batch smaller than one burst submits exactly that many."""
    client = FakeClient()
    launcher = OrderLauncher(client, rng=random.Random(3))

    launcher.submit(2)
    await drain(launcher)

    assert len(client.started) == 2


async def test_stop_lands_inside_the_batch_not_after_it():
    """Stopping must not have to wait out all 1000 submissions."""
    client = FakeClient()
    launcher = OrderLauncher(client, rng=random.Random(5))

    launcher.submit(1000)
    await asyncio.sleep(0.05)
    launcher.stop()
    await drain(launcher, timeout=30.0)

    assert len(client.started) < 1000, "stop did not cut the batch short"
    assert launcher.progress()["submitting"] is False
