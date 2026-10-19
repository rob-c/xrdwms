"""Load generation for the stress suite: a thread-safe fake grid with latency and faults.

:class:`LoadBackend` is a :class:`~xrdwms.testing.FakeBackend` that can be
shared by managers on different threads and event loops (one lock around its
state), answers after a jittered latency, and injects faults at configurable
rates from a seeded RNG - so a failing run can be replayed with the same seed.

:func:`check` is the invariant every scenario ends with; :func:`concurrently`
runs a scenario's callers side by side.
"""

from __future__ import annotations

import asyncio
import functools
import os
import random
import resource
import threading
import time
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeVar

from xrdcore.fanout import run_all

from xrdwms.journal import Journal
from xrdwms.model import JobHandle, JobInfo, JobState, KeyedSpec, OpResult, Retrieved, SubmitResult
from xrdwms.testing import FakeBackend

SCALE = float(os.environ.get("XRDWMS_STRESS_SCALE", "1"))
SEED = int(os.environ.get("XRDWMS_STRESS_SEED", "1234"))

T = TypeVar("T")


def concurrently(caller: Callable[[int], T], count: int) -> list[T]:
    """``caller(0)`` ... ``caller(count - 1)`` on threads of their own; their results, in order.

    The chain's fan-out (:func:`xrdcore.fanout.run_all`): every caller starts at
    once, and the first to fail is raised here as itself.
    """
    return run_all([functools.partial(caller, index) for index in range(count)], workers=count)


def scaled(n: int) -> int:
    """``n`` times the stress scale (``XRDWMS_STRESS_SCALE``), at least 1."""
    return max(1, int(n * SCALE))


@dataclass
class Faults:
    """Probabilities, per call or per job, of each kind of trouble."""

    submit_error: float = 0.0  # a whole submit request raises
    status_error: float = 0.0  # a whole status request raises
    transient: float = 0.0  # one job's answer is "no news"
    infra_failure: float = 0.0  # a job fails retryably at its end
    lost: float = 0.0  # a job vanishes from the backend


@dataclass
class Latency:
    """Seconds each request takes: ``base`` plus up to ``jitter``, plus ``per_job`` each."""

    base: float = 0.0
    jitter: float = 0.0
    per_job: float = 0.0


@dataclass
class Stats:
    calls: Counter[str] = field(default_factory=Counter)
    jobs_in_calls: Counter[str] = field(default_factory=Counter)
    concurrent: int = 0
    max_concurrent: int = 0
    loops: set[int] = field(default_factory=set)


class LoadBackend(FakeBackend):
    """A fake grid for load: shared across threads, slow on purpose, unreliable on purpose."""

    def __init__(
        self,
        name: str = "load",
        *,
        steps: Sequence[str] = ("queued", "running", "done"),
        latency: Latency | None = None,
        faults: Faults | None = None,
        seed: int = SEED,
        **kwargs: Any,
    ) -> None:
        super().__init__(name, steps=steps, **kwargs)
        self.latency = latency or Latency()
        self.faults = faults or Faults()
        self.rng = random.Random(seed)
        self.lock = threading.RLock()
        self.stats = Stats()
        #: Every attempt key ever submitted, with how many times: >1 is a duplicate.
        self.submitted: Counter[str] = Counter()

    async def _enter(self, method: str, count: int) -> None:
        with self.lock:
            self.stats.calls[method] += 1
            self.stats.jobs_in_calls[method] += count
            self.stats.concurrent += 1
            self.stats.max_concurrent = max(self.stats.max_concurrent, self.stats.concurrent)
            self.stats.loops.add(id(asyncio.get_running_loop()))
            pause = self.latency.base + self.rng.uniform(0, self.latency.jitter)
        await asyncio.sleep(pause + self.latency.per_job * count)

    def _leave(self) -> None:
        with self.lock:
            self.stats.concurrent -= 1

    def _chance(self, p: float) -> bool:
        with self.lock:
            return p > 0 and self.rng.random() < p

    async def submit(self, specs: Sequence[KeyedSpec]) -> list[SubmitResult]:
        await self._enter("submit", len(specs))
        try:
            if self._chance(self.faults.submit_error):
                raise OSError("injected: CE unreachable")
            with self.lock:
                results = await FakeBackend.submit(self, specs)
                for keyed, result in zip(specs, results, strict=True):
                    if result.handle is not None:
                        self.submitted[keyed.key] += 1
                        self._doom(result.handle)
                return results
        finally:
            self._leave()

    def _doom(self, handle: JobHandle) -> None:
        """Decide now whether this job will fail retryably or get lost (seeded, so replayable)."""
        job = self.jobs[handle.id]
        if self._chance(self.faults.infra_failure):
            job.steps = (*job.steps[:-1], "failed:infra")
        elif self._chance(self.faults.lost):
            job.steps = (*job.steps[:-1], "gone")

    async def status(self, handles: Sequence[JobHandle]) -> list[JobInfo]:
        await self._enter("status", len(handles))
        try:
            if self._chance(self.faults.status_error):
                raise OSError("injected: status timed out")
            with self.lock:
                infos = [self._advance(h) for h in handles]
            return [
                JobInfo(i.handle, JobState.UNKNOWN, "transient", transient=True)
                if not i.state.terminal and self._chance(self.faults.transient)
                else i
                for i in infos
            ]
        finally:
            self._leave()

    async def cancel(self, handles: Sequence[JobHandle]) -> list[OpResult]:
        await self._enter("cancel", len(handles))
        try:
            with self.lock:
                return await FakeBackend.cancel(self, handles)
        finally:
            self._leave()

    async def find(self, keys: Sequence[str]) -> dict[str, JobHandle]:
        await self._enter("find", len(keys))
        try:
            with self.lock:
                return await FakeBackend.find(self, keys)
        finally:
            self._leave()

    async def retrieve(self, handle: JobHandle, directory: str) -> Retrieved:
        await self._enter("retrieve", 1)
        try:
            return await FakeBackend.retrieve(self, handle, directory)
        finally:
            self._leave()


# -- invariants --------------------------------------------------------------------------------


@dataclass
class Report:
    """What one scenario measured; printed by the suite and collected into the report file."""

    name: str
    jobs: int
    seconds: float
    states: dict[str, int]
    duplicates: int
    calls: dict[str, int]
    max_rss_mb: float
    threads_before: int
    threads_after: int
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def rate(self) -> float:
        return self.jobs / self.seconds if self.seconds else 0.0

    def line(self) -> str:
        states = " ".join(f"{k}={v}" for k, v in sorted(self.states.items()))
        calls = " ".join(f"{k}={v}" for k, v in sorted(self.calls.items()))
        extra = " ".join(f"{k}={v}" for k, v in self.extra.items())
        return (
            f"[{self.name}] {self.jobs} jobs in {self.seconds:.1f}s ({self.rate:.0f}/s) | "
            f"{states} | dup={self.duplicates} | calls: {calls} | "
            f"rss={self.max_rss_mb:.0f}MB threads {self.threads_before}->{self.threads_after}"
            + (f" | {extra}" if extra else "")
        )


def max_rss_mb() -> float:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss / 1e6 if os.uname().sysname == "Darwin" else rss / 1e3


def check(
    name: str,
    journal: Journal,
    backends: Sequence[LoadBackend],
    *,
    started: float,
    threads_before: int,
    expected: int,
    allowed: set[JobState] | None = None,
    **extra: Any,
) -> Report:
    """Every job ended, none was submitted twice, nothing was left behind; the measurements."""
    records = journal.records()
    assert len(records) == expected, f"{len(records)} jobs in the journal, expected {expected}"
    stuck = [r for r in records if not r.terminal]
    assert not stuck, f"{len(stuck)} jobs never ended, e.g. {stuck[0]}"
    allowed = allowed or {JobState.DONE}
    wrong = [r for r in records if r.state not in allowed]
    assert not wrong, f"{len(wrong)} jobs ended {wrong[0].state}: {wrong[0].info}"
    submitted: Counter[str] = Counter()
    calls: Counter[str] = Counter()
    for backend in backends:
        submitted.update(backend.submitted)
        calls.update(backend.stats.calls)
    duplicates = sum(n - 1 for n in submitted.values() if n > 1)
    assert duplicates == 0, f"{duplicates} duplicate submissions: {submitted.most_common(3)}"
    # Every record that holds a handle points at a job the backend really has.
    known = {job_id for b in backends for job_id in b.jobs}
    dangling = [r for r in records if r.handle and r.handle.id not in known]
    assert not dangling, f"{len(dangling)} records point at unknown jobs"
    time.sleep(0.2)  # let executor threads wind down
    threads_after = threading.active_count()
    report = Report(
        name,
        expected,
        time.monotonic() - started,
        dict(Counter(r.state.value for r in records)),
        duplicates,
        dict(calls),
        max_rss_mb(),
        threads_before,
        threads_after,
        extra,
    )
    print("\n" + report.line())
    return report
