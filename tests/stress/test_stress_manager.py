"""Stress: the JobManager and journal at scale, under faults, from many threads, and shared."""

from __future__ import annotations

import asyncio
import random
import threading
import time
from collections.abc import Coroutine
from pathlib import Path
from typing import Any, TypeVar

import pytest

from stress._load import SEED, Faults, Latency, LoadBackend, Report, check, concurrently, scaled
from xrdwms import JobManager, JobSpec, Journal, PollPolicy
from xrdwms.model import JobState

T = TypeVar("T")

#: Fast enough that a job's few steps take well under a second each.
POLICY = PollPolicy(
    interval=0.05, max_interval=0.5, batch_delay=0.02, backoff_cap=0.5, lost_after=3, lease=10
)


def specs(n: int, *, retries: int = 0) -> list[JobSpec]:
    return [JobSpec(f"job{i}", "/bin/true", max_retries=retries) for i in range(n)]


async def test_scale_one_manager(tmp_path: Path, record_report: list[Report]) -> None:
    """Tens of thousands of jobs through one manager, on-disk journal, slow-ish backend."""
    n = scaled(20_000)
    backend = LoadBackend(latency=Latency(base=0.005, jitter=0.01, per_job=0.00002))
    threads, started = threading.active_count(), time.monotonic()
    with Journal(tmp_path / "j.sqlite") as journal:
        async with JobManager([backend], journal=journal, policy=POLICY) as jm:
            keys = await jm.submit(specs(n), backend="load")
            await jm.wait_all(keys)
        record_report.append(
            check(
                "scale-one-manager",
                journal,
                [backend],
                started=started,
                threads_before=threads,
                expected=n,
                mean_bulk=backend.stats.jobs_in_calls["status"]
                // max(1, backend.stats.calls["status"]),
            )
        )
    # Bulk, not per job: far fewer status requests than jobs times polls.
    assert backend.stats.calls["status"] < n


async def test_faults_under_load(tmp_path: Path, record_report: list[Report]) -> None:
    """Every kind of fault at once; every job still ends, retried where it should be."""
    n = scaled(5_000)
    faults = Faults(
        submit_error=0.05, status_error=0.05, transient=0.05, infra_failure=0.1, lost=0.02
    )
    backend = LoadBackend(
        steps=("queued", "running", "running", "done"),
        latency=Latency(base=0.002, jitter=0.01),
        faults=faults,
    )
    policy = PollPolicy(
        interval=0.05,
        max_interval=0.3,
        batch_delay=0.02,
        backoff_cap=0.2,
        lost_after=3,
        max_submit_tries=50,
        lease=10,
    )
    threads, started = threading.active_count(), time.monotonic()
    with Journal(tmp_path / "j.sqlite") as journal:
        async with JobManager([backend], journal=journal, policy=policy) as jm:
            keys = await jm.submit(specs(n, retries=5), backend="load")
            records = await jm.wait_all(keys)
        retried = sum(1 for r in records if r.attempt > 0)
        record_report.append(
            check(
                "faults-under-load",
                journal,
                [backend],
                started=started,
                threads_before=threads,
                expected=n,
                allowed={JobState.DONE, JobState.FAILED},
                retried=retried,
            )
        )
    failed = [r for r in records if r.state is JobState.FAILED]
    # With 5 retries and ~12% retryable failure per attempt, essentially nothing is left failed.
    assert len(failed) <= max(2, n // 1000), failed[:3]
    assert retried > 0


async def test_cancel_storm(tmp_path: Path, record_report: list[Report]) -> None:
    """Half the jobs cancelled at random moments while the rest run on."""
    n = scaled(5_000)
    backend = LoadBackend(
        steps=("queued",) * 5 + ("running",) * 5 + ("done",),
        latency=Latency(base=0.002, jitter=0.005),
    )
    rng = random.Random(SEED)
    threads, started = threading.active_count(), time.monotonic()
    with Journal(tmp_path / "j.sqlite") as journal:
        async with JobManager([backend], journal=journal, policy=POLICY) as jm:
            keys = await jm.submit(specs(n), backend="load")
            victims = rng.sample(keys, n // 2)

            accepted: list[str] = []

            async def cancel_later(batch: list[str]) -> None:
                await asyncio.sleep(rng.uniform(0, 1.0))
                answers = await jm.cancel(batch)
                accepted.extend(k for k, a in zip(batch, answers, strict=True) if a.ok)

            batches = [victims[i : i + 50] for i in range(0, len(victims), 50)]
            await asyncio.gather(*(cancel_later(b) for b in batches))
            records = await jm.wait_all(keys)
        record_report.append(
            check(
                "cancel-storm",
                journal,
                [backend],
                started=started,
                threads_before=threads,
                expected=n,
                allowed={JobState.DONE, JobState.CANCELLED},
            )
        )
    cancelled = {r.key for r in records if r.state is JobState.CANCELLED}
    assert cancelled <= set(victims)
    # A cancel that was accepted sticks: no accepted job may run on to the end.
    survivors = set(accepted) - cancelled
    assert not survivors, f"{len(survivors)} accepted cancellations were lost"
    # Nothing cancelled is still running on the backend.
    for record in records:
        if record.state is JobState.CANCELLED and record.handle:
            assert backend.job(record.handle).cancelled


def test_many_threads_drive_one_manager(tmp_path: Path, record_report: list[Report]) -> None:
    """Dozens of OS threads submit, wait, cancel and read the journal at once."""
    n_threads, per_thread = 32, scaled(300)
    backend = LoadBackend(latency=Latency(base=0.002, jitter=0.01))
    journal = Journal(tmp_path / "j.sqlite")
    loop = asyncio.new_event_loop()
    ready = threading.Event()
    jm = JobManager([backend], journal=journal, policy=POLICY)

    def run_loop() -> None:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(jm.start())
        ready.set()
        loop.run_forever()

    loop_thread = threading.Thread(target=run_loop, daemon=True)
    threads, started = threading.active_count(), time.monotonic()
    loop_thread.start()
    ready.wait()

    def call(coro: Coroutine[Any, Any, T]) -> T:
        return asyncio.run_coroutine_threadsafe(coro, loop).result(timeout=300)

    def worker(index: int) -> tuple[int, int]:
        rng = random.Random(SEED + index)
        mine = call(jm.submit(specs(per_thread), backend="load", group=f"t{index}"))
        assert isinstance(mine, list)
        cancelled = rng.sample(mine, per_thread // 10)
        answers = call(jm.cancel(cancelled))
        assert isinstance(answers, list) and all(a.ok for a in answers)
        # A CLI-like reader on its own connection, hammering the journal meanwhile.
        with Journal(tmp_path / "j.sqlite") as reader:
            for _ in range(20):
                reader.records(group=f"t{index}")
        done = call(jm.wait_all(mine))
        assert isinstance(done, list)
        ended = {r.key: r.state for r in done}
        lost = [k for k in cancelled if ended[k] is not JobState.CANCELLED]
        assert not lost, f"{len(lost)} of {len(cancelled)} cancellations lost: {ended[lost[0]]}"
        return len(done), len(cancelled)

    results = concurrently(worker, n_threads)
    call(jm.close())
    loop.call_soon_threadsafe(loop.stop)
    loop_thread.join(10)
    total = sum(r[0] for r in results)
    record_report.append(
        check(
            "many-threads",
            journal,
            [backend],
            started=started,
            threads_before=threads,
            expected=total,
            allowed={JobState.DONE, JobState.CANCELLED},
            threads=n_threads,
        )
    )
    journal.close()
    loop.close()


def test_managers_sharing_a_journal(tmp_path: Path, record_report: list[Report]) -> None:
    """Several managers (several dask workers, or a CLI beside a dask session) on one journal.

    Each submits its own jobs; none may submit, poll to death, or settle another's.
    """
    n_managers, per_manager = 4, scaled(1_000)
    backend = LoadBackend(latency=Latency(base=0.002, jitter=0.01))  # one CE, shared
    path = tmp_path / "shared.sqlite"
    threads, started = threading.active_count(), time.monotonic()

    def one(index: int) -> int:
        async def go() -> int:
            with Journal(path) as journal:
                async with JobManager({"load": backend}, journal=journal, policy=POLICY) as jm:
                    keys = await jm.submit(specs(per_manager), backend="load", group=f"m{index}")
                    records = await jm.wait_all(keys)
                    return sum(1 for r in records if r.state is JobState.DONE)

        return asyncio.run(go())

    done = concurrently(one, n_managers)
    with Journal(path) as journal:
        record_report.append(
            check(
                "shared-journal",
                journal,
                [backend],
                started=started,
                threads_before=threads,
                expected=n_managers * per_manager,
                managers=n_managers,
                loops=len(backend.stats.loops),
            )
        )
    assert done == [per_manager] * n_managers


@pytest.mark.parametrize("readers", [8])
def test_journal_hammer(tmp_path: Path, readers: int, record_report: list[Report]) -> None:
    """Raw journal contention: writers updating while readers list, all on separate threads."""
    n = scaled(5_000)
    path = tmp_path / "j.sqlite"
    spec = JobSpec("j", "/bin/true")
    with Journal(path) as journal:
        for i in range(n):
            journal.add(f"k{i}", "load", spec)
    stop = threading.Event()
    errors: list[BaseException] = []

    def write(offset: int) -> None:
        try:
            with Journal(path) as journal:
                for i in range(offset, n, 4):
                    journal.update(f"k{i}", state=JobState.RUNNING)
                    journal.update(f"k{i}", state=JobState.DONE)
        except BaseException as exc:
            errors.append(exc)

    def read() -> None:
        try:
            with Journal(path) as journal:
                while not stop.is_set():
                    journal.records(active=True)
        except BaseException as exc:
            errors.append(exc)

    started = time.monotonic()
    reader_threads = [threading.Thread(target=read) for _ in range(readers)]
    for t in reader_threads:
        t.start()
    concurrently(write, 4)
    stop.set()
    for t in reader_threads:
        t.join()
    assert not errors, errors[:3]
    with Journal(path) as journal:
        records = journal.records()
        assert all(r.state is JobState.DONE for r in records)
        elapsed = time.monotonic() - started
        print(
            f"\n[journal-hammer] {2 * n} updates by 4 writers with {readers} readers in "
            f"{elapsed:.1f}s ({2 * n / elapsed:.0f} updates/s)"
        )
