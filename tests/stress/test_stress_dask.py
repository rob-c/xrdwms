"""Stress: grid jobs as dask tasks - many of them, from many threads, on many workers."""

from __future__ import annotations

import asyncio
import json
import os
import random
import signal
import subprocess
import sys
import textwrap
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Any

import pytest
from dask.delayed import delayed
from distributed import Client, LocalCluster

import xrdwms.dask as gw
from stress._load import SEED, LoadBackend, Report, check, concurrently, max_rss_mb, scaled
from xrdwms import JobManager, JobSpec, Journal, PollPolicy, backends
from xrdwms.backends.local import LocalBackend
from xrdwms.model import JobState

POLICY = PollPolicy(
    interval=0.05, max_interval=0.5, batch_delay=0.05, backoff_cap=0.5, lost_after=3, lease=4
)


@pytest.fixture(autouse=True)
def _load_kind(monkeypatch: pytest.MonkeyPatch) -> None:
    """Let plugins build LoadBackends by kind, as they build any backend from configuration."""
    monkeypatch.setitem(backends.BUILTIN, "load", "stress._load:LoadBackend")


def plugin_backends(cluster: LocalCluster) -> list[LoadBackend]:
    return [
        b
        for w in cluster.workers.values()
        for b in w.plugins[gw.PLUGIN].manager.backends.values()
        if isinstance(b, LoadBackend)
    ]


def cpu_work(values: list[Any]) -> int:
    """An ordinary dask task that burns a little CPU on the grid results."""
    total = 0
    for _ in range(2_000):
        total = (total + len(values)) % 1_000_003
    return len(values)


def test_ten_thousand_grid_tasks(tmp_path: Path, record_report: list[Report]) -> None:
    n = scaled(10_000)
    threads, started = threading.active_count(), time.monotonic()
    cluster = gw.gateway_cluster(
        backends={"load": {"kind": "load", "steps": ["queued", "running", "done"]}},
        journal=tmp_path / "j.sqlite",
        policy=POLICY,
        dashboard_address=":0",
        grid_slots=n + 10,
        threads=4,
    )
    with Client(cluster) as client:
        futures = [
            gw.submit_job(client, JobSpec(f"t{i}", "/bin/true"), backend="load", fetch=False)
            for i in range(n)
        ]
        results = client.gather(futures)
        assert all(r.record.state is JobState.DONE for r in results)
        load = plugin_backends(cluster)
    cluster.close()
    with Journal(tmp_path / "j.sqlite") as journal:
        record_report.append(
            check("dask-10k", journal, load, started=started, threads_before=threads, expected=n)
        )


def test_concurrent_clients_and_cancellations(tmp_path: Path, record_report: list[Report]) -> None:
    """Eight threads each build and compute graphs mixing grid and CPU tasks; some are cancelled."""
    n_threads, per_thread = 8, scaled(400)
    threads, started = threading.active_count(), time.monotonic()
    cluster = gw.gateway_cluster(
        # Jobs that outlive the cancel grace period, so a cancelled future has a job to cancel.
        backends={"load": {"kind": "load", "steps": ["queued"] * 40 + ["running"] * 10 + ["done"]}},
        journal=tmp_path / "j.sqlite",
        policy=POLICY,
        dashboard_address=":0",
        grid_slots=n_threads * per_thread + 10,
        threads=4,
    )
    cancelled: list[str] = []
    lock = threading.Lock()

    def run(index: int) -> int:
        rng = random.Random(SEED + index)
        with Client(cluster, set_as_default=False) as client:  # one client per thread
            jobs = [
                gw.submit_job(
                    client, JobSpec(f"c{index}-{i}", "/bin/true"), backend="load", fetch=False
                )
                for i in range(per_thread)
            ]
            victims = rng.sample(range(per_thread), per_thread // 10)
            time.sleep(rng.uniform(0.1, 0.5))
            for v in victims:
                jobs[v].cancel()
                with lock:
                    cancelled.append(str(jobs[v].key))
            kept = [f for i, f in enumerate(jobs) if i not in set(victims)]
            summary = client.submit(cpu_work, kept, pure=False)
            return int(summary.result(timeout=600))

    sums = concurrently(run, n_threads)
    assert sums == [per_thread - per_thread // 10] * n_threads
    time.sleep(gw.CANCEL_GRACE + 2)  # cancelled futures cancel their grid jobs after the grace
    load = plugin_backends(cluster)
    manager = next(iter(cluster.workers.values())).plugins[gw.PLUGIN].manager
    deadline = time.monotonic() + 60
    while any(not r.terminal for r in manager.journal.records()) and time.monotonic() < deadline:
        time.sleep(0.2)
    cluster.close()
    with Journal(tmp_path / "j.sqlite") as journal:
        records = {r.key: r for r in journal.records()}
        # A future cancelled before its task ever ran made no job at all; every other one did.
        assert len(records) >= n_threads * per_thread - len(cancelled)
        record_report.append(
            check(
                "dask-threads-cancel",
                journal,
                load,
                started=started,
                threads_before=threads,
                expected=len(records),
                allowed={JobState.DONE, JobState.CANCELLED},
                cancelled_futures=len(cancelled),
                never_started=n_threads * per_thread - len(records),
            )
        )
    # The dask key of a grid task ends in its job key: every cancelled future that got as far
    # as a grid job must have had that job cancelled, and nothing else may have been.
    wanted_gone = {key.rsplit("-", 1)[-1] for key in cancelled}
    for key, record in records.items():
        if key in wanted_gone:
            assert record.state is JobState.CANCELLED, (key, record.state, record.info)
        else:
            assert record.state is JobState.DONE, (key, record.state)


def test_many_workers_share_a_journal(tmp_path: Path, record_report: list[Report]) -> None:
    """Four gateway workers, one journal: each runs only its own grid jobs."""
    n = scaled(4_000)
    threads, started = threading.active_count(), time.monotonic()
    cluster = LocalCluster(
        n_workers=4,
        processes=False,
        threads_per_worker=n,
        resources={gw.GRID: n},
        dashboard_address=":0",
    )
    with Client(cluster) as client:
        gw.install(client, {"load": {"kind": "load"}}, journal=tmp_path / "j.sqlite", policy=POLICY)
        futures = [
            gw.submit_job(client, JobSpec(f"w{i}", "/bin/true"), backend="load", fetch=False)
            for i in range(n)
        ]
        results = client.gather(futures)
        assert all(r.record.state is JobState.DONE for r in results)
        load = plugin_backends(cluster)
        owners = Counter(r.record.owner for r in results)
    cluster.close()
    assert len(owners) > 1, "the scheduler should have spread grid tasks over the workers"
    with Journal(tmp_path / "j.sqlite") as journal:
        record_report.append(
            check(
                "dask-4-workers",
                journal,
                load,
                started=started,
                threads_before=threads,
                expected=n,
                owners=dict(owners),
            )
        )


def square(x: int) -> int:
    return x * x


def test_function_jobs_as_real_processes(tmp_path: Path, record_report: list[Report]) -> None:
    """Hundreds of Python functions run as local jobs: real interpreters, real pickles."""
    n = scaled(300)
    started = time.monotonic()
    cluster = gw.gateway_cluster(
        backends={
            "local": {
                "kind": "local",
                "root": str(tmp_path / "jobs"),
                "max_running": os.cpu_count(),
            }
        },
        journal=tmp_path / "j.sqlite",
        policy=POLICY,
        workdir=str(tmp_path / "work"),
        dashboard_address=":0",
        threads=4,
    )
    task = gw.grid_task(square, backend="local", python=sys.executable)
    with Client(cluster) as client:
        total = delayed(sum)([task(i) for i in range(n)])
        assert client.compute(total).result(timeout=1200) == sum(i * i for i in range(n))
    cluster.close()
    elapsed = time.monotonic() - started
    print(
        f"\n[function-jobs] {n} Python function jobs as processes in {elapsed:.1f}s "
        f"({n / elapsed:.1f}/s) rss={max_rss_mb():.0f}MB"
    )
    with Journal(tmp_path / "j.sqlite") as journal:
        assert all(r.state is JobState.DONE for r in journal.records())


CRASHING_CLIENT = textwrap.dedent(
    """
    import asyncio, sys
    from xrdwms import JobManager, JobSpec, Journal, PollPolicy
    from xrdwms.backends.local import LocalBackend

    async def main(journal, root, n):
        policy = PollPolicy(interval=0.05, max_interval=0.2, batch_delay=0.05, lease=2)
        with Journal(journal) as j:
            async with JobManager([LocalBackend(root=root, max_running=8)], journal=j,
                                  policy=policy) as jm:
                await jm.submit([JobSpec(f"s{i}", "/bin/sleep", ("0.5",)) for i in range(n)],
                                backend="local")
                print("submitted", flush=True)
                await asyncio.sleep(3600)

    asyncio.run(main(sys.argv[1], sys.argv[2], int(sys.argv[3])))
    """
)


async def _take_over(journal_path: Path, root: Path) -> None:
    """A new client on the dead one's journal and job root, until every job has ended."""
    with Journal(journal_path) as journal:
        policy = PollPolicy(interval=0.05, max_interval=0.2, batch_delay=0.05, lease=2)
        backend = LocalBackend(root=str(root), max_running=8)
        async with JobManager([backend], journal=journal, policy=policy) as jm:
            keys = [r.key for r in journal.records()]
            deadline = time.monotonic() + 600
            while time.monotonic() < deadline:
                if all(r.terminal for r in journal.get_many(keys).values()):
                    break
                await asyncio.sleep(0.2)
            await jm.wait_all(keys)


def test_a_killed_client_is_taken_over(tmp_path: Path, record_report: list[Report]) -> None:
    """SIGKILL a client mid-run; once its lease lapses, a new one finishes every job, once."""
    n = scaled(300)
    journal_path, root = tmp_path / "j.sqlite", tmp_path / "jobs"
    script = tmp_path / "client.py"
    script.write_text(CRASHING_CLIENT)
    started = time.monotonic()
    child = subprocess.Popen(
        [sys.executable, str(script), str(journal_path), str(root), str(n)],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert child.stdout is not None and child.stdout.readline().strip() == "submitted"
    time.sleep(1.5)  # some jobs done, some running, some still queued or unsent
    child.send_signal(signal.SIGKILL)
    child.wait()
    child.stdout.close()
    with Journal(journal_path) as journal:
        before = Counter(r.state for r in journal.records())

    asyncio.run(_take_over(journal_path, root))
    # Every journal key ran exactly once per attempt on the "CE" (the job root on disk).
    attempts = Counter(
        json.loads((d / ".xrdwms-job.json").read_text())["key"] for d in root.iterdir()
    )
    duplicates = {k: c for k, c in attempts.items() if c > 1}
    with Journal(journal_path) as journal:
        records = journal.records()
        states = Counter(r.state for r in records)
    elapsed = time.monotonic() - started
    at_kill = {s.value: c for s, c in before.items()}
    after = {s.value: c for s, c in states.items()}
    print(
        f"\n[killed-client] {n} jobs; at the kill: {at_kill}; after takeover: {after}; "
        f"duplicates={len(duplicates)}; {elapsed:.1f}s"
    )
    assert states == Counter({JobState.DONE: n})
    assert not duplicates, duplicates
