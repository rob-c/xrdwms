"""Grid jobs as dask tasks, on an in-process gateway cluster with the local and fake backends."""

from __future__ import annotations

import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from dask.delayed import delayed
from distributed import Client, LocalCluster

import xrdwms.dask as gw
from _shared import FAST
from xrdwms.errors import JobFailedError, RemoteError, WMSError
from xrdwms.journal import Journal
from xrdwms.model import JobSpec, JobState


def square(x: int) -> int:
    return x * x


def explode(x: int) -> int:
    raise ArithmeticError(f"no {x}")


@pytest.fixture(scope="module")
def roots(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    base = tmp_path_factory.mktemp("dask")
    return {"jobs": base / "jobs", "work": base / "work", "journal": base / "journal.sqlite"}


def make_cluster(roots: dict[str, Path], **kwargs: Any) -> LocalCluster:
    backends = {
        "local": {"kind": "local", "root": str(roots["jobs"])},
        "fake": {"kind": "fake"},
    }
    return gw.gateway_cluster(
        backends=backends,
        journal=roots["journal"],
        policy=FAST,
        workdir=str(roots["work"]),
        dashboard_address=":0",
        threads=2,
        grid_slots=500,
        **kwargs,
    )


@pytest.fixture(scope="module")
def client(roots: dict[str, Path]) -> Iterator[Client]:
    cluster = make_cluster(roots)
    with Client(cluster) as connected:
        yield connected
    cluster.close()


def manager_of(client: Client) -> Any:
    cluster = client.cluster
    assert cluster is not None
    [worker] = cluster.workers.values()
    return worker.plugins[gw.PLUGIN].manager


def test_a_plain_job(client: Client) -> None:
    spec = JobSpec("echo", "/bin/echo", ("hello", "grid"))
    result = gw.submit_job(client, spec, backend="local").result(timeout=60)
    assert result.record.state is JobState.DONE
    assert result.path("stdout.txt").read_text() == "hello grid\n"


def test_a_plain_job_without_its_sandbox(client: Client) -> None:
    result = gw.submit_job(client, JobSpec("t", "true"), backend="local", fetch=False).result(60)
    assert result.directory is None
    with pytest.raises(WMSError, match="not retrieved"):
        result.path("x")


def test_a_failing_job_raises(client: Client) -> None:
    future = gw.submit_job(client, JobSpec("bad", "/bin/sh", ("-c", "exit 2")), backend="local")
    with pytest.raises(JobFailedError, match="exited with code 2"):
        future.result(timeout=60)


def test_functions_on_the_grid_compose_with_dask(client: Client) -> None:
    task = gw.grid_task(square, backend="local", python=sys.executable)
    total = delayed(sum)([task(i) for i in range(4)])
    assert client.compute(total).result(timeout=120) == 0 + 1 + 4 + 9
    nested = task(task(2))
    assert client.compute(nested).result(timeout=120) == 16
    assert task.submit(client, 5).result(timeout=120) == 25


def test_a_raising_function(client: Client) -> None:
    task = gw.grid_task(backend="local", python=sys.executable)(explode)
    with pytest.raises(RemoteError, match="ArithmeticError: no 3"):
        client.compute(task(3)).result(timeout=120)


def test_backend_failures_reach_the_client(client: Client) -> None:
    fake = next(iter(manager_of(client).backends.values()))
    fake = manager_of(client).backends["fake"]
    fake.script("doomed", "running", "failed:payload")
    future = gw.submit_job(client, JobSpec("doomed", "/bin/true"), backend="fake")
    with pytest.raises(JobFailedError, match="fake payload failure"):
        future.result(timeout=60)


def test_pure_calls_share_a_job(client: Client) -> None:
    task = gw.grid_task(square, backend="local", python=sys.executable)
    assert task(7).key == task(7).key
    assert task(7).key != task(8).key
    impure = gw.grid_task(square, backend="local", pure=False)
    assert impure(7).key != impure(7).key
    assert gw.job_key("a", pure=False) != gw.job_key("a", pure=False)
    assert gw.job_key("a") == gw.job_key("a")


def test_cancelling_the_future_cancels_the_job(client: Client) -> None:
    future = gw.submit_job(client, JobSpec("nap", "/bin/sleep", ("60",)), backend="local")
    manager = manager_of(client)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        running = [r for r in manager.journal.records(active=True) if r.spec.name == "nap"]
        if running and running[0].state is JobState.RUNNING:
            break
        time.sleep(0.05)
    future.cancel()
    while time.monotonic() < deadline:
        [record] = [r for r in manager.journal.records() if r.spec.name == "nap"]
        if record.state is JobState.CANCELLED:
            break
        time.sleep(0.05)
    assert record.state is JobState.CANCELLED


def test_a_recomputed_graph_reattaches(roots: dict[str, Path], client: Client) -> None:
    task = gw.grid_task(square, backend="local", python=sys.executable)
    assert client.compute(task(11)).result(timeout=120) == 121
    before = len(list(roots["jobs"].iterdir()))
    # A second cluster on the same journal and job root: a client restarted after a crash.
    second = make_cluster(roots)
    with Client(second, set_as_default=False) as other:
        assert other.compute(task(11)).result(timeout=120) == 121
    second.close()
    assert len(list(roots["jobs"].iterdir())) == before


def test_without_the_plugin() -> None:
    with (
        LocalCluster(
            n_workers=1, processes=False, resources={gw.GRID: 10}, dashboard_address=":0"
        ) as cluster,
        Client(cluster) as bare,
    ):
        future = gw.submit_job(bare, JobSpec("x", "true"), backend="local")
        with pytest.raises(WMSError, match="not installed on this dask worker"):
            future.result(timeout=30)
        gw.install(bare, {"fake": {"kind": "fake"}}, policy=FAST)
        result = gw.submit_job(bare, JobSpec("x", "true"), backend="fake").result(timeout=30)
        assert result.record.state is JobState.DONE


def test_the_gateway_without_backends() -> None:
    cluster = gw.gateway_cluster(dashboard_address=":0", grid_slots=10)
    [worker] = cluster.workers.values()
    assert worker.state.nthreads == 10 and gw.PLUGIN not in worker.plugins
    cluster.close()


def test_a_job_is_kept_in_the_journal_on_disk(client: Client, roots: dict[str, Path]) -> None:
    """Self-contained (it runs its own job), so it holds whatever ran before it, on any worker."""
    result = gw.submit_job(client, JobSpec("kept", "/bin/echo"), backend="local").result(timeout=60)
    with Journal(roots["journal"]) as journal:
        kept = journal.get(result.record.key)
    assert kept.backend == "local" and kept.state is JobState.DONE
