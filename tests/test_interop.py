"""Interop: xrdwms against a real ARC CE (the docker ARC 7 + SLURM cluster), opt in only.

Run through ``tools/interop-docker.sh``, which sets::

    XRDWMS_INTEROP=1  XRDWMS_INTEROP_CE=arc-ce.arctest.local  [XRDWMS_INTEROP_QUEUE=main]

with a proxy and the CE's CA in the environment.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from dask.delayed import delayed
from distributed import Client, LocalCluster

import xrdwms.dask as gw
from _shared import failure_of
from xrdwms import JobManager, JobSpec, Journal, PollPolicy, backends, cli
from xrdwms.errors import JobFailedError, RemoteError
from xrdwms.model import FailureKind, Input, JobState, Output, Resources

pytestmark = pytest.mark.interop

CE = os.environ.get("XRDWMS_INTEROP_CE", "arc-ce.arctest.local")
QUEUE = os.environ.get("XRDWMS_INTEROP_QUEUE") or None
TIMEOUT = float(os.environ.get("XRDWMS_INTEROP_TIMEOUT", "600"))
POLICY = PollPolicy(interval=3, max_interval=10, batch_delay=0.2)
#: XRDWMS_INTEROP_VERIFY=0: the CE is reached by a name its certificate does not carry.
VERIFY = os.environ.get("XRDWMS_INTEROP_VERIFY", "1") != "0"
CONFIG = {"kind": "arc", "ce": CE, "verify": VERIFY, **({"queue": QUEUE} if QUEUE else {})}


def arc() -> backends.Backend:
    return backends.from_mapping("ce", {**CONFIG, "joblist": False})


async def test_a_job_with_inputs_and_outputs(tmp_path: Path) -> None:
    (tmp_path / "in.txt").write_text("from the client\n")
    spec = JobSpec(
        "interop-io",
        "/bin/sh",
        ("-c", "cat in.txt > out.txt; hostname >> out.txt; echo ran"),
        inputs=(Input(str(tmp_path / "in.txt"), "in.txt"),),
        outputs=(Output("out.txt"),),
        resources=Resources(walltime_s=600),
    )
    with Journal(tmp_path / "j.sqlite") as journal:
        async with JobManager([arc()], journal=journal, policy=POLICY) as jm:
            [key] = await jm.submit([spec], backend="ce")
            record = await jm.wait(key)
            assert record.state is JobState.DONE, record.info
            assert record.info is not None and record.info.worker_nodes
            got = await jm.retrieve(key, str(tmp_path / "results"))
            out = Path(got.path, "out.txt").read_text()
            assert out.startswith("from the client\n")
            assert Path(got.path, "stdout.txt").read_text() == "ran\n"
            [cleaned] = await jm.clean([key], forget=True)
            assert cleaned.ok


async def test_failure_and_cancel(tmp_path: Path) -> None:
    failing = JobSpec("interop-fail", "/bin/sh", ("-c", "exit 3"))
    sleepy = JobSpec("interop-sleep", "/bin/sleep", ("600",))
    with Journal(tmp_path / "j.sqlite") as journal:
        async with JobManager([arc()], journal=journal, policy=POLICY) as jm:
            fail_key, sleep_key = await jm.submit([failing, sleepy], backend="ce")
            await jm.submitted([sleep_key])
            [answer] = await jm.cancel([sleep_key])
            assert answer.ok, answer.reason
            failed, cancelled = await jm.wait_all([fail_key, sleep_key])
            assert failed.state is JobState.FAILED, failed.info
            assert failure_of(failed).kind is FailureKind.PAYLOAD
            assert cancelled.state is JobState.CANCELLED, cancelled.info


@pytest.fixture(scope="module")
def client(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Client]:
    work = tmp_path_factory.mktemp("dask")
    cluster: LocalCluster = gw.gateway_cluster(
        backends={"ce": {**CONFIG, "joblist": False}},
        journal=work / "j.sqlite",
        policy=POLICY,
        workdir=str(work),
        dashboard_address=":0",
        threads=2,
    )
    with Client(cluster) as connected:
        yield connected
    cluster.close()


def test_dask_fans_plain_jobs_out(client: Client) -> None:
    futures = [
        gw.submit_job(
            client, JobSpec(f"fan-{i}", "/bin/sh", ("-c", f"echo $(( {i} * {i} ))")), backend="ce"
        )
        for i in range(6)
    ]
    results = client.gather(futures)
    squares = [int(r.path("stdout.txt").read_text()) for r in results]
    assert squares == [i * i for i in range(6)]
    nodes = {r.record.info.worker_nodes for r in results}
    assert nodes  # where they ran, as SLURM reported it


def test_dask_reports_a_failed_grid_job(client: Client) -> None:
    future = gw.submit_job(client, JobSpec("fan-bad", "/bin/sh", ("-c", "exit 7")), backend="ce")
    with pytest.raises(JobFailedError):
        future.result(timeout=TIMEOUT)


def square(x: int) -> int:
    return x * x


def test_function_jobs_say_why_the_node_cannot_run_them(client: Client) -> None:
    """The cluster's nodes run Python 3.9 without cloudpickle: the runner must say so plainly."""
    task = gw.grid_task(square, backend="ce", python="python3")
    total = delayed(sum)([task(2), task(3)])
    try:
        assert client.compute(total).result(timeout=TIMEOUT) == 13
    except RemoteError as exc:
        here = f"{sys.version_info[0]}.{sys.version_info[1]}"
        assert "PythonVersionMismatch" in str(exc) and here in str(exc)


def test_the_command_line(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    journal = str(tmp_path / "cli.sqlite")
    config = tmp_path / "config.toml"
    queue = f'queue = "{QUEUE}"\n' if QUEUE else ""
    config.write_text(
        f'[backends.ce]\nkind = "arc"\nce = "{CE}"\nverify = {str(VERIFY).lower()}\n'
        f"joblist = false\n{queue}"
    )
    code = cli.main(
        [
            "submit",
            "-b",
            "ce",
            "-t",
            "10m",
            "-w",
            "--interval",
            "3",
            "--journal",
            journal,
            "--config",
            str(config),
            "--",
            "/bin/echo",
            "cli",
        ]
    )
    out = capsys.readouterr().out
    assert code == 0, out
    assert "done" in out
    code = cli.main(
        ["get", "--journal", journal, "--config", str(config), "-a", "-D", str(tmp_path / "got")]
    )
    assert code == 0
    [job] = (tmp_path / "got").iterdir()
    assert (job / "stdout.txt").read_text() == "cli\n"
