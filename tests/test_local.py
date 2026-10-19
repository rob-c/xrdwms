"""The local backend: real processes, sandboxes on disk, URL staging through GridStorage."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

from _shared import FAST, failure_of, handle_of, handles_of, info_of
from xrdwms import JobManager, JobSpec, Journal
from xrdwms.backends.local import LocalBackend, _alive
from xrdwms.journal import Record
from xrdwms.model import (
    FailureKind,
    Input,
    JobHandle,
    JobState,
    KeyedSpec,
    Output,
    Placement,
    Runtime,
)


@pytest.fixture
def local(tmp_path: Path) -> LocalBackend:
    return LocalBackend(root=str(tmp_path / "jobs"), workdir=str(tmp_path), max_running=2)


async def run(backend: LocalBackend, *specs: JobSpec) -> list[Record]:
    async with JobManager([backend], policy=FAST) as jm:
        keys = await jm.submit(list(specs), backend=backend.name)
        return list(await jm.wait_all(keys))


async def test_a_job_runs_with_its_inputs_and_outputs(local: LocalBackend, tmp_path: Path) -> None:
    (tmp_path / "in.txt").write_text("hello")
    (tmp_path / "remote.txt").write_text("from afar")
    script = tmp_path / "job.sh"
    script.write_text('cat in.txt far.txt > out.txt; echo "$GREETING" > env.txt; echo done\n')
    spec = JobSpec(
        "job",
        "job.sh",
        inputs=(
            Input("job.sh"),
            Input("in.txt"),
            Input((tmp_path / "remote.txt").as_uri(), "far.txt"),
        ),
        outputs=(Output("out.txt"), Output("env.txt", (tmp_path / "up" / "env.txt").as_uri())),
        environment={"GREETING": "hi"},
    )
    async with JobManager([local], policy=FAST) as jm:
        [key] = await jm.submit([spec], backend="local")
        record = await jm.wait(key)
        assert record.state is JobState.DONE, (info_of(record).failure, info_of(record).exit_code)
        got = await jm.retrieve(key, str(tmp_path / "results"))
    assert Path(got.path, "out.txt").read_text() == "hellofrom afar"
    assert Path(got.path, "stdout.txt").read_text() == "done\n"
    assert not any(name.startswith(".xrdwms") for name in got.files)
    assert (tmp_path / "up" / "env.txt").read_text() == "hi\n"


async def test_exit_codes_and_missing_outputs(local: LocalBackend, tmp_path: Path) -> None:
    failing = JobSpec("fail", "/bin/sh", ("-c", "exit 3"))
    unstaged = JobSpec("nout", "true", outputs=(Output("x", (tmp_path / "x").as_uri()),))
    records = await run(local, failing, unstaged)
    first, second = records
    assert info_of(first).exit_code == 3 and failure_of(first).kind is FailureKind.PAYLOAD
    assert second.state is JobState.FAILED
    assert failure_of(second).kind is FailureKind.DATA
    # Asked again, the stage-out problems are remembered, not retried.
    reopened = LocalBackend(root=str(local.root))
    [again] = await reopened.status([handle_of(second)])
    assert failure_of(again).kind is FailureKind.DATA
    await reopened.close()


async def test_refusals_and_bad_inputs(local: LocalBackend) -> None:
    results = await local.submit(
        [
            KeyedSpec("a", JobSpec("c", "true", runtime=Runtime(container="x"))),
            KeyedSpec("b", JobSpec("p", "true", placement=Placement(sites=("s",)))),
            KeyedSpec("c", JobSpec("i", "true", inputs=(Input("no-such-file"),))),
        ]
    )
    kinds = [failure_of(r).kind for r in results]
    assert kinds == [FailureKind.PAYLOAD, FailureKind.PAYLOAD, FailureKind.DATA]
    await local.close()


async def test_queueing_cancel_clean_find(local: LocalBackend, tmp_path: Path) -> None:
    sleepy = JobSpec("sleep", "/bin/sleep", ("30",))
    results = await local.submit([KeyedSpec(f"k{i}", sleepy) for i in range(3)])
    handles = handles_of(results)
    infos = await local.status(handles)
    assert [i.state for i in infos].count(JobState.RUNNING) == 2
    assert [i.state for i in infos].count(JobState.QUEUED) == 1
    assert await local.find(["k0", "k2", "zz"]) == {"k0": handles[0], "k2": handles[2]}
    running = [h for h, i in zip(handles, infos, strict=True) if i.state is JobState.RUNNING]
    [busy] = await local.clean(running[:1])
    assert not busy.ok and "still running" in busy.reason
    answers = await local.cancel([*handles, JobHandle("local", "nope")])
    assert [a.ok for a in answers] == [True, True, True, False]
    infos = await local.status(handles)
    assert {i.state for i in infos} == {JobState.CANCELLED}
    for _ in range(100):
        if not any(local._live(local.root / h.id) for h in handles):
            break
        await asyncio.sleep(0.05)
    cleaned = await local.clean(handles)
    assert all(c.ok for c in cleaned)
    [gone] = await local.status(handles[:1])
    assert gone.state is JobState.UNKNOWN
    with pytest.raises(FileNotFoundError):
        await local.retrieve(handles[0], str(tmp_path))
    await local.close()


async def test_cancelling_an_ended_job_is_refused(local: LocalBackend) -> None:
    [record] = await run(local, JobSpec("t", "true"))
    again = LocalBackend(root=str(local.root))  # run() closed the first one
    [answer] = await again.cancel([handle_of(record)])
    await again.close()
    assert not answer.ok and "already ended" in answer.reason


async def test_jobs_survive_a_new_backend_object(local: LocalBackend, tmp_path: Path) -> None:
    """A second backend over the same root sees the first one's jobs (a restarted client)."""
    [result] = await local.submit([KeyedSpec("k", JobSpec("s", "/bin/sleep", ("0.3",)))])
    await local.status([handle_of(result)])
    again = LocalBackend(root=str(local.root))
    for _ in range(100):
        [info] = await again.status([handle_of(result)])
        if info.state.terminal:
            break
        assert info.state is JobState.RUNNING
        await asyncio.sleep(0.05)
    assert info.state is JobState.DONE
    await local.close()
    await again.close()


async def test_a_vanished_process_is_an_infra_failure(local: LocalBackend) -> None:
    [result] = await local.submit([KeyedSpec("k", JobSpec("s", "true"))])
    sandbox = local.root / handle_of(result).id
    (sandbox / ".xrdwms-pid").write_text("999999")  # never started by us, and not alive
    [info] = await LocalBackend(root=str(local.root)).status([handle_of(result)])
    assert info.state is JobState.FAILED and failure_of(info).kind is FailureKind.INFRA
    await local.close()


async def test_a_job_ending_between_looks(
    local: LocalBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    [result] = await local.submit([KeyedSpec("k", JobSpec("s", "true"))])
    sandbox = local.root / handle_of(result).id
    (sandbox / ".xrdwms-pid").write_text("1")

    def ended(_sandbox: Path) -> bool:
        (sandbox / ".xrdwms-exit").write_text("0\n")
        return False

    monkeypatch.setattr(local, "_live", ended)
    [info] = await local.status([handle_of(result)])
    assert info.state is JobState.DONE
    await local.close()


def test_alive() -> None:
    assert _alive(os.getpid())
    assert not _alive(999999)
    assert _alive(1)  # someone else's: permission denied still means alive


async def test_empty_root_and_default_root(tmp_path: Path, home: Path) -> None:
    backend = LocalBackend(root=str(tmp_path / "none"))
    assert backend._jobs() == []
    assert LocalBackend().root == home / ".local/state/xrdwms/local"
    os.environ["XDG_STATE_HOME"] = str(tmp_path)
    try:
        assert LocalBackend().root == tmp_path / "xrdwms/local"
    finally:
        del os.environ["XDG_STATE_HOME"]
    await backend.close()


async def test_meta_is_json(local: LocalBackend) -> None:
    [result] = await local.submit([KeyedSpec("k", JobSpec("t", "true"))])
    meta = json.loads((local.root / handle_of(result).id / ".xrdwms-job.json").read_text())
    assert meta["key"] == "k" and meta["spec"]["name"] == "t"
    await local.close()


async def test_journal_restart_with_local_jobs(tmp_path: Path) -> None:
    """Kill the client mid-run; a new manager on the same journal and root finishes the job."""
    journal_path = tmp_path / "j.sqlite"
    root = str(tmp_path / "jobs")
    spec = JobSpec("s", "/bin/sleep", ("0.2",))
    journal = Journal(journal_path)
    async with JobManager([LocalBackend(root=root)], journal=journal, policy=FAST) as jm:
        [key] = await jm.submit([spec], backend="local")
        await jm.submitted([key])
    journal.close()
    journal = Journal(journal_path)
    async with JobManager([LocalBackend(root=root)], journal=journal, policy=FAST) as jm:
        jm.resume()
        assert (await jm.wait(key)).state is JobState.DONE
    journal.close()
