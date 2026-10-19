"""The ARC backend: the xRSL translator, state mapping, and the backend against a fake A-REX."""

from __future__ import annotations

import datetime as dt
import socket
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from xrdarc import ARCClient, Credentials, Job
from xrdarc import JobState as ARCState
from xrdarc.testing.arex import FakeARex
from xrdarc.xrsl import parse
from xrdsec.testing.pki import PKI

from _shared import FAST, USER_DN, failure_of, handle_of, handles_of, info_of
from xrdwms import JobManager, JobSpec, Journal
from xrdwms.backends.arc import ARCBackend, _state, classify, job_name, key_of, to_xrsl
from xrdwms.errors import UnsupportedError
from xrdwms.model import (
    FailureKind,
    Input,
    JobHandle,
    JobState,
    KeyedSpec,
    Output,
    Placement,
    Resources,
    Runtime,
)

MakeARC = Callable[..., ARCClient]


def relations(text: str) -> dict[str, list[tuple[object, ...]]]:
    [job] = parse(text)
    found: dict[str, list[tuple[object, ...]]] = {}
    for relation in job:
        found.setdefault(relation.attribute, []).append(relation.values)
    return found


# -- the translator --------------------------------------------------------------------------


def test_a_full_spec_in_xrsl() -> None:
    spec = JobSpec(
        name="sim",
        executable="run.sh",
        arguments=("a b", 'quote"d'),
        environment={"SEED": "3"},
        inputs=(Input("run.sh"), Input("root://se//f.root", "f.root")),
        outputs=(Output("out.txt"), Output("big", "davs://se/big")),
        resources=Resources(cores=4, memory_mb=8001, walltime_s=3600, cputime_s=7200, disk_mb=500),
        runtime=Runtime(environments=("ENV/PROXY", "APPS/X")),
        placement=Placement(queue="short"),
    )
    found = relations(to_xrsl(spec, "k-0"))
    assert found["executable"] == [("run.sh",)]
    assert found["arguments"] == [("a b", 'quote"d')]
    assert found["jobname"] == [("sim~k-0",)]
    assert found["inputfiles"] == [(("run.sh", "run.sh"), ("f.root", "root://se//f.root"))]
    assert found["outputfiles"] == [(("out.txt", ""), ("big", "davs://se/big"))]
    assert found["environment"] == [(("SEED", "3"),)]
    assert found["count"] == [("4",)]
    assert found["memory"] == [("2001",)]  # per core, rounded up
    assert found["walltime"] == [("PT3600S",)] and found["cputime"] == [("PT7200S",)]
    assert found["disk"] == [("500",)]
    assert found["runtimeenvironment"] == [("ENV/PROXY",), ("APPS/X",)]
    assert found["queue"] == [("short",)]
    assert found["stdout"] == [("stdout.txt",)] and found["gmlog"] == [("gmlog",)]


def test_a_minimal_spec_says_only_what_it_needs() -> None:
    found = relations(to_xrsl(JobSpec("x", "/bin/true"), "k"))
    assert set(found) == {"executable", "jobname", "stdout", "stderr", "gmlog"}


def test_native_attributes_are_added_and_win() -> None:
    spec = JobSpec(
        "x",
        "/bin/true",
        native={"arc": {"Std_Out": "mine.txt", "rerun": 2, "inputfiles": [["a", ""], ["b", ""]]}},
    )
    found = relations(to_xrsl(spec, "k"))
    assert found["stdout"] == [("mine.txt",)]
    assert found["rerun"] == [("2",)]
    assert found["inputfiles"] == [(("a", ""), ("b", ""))]


@pytest.mark.parametrize(
    ("change", "field"),
    [
        ({"resources": Resources(gpus=1)}, "resources.gpus"),
        ({"runtime": Runtime(container="docker://x")}, "runtime.container"),
        ({"runtime": Runtime(platform="x86_64-el9")}, "runtime.platform"),
        ({"placement": Placement(sites=("A",))}, "placement.sites"),
    ],
)
def test_what_xrsl_cannot_say_is_refused(change: dict[str, object], field: str) -> None:
    with pytest.raises(UnsupportedError, match=field.replace(".", r"\.")):
        to_xrsl(JobSpec("x", "/bin/true").with_(**change), "k")


def test_job_names_carry_the_key() -> None:
    spec = JobSpec("my~job", "/bin/true")
    assert key_of(job_name(spec, "abc-1")) == "abc-1"
    assert key_of("plain") is None and key_of(None) is None and key_of("x~") is None


# -- states --------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("errors", "exit_code", "kind"),
    [
        (["The proxy has expired"], None, FailureKind.AUTH),
        (["LRMS error: job killed: wall time exceeded"], 271, FailureKind.LIMIT),
        (["Failed in files download (pre-processing)"], None, FailureKind.DATA),
        (["Job failed with exit code 3"], 3, FailureKind.PAYLOAD),
        (["Submission to LRMS failed"], None, FailureKind.INFRA),
        ([], 0, FailureKind.INFRA),
    ],
)
def test_failures_are_classified(
    errors: list[str], exit_code: int | None, kind: FailureKind
) -> None:
    assert classify(errors, exit_code) is kind


def test_every_arc_state_maps() -> None:
    def state(arc: ARCState, **fields: Any) -> tuple[JobState, FailureKind | None]:
        mapped, failure = _state(Job("https://ce:443/arex", "j", state=arc, **fields))
        return mapped, failure.kind if failure else None

    assert state(ARCState.FINISHED) == (JobState.DONE, None)
    assert state(ARCState.FINISHED, exit_code=0) == (JobState.DONE, None)
    assert state(ARCState.FINISHED, exit_code=2) == (JobState.FAILED, FailureKind.PAYLOAD)
    assert state(ARCState.FAILED, errors=("proxy expired",)) == (JobState.FAILED, FailureKind.AUTH)
    assert state(ARCState.FAILED) == (JobState.FAILED, FailureKind.INFRA)
    assert state(ARCState.DELETED) == (JobState.FAILED, FailureKind.INFRA)
    assert state(ARCState.KILLED) == (JobState.CANCELLED, None)
    assert state(ARCState.QUEUING) == (JobState.QUEUED, None)
    assert state(ARCState.PREPARING) == (JobState.STAGING_IN, None)
    assert state(ARCState.HOLD) == (JobState.HELD, None)
    assert state(ARCState.OTHER) == (JobState.RUNNING, None)
    assert state(ARCState.FINISHING) == (JobState.FINISHING, None)
    assert state(ARCState.UNDEFINED) == (JobState.UNKNOWN, None)


# -- the backend against a fake A-REX ----------------------------------------------------------


@pytest.fixture
def backend(ce: FakeARex, make_arc: MakeARC, tmp_path: Path) -> ARCBackend:
    return ARCBackend("ce", ce=ce.url, client=make_arc(), workdir=str(tmp_path))


async def test_submit_poll_retrieve_clean(
    ce: FakeARex, backend: ARCBackend, tmp_path: Path
) -> None:
    (tmp_path / "in.txt").write_text("input")
    spec = JobSpec(
        "hello", "/bin/echo", ("hi",), inputs=(Input("in.txt"),), outputs=(Output("out.txt"),)
    )
    journal = Journal()
    async with JobManager([backend], journal=journal, policy=FAST) as jm:
        [key] = await jm.submit([spec], backend="ce")
        [record] = await jm.submitted([key])
        assert record.state is JobState.QUEUED
        fake = ce.jobs[handle_of(record).id.rsplit("/", 1)[1]]
        assert fake.name == f"hello~{key}-0"
        assert (fake.session / "in.txt").read_text() == "input"
        (fake.session / "out.txt").write_text("result")
        ce.set_state(fake.id, "RUNNING")
        while jm.journal.get(key).state is not JobState.RUNNING:
            await jm.refresh([key])
        ce.set_state(fake.id, "FINISHED", exit_code=0)
        record = await jm.wait(key)
        assert record.state is JobState.DONE and info_of(record).site == "127.0.0.1"
        got = await jm.retrieve(key, str(tmp_path / "results"))
        assert Path(got.path, "out.txt").read_text() == "result"
        assert "out.txt" in got.files
        [cleaned] = await jm.clean([key], forget=True)
        assert cleaned.ok and jm.journal.find(key) is None  # A-REX cleans asynchronously
    journal.close()


async def test_payload_failure_with_exit_code(ce: FakeARex, backend: ARCBackend) -> None:
    [result] = await backend.submit([KeyedSpec("k", JobSpec("x", "/bin/false"))])
    assert result.handle is not None
    job_id = result.handle.id.rsplit("/", 1)[1]
    ce.fail_job(job_id, "Job failed with exit code 1", exit_code=1)
    [info] = await backend.status([result.handle])
    assert info.state is JobState.FAILED and info.exit_code == 1
    assert failure_of(info).kind is FailureKind.PAYLOAD
    await backend.close()


async def test_one_bad_spec_fails_alone(ce: FakeARex, backend: ARCBackend) -> None:
    specs = [
        KeyedSpec("a", JobSpec("ok", "/bin/true")),
        KeyedSpec("b", JobSpec("missing", "/bin/true", inputs=(Input("nope.txt"),))),
        KeyedSpec("c", JobSpec("gpu", "/bin/true", resources=Resources(gpus=1))),
        KeyedSpec("d", JobSpec("ok2", "/bin/true")),
    ]
    results = await backend.submit(specs)
    assert [r.key for r in results] == ["a", "b", "c", "d"]
    assert results[0].ok and results[3].ok
    assert failure_of(results[1]).kind is FailureKind.PAYLOAD
    assert "gpus" in failure_of(results[2]).message
    assert len(ce.jobs) == 2
    await backend.close()


async def test_refusals_and_outages(
    ce: FakeARex, make_arc: MakeARC, pki: PKI, tmp_path: Path
) -> None:
    backend = ARCBackend("ce", ce=ce.url, client=make_arc())
    ce.reject_next("No space left", times=1)
    [refused] = await backend.submit([KeyedSpec("k", JobSpec("x", "/bin/true"))])
    assert failure_of(refused).kind is FailureKind.INFRA
    assert "No space left" in failure_of(refused).message
    ce.denied.add("JobCreate")
    [denied] = await backend.submit([KeyedSpec("k", JobSpec("x", "/bin/true"))])
    assert failure_of(denied).kind is FailureKind.AUTH
    ce.denied.clear()
    await backend.close()
    nobody = make_arc(credentials=Credentials(ca_dir=str(pki.ca_dir)))
    anonymous = ARCBackend("ce", ce=ce.url, client=nobody)
    [no_cred] = await anonymous.submit([KeyedSpec("k", JobSpec("x", "/bin/true"))])
    assert failure_of(no_cred).kind is FailureKind.AUTH
    await anonymous.close()
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        dead = f"https://127.0.0.1:{probe.getsockname()[1]}/arex"
    down = ARCBackend("ce", ce=dead, client=make_arc())
    [unreachable] = await down.submit([KeyedSpec("k", JobSpec("x", "/bin/true"))])
    assert failure_of(unreachable).kind is FailureKind.INFRA
    [info] = await down.status([JobHandle("ce", f"{dead}/rest/1.0/jobs/abc")])
    assert info.transient and info.state is JobState.UNKNOWN
    await down.close()


async def test_status_of_a_job_the_ce_does_not_know(ce: FakeARex, backend: ARCBackend) -> None:
    [info] = await backend.status([JobHandle("ce", f"{ce.url}/rest/1.0/jobs/nosuchjob")])
    assert info.state is JobState.UNKNOWN and not info.transient
    await backend.close()


async def test_cancel_renew_find(ce: FakeARex, backend: ARCBackend) -> None:
    results = await backend.submit(
        [
            KeyedSpec("key-0", JobSpec("a", "/bin/sleep")),
            KeyedSpec("key2-0", JobSpec("b", "/bin/true")),
        ]
    )
    handles = handles_of(results)
    found = await backend.find(["key-0", "key2-0", "other"])
    assert found == {"key-0": handles[0], "key2-0": handles[1]}
    for handle in handles:
        ce.set_state(handle.id.rsplit("/", 1)[1], "RUNNING")
    renewed = await backend.renew(handles)
    assert all(r.ok for r in renewed)
    [cancelled, _other] = await backend.cancel(handles)
    assert cancelled.ok
    ce.add_job(USER_DN, '&(executable="/bin/true")(jobname="not-ours")')
    assert set(await backend.find(["key-0"])) == {"key-0"}
    await backend.close()


async def test_find_on_an_empty_ce(backend: ARCBackend) -> None:
    assert await backend.find(["k"]) == {}
    await backend.close()


async def test_timestamps_and_nodes(ce: FakeARex, backend: ARCBackend) -> None:
    [result] = await backend.submit([KeyedSpec("k", JobSpec("x", "/bin/true"))])
    assert result.handle is not None
    fake = ce.jobs[result.handle.id.rsplit("/", 1)[1]]
    fake.started = dt.datetime(2026, 1, 1, tzinfo=dt.UTC).timestamp()
    ce.set_state(fake.id, "FINISHED", exit_code=0)
    [info] = await backend.status([result.handle])
    assert info.state is JobState.DONE and info.submitted and info.ended
    await backend.close()


async def test_the_client_is_built_from_discovery(
    ce: FakeARex, pki: PKI, monkeypatch: pytest.MonkeyPatch, home: Path
) -> None:
    for name, value in pki.environment().items():
        monkeypatch.setenv(name, value)
    backend = ARCBackend("ce", ce=ce.url, joblist=str(home / "jobs.dat"), auth="x509")
    [result] = await backend.submit([KeyedSpec("k", JobSpec("x", "/bin/true"))])
    assert result.ok and (home / "jobs.dat").is_file()
    await backend.close()
    unused = ARCBackend("ce", ce=ce.url)
    await unused.close()  # never built a client: nothing to close
    trusting = ARCBackend("ce", ce=ce.url, joblist=False, verify=False)
    assert trusting.client.credentials.verify is False
    await trusting.close()


async def test_a_batch_is_one_post(ce: FakeARex, backend: ARCBackend) -> None:
    specs = [KeyedSpec(f"k{i}-0", JobSpec(f"j{i}", "/bin/true")) for i in range(5)]
    results = await backend.submit(specs)
    assert all(r.ok for r in results) and len(ce.jobs) == 5
    names = {job.name for job in ce.jobs.values()}
    assert names == {f"j{i}~k{i}-0" for i in range(5)}
    posts = [
        r
        for r in ce.requests
        if r.method == "POST" and r.path.endswith("/jobs") and r.query.get("action") == "new"
    ]
    assert len(posts) == 1
    # And each answer belongs to its own spec, in order.
    for keyed, result in zip(specs, results, strict=True):
        job_id = handle_of(result).id.rsplit("/", 1)[1]
        assert ce.jobs[job_id].name.endswith(keyed.key)
    await backend.close()
