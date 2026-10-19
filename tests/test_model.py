"""The job model: validation, conversions, and the JSON round trip the journal relies on."""

from __future__ import annotations

import json
import pickle
from typing import Any

import pytest

from xrdwms import errors
from xrdwms.journal import Record
from xrdwms.model import (
    Failure,
    FailureKind,
    GridRef,
    Input,
    JobHandle,
    JobInfo,
    JobSpec,
    JobState,
    Output,
    Placement,
    Resources,
    Runtime,
    SubmitResult,
    is_url,
)


def full_spec() -> JobSpec:
    return JobSpec(
        name="sim",
        executable="run.sh",
        # Deliberately loose, to pin JobSpec.__post_init__'s coercion (lists, a non-str
        # argument, an input or output as a string, a pair or a mapping): the annotations
        # name only the normalised types, so each such argument is a type error by design.
        arguments=["--seed", 3],  # type: ignore[arg-type]  # coerced: list, int -> tuple[str]
        environment={"A": "1"},
        inputs=["run.sh", ("root://se//f.root", "f.root"), {"source": "data.txt", "name": "d"}],  # type: ignore[arg-type]  # coerced
        outputs=["out.txt", ("big.root", "davs://se/big.root"), {"name": "x"}],  # type: ignore[arg-type]  # coerced
        resources=Resources(cores=4, memory_mb=8000, walltime_s=3600, cputime_s=7200, disk_mb=10),
        runtime=Runtime(environments=("ENV/PROXY",)),
        placement=Placement(queue="main"),
        max_retries=2,
        labels={"campaign": "c1"},
        native={"arc": {"rerun": "1"}},
    )


def test_spec_normalises_its_fields() -> None:
    spec = full_spec()
    assert spec.arguments == ("--seed", "3")
    assert spec.inputs == (
        Input("run.sh", "run.sh"),
        Input("root://se//f.root", "f.root"),
        Input("data.txt", "d"),
    )
    assert spec.outputs[0] == Output("out.txt") and spec.outputs[0].in_sandbox
    assert not spec.outputs[1].in_sandbox
    assert spec.inputs[1].is_url and not spec.inputs[0].is_url


def test_spec_round_trips_through_json() -> None:
    spec = full_spec()
    again = JobSpec.from_dict(json.loads(json.dumps(spec.to_dict())))
    assert again == spec


def test_spec_pickles() -> None:
    spec = full_spec()
    assert pickle.loads(pickle.dumps(spec)) == spec


def test_from_dict_takes_objects_too() -> None:
    data = {"name": "x", "executable": "y", "resources": Resources(cores=2)}
    assert JobSpec.from_dict(data).resources.cores == 2


def test_with_replaces_fields() -> None:
    assert full_spec().with_(name="other").name == "other"


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"name": ""}, "name"),
        ({"name": "a/b"}, "name"),
        ({"executable": ""}, "executable"),
        ({"max_retries": -1}, "max_retries"),
    ],
)
def test_spec_refuses_nonsense(kwargs: dict[str, object], message: str) -> None:
    values: dict[str, Any] = {"name": "x", "executable": "/bin/true", **kwargs}
    with pytest.raises(ValueError, match=message):
        JobSpec(**values)


def test_from_dict_refuses_unknown_fields() -> None:
    with pytest.raises(ValueError, match="Unknown JobSpec fields: colour"):
        JobSpec.from_dict({"name": "x", "executable": "y", "colour": "red"})


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"cores": 0}, "cores"),
        ({"memory_mb": 0}, "memory_mb"),
        ({"walltime_s": -5}, "walltime_s"),
        ({"gpus": -1}, "gpus"),
    ],
)
def test_resources_refuse_nonsense(kwargs: dict[str, int], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        Resources(**kwargs)


def test_input_names_itself_after_its_source() -> None:
    assert Input("https://h/a/b.txt?x=1").name == "b.txt"
    assert Input("dir/file/").name == "file"
    with pytest.raises(ValueError, match="Cannot name"):
        Input("root://host/")


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("root://h//f", True),
        ("davs://h/f", True),
        ("file:///tmp/x", True),
        ("git+ssh://h/x", True),
        ("/tmp/x", False),
        ("relative/x", False),
        ("c:\\x", False),
        ("weird scheme://x", False),
    ],
)
def test_is_url(text: str, expected: bool) -> None:
    assert is_url(text) is expected


def test_states_and_failure_kinds() -> None:
    assert {s for s in JobState if s.terminal} == {
        JobState.DONE,
        JobState.FAILED,
        JobState.CANCELLED,
    }
    assert str(JobState.RUNNING) == "running"
    assert {k for k in FailureKind if k.retryable} == {FailureKind.INFRA, FailureKind.DATA}


def test_handles_parse_and_print() -> None:
    handle = JobHandle("ce", "https://h/arex/rest/1.0/jobs/x")
    assert JobHandle.parse(str(handle)) == handle
    for bad in ("nohandle", ":x", "b:"):
        with pytest.raises(ValueError, match="Not a job handle"):
            JobHandle.parse(bad)


def test_info_round_trips_with_and_without_failure() -> None:
    handle = JobHandle("ce", "j1")
    plain = JobInfo(handle, JobState.RUNNING, "RUNNING", worker_nodes=("n1",))
    failed = JobInfo(handle, JobState.FAILED, failure=Failure(FailureKind.DATA, "lost"))
    for info in (plain, failed):
        assert JobInfo.from_dict(json.loads(json.dumps(info.to_dict()))) == info


def test_small_values() -> None:
    handle = JobHandle("b", "1")
    assert SubmitResult("k", handle=handle).ok and not SubmitResult("k").ok
    assert str(GridRef("root://h//f", 3)) == "root://h//f"


def test_errors_pickle_and_read_well() -> None:
    unsupported = errors.UnsupportedError("arc", "runtime.container", "no")
    assert "cannot express runtime.container: no" in str(unsupported)
    again = pickle.loads(pickle.dumps(unsupported))
    assert again.field == "runtime.container"
    assert str(errors.UnknownJobError("No job 'x'")) == "No job 'x'"
    assert str(errors.UnknownJobError()) == ""
    remote = errors.RemoteError("ValueError: bad", "Traceback...")
    assert "Remote traceback:\nTraceback..." in str(remote)
    assert str(pickle.loads(pickle.dumps(remote))) == str(remote)
    assert str(errors.RemoteError("plain")) == "plain"
    spec = JobSpec("x", "/bin/true")
    record = Record("k", "b", spec, JobState.FAILED)
    failed = errors.JobFailedError(record)
    assert "no reason given" in str(failed)
    info = JobInfo(JobHandle("b", "1"), JobState.FAILED, failure=Failure(FailureKind.INFRA, "boom"))
    failed = errors.JobFailedError(Record("k", "b", spec, JobState.FAILED, info=info))
    assert str(failed) == "Grid job k (x) failed: boom"
    assert pickle.loads(pickle.dumps(failed)).record.key == "k"
