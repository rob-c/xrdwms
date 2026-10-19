"""The job model every backend shares: what to run, where it is, how it is doing.

A :class:`JobSpec` says what a job needs - an executable, its inputs and
outputs, resources, a runtime - in terms no backend owns; each backend
translates it to its own language (xRSL for ARC) and raises
:class:`~xrdwms.errors.UnsupportedError` for anything it cannot express,
rather than dropping it. ``native`` is the escape hatch: per-backend
attributes passed through untouched.

Every value here is frozen and made of plain types, so it pickles to dask
workers and round-trips through the journal's JSON (``to_dict``/``from_dict``).

Units: memory and disk in MB for the **whole job** (a backend that counts per
core divides), times in seconds.
"""

from __future__ import annotations

import enum
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields, replace
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlsplit

__all__ = [
    "SANDBOX",
    "Capabilities",
    "Failure",
    "FailureKind",
    "GridRef",
    "Input",
    "JobHandle",
    "JobInfo",
    "JobSpec",
    "JobState",
    "KeyedSpec",
    "OpResult",
    "Output",
    "Placement",
    "Resources",
    "Retrieved",
    "Runtime",
    "SubmitResult",
]

#: The ``Output.destination`` that means "keep it with the job, retrieve it later".
SANDBOX = "sandbox"


class JobState(enum.Enum):
    """Where a job is in its life, the same for every backend.

    The backend's own word for it is kept alongside (``JobInfo.native_state``).
    """

    #: In the journal, not yet sent: the manager's task will submit it.
    NEW = "new"
    #: Its large local inputs are being uploaded to the staging area (or, for
    #: ARC, A-REX is fetching its inputs) before it can be queued.
    STAGING_IN = "staging-in"
    #: The submission request is in flight. A job found in this state after a
    #: crash may or may not have landed: the backend's ``find`` decides.
    SUBMITTING = "submitting"
    #: Accepted by the backend (it has a handle), waiting for a slot.
    QUEUED = "queued"
    RUNNING = "running"
    #: Suspended by the batch system or an administrator; it may resume.
    HELD = "held"
    #: The payload has ended and the backend is wrapping up (ARC's FINISHING).
    FINISHING = "finishing"
    #: Its outputs are being uploaded to their destinations.
    STAGING_OUT = "staging-out"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"
    #: The backend does not know the job (yet, or any more); after
    #: ``PollPolicy.lost_after`` such answers in a row the manager fails it.
    UNKNOWN = "unknown"

    @property
    def terminal(self) -> bool:
        """The job will not change again."""
        return self in _TERMINAL

    def __str__(self) -> str:
        """The value (``running``), so f-strings read naturally."""
        return self.value


_TERMINAL = frozenset({JobState.DONE, JobState.FAILED, JobState.CANCELLED})


class FailureKind(enum.Enum):
    """Why a job failed, which decides whether running it again could help."""

    #: The job's own program failed (non-zero exit, a Python exception).
    PAYLOAD = "payload"
    #: The grid failed it: a node, the batch system, the CE, a lost job.
    INFRA = "infra"
    #: Credentials: none, expired, refused.
    AUTH = "auth"
    #: A resource limit: wall time, memory, quota.
    LIMIT = "limit"
    #: Inputs could not be fetched or outputs were missing or corrupt.
    DATA = "data"

    @property
    def retryable(self) -> bool:
        """Resubmitting the same job could succeed: the fault was not the job's own."""
        return self in (FailureKind.INFRA, FailureKind.DATA)


@dataclass(frozen=True, slots=True)
class Failure:
    """A failure's kind and the backend's (or xrdwms's) words for it."""

    kind: FailureKind
    message: str


@dataclass(frozen=True, slots=True)
class Resources:
    """What a job needs: cores, memory and disk in MB for the whole job, times in seconds."""

    #: Cores (ARC's ``count``); memory and disk below are for all of them together.
    cores: int = 1
    #: Memory for the whole job; ``None`` takes the backend's default.
    memory_mb: int | None = None
    #: The wall-clock limit; the batch system kills the job past it (a ``LIMIT`` failure).
    walltime_s: int | None = None
    #: The CPU-time limit, summed over cores.
    cputime_s: int | None = None
    #: Scratch disk for the job's directory.
    disk_mb: int | None = None
    #: GPUs; not every backend can ask for them (ARC's xRSL cannot, use ``native``).
    gpus: int = 0

    def __post_init__(self) -> None:
        """Refuse what no backend could mean: no cores, negative amounts."""
        if self.cores < 1:
            raise ValueError("Resources.cores must be at least 1")
        for name in ("memory_mb", "walltime_s", "cputime_s", "disk_mb"):
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise ValueError(f"Resources.{name} must be positive")
        if self.gpus < 0:
            raise ValueError("Resources.gpus cannot be negative")


@dataclass(frozen=True, slots=True)
class Input:
    """A file the job needs: a local path or a URL, and its name in the job's directory."""

    source: str
    name: str = ""

    def __post_init__(self) -> None:
        """Default the name to the source's last path component."""
        if not self.name:
            path = urlsplit(self.source).path if is_url(self.source) else self.source
            leaf = PurePosixPath(path.rstrip("/")).name
            if not leaf:
                raise ValueError(f"Cannot name the input {self.source!r}; give Input(name=...)")
            object.__setattr__(self, "name", leaf)

    @property
    def is_url(self) -> bool:
        """The source is a URL (``scheme://``), not a local path."""
        return is_url(self.source)


@dataclass(frozen=True, slots=True)
class Output:
    """A file the job writes: kept in the sandbox, or sent to a URL by the backend."""

    name: str
    destination: str = SANDBOX

    @property
    def in_sandbox(self) -> bool:
        """Retrieved with the job, rather than uploaded somewhere by it."""
        return self.destination == SANDBOX


@dataclass(frozen=True, slots=True)
class Runtime:
    """The software a job runs in: runtime environments, a container image, a platform."""

    environments: tuple[str, ...] = ()
    container: str | None = None
    platform: str | None = None


@dataclass(frozen=True, slots=True)
class Placement:
    """Where a job may run."""

    queue: str | None = None
    sites: tuple[str, ...] = ()
    banned_sites: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class JobSpec:
    """One job, in no backend's language; see the module docstring."""

    name: str
    executable: str
    arguments: tuple[str, ...] = ()
    environment: Mapping[str, str] = field(default_factory=dict)
    inputs: tuple[Input, ...] = ()
    outputs: tuple[Output, ...] = ()
    resources: Resources = field(default_factory=Resources)
    runtime: Runtime = field(default_factory=Runtime)
    placement: Placement = field(default_factory=Placement)
    #: Resubmissions allowed after a retryable failure (``FailureKind.retryable``).
    max_retries: int = 0
    labels: Mapping[str, str] = field(default_factory=dict)
    #: Per-backend attributes, by backend kind: ``{"arc": {"rerun": "2"}}``.
    native: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Accept lists where tuples are meant and a plain string as one input or output."""
        if not self.name or "/" in self.name:
            raise ValueError(f"JobSpec.name must be a non-empty name without '/': {self.name!r}")
        if not self.executable:
            raise ValueError("JobSpec.executable cannot be empty")
        if self.max_retries < 0:
            raise ValueError("JobSpec.max_retries cannot be negative")
        object.__setattr__(self, "arguments", tuple(str(a) for a in self.arguments))
        object.__setattr__(self, "inputs", tuple(_as_input(i) for i in self.inputs))
        object.__setattr__(self, "outputs", tuple(_as_output(o) for o in self.outputs))
        object.__setattr__(self, "environment", dict(self.environment))
        object.__setattr__(self, "labels", dict(self.labels))
        object.__setattr__(self, "native", {k: dict(v) for k, v in self.native.items()})

    def with_(self, **changes: Any) -> JobSpec:
        """A copy with ``changes`` applied (``dataclasses.replace``)."""
        return replace(self, **changes)

    def to_dict(self) -> dict[str, Any]:
        """Plain JSON-able data; :meth:`from_dict` reads it back.

        Written out by hand rather than with ``dataclasses.asdict``, which
        deep-copies and is the slowest part of journalling thousands of jobs.
        """
        return {
            "name": self.name,
            "executable": self.executable,
            "arguments": list(self.arguments),
            "environment": dict(self.environment),
            "inputs": [{"source": i.source, "name": i.name} for i in self.inputs],
            "outputs": [{"name": o.name, "destination": o.destination} for o in self.outputs],
            "resources": _fields(self.resources),
            "runtime": _fields(self.runtime),
            "placement": _fields(self.placement),
            "max_retries": self.max_retries,
            "labels": dict(self.labels),
            "native": {kind: dict(values) for kind, values in self.native.items()},
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> JobSpec:
        """A spec from :meth:`to_dict`'s data (or a hand-written job file's)."""
        known = {f.name for f in fields(cls)}
        unknown = sorted(set(data) - known)
        if unknown:
            raise ValueError(f"Unknown JobSpec fields: {', '.join(unknown)}")
        values = dict(data)
        values["inputs"] = tuple(_as_input(i) for i in values.get("inputs", ()))
        values["outputs"] = tuple(_as_output(o) for o in values.get("outputs", ()))
        values["arguments"] = tuple(values.get("arguments", ()))
        for name, kind in (
            ("resources", Resources),
            ("runtime", Runtime),
            ("placement", Placement),
        ):
            part = values.get(name)
            if isinstance(part, Mapping):
                values[name] = kind(**_tuples(part))
        return cls(**values)


def _fields(part: Any) -> dict[str, Any]:
    """A flat dataclass's fields as JSON-able values (tuples as lists)."""
    return {
        f.name: list(value) if isinstance(value := getattr(part, f.name), tuple) else value
        for f in fields(part)
    }


def _tuples(part: Mapping[str, Any]) -> dict[str, Any]:
    """JSON gives lists where the dataclasses keep tuples."""
    return {k: tuple(v) if isinstance(v, list) else v for k, v in part.items()}


def _as_input(value: Input | str | Mapping[str, Any] | Sequence[str]) -> Input:
    """An ``Input`` from itself, a source string, a mapping, or ``(source, name)``."""
    if isinstance(value, Input):
        return value
    if isinstance(value, str):
        return Input(value)
    if isinstance(value, Mapping):
        return Input(**value)
    return Input(*value)


def _as_output(value: Output | str | Mapping[str, Any] | Sequence[str]) -> Output:
    """An ``Output`` from itself, a name, a mapping, or ``(name, destination)``."""
    if isinstance(value, Output):
        return value
    if isinstance(value, str):
        return Output(value)
    if isinstance(value, Mapping):
        return Output(**value)
    return Output(*value)


def is_url(text: str) -> bool:
    """``scheme://...`` (``file://`` included), as opposed to a local path."""
    scheme, sep, _rest = text.partition("://")
    return bool(sep) and scheme.replace("+", "").replace("-", "").replace(".", "").isalnum()


@dataclass(frozen=True, slots=True)
class KeyedSpec:
    """A spec and the idempotency key a backend must attach to the job it makes of it."""

    key: str
    spec: JobSpec


@dataclass(frozen=True, slots=True)
class JobHandle:
    """A job on a backend: the backend's configured name and the job's native id.

    ``str()`` gives ``<backend>:<id>`` (``gridlab:https://ce/arex/rest/1.0/jobs/ab12``),
    which :meth:`parse` reads back.
    """

    backend: str
    id: str

    def __str__(self) -> str:
        """``backend:id``."""
        return f"{self.backend}:{self.id}"

    @classmethod
    def parse(cls, text: str) -> JobHandle:
        """A handle from its ``str()``; ``ValueError`` without a backend name."""
        backend, sep, native = text.partition(":")
        if not sep or not backend or not native:
            raise ValueError(f"Not a job handle (backend:id): {text!r}")
        return cls(backend, native)


@dataclass(frozen=True, slots=True)
class JobInfo:
    """What a backend last said about a job.

    ``transient`` marks "no news": the backend could not ask this time (the
    CE was unreachable). Such an answer never changes a job's state.
    """

    handle: JobHandle
    state: JobState
    #: The backend's own word for the state (A-REX's ``Running``, ``Finishing``...):
    #: more precise than :class:`JobState`, and a change in it alone is still news.
    native_state: str | None = None
    #: The payload's exit code, once it has ended and the backend reports one.
    exit_code: int | None = None
    #: Why the job failed (only for ``FAILED`` and ``CANCELLED``).
    failure: Failure | None = None
    #: Where it ran: the CE's host name for ARC, ``localhost`` for the local backend.
    site: str | None = None
    worker_nodes: tuple[str, ...] = ()
    #: When the backend accepted, started and ended the job (epoch seconds), as far
    #: as it says; the journal keeps them for ``xrdwms status``.
    submitted: float | None = None
    started: float | None = None
    ended: float | None = None
    transient: bool = False
    #: When this answer was made (here, not on the backend).
    updated: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        """Plain JSON-able data; :meth:`from_dict` reads it back."""
        failure = self.failure
        return {
            "handle": str(self.handle),
            "state": self.state.value,
            "native_state": self.native_state,
            "exit_code": self.exit_code,
            "failure": {"kind": failure.kind.value, "message": failure.message}
            if failure
            else None,
            "site": self.site,
            "worker_nodes": list(self.worker_nodes),
            "submitted": self.submitted,
            "started": self.started,
            "ended": self.ended,
            "transient": self.transient,
            "updated": self.updated,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> JobInfo:
        """The inverse of :meth:`to_dict`."""
        values = dict(data)
        values["handle"] = JobHandle.parse(values["handle"])
        values["state"] = JobState(values["state"])
        values["worker_nodes"] = tuple(values.get("worker_nodes", ()))
        failure = values.get("failure")
        if failure is not None:
            values["failure"] = Failure(FailureKind(failure["kind"]), failure["message"])
        return cls(**values)


@dataclass(frozen=True, slots=True)
class SubmitResult:
    """One keyed spec's fate: a handle, or why the backend did not take it."""

    #: The ``KeyedSpec.key`` it answers, so a bulk answer can be matched up.
    key: str
    #: The new job's handle, when the backend accepted it.
    handle: JobHandle | None = None
    #: Why not, when it did not.
    failure: Failure | None = None

    @property
    def ok(self) -> bool:
        """The backend accepted the job."""
        return self.handle is not None


@dataclass(frozen=True, slots=True)
class OpResult:
    """What a bulk operation (cancel, clean, renew) did to one job."""

    handle: JobHandle
    ok: bool
    reason: str = ""


@dataclass(frozen=True, slots=True)
class Retrieved:
    """A job's sandbox, downloaded: the directory and the files in it (relative paths)."""

    handle: JobHandle
    path: str
    files: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Capabilities:
    """What a backend can do, so callers ask for features rather than backend names."""

    #: Largest batch one ``submit``/``status`` call should carry.
    max_bulk: int = 100
    #: Inputs above this many bytes are better staged to storage than sent with the job.
    sandbox_limit: int = 100 * 1024 * 1024
    #: The backend fetches URL inputs and uploads URL outputs itself.
    data_staging: bool = False
    #: ``renew`` refreshes the credential running jobs hold.
    proxy_renewal: bool = False
    x509_auth: bool = False
    token_auth: bool = False
    #: ``find`` can look jobs up by idempotency key (crash recovery).
    find: bool = False


@dataclass(frozen=True, slots=True)
class GridRef:
    """A grid job's result that lives in storage: open it with fsspec or xrdclient.

    Return one from a grid task instead of a large value; downstream tasks
    receive it and read ``url`` themselves.
    """

    url: str
    size: int | None = None
    checksum: str | None = None

    def __str__(self) -> str:
        """The URL."""
        return self.url
