"""A scriptable fake backend, for testing code built on xrdwms without a grid.

:class:`FakeBackend` keeps its jobs in memory and moves each one along a
*timeline* - one step per ``status`` call - so a test says exactly what the
grid does::

    fake = FakeBackend(steps=("queued", "running", "done"))
    fake.script("flaky", "queued", "failed:infra", "done")   # jobs whose spec is named "flaky"
    fake.refuse("bad", "payload", "no such queue")            # submission refused
    fake.fail("status", OSError("CE down"), times=2)          # the next two status calls raise

A step is a state name (``queued``, ``running``, ``held``, ``done``,
``cancelled``), ``failed:<kind>`` (a :class:`~xrdwms.model.FailureKind`
value), ``transient`` (no news), ``gone`` (the backend does not know the
job), or ``exit:<code>`` (``done`` for 0, a payload failure otherwise).
The last step repeats. Every call is recorded in :attr:`FakeBackend.calls`.
"""

from __future__ import annotations

import itertools
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

from ..model import (
    Capabilities,
    Failure,
    FailureKind,
    JobHandle,
    JobInfo,
    JobSpec,
    JobState,
    KeyedSpec,
    OpResult,
    Retrieved,
    SubmitResult,
)

__all__ = ["FakeBackend", "FakeJob"]


@dataclass
class FakeJob:
    """One job in the fake: its key, spec, timeline and where it is on it."""

    #: The fake's own id for it (``fake-<n>``), the handle's ``id``.
    id: str
    #: The idempotency key it was submitted under, what ``find`` matches.
    key: str
    spec: JobSpec
    #: The states it walks through, one per status poll; the last one repeats.
    steps: tuple[str, ...]
    #: How far along ``steps`` it is.
    position: int = 0
    cancelled: bool = False
    #: How many times its credential was renewed, for tests of the renewal path.
    renewed: int = 0
    #: Whether ``clean`` removed it; a cleaned job is "gone" to ``status``.
    cleaned: bool = False
    files: dict[str, bytes] = field(default_factory=dict)


class FakeBackend:
    """An in-memory backend driven by timelines; see the module docstring."""

    kind: ClassVar[str] = "fake"

    def __init__(
        self,
        name: str = "fake",
        *,
        steps: Sequence[str] = ("queued", "running", "done"),
        capabilities: Capabilities | None = None,
    ) -> None:
        """``steps`` is the default timeline every job walks, one step per status poll."""
        self.name = name
        self.steps = tuple(steps)
        self.capabilities = capabilities or Capabilities(find=True, proxy_renewal=True)
        self.jobs: dict[str, FakeJob] = {}
        self.calls: list[tuple[str, int]] = []
        self.closed = False
        self._timelines: dict[str, tuple[str, ...]] = {}
        self._refusals: dict[str, Failure] = {}
        self._faults: dict[str, list[BaseException]] = defaultdict(list)
        self._ids = itertools.count(1)

    # -- scripting -----------------------------------------------------------------------

    def script(self, spec_name: str, *steps: str) -> None:
        """Jobs whose spec is called ``spec_name`` follow ``steps`` instead of the default."""
        self._timelines[spec_name] = steps

    def refuse(self, spec_name: str, kind: str = "payload", message: str = "refused") -> None:
        """Refuse to submit specs called ``spec_name``, with a failure of ``kind``."""
        self._refusals[spec_name] = Failure(FailureKind(kind), message)

    def fail(self, method: str, error: BaseException, times: int = 1) -> None:
        """The next ``times`` calls of ``method`` raise ``error``."""
        self._faults[method].extend([error] * times)

    def _call(self, method: str, count: int) -> None:
        """Record a call (method, batch size) and raise the next scripted fault, if any."""
        self.calls.append((method, count))
        if self._faults[method]:
            raise self._faults[method].pop(0)

    def job(self, handle: JobHandle | str) -> FakeJob:
        """The fake job behind ``handle`` (or its id)."""
        return self.jobs[handle.id if isinstance(handle, JobHandle) else handle]

    # -- the backend ---------------------------------------------------------------------

    async def submit(self, specs: Sequence[KeyedSpec]) -> list[SubmitResult]:
        """Accept each spec (unless refused), with a fresh id ``fake-<n>``."""
        self._call("submit", len(specs))
        results = []
        for keyed in specs:
            refusal = self._refusals.get(keyed.spec.name)
            if refusal is not None:
                results.append(SubmitResult(keyed.key, failure=refusal))
                continue
            job_id = f"fake-{next(self._ids)}"
            steps = self._timelines.get(keyed.spec.name, self.steps)
            self.jobs[job_id] = FakeJob(job_id, keyed.key, keyed.spec, steps)
            results.append(SubmitResult(keyed.key, handle=JobHandle(self.name, job_id)))
        return results

    async def status(self, handles: Sequence[JobHandle]) -> list[JobInfo]:
        """Each job's current step, then a step along its timeline."""
        self._call("status", len(handles))
        return [self._advance(handle) for handle in handles]

    def _advance(self, handle: JobHandle) -> JobInfo:
        """The job's state now, then one step along its timeline.

        A cleaned or unknown job is gone and a cancelled one stays cancelled;
        the last step repeats once the timeline is used up.
        """
        job = self.jobs.get(handle.id)
        if job is None or job.cleaned:
            return JobInfo(handle, JobState.UNKNOWN, native_state="gone")
        if job.cancelled:
            return JobInfo(handle, JobState.CANCELLED, native_state="cancelled")
        step = job.steps[min(job.position, len(job.steps) - 1)]
        job.position += 1
        return _info(handle, step)

    async def cancel(self, handles: Sequence[JobHandle]) -> list[OpResult]:
        """Mark jobs cancelled; the next status says so."""
        self._call("cancel", len(handles))
        results = []
        for handle in handles:
            job = self.jobs.get(handle.id)
            if job is not None:
                job.cancelled = True
            results.append(OpResult(handle, job is not None, "" if job else "No such job"))
        return results

    async def retrieve(self, handle: JobHandle, directory: str) -> Retrieved:
        """Write the job's ``files`` (and an empty ``stdout.txt``) into ``directory/<id>``."""
        self._call("retrieve", 1)
        job = self.jobs.get(handle.id)
        if job is None:
            raise FileNotFoundError(f"No such job: {handle}")
        target = Path(directory) / job.id
        target.mkdir(parents=True, exist_ok=True)
        files = {"stdout.txt": b"", **job.files}
        for name, data in files.items():
            (target / name).write_bytes(data)
        return Retrieved(handle, str(target), tuple(sorted(files)))

    async def clean(self, handles: Sequence[JobHandle]) -> list[OpResult]:
        """Forget jobs; a later status calls them gone."""
        self._call("clean", len(handles))
        results = []
        for handle in handles:
            job = self.jobs.get(handle.id)
            if job is not None:
                job.cleaned = True
            results.append(OpResult(handle, job is not None))
        return results

    async def renew(self, handles: Sequence[JobHandle]) -> list[OpResult]:
        """Count a renewal on each job."""
        self._call("renew", len(handles))
        results = []
        for handle in handles:
            job = self.jobs.get(handle.id)
            if job is not None:
                job.renewed += 1
            results.append(OpResult(handle, job is not None))
        return results

    async def find(self, keys: Sequence[str]) -> dict[str, JobHandle]:
        """The jobs submitted with these keys."""
        self._call("find", len(keys))
        wanted = set(keys)
        return {
            job.key: JobHandle(self.name, job.id) for job in self.jobs.values() if job.key in wanted
        }

    async def close(self) -> None:
        """Note that the backend was closed."""
        self.closed = True


def _info(handle: JobHandle, step: str) -> JobInfo:
    """A timeline step as a status answer.

    The step vocabulary (what :meth:`FakeBackend.script` takes):

    * a :class:`~xrdwms.model.JobState` value - ``queued``, ``running``,
      ``held``, ``done``... - answered as that state;
    * ``transient`` - no news this time (the backend "could not ask"), which
      must never change the job;
    * ``gone`` - the backend does not know the job, which only fails it after
      ``PollPolicy.lost_after`` such answers in a row;
    * ``failed`` or ``failed:<kind>`` - a failure of that
      :class:`~xrdwms.model.FailureKind` (default ``infra``, retryable);
    * ``exit:<code>`` - the payload ended: ``DONE`` for 0, a ``PAYLOAD``
      failure (never retried) otherwise.
    """
    word, _, detail = step.partition(":")
    if word == "transient":
        return JobInfo(handle, JobState.UNKNOWN, native_state="transient", transient=True)
    if word == "gone":
        return JobInfo(handle, JobState.UNKNOWN, native_state="gone")
    if word == "failed":
        kind = FailureKind(detail or "infra")
        failure = Failure(kind, f"fake {kind.value} failure")
        return JobInfo(handle, JobState.FAILED, native_state=step, failure=failure)
    if word == "exit":
        code = int(detail)
        if code == 0:
            return JobInfo(handle, JobState.DONE, native_state=step, exit_code=0)
        failure = Failure(FailureKind.PAYLOAD, f"exit code {code}")
        return JobInfo(handle, JobState.FAILED, native_state=step, exit_code=code, failure=failure)
    return JobInfo(handle, JobState(word), native_state=step)
