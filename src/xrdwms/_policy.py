"""The job manager's policy, and the decisions it drives, as plain functions.

:class:`~xrdwms.manager.JobManager` is a loop of requests and journal
writes; what it *decides* on each answer - is this news, is this failure
worth another attempt, how long until the next poll, which inputs are too
big for the sandbox - depends on nothing but the answer, the record and
:class:`PollPolicy`. Those decisions live here, out of the class, so each
can be read in one place and tested without an event loop or a backend.
Everything here is re-exported by :mod:`xrdwms.manager`, which is where the
public names (:class:`PollPolicy`, :func:`failure_for`, :func:`new_key`,
:data:`UNSUBMITTED`) are documented to live.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass, replace
from pathlib import Path

from xrdcore.errors import AuthenticationError

from .journal import Record
from .model import Failure, FailureKind, Input, JobHandle, JobInfo, JobSpec, JobState, OpResult

__all__ = [
    "UNSUBMITTED",
    "PollPolicy",
    "clean_unasked",
    "failure_for",
    "first_poll",
    "handle_of",
    "is_news",
    "large_inputs",
    "lost_info",
    "new_key",
    "next_interval",
    "retryable_failure",
    "staged_spec",
    "unsubmitted_failure",
    "wake_by",
]

#: The handle recorded for a job that never reached its backend.
UNSUBMITTED = "unsubmitted"


@dataclass(frozen=True, slots=True)
class PollPolicy:
    """How often to ask, how long to back off, and when to give up."""

    #: Seconds before the first status poll of new jobs, and after any change.
    interval: float = 30.0
    #: The longest pause between polls while nothing changes.
    max_interval: float = 300.0
    #: How much the pause grows after a poll with no change.
    factor: float = 1.5
    #: The longest extra pause after failed requests (xrdcore.retry's cap).
    backoff_cap: float = 600.0
    #: Seconds to gather a burst of submissions into one request.
    batch_delay: float = 0.5
    #: Consecutive "no such job" answers before a job is failed as lost.
    lost_after: int = 3
    #: Attempts at a submission the backend keeps refusing for retryable reasons.
    max_submit_tries: int = 5
    #: Warn when the credential has less than this many seconds left.
    expiry_warning: float = 3600.0
    #: Seconds a manager's hold on its jobs lasts unrenewed; after that (its manager having
    #: died) another manager on the same journal takes them over. Renewed every quarter.
    lease: float = 120.0


def new_key() -> str:
    """A fresh job key: 16 hex digits, unique enough for one user's jobs."""
    return uuid.uuid4().hex[:16]


def failure_for(exc: BaseException) -> Failure:
    """The failure an exception from a backend means: credentials, a bad spec, or the grid.

    The kind decides what happens next: ``AUTH`` waits for a new credential,
    ``PAYLOAD`` (a spec the backend cannot express) is never retried, and
    ``INFRA`` - anything else that went wrong on the way - is.
    """
    if isinstance(exc, AuthenticationError):
        return Failure(FailureKind.AUTH, str(exc))
    if isinstance(exc, ValueError):
        return Failure(FailureKind.PAYLOAD, str(exc))
    return Failure(FailureKind.INFRA, f"{type(exc).__name__}: {exc}")


def handle_of(record: Record) -> JobHandle:
    """The job's backend handle, or the :data:`UNSUBMITTED` placeholder for one never sent.

    Results of bulk operations name each job by handle; a job that never
    reached its backend still needs one to be reported against.
    """
    return record.handle or JobHandle(record.backend, UNSUBMITTED)


def next_interval(policy: PollPolicy, interval: float, changed: bool) -> float:
    """The pause before the next poll: back to the base on any change, else grown, capped.

    A quiet backend is asked less and less often; the first change brings
    the polling back to its base rate, because changes come in bursts.
    """
    if changed:
        return policy.interval
    return min(interval * policy.factor, policy.max_interval)


def lost_info(info: JobInfo) -> JobInfo:
    """``info`` turned into the failure of a job its backend has forgotten.

    A job the backend no longer knows (after ``PollPolicy.lost_after`` such
    answers in a row) is failed as ``INFRA``, so it is retried like any job
    lost to the grid.
    """
    failure = Failure(FailureKind.INFRA, "The backend no longer knows of the job")
    return replace(info, state=JobState.FAILED, failure=failure)


def is_news(current: Record, info: JobInfo) -> bool:
    """Whether a status answer changes the job enough to be written to the journal.

    Only a new state, or a new native state of the backend's (ARC's
    ``Running`` to ``Finishing``, say), is news; a repeat answer is kept in
    memory only, so a quiet poll of a thousand jobs costs no write.
    """
    same_native = current.info is None or info.native_state == current.info.native_state
    return not (info.state is current.state and same_native)


def retryable_failure(current: Record, info: JobInfo) -> Failure | None:
    """The failure to retry the job for, or ``None`` when its end is final.

    A job is run again only when it failed for a retryable reason
    (``FailureKind.retryable``) and its spec allows another attempt.
    """
    failure = info.failure
    if info.state is not JobState.FAILED or failure is None or not failure.kind.retryable:
        return None
    return failure if current.attempt < current.spec.max_retries else None


def large_inputs(spec: JobSpec, base: Path, limit: int) -> list[Input]:
    """The local inputs of ``spec`` (relative to ``base``) larger than the sandbox allows.

    These are uploaded to the staging area before submission and replaced
    by their URL, because the backend would refuse (or choke on) a sandbox
    that big. URL inputs, and local files that do not exist (the backend
    reports those itself), are left alone.
    """
    return [
        item
        for item in spec.inputs
        if not item.is_url
        and (base / item.source).is_file()
        and (base / item.source).stat().st_size > limit
    ]


def staged_spec(spec: JobSpec, replaced: dict[str, Input]) -> JobSpec:
    """``spec`` with its staged inputs swapped for their URLs, in the original order."""
    return spec.with_(inputs=tuple(replaced.get(item.name, item) for item in spec.inputs))


def clean_unasked(record: Record) -> OpResult | None:
    """The answer to cleaning a job no backend need be asked about, else ``None``.

    A job still going is refused (cancel it first); one never sent has
    nothing on any backend to clean.
    """
    if not record.terminal:
        why = f"The job is still {record.state}; cancel it first"
        return OpResult(handle_of(record), False, why)
    if record.handle is None:
        return OpResult(handle_of(record), True, "never submitted")
    return None


def unsubmitted_failure(name: str, failure: Failure) -> JobInfo:
    """The info of a job whose submission failed for good: it never got a handle."""
    return JobInfo(JobHandle(name, UNSUBMITTED), JobState.FAILED, failure=failure)


def first_poll(due: float | None, now: float, policy: PollPolicy) -> float:
    """When new jobs are first looked at: one base interval after they went in.

    An earlier poll already scheduled stands, so new jobs never delay the
    look at older ones.
    """
    first = now + policy.interval
    return first if due is None else min(due, first)


def wake_by(times: Sequence[float | None]) -> float | None:
    """The earliest of the given moments (``None`` ones skipped), or ``None`` if there are none."""
    present = [moment for moment in times if moment is not None]
    return min(present) if present else None
