"""Constants and narrowing helpers the test modules share (kept out of conftest.py,
which each test directory has).

The helpers turn the model's optional fields - a record's ``info``, a result's
``failure`` or ``handle`` - into the value a test expects to be there, failing
the test with an ``AssertionError`` when it is not. So a test reads
``failure_of(record).kind`` rather than reaching through two ``None``\\ s.
"""

from __future__ import annotations

from collections.abc import Iterable

from xrdwms import PollPolicy
from xrdwms.journal import Record
from xrdwms.model import Failure, JobHandle, JobInfo, SubmitResult

#: The identity the fake CE sees for the PKI's proxy.
USER_DN = "/DC=org/DC=xrdsec/OU=People/CN=Test User"

#: Polling fast enough for tests: a job moves one step per poll.
FAST = PollPolicy(interval=0.01, max_interval=0.05, batch_delay=0.0, backoff_cap=0.05, lease=5.0)


def info_of(record: Record) -> JobInfo:
    """The record's last backend answer, which the test expects it to have."""
    assert record.info is not None, f"{record.key} has no info"
    return record.info


def failure_of(source: Record | JobInfo | SubmitResult) -> Failure:
    """The failure a record, a status answer or a submission ended with."""
    found = info_of(source).failure if isinstance(source, Record) else source.failure
    assert found is not None, f"no failure in {source!r}"
    return found


def handle_of(source: Record | SubmitResult) -> JobHandle:
    """The backend handle a record or a submission carries."""
    assert source.handle is not None, f"no handle in {source!r}"
    return source.handle


def handles_of(sources: Iterable[Record | SubmitResult]) -> list[JobHandle]:
    """:func:`handle_of` for each."""
    return [handle_of(source) for source in sources]
