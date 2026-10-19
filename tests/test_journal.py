"""The journal: rows, events, filters, persistence and the schema guard."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from _shared import info_of
from xrdwms import JobSpec, Journal
from xrdwms.errors import UnknownJobError, WMSError
from xrdwms.journal import default_path
from xrdwms.model import Failure, FailureKind, JobHandle, JobInfo, JobState

SPEC = JobSpec("hello", "/bin/echo", ("hi",))


def test_add_get_update_reset_remove() -> None:
    with Journal() as journal:
        record = journal.add("k1", "ce", SPEC, group="g")
        assert (record.state, record.handle, record.group, record.attempt) == (
            JobState.NEW,
            None,
            "g",
            0,
        )
        assert record.attempt_key == "k1-0" and not record.terminal
        handle = JobHandle("ce", "job-1")
        info = JobInfo(handle, JobState.RUNNING, "RUNNING")
        record = journal.update("k1", state=JobState.RUNNING, handle=handle, info=info)
        assert record.handle == handle and record.info == info
        # Nothing given: only the time changes, nothing is logged.
        assert journal.update("k1").state is JobState.RUNNING
        record = journal.reset("k1", attempt=1, message="retry")
        assert (record.state, record.handle, record.info, record.attempt_key) == (
            JobState.NEW,
            None,
            None,
            "k1-1",
        )
        record = journal.update("k1", submit_tries=2, attempt=3)
        assert (record.submit_tries, record.attempt) == (2, 3)
        states = [e.state for e in journal.events("k1")]
        assert states == [JobState.NEW, JobState.RUNNING, JobState.NEW]
        assert journal.remove(["k1"]) == 1
        assert journal.find("k1") is None and journal.events("k1") == []


def test_same_state_is_logged_only_with_a_message() -> None:
    with Journal() as journal:
        journal.add("k", "ce", SPEC)
        journal.update("k", state=JobState.NEW)
        journal.update("k", state=JobState.NEW, message="again")
        assert [e.message for e in journal.events("k")] == ["added", "again"]


def test_unknown_keys_and_duplicates() -> None:
    with Journal() as journal:
        journal.add("k", "ce", SPEC)
        with pytest.raises(WMSError, match="already in the journal"):
            journal.add("k", "ce", SPEC)
        with pytest.raises(UnknownJobError):
            journal.get("nope")
        with pytest.raises(UnknownJobError):
            journal.update("nope", state=JobState.DONE)
        with pytest.raises(UnknownJobError):
            journal.reset("nope", attempt=1, message="")


def test_filters() -> None:
    with Journal() as journal:
        journal.add("a", "ce", SPEC, group="g1")
        journal.add("b", "ce", SPEC, group="g2")
        journal.add("c", "other", SPEC, group="g1")
        failure = Failure(FailureKind.PAYLOAD, "x")
        journal.update(
            "b",
            state=JobState.FAILED,
            info=JobInfo(JobHandle("ce", "1"), JobState.FAILED, failure=failure),
        )
        assert [r.key for r in journal.records()] == ["a", "b", "c"]
        assert [r.key for r in journal.records(backend="ce")] == ["a", "b"]
        assert [r.key for r in journal.records(group="g1")] == ["a", "c"]
        assert [r.key for r in journal.records(active=True)] == ["a", "c"]
        assert [r.key for r in journal.records(active=False)] == ["b"]
        assert info_of(journal.get("b")).failure == failure


def test_persists_and_rolls_back(tmp_path: Path) -> None:
    path = tmp_path / "sub" / "j.sqlite"
    with Journal(path) as journal:
        journal.add("k", "ce", SPEC)
        with pytest.raises(RuntimeError), journal._transaction() as db:
            db.execute("UPDATE jobs SET state = 'done'")
            raise RuntimeError("abandon")
    with Journal(path) as journal:
        assert journal.get("k").state is JobState.NEW


def test_refuses_a_newer_schema(tmp_path: Path) -> None:
    path = tmp_path / "j.sqlite"
    db = sqlite3.connect(path)
    db.execute("PRAGMA user_version=99")
    db.close()
    with pytest.raises(WMSError, match="schema 99"):
        Journal(path)


def test_default_path(home: Path) -> None:
    assert default_path({"XRDWMS_JOURNAL": "/x/j"}) == Path("/x/j")
    assert default_path({"XDG_STATE_HOME": "/s"}) == Path("/s/xrdwms/journal.sqlite")
    assert default_path({}) == home / ".local/state/xrdwms/journal.sqlite"
    assert default_path() == home / ".local/state/xrdwms/journal.sqlite"
