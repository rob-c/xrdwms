"""Managers sharing a journal: leases, takeovers, foreign waits and cancellations, migration."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from pathlib import Path

import pytest

from _shared import FAST
from xrdwms import JobManager, JobSpec, Journal
from xrdwms import journal as journal_module
from xrdwms.model import JobState
from xrdwms.testing import FakeBackend

SPEC = JobSpec("hello", "/bin/echo")
FAR = 9e12  # a lease that never runs out


def test_a_version_1_journal_is_migrated(tmp_path: Path) -> None:
    path = tmp_path / "v1.sqlite"
    db = sqlite3.connect(path)
    db.executescript(
        """
        CREATE TABLE jobs (key TEXT PRIMARY KEY, backend TEXT NOT NULL, grp TEXT,
            spec TEXT NOT NULL, state TEXT NOT NULL, handle TEXT,
            attempt INTEGER NOT NULL DEFAULT 0, submit_tries INTEGER NOT NULL DEFAULT 0,
            info TEXT, created REAL NOT NULL, updated REAL NOT NULL);
        CREATE TABLE events (seq INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT NOT NULL,
            time REAL NOT NULL, state TEXT NOT NULL, message TEXT NOT NULL DEFAULT '');
        INSERT INTO jobs VALUES ('old', 'ce', NULL, '{"name": "x", "executable": "y"}', 'running',
            NULL, 0, 0, NULL, 1.0, 1.0);
        PRAGMA user_version=1;
        """
    )
    db.close()
    with Journal(path) as journal:
        record = journal.get("old")
        assert record.state is JobState.RUNNING and record.owner is None
        assert [r.key for r in journal.claim("ce", "me", FAR)] == ["old"]


def test_the_spec_cache_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(journal_module, "_SPEC_CACHE_LIMIT", 1)
    with Journal() as journal:
        journal.add("a", "ce", SPEC)
        journal.add("b", "ce", SPEC.with_(name="other"))
        assert {r.spec.name for r in journal.records()} == {"hello", "other"}
        assert len(journal._specs) == 1


def test_journal_ownership_primitives() -> None:
    with Journal() as journal:
        journal.renew("alive", FAR)
        journal.add_many([("a", "ce", SPEC, None)], owner="alive")
        journal.add_many([("d", "ce", SPEC, None)], owner="dead")  # never renewed: no lease
        journal.add("free", "ce", SPEC)
        assert sorted(r.key for r in journal.claim("ce", "me", FAR)) == ["d", "free"]
        assert journal.claim("ce", "me", FAR) == []  # nothing new
        assert journal.request_cancel(["a", "nope"]) == 1
        assert journal.renew("alive", FAR) == ["a"]
        assert journal.renew("alive", FAR) == []  # asked once, answered once
        lost = journal.save([journal.get("a")], owner="me")
        assert lost == {"a"}
        assert journal.save([journal.get("d")], owner="me") == set()
        assert journal.release("me") == 2
        assert journal.get("d").owner is None
        assert set(journal.get_many(["a", "d", "zz"])) == {"a", "d"}


async def test_waiting_on_and_cancelling_another_managers_job(tmp_path: Path) -> None:
    path = tmp_path / "shared.sqlite"
    backend = FakeBackend(steps=("queued",) * 1000)  # one CE for both managers
    with Journal(path) as ja, Journal(path) as jb:
        async with (
            JobManager({"fake": backend}, journal=ja, policy=FAST, owner="A") as a,
            JobManager({"fake": backend}, journal=jb, policy=FAST, owner="B") as b,
        ):
            [key] = await a.submit([SPEC], backend="fake")
            await a.submitted([key])
            # B sees it, but does not take it: A's lease is alive.
            assert b.resume() == [key] and b._work.mine(key) is None
            waiter = asyncio.create_task(b.wait(key))
            [answer] = await b.cancel([key])  # through the CE, and A is asked too
            assert answer.ok
            record = await asyncio.wait_for(waiter, 10)  # B learns of it from the journal
            assert record.state is JobState.CANCELLED and record.owner == "A"
            [again] = await b.cancel([key])
            assert not again.ok and "cancelled" in again.reason
    assert len(backend.jobs) == 1


async def test_cancelling_an_unsent_job_of_a_live_owner() -> None:
    with Journal() as journal:
        journal.renew("other", FAR)
        journal.add_many([("k", "fake", SPEC, None)], owner="other")
        async with JobManager([FakeBackend()], journal=journal, policy=FAST) as jm:
            [answer] = await jm.cancel(["k"])
            assert answer.ok and "owner was asked" in answer.reason
        assert journal.renew("other", FAR) == ["k"]


async def test_cancelling_a_job_of_a_backend_we_do_not_have() -> None:
    with Journal() as journal:
        journal.add("k", "elsewhere", SPEC)
        async with JobManager([FakeBackend()], journal=journal, policy=FAST) as jm:
            [answer] = await jm.cancel(["k"])
        assert answer.ok and journal.get("k").cancel_requested


async def test_resubmitting_a_dead_owners_key_takes_it_over() -> None:
    backend = FakeBackend()
    with Journal() as journal:
        journal.add_many([("k", "fake", SPEC, None)], owner="dead")  # no lease at all
        async with JobManager([backend], journal=journal, policy=FAST) as jm:
            await jm.submit([SPEC], backend="fake", keys=["k"])
            record = await jm.wait("k")
        assert record.state is JobState.DONE and record.owner == jm.owner


async def test_resubmitting_a_key_we_already_hold_changes_nothing() -> None:
    backend = FakeBackend(steps=("queued", "queued", "done"))
    async with JobManager([backend], policy=FAST) as jm:
        await jm.submit([SPEC], backend="fake", keys=["k"])
        await jm.submit([SPEC], backend="fake", keys=["k"])
        assert (await jm.wait("k")).state is JobState.DONE
    assert len(backend.jobs) == 1


async def test_a_job_taken_from_us_is_left_alone(caplog: pytest.LogCaptureFixture) -> None:
    backend = FakeBackend(steps=("queued",) * 1000)
    with Journal() as journal:
        async with JobManager([backend], journal=journal, policy=FAST) as jm:
            key, unwatched = await jm.submit([SPEC, SPEC], backend="fake")
            await jm.submitted([key, unwatched])
            waiter = asyncio.create_task(jm.wait(key))
            await asyncio.sleep(0)  # the waiter is registered before the job is lost
            # Our lease "ran out" and a thief took both jobs.
            with journal._transaction() as db:
                db.execute("UPDATE jobs SET owner = 'thief'")
            with caplog.at_level(logging.WARNING, logger="xrdwms"):
                await jm.cancel([key, unwatched])  # our writes are refused: not ours any more
                for _ in range(200):
                    if jm._work.mine(key) is None:
                        break
                    await asyncio.sleep(0.01)
            assert jm._work.mine(key) is None and jm._work.mine(unwatched) is None
            assert "taken over by another manager" in caplog.text
            journal.update(key, state=JobState.DONE)  # the thief finished it
            assert (await asyncio.wait_for(waiter, 10)).state is JobState.DONE


async def test_housekeeping_cancels_what_others_ask_us_to() -> None:
    backend = FakeBackend(steps=("queued",) * 1000)
    with Journal() as journal:
        async with JobManager([backend], journal=journal, policy=FAST, owner="me") as jm:
            [key] = await jm.submit([SPEC], backend="fake")
            await jm.submitted([key])
            journal.request_cancel([key])
            deadline = time.monotonic() + 10
            while jm.journal.get(key).state is not JobState.CANCELLED:
                assert time.monotonic() < deadline
                await asyncio.sleep(0.05)
    [job] = backend.jobs.values()
    assert job.cancelled
