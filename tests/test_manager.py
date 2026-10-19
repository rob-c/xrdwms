"""The JobManager against the scriptable fake backend: every path a job can take."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from xrdcore.errors import CredentialError

from _shared import FAST, failure_of, handle_of, info_of
from xrdwms import JobManager, JobSpec, Journal, Output
from xrdwms.credentials import Credential
from xrdwms.errors import ConfigError, WMSError
from xrdwms.manager import UNSUBMITTED, failure_for, new_key
from xrdwms.model import Capabilities, FailureKind, JobHandle, JobState, SubmitResult
from xrdwms.testing import FakeBackend

SPEC = JobSpec("hello", "/bin/echo")


class FakeStorage:
    def __init__(self, present: set[str] | None = None, broken: bool = False) -> None:
        self.present = present or set()
        self.broken = broken

    def exists(self, url: str) -> bool:
        if self.broken:
            raise OSError("storage down")
        return url in self.present

    def size(self, url: str) -> int:
        return 0

    def checksum(self, url: str, algorithm: str = "adler32") -> str:
        return ""

    def copy(self, source: str, target: str) -> None:
        self.present.add(target)


class Creds:
    def __init__(self, *creds: Credential | None) -> None:
        self.creds = list(creds)

    def current(self) -> Credential | None:
        return self.creds.pop(0) if len(self.creds) > 1 else self.creds[0]


def manager(fake: FakeBackend, **kwargs: Any) -> JobManager:
    kwargs.setdefault("policy", FAST)
    return JobManager([fake], **kwargs)


async def test_a_job_goes_from_new_to_done() -> None:
    fake = FakeBackend()
    async with manager(fake) as jm:
        assert jm.credentials is None
        [key] = await jm.submit([SPEC], backend="fake", group="g")
        [submitted] = await jm.submitted([key])
        assert submitted.handle is not None
        record = await jm.wait(key)
        assert record.state is JobState.DONE and record.group == "g"
        states = [e.state for e in jm.journal.events(key)]
        assert states == [
            JobState.NEW,
            JobState.SUBMITTING,
            JobState.QUEUED,
            JobState.RUNNING,
            JobState.DONE,
        ]
        # Waiting on a finished job answers at once.
        assert (await jm.wait(key)).state is JobState.DONE
    assert fake.closed


async def test_burst_goes_as_bulk_requests() -> None:
    fake = FakeBackend(capabilities=Capabilities(max_bulk=10))
    policy = replace(FAST, batch_delay=0.05)
    async with manager(fake, policy=policy) as jm:
        keys = await jm.submit([SPEC] * 25, backend="fake")
        records = await jm.wait_all(keys)
    assert all(r.state is JobState.DONE for r in records)
    submits = [n for method, n in fake.calls if method == "submit"]
    assert submits == [10, 10, 5]
    assert max(n for method, n in fake.calls if method == "status") == 10


async def test_iterable_of_backends_and_mapping() -> None:
    fake = FakeBackend("a")
    for backends, expected in (({"x": fake}, {"x": fake}), ([fake], {"a": fake})):
        jm = JobManager(backends)
        assert jm.backends == expected
        await jm.close(close_backends=False)


async def test_submit_validates() -> None:
    async with manager(FakeBackend()) as jm:
        with pytest.raises(ConfigError, match="No backend called 'nope'"):
            await jm.submit([SPEC], backend="nope")
        with pytest.raises(ValueError, match="one key per spec"):
            await jm.submit([SPEC], backend="fake", keys=["a", "b"])


async def test_known_keys_are_adopted_not_resubmitted() -> None:
    fake = FakeBackend()
    async with manager(fake) as jm:
        await jm.submit([SPEC], backend="fake", keys=["same"])
        await jm.wait("same")
        await jm.submit([SPEC], backend="fake", keys=["same"])
        assert (await jm.wait("same")).state is JobState.DONE
    assert len(fake.jobs) == 1


async def test_payload_failure_is_final() -> None:
    fake = FakeBackend(steps=("running", "exit:3"))
    async with manager(fake) as jm:
        [key] = await jm.submit([SPEC.with_(max_retries=5)], backend="fake")
        record = await jm.wait(key)
    assert record.state is JobState.FAILED and info_of(record).exit_code == 3
    assert record.attempt == 0 and len(fake.jobs) == 1


async def test_infra_failures_are_retried_then_given_up() -> None:
    fake = FakeBackend(steps=("running", "failed:infra"))
    async with manager(fake) as jm:
        [key] = await jm.submit([SPEC.with_(max_retries=2)], backend="fake")
        record = await jm.wait(key)
    assert record.state is JobState.FAILED and record.attempt == 2
    assert len(fake.jobs) == 3
    assert sorted(j.key for j in fake.jobs.values()) == [f"{key}-0", f"{key}-1", f"{key}-2"]


async def test_a_retry_can_succeed() -> None:
    fake = FakeBackend()
    fake.script("flaky", "failed:data")
    async with manager(fake) as jm:
        [key] = await jm.submit([SPEC.with_(name="flaky", max_retries=1)], backend="fake")
        while not fake.jobs:
            await asyncio.sleep(0.005)
        fake.script("flaky", "done")  # the second attempt works
        record = await jm.wait(key)
    assert record.state is JobState.DONE and record.attempt == 1


async def test_transient_answers_change_nothing_and_gone_jobs_are_lost() -> None:
    fake = FakeBackend(steps=("transient", "transient", "running", "gone"))
    async with manager(fake) as jm:
        [key] = await jm.submit([SPEC], backend="fake")
        record = await jm.wait(key)
    assert record.state is JobState.FAILED
    assert failure_of(record).kind is FailureKind.INFRA
    assert "no longer knows" in failure_of(record).message


async def test_a_job_that_reappears_is_not_lost() -> None:
    fake = FakeBackend(steps=("gone", "gone", "running", "gone", "done"))
    async with manager(fake) as jm:
        [key] = await jm.submit([SPEC], backend="fake")
        assert (await jm.wait(key)).state is JobState.DONE


async def test_status_errors_back_off_and_recover(caplog: pytest.LogCaptureFixture) -> None:
    fake = FakeBackend()
    fake.fail("status", OSError("CE down"), times=2)
    with caplog.at_level(logging.WARNING, logger="xrdwms"):
        async with manager(fake) as jm:
            [key] = await jm.submit([SPEC], backend="fake")
            assert (await jm.wait(key)).state is JobState.DONE
    assert "2 in a row" in caplog.text


async def test_submission_refused_for_payload_reasons_fails_at_once() -> None:
    fake = FakeBackend()
    fake.refuse("bad", "payload", "no such queue")
    async with manager(fake) as jm:
        [key] = await jm.submit([SPEC.with_(name="bad")], backend="fake")
        [record] = await jm.submitted([key])
    assert record.state is JobState.FAILED and record.handle is None
    assert info_of(record).handle == JobHandle("fake", UNSUBMITTED)
    assert "no such queue" in failure_of(record).message


async def test_retryable_submission_failures_are_tried_again() -> None:
    fake = FakeBackend()
    fake.fail("submit", OSError("CE unreachable"), times=2)
    async with manager(fake) as jm:
        [key] = await jm.submit([SPEC], backend="fake")
        record = await jm.wait(key)
    assert record.state is JobState.DONE
    assert [n for m, n in fake.calls if m == "submit"] == [1, 1, 1]


async def test_submission_gives_up_after_max_tries() -> None:
    fake = FakeBackend()
    fake.refuse("down", "infra", "CE down")
    async with manager(fake, policy=replace(FAST, max_submit_tries=2)) as jm:
        [key] = await jm.submit([SPEC.with_(name="down")], backend="fake")
        record = await jm.wait(key)
    assert record.state is JobState.FAILED and record.submit_tries == 1


async def test_credential_errors_are_not_retried() -> None:
    fake = FakeBackend()
    fake.fail("submit", CredentialError("proxy expired"))
    async with manager(fake) as jm:
        [key] = await jm.submit([SPEC], backend="fake")
        record = await jm.wait(key)
    assert failure_of(record).kind is FailureKind.AUTH


async def test_a_backend_that_forgets_a_spec() -> None:
    class Forgetful(FakeBackend):
        async def submit(self, specs: Any) -> list[SubmitResult]:
            return []

    async with manager(Forgetful(), policy=replace(FAST, max_submit_tries=1)) as jm:
        [key] = await jm.submit([SPEC], backend="fake")
        record = await jm.wait(key)
    assert "did not answer" in failure_of(record).message


def test_failure_for() -> None:
    assert failure_for(CredentialError("x")).kind is FailureKind.AUTH
    assert failure_for(ValueError("x")).kind is FailureKind.PAYLOAD
    assert failure_for(OSError("x")).kind is FailureKind.INFRA
    assert len(new_key()) == 16


async def test_missing_url_outputs_are_a_data_failure() -> None:
    spec = SPEC.with_(outputs=(Output("o", "davs://se/o"), Output("log")))
    async with manager(FakeBackend(), storage=FakeStorage()) as jm:
        [key] = await jm.submit([spec], backend="fake")
        record = await jm.wait(key)
    assert record.state is JobState.FAILED
    assert failure_of(record).kind is FailureKind.DATA
    async with manager(FakeBackend(), storage=FakeStorage({"davs://se/o"})) as jm:
        [key] = await jm.submit([spec], backend="fake")
        assert (await jm.wait(key)).state is JobState.DONE
    async with manager(FakeBackend(), storage=FakeStorage(broken=True)) as jm:
        [key] = await jm.submit([spec], backend="fake")
        record = await jm.wait(key)
    assert "cannot check" in failure_of(record).message


async def test_cancel_before_and_after_submission() -> None:
    fake = FakeBackend(steps=("queued",))
    async with manager(fake) as jm:
        [early] = await jm.submit([SPEC], backend="fake")
        [answer] = await jm.cancel([early])  # before the backend's task got to it
        assert answer.ok and (await jm.wait(early)).state is JobState.CANCELLED
        [late] = await jm.submit([SPEC], backend="fake")
        await jm.submitted([late])
        [answer] = await jm.cancel([late])
        assert answer.ok
        assert (await jm.wait(late)).state is JobState.CANCELLED
        [again] = await jm.cancel([late])
        assert not again.ok and "cancelled" in again.reason
    assert len(fake.jobs) == 1


async def test_cancelled_while_submitting_is_cancelled_on_arrival() -> None:
    gate = asyncio.Event()

    class Slow(FakeBackend):
        async def submit(self, specs: Any) -> list[SubmitResult]:
            await gate.wait()
            return await super().submit(specs)

    fake = Slow()
    journal = Journal()
    async with manager(fake, journal=journal) as jm:
        [key] = await jm.submit([SPEC], backend="fake")
        while jm.journal.get(key).state is not JobState.SUBMITTING:
            await asyncio.sleep(0.005)
        await jm.cancel([key])
        gate.set()
        while ("cancel", 1) not in fake.calls:
            await asyncio.sleep(0.005)
    [job] = fake.jobs.values()
    assert job.cancelled
    record = journal.get(key)
    assert record.state is JobState.CANCELLED and record.handle is not None
    journal.close()


async def test_retrieve_and_clean(tmp_path: Path) -> None:
    fake = FakeBackend()
    async with manager(fake) as jm:
        [key, other] = await jm.submit([SPEC, SPEC], backend="fake")
        await jm.wait(key)
        fake.job(handle_of(jm.journal.get(key))).files["out"] = b"data"
        got = await jm.retrieve(key, str(tmp_path))
        assert Path(got.path, "out").read_bytes() == b"data"
        answers = await jm.clean([key], forget=True)
        assert answers[0].ok and jm.journal.find(key) is None
        await jm.cancel([other])
        never = (await jm.submit([SPEC.with_(name="x")], backend="fake", keys=["never"]))[0]
        await jm.cancel([never])
        with pytest.raises(WMSError, match="never submitted"):
            await jm.retrieve(never, str(tmp_path))
        [answer] = await jm.clean([never])
        assert answer.ok and answer.reason == "never submitted"


async def test_refresh_and_resume() -> None:
    fake = FakeBackend(steps=("queued", "running"))
    journal = Journal()
    jm = manager(fake, journal=journal)
    keys = await jm.submit([SPEC, SPEC], backend="fake")
    assert not jm._own_journal
    await jm.start()
    await jm.submitted(keys)
    records = await jm.refresh(keys)
    assert {r.state for r in records} <= {JobState.QUEUED, JobState.RUNNING}
    assert len(await jm.refresh()) == 2
    assert sorted(jm.resume()) == sorted(keys)
    await jm.close()
    await jm.close()  # twice is harmless
    journal.close()


async def test_closing_fails_waiters_and_ends_events() -> None:
    fake = FakeBackend(steps=("queued",))
    jm = manager(fake)
    await jm.start()
    events: list[str] = []

    async def listen() -> None:
        async for event in jm.events():
            events.append(event.message)

    listener = asyncio.create_task(listen())
    await asyncio.sleep(0)  # subscribed before anything happens
    [key] = await jm.submit([SPEC], backend="fake")
    waiter = asyncio.create_task(jm.wait(key))
    await jm.submitted([key])
    await asyncio.sleep(0.02)
    await jm.close(close_backends=False)
    with pytest.raises(WMSError, match="closed"):
        await waiter
    await listener
    assert "submitted" in events and not fake.closed
    with pytest.raises(WMSError, match="closed"):
        await jm.wait(key)


async def test_a_cancelled_waiter_is_forgotten() -> None:
    async with manager(FakeBackend(steps=("queued",))) as jm:
        [key] = await jm.submit([SPEC], backend="fake")
        waiter = asyncio.create_task(jm.wait(key))
        await asyncio.sleep(0.01)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert key not in jm._work.waiters


async def test_interrupted_submissions_are_found_not_resent() -> None:
    fake = FakeBackend(steps=("running", "done"))
    journal = Journal()
    # A previous run: the job reached the backend, but the run stopped before recording it.
    record = journal.add("k", "fake", SPEC)
    await fake.submit([__import__("xrdwms.model").model.KeyedSpec(record.attempt_key, SPEC)])
    journal.update("k", state=JobState.SUBMITTING)
    journal.add("lost", "fake", SPEC)
    journal.update("lost", state=JobState.SUBMITTING)
    async with manager(fake, journal=journal) as jm:
        assert (await jm.wait("k")).state is JobState.DONE
        assert (await jm.wait("lost")).state is JobState.DONE
    assert len(fake.jobs) == 2
    assert "found after restart" in [e.message for e in journal.events("k")]
    journal.close()


async def test_recovery_without_find_or_with_a_failing_find(
    caplog: pytest.LogCaptureFixture,
) -> None:
    for fake in (FakeBackend(capabilities=Capabilities(find=False)), FakeBackend()):
        fake.fail("find", OSError("no listing"))
        journal = Journal()
        journal.add("k", "fake", SPEC)
        journal.update("k", state=JobState.SUBMITTING)
        with caplog.at_level(logging.WARNING, logger="xrdwms"):
            async with manager(fake, journal=journal) as jm:
                assert (await jm.wait("k")).state is JobState.DONE
        journal.close()
    assert "Could not look for interrupted submissions" in caplog.text


async def test_add_backend() -> None:
    first, second = FakeBackend("a"), FakeBackend("b")
    jm = manager(first)
    await jm.add_backend(second)  # before start: started with the others
    with pytest.raises(ConfigError, match="already"):
        await jm.add_backend(FakeBackend("a"))
    await jm.start()
    await jm.add_backend(FakeBackend("c"))  # after start: started at once
    [key] = await jm.submit([SPEC], backend="c")
    assert (await jm.wait(key)).state is JobState.DONE
    with pytest.raises(ConfigError):
        jm._backend("zzz")
    await jm.close()


async def test_new_credentials_renew_running_jobs(caplog: pytest.LogCaptureFixture) -> None:
    old = Credential("proxy", "/p", 1.0)  # long expired: warned about once
    new = Credential("proxy", "/p", 9e12)
    fake = FakeBackend(steps=("running",) * 6 + ("done",))
    with caplog.at_level(logging.INFO, logger="xrdwms"):
        async with manager(fake, credentials=Creds(old, old, new)) as jm:
            [key] = await jm.submit([SPEC], backend="fake")
            await jm.wait(key)
    [job] = fake.jobs.values()
    assert job.renewed == 1
    assert caplog.text.count("credential expires") == 1
    assert "Renewed 1 jobs" in caplog.text


async def test_credentials_absent_or_not_renewable() -> None:
    fake = FakeBackend(steps=("queued", "running", "done"), capabilities=Capabilities())
    creds = Creds(Credential("token", None, None), Credential("token", None, 5e12))
    async with manager(fake, credentials=creds) as jm:
        [key] = await jm.submit([SPEC], backend="fake")
        await jm.wait(key)
    nothing = Creds(None)
    async with manager(FakeBackend(), credentials=nothing) as jm:
        assert jm.credentials is nothing
        [key] = await jm.submit([SPEC], backend="fake")
        await jm.wait(key)
    # A new credential while nothing is running renews nothing.
    fake = FakeBackend(steps=("held", "held", "held", "done"))
    async with manager(
        fake, credentials=Creds(Credential("proxy", "/a", 5e12), Credential("proxy", "/b", 5e12))
    ) as jm:
        [key] = await jm.submit([SPEC], backend="fake")
        await jm.wait(key)
    assert all(j.renewed == 0 for j in fake.jobs.values())


async def test_stale_answers_are_ignored() -> None:
    fake = FakeBackend(steps=("running",))
    async with manager(fake) as jm:
        [key] = await jm.submit([SPEC], backend="fake")
        [record] = await jm.submitted([key])
        stale = replace(record, handle=JobHandle("fake", "other"))
        from xrdwms.model import JobInfo

        assert not await jm._apply(stale, JobInfo(JobHandle("fake", "other"), JobState.DONE))


async def test_a_cancel_racing_a_poke_still_stops_the_backend_task() -> None:
    """The cancel that ``close`` sends must win even if a poke lands in the same loop turn.

    Python 3.11's ``asyncio.wait_for`` dropped a cancellation when the awaited
    event fired at the same moment: the backend's task then slept on, and
    ``close`` waited for it forever.
    """
    jm = manager(FakeBackend())
    jm._wake["fake"] = asyncio.Event()
    jm._next_poll["fake"] = time.monotonic() + 60  # a deadline: the timed wait
    sleeper = asyncio.create_task(jm._sleep("fake"))
    await asyncio.sleep(0)  # parked on the event
    jm._wake["fake"].set()
    sleeper.cancel()
    with pytest.raises(asyncio.CancelledError):
        await sleeper
    assert sleeper.cancelled()
    await jm.close()


async def test_odd_corners() -> None:
    fake = FakeBackend(steps=("queued",))
    async with manager(fake) as jm:
        await jm.start()  # a second start changes nothing
        [key] = await jm.submit([SPEC], backend="fake")
        await jm.submitted([key])
        fake.jobs.clear()  # the backend forgot it: the cancel is refused
        [answer] = await jm.cancel([key])
        assert not answer.ok
        done: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        done.set_result(None)
        jm._work.waiters["x"] = [(lambda r: True, done)]  # resolved, not yet collected


class CopyingStorage(FakeStorage):
    def __init__(self, fail: bool = False) -> None:
        super().__init__()
        self.fail = fail
        self.copies: list[tuple[str, str]] = []

    def copy(self, source: str, target: str) -> None:
        if self.fail:
            raise OSError("SE full")
        self.copies.append((source, target))
        super().copy(source, target)


def staging_spec(tmp_path: Path) -> JobSpec:
    (tmp_path / "big.dat").write_bytes(b"x" * 100)
    (tmp_path / "small.dat").write_bytes(b"x")
    return SPEC.with_(
        inputs=(str(tmp_path / "big.dat"), str(tmp_path / "small.dat"), "root://se//remote.dat")
    )


async def test_large_inputs_are_staged_to_storage(tmp_path: Path) -> None:
    fake = FakeBackend(capabilities=Capabilities(data_staging=True, sandbox_limit=10))
    storage = CopyingStorage()
    async with manager(fake, storage=storage, staging="davs://se/stage/") as jm:
        [key] = await jm.submit([staging_spec(tmp_path)], backend="fake")
        assert (await jm.wait(key)).state is JobState.DONE
        sent = fake.jobs["fake-1"].spec
        assert [i.source for i in sent.inputs] == [
            f"davs://se/stage/{key}/big.dat",
            str(tmp_path / "small.dat"),
            "root://se//remote.dat",
        ]
        assert jm.journal.get(key).spec == sent
        assert storage.copies == [(str(tmp_path / "big.dat"), f"davs://se/stage/{key}/big.dat")]
        states = [e.state for e in jm.journal.events(key)]
        assert JobState.STAGING_IN in states


async def test_a_failed_upload_is_a_retried_submission(tmp_path: Path) -> None:
    fake = FakeBackend(capabilities=Capabilities(data_staging=True, sandbox_limit=10))
    policy = replace(FAST, max_submit_tries=2)
    async with manager(
        fake, storage=CopyingStorage(fail=True), staging="davs://se/s", policy=policy
    ) as jm:
        [key] = await jm.submit([staging_spec(tmp_path)], backend="fake")
        record = await jm.wait(key)
    assert record.state is JobState.FAILED and not fake.jobs
    assert failure_of(record).kind is FailureKind.DATA
    assert "SE full" in failure_of(record).message


async def test_no_staging_without_data_staging(tmp_path: Path) -> None:
    fake = FakeBackend(capabilities=Capabilities(data_staging=False, sandbox_limit=10))
    storage = CopyingStorage()
    async with manager(fake, storage=storage, staging="davs://se/s") as jm:
        [key] = await jm.submit([staging_spec(tmp_path)], backend="fake")
        await jm.wait(key)
    assert storage.copies == []


async def test_small_inputs_stay_in_the_sandbox(tmp_path: Path) -> None:
    fake = FakeBackend(capabilities=Capabilities(data_staging=True, sandbox_limit=1000))
    storage = CopyingStorage()
    async with manager(fake, storage=storage, staging="davs://se/s") as jm:
        [key] = await jm.submit([staging_spec(tmp_path)], backend="fake")
        await jm.wait(key)
    assert storage.copies == []


async def test_a_job_cancelled_while_an_earlier_chunk_submits_stays_cancelled() -> None:
    """Regression: chunks went out from a list made before the earlier ones were awaited."""
    gate = asyncio.Event()

    class SlowFirst(FakeBackend):
        async def submit(self, specs: Any) -> list[SubmitResult]:
            if not self.jobs:
                await gate.wait()
            return await super().submit(specs)

    fake = SlowFirst(capabilities=Capabilities(max_bulk=1, find=True))
    async with manager(fake) as jm:
        first, second = await jm.submit([SPEC, SPEC], backend="fake")
        while jm.journal.get(first).state is not JobState.SUBMITTING:
            await asyncio.sleep(0.005)
        [answer] = await jm.cancel([second])
        assert answer.ok
        gate.set()
        assert (await jm.wait(second)).state is JobState.CANCELLED
        await jm.wait(first)
    assert len(fake.jobs) == 1


async def test_cancelled_during_stage_in(tmp_path: Path) -> None:
    import threading

    started, release = threading.Event(), threading.Event()

    class BlockingStorage(CopyingStorage):
        def copy(self, source: str, target: str) -> None:
            started.set()
            release.wait(10)
            super().copy(source, target)

    fake = FakeBackend(capabilities=Capabilities(data_staging=True, sandbox_limit=10))
    storage = BlockingStorage()
    async with manager(fake, storage=storage, staging="davs://se/s") as jm:
        # Two jobs: the first blocks in its upload; the second is cancelled meanwhile, and
        # then the first too, before its upload returns.
        first, second = await jm.submit(
            [staging_spec(tmp_path), staging_spec(tmp_path)], backend="fake"
        )
        while not started.is_set():
            await asyncio.sleep(0.005)
        await jm.cancel([second, first])
        release.set()
        while not storage.copies:  # the first upload finishes after its job was cancelled
            await asyncio.sleep(0.005)
        await asyncio.sleep(0.2)  # and the backend's task moves on to the second job
        assert (await jm.wait(first)).state is JobState.CANCELLED
        assert (await jm.wait(second)).state is JobState.CANCELLED
    assert not fake.jobs
