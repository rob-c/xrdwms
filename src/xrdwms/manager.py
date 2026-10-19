"""The JobManager: submits, watches and retries grid jobs, with the journal as its memory.

One asyncio task per backend does all the talking to it:

* **submission** - :meth:`JobManager.submit` only writes ``NEW`` rows to the
  journal and wakes the backend's task, which waits a moment
  (``PollPolicy.batch_delay``) so a burst of submissions goes as one bulk
  request. A row is marked ``SUBMITTING`` before the request; a row still
  ``SUBMITTING`` when a manager takes it over was interrupted mid-request,
  so the backend's ``find`` is asked whether the job landed before it is
  sent again;
* **polling** - every active job of the backend in one bulk ``status`` per
  ``max_bulk`` jobs, never one request per job. The interval starts at
  ``PollPolicy.interval``, grows by ``factor`` while nothing changes (to
  ``max_interval``) and snaps back on any change; a failed request backs off
  with :func:`xrdcore.retry.delay`;
* **retries** - a job that fails retryably (``FailureKind.retryable``) is
  reset to ``NEW`` for another attempt while ``JobSpec.max_retries``
  allows; a failed *submission* that is retryable is tried again up to
  ``PollPolicy.max_submit_tries``;
* **outputs** - a job the backend calls ``DONE`` is checked for its URL
  outputs (:func:`xrdwms.staging.verify_outputs`); a missing one makes it a
  retryable ``DATA`` failure;
* **credentials** - a :class:`~xrdwms.credentials.CredentialProvider` is
  watched through :class:`xrdsec.lifetime.CredentialWatch`: one warning per
  expiry under ``PollPolicy.expiry_warning``, and when it shows a *new*
  credential (a fresh proxy), backends that can renew are told to renew
  their running jobs'.

**Working set and ownership.** A manager keeps the active jobs it *owns* in
memory and writes their changes to the journal in batches - one
transaction per round, not one per job - so its cost per round grows with
the jobs that changed, not with the journal. It owns the jobs it submitted
and any it takes over (:meth:`~xrdwms.journal.Journal.claim`): jobs nobody
holds, or whose owner's lease ran out (``PollPolicy.lease``). A
housekeeping task renews its leases, takes over orphaned jobs, acts on
cancellations other managers asked for, and keeps waiters on *other*
managers' jobs informed from the journal. So several managers - dask
workers, a CLI beside a dask session - can share one journal without
submitting or polling each other's jobs.

Waiting is cheap: :meth:`JobManager.wait` awaits a future the backend's
task resolves, so thousands of waiters cost no threads and no requests.

The manager runs on one event loop; every public coroutine must be awaited
on it.

Where to start reading: :meth:`JobManager.submit` and :meth:`JobManager.wait`
for the public side, :meth:`JobManager._run` for a backend's task. The
working set (owned jobs, pending writes, waiters) is
:class:`~xrdwms._workset.WorkingSet`; every decision taken on a backend's
answer is a plain function in :mod:`xrdwms._policy`.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import time
import uuid
from collections import Counter
from collections.abc import AsyncIterator, Iterable, Mapping, Sequence
from dataclasses import replace
from pathlib import Path

from xrdcore import retry
from xrdcore.log import get_logger
from xrdsec.lifetime import CredentialWatch, Sighting

from ._policy import UNSUBMITTED as UNSUBMITTED
from ._policy import PollPolicy as PollPolicy
from ._policy import (
    clean_unasked,
    first_poll,
    handle_of,
    is_news,
    large_inputs,
    lost_info,
    next_interval,
    retryable_failure,
    staged_spec,
    unsubmitted_failure,
    wake_by,
)
from ._policy import failure_for as failure_for
from ._policy import new_key as new_key
from ._workset import ManagerEvent as ManagerEvent
from ._workset import Predicate, WorkingSet
from .backends import Backend
from .credentials import CredentialProvider
from .errors import ConfigError, WMSError
from .journal import Journal, Record
from .model import (
    Failure,
    FailureKind,
    Input,
    JobHandle,
    JobInfo,
    JobSpec,
    JobState,
    KeyedSpec,
    OpResult,
    Retrieved,
    SubmitResult,
)
from .staging import GridStorage, Storage, verify_outputs

__all__ = ["JobManager", "ManagerEvent", "PollPolicy", "failure_for", "new_key"]

_log = get_logger("xrdwms")


def _named(backends: Mapping[str, Backend] | Iterable[Backend]) -> dict[str, Backend]:
    """``backends`` by name: a mapping as given, an iterable by each backend's ``name``."""
    if isinstance(backends, Mapping):
        return dict(backends)
    return {backend.name: backend for backend in backends}


def _default_owner() -> str:
    """This manager's name in the journal: host, process and a random part.

    Host and process say where a job's manager runs (for a person reading
    the journal); the random part keeps two managers in one process apart.
    """
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


class JobManager:
    """Grid jobs over several backends, kept in a journal; see the module docstring.

    ``backends`` maps names to backends (or is an iterable of backends,
    named by their ``name``). ``journal`` defaults to an in-memory one
    (closed with the manager; a journal passed in is the caller's to close);
    ``storage`` checks URL outputs (default :class:`~xrdwms.staging.GridStorage`);
    ``credentials`` is watched for expiry and renewal. ``staging`` is a storage
    URL directory: local inputs larger than a backend's ``sandbox_limit`` are
    uploaded to ``<staging>/<key>/<name>`` before submission, and the job gets
    the URL instead (for backends with ``data_staging``). ``owner`` names this
    manager in the journal (default: host, process and a random part).
    """

    def __init__(
        self,
        backends: Mapping[str, Backend] | Iterable[Backend],
        *,
        journal: Journal | None = None,
        policy: PollPolicy | None = None,
        storage: Storage | None = None,
        credentials: CredentialProvider | None = None,
        staging: str | None = None,
        owner: str | None = None,
    ) -> None:
        self.backends: dict[str, Backend] = _named(backends)
        self.journal = journal if journal is not None else Journal()
        #: A journal the manager made itself is closed with it; one it was given is not.
        self._own_journal = journal is None
        self.policy = policy or PollPolicy()
        self.storage: Storage = storage or GridStorage()
        #: The credential bookkeeping (warned expiry, what each backend last saw).
        self._watch = CredentialWatch(credentials) if credentials is not None else None
        self.staging = staging.rstrip("/") if staging else None
        self.owner = owner or _default_owner()
        #: The owned jobs, their pending writes, the waiters and the subscribers.
        self._work = WorkingSet(self.journal, self.owner, self.backends)
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._keeper: asyncio.Task[None] | None = None
        self._wake: dict[str, asyncio.Event] = {}
        #: Consecutive failed status requests per backend: the polling backoff's count.
        self._errors: Counter[str] = Counter()
        #: When a backend's refused submissions may be tried again (monotonic time).
        self._submit_after: dict[str, float] = {}
        #: When each backend is next polled (monotonic time; ``None``: nothing to poll).
        self._next_poll: dict[str, float | None] = {}
        self._renewed = False
        self._closed = False

    @property
    def credentials(self) -> CredentialProvider | None:
        """The provider watched for expiry and renewal, if one was given."""
        return None if self._watch is None else self._watch.provider

    # -- lifecycle -----------------------------------------------------------------------

    async def __aenter__(self) -> JobManager:
        """Start, and close on leaving the block."""
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Close the manager and its backends."""
        await self.close()

    async def start(self) -> None:
        """Take over unowned jobs, start one task per backend and the housekeeping task."""
        for name in self.backends:
            self._start_backend(name)
        if self._keeper is None:
            self._keeper = asyncio.create_task(self._keep(), name="xrdwms-housekeeping")

    def _start_backend(self, name: str) -> None:
        """Take over the backend's unowned jobs and start its task (once)."""
        if name in self._tasks:
            return
        self._work.owned.setdefault(name, {})
        self._adopt(self.journal.claim(name, self.owner, self._lease()))
        self._wake[name] = asyncio.Event()
        self._next_poll[name] = None
        self._tasks[name] = asyncio.create_task(self._run(name), name=f"xrdwms-{name}")

    async def add_backend(self, backend: Backend) -> None:
        """Manage another backend from now on (its name must be new)."""
        if backend.name in self.backends:
            raise ConfigError(f"There is already a backend called {backend.name!r}")
        self.backends[backend.name] = backend
        self._work.owned.setdefault(backend.name, {})
        if self._tasks:
            self._start_backend(backend.name)

    async def close(self, *, close_backends: bool = True) -> None:
        """Stop polling, hand the jobs back, fail anyone still waiting, close the backends."""
        if self._closed:
            return
        self._closed = True
        tasks = [*self._tasks.values(), *([self._keeper] if self._keeper else [])]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._work.flush()
        # Released, not left to expire: another manager can carry on with them at once.
        self.journal.release(self.owner)
        self._work.close()
        if close_backends:
            for backend in self.backends.values():
                await backend.close()
        if self._own_journal:
            self.journal.close()

    # -- the public operations -------------------------------------------------------------

    async def submit(
        self,
        specs: Sequence[JobSpec],
        *,
        backend: str,
        group: str | None = None,
        keys: Sequence[str] | None = None,
    ) -> list[str]:
        """Queue jobs for ``backend``; their keys. The backend's task submits them shortly.

        A key already in the journal is *adopted*, not submitted again: the
        dask layer passes stable keys, so recomputing a graph after a crash
        reattaches to the jobs it had. One that another live manager owns
        stays its: waiting on it works all the same.
        """
        if backend not in self.backends:
            raise ConfigError(f"No backend called {backend!r}")
        chosen = list(keys) if keys is not None else [new_key() for _ in specs]
        if len(chosen) != len(specs):
            raise ValueError("submit needs one key per spec")
        entries = [(key, backend, spec, group) for key, spec in zip(chosen, specs, strict=True)]
        if not self._renewed:
            self.journal.renew(self.owner, self._lease())  # our lease exists before our jobs do
            self._renewed = True
        added, existing = self.journal.add_many(entries, owner=self.owner)
        for record in added:
            self._work.owned[backend][record.key] = record
            self._work.notify(record, "added")
        self._adopt_existing(backend, existing)
        self._poke(backend)
        return chosen

    def _adopt_existing(self, backend: str, existing: Iterable[str]) -> None:
        """Take over the already-journalled keys of a submission that nobody else holds.

        Keys this manager owns already need nothing; the rest are claimed,
        and the journal hands over only those whose owner's lease has run out.
        """
        theirs = [key for key in existing if self._work.mine(key) is None]
        if theirs:
            self._adopt(self.journal.claim(backend, self.owner, self._lease(), keys=theirs))

    async def wait(self, key: str) -> Record:
        """The job's record once it is ``DONE``, ``FAILED`` or ``CANCELLED``."""
        return await self._until(key, lambda r: r.terminal)

    async def wait_all(self, keys: Sequence[str]) -> list[Record]:
        """:meth:`wait` for each key, in order."""
        return list(await asyncio.gather(*(self.wait(key) for key in keys)))

    async def submitted(self, keys: Sequence[str]) -> list[Record]:
        """Each job's record once its backend has accepted it (or it has ended)."""
        return list(
            await asyncio.gather(
                *(self._until(k, lambda r: r.handle is not None or r.terminal) for k in keys)
            )
        )

    async def cancel(self, keys: Sequence[str]) -> list[OpResult]:
        """Cancel jobs: one not yet submitted ends here, the others are asked of their backend.

        A job another live manager owns is asked of the backend directly when
        it has a handle, and its owner is asked to cancel it too.
        """
        results: dict[str, OpResult] = {}
        by_backend: dict[str, list[Record]] = {}
        for key in keys:
            record = self._work.mine(key) or self._take(key)
            if record is None:
                results[key] = await self._cancel_foreign(self.journal.get(key))
            elif record.handle is None:
                results[key] = self._cancel_unsubmitted(record)
            else:
                by_backend.setdefault(record.backend, []).append(record)
        for name, records in by_backend.items():
            results.update(await self._cancel_on(name, records))
        self._work.flush()
        return [results[key] for key in keys]

    def _cancel_unsubmitted(self, record: Record) -> OpResult:
        """End a job that never reached its backend: there is nothing to ask anyone."""
        failure = Failure(FailureKind.PAYLOAD, "Cancelled before it was submitted")
        info = JobInfo(handle_of(record), JobState.CANCELLED, failure=failure)
        self._work.set(
            replace(record, state=JobState.CANCELLED, info=info), "cancelled before submission"
        )
        return OpResult(info.handle, True)

    async def _cancel_on(self, name: str, records: list[Record]) -> dict[str, OpResult]:
        """Ask one backend to cancel its jobs, in one request; the answers by key.

        An accepted cancel is logged now; the job's end comes from the next
        poll, which is asked for at once.
        """
        answers = await self._backend(name).cancel([handle_of(r) for r in records])
        for record, answer in zip(records, answers, strict=True):
            if answer.ok:
                self._work.set(record, "cancel requested")
        self._poke(name, poll=True)
        return {record.key: answer for record, answer in zip(records, answers, strict=True)}

    def _take(self, key: str) -> Record | None:
        """The job, taken over if it is active, of one of our backends, and nobody else's."""
        record = self.journal.get(key)
        if record.terminal or record.backend not in self.backends:
            return None
        self._adopt(self.journal.claim(record.backend, self.owner, self._lease(), keys=[key]))
        return self._work.mine(key)

    async def _cancel_foreign(self, record: Record) -> OpResult:
        """Cancel a job we do not own: through its backend if it has a handle, and its owner."""
        if record.terminal:
            return OpResult(handle_of(record), False, f"The job is {record.state}")
        self.journal.request_cancel([record.key])
        if record.handle is not None and record.backend in self.backends:
            [answer] = await self.backends[record.backend].cancel([record.handle])
            return answer
        return OpResult(handle_of(record), True, "its owner was asked to cancel it")

    async def retrieve(self, key: str, directory: str) -> Retrieved:
        """Download a submitted job's sandbox into ``directory``."""
        record = self._work.get(key)
        if record.handle is None:
            raise WMSError(f"Job {key} was never submitted, so it has no sandbox")
        return await self._backend(record.backend).retrieve(record.handle, directory)

    async def clean(self, keys: Sequence[str], *, forget: bool = False) -> list[OpResult]:
        """Remove ended jobs from their backends (and, with ``forget``, from the journal).

        A job still going is refused, whatever the backend: cancel it first.
        """
        records = [self._work.get(key) for key in keys]
        # What can be answered without asking a backend: still going, or never sent.
        results = {r.key: answer for r in records if (answer := clean_unasked(r)) is not None}
        results.update(await self._clean_on_backends([r for r in records if r.key not in results]))
        if forget:
            self.journal.remove([k for k in keys if results[k].ok])
        return [results[key] for key in keys]

    async def _clean_on_backends(self, records: list[Record]) -> dict[str, OpResult]:
        """Each backend's answers for its jobs among ``records``: one bulk call each, by key."""
        results: dict[str, OpResult] = {}
        for name in {r.backend for r in records}:
            batch = [r for r in records if r.backend == name]
            answers = await self._backend(name).clean([handle_of(r) for r in batch])
            results.update({r.key: a for r, a in zip(batch, answers, strict=True)})
        return results

    async def refresh(self, keys: Sequence[str] | None = None) -> list[Record]:
        """Ask the backends about these (default: all our active) jobs now; their records.

        Only jobs this manager owns are asked about; others are read from the journal.
        """
        if keys is None:
            keys = [k for owned in self._work.owned.values() for k in owned]
        for name, owned in self._work.owned.items():
            mine = [owned[k] for k in keys if k in owned and owned[k].handle is not None]
            if mine:
                await self._refresh(name, mine)
        return [self._work.get(key) for key in keys]

    def resume(self, group: str | None = None) -> list[str]:
        """Take over unowned jobs now; the active jobs (of ``group``) of this manager's backends."""
        for name in self.backends:
            self._adopt(self.journal.claim(name, self.owner, self._lease()))
            self._poke(name, poll=True)
        return [
            r.key
            for r in self.journal.records(group=group, active=True)
            if r.backend in self.backends
        ]

    def events(self) -> AsyncIterator[ManagerEvent]:
        """Every change from now on, until the manager closes."""
        return self._work.events()

    # -- the working set -----------------------------------------------------------------

    def _lease(self) -> float:
        """When a hold taken (or renewed) now runs out, in journal (wall-clock) time."""
        return time.time() + self.policy.lease

    def _adopt(self, records: Iterable[Record]) -> None:
        """Make taken-over jobs part of the working set, and wake their backends' tasks."""
        for name in self._work.adopt(records):
            self._poke(name)

    # -- the backend task ----------------------------------------------------------------

    async def _run(self, name: str) -> None:
        """Submit what is new, poll what is due, sleep until the next of those or a poke."""
        backend = self.backends[name]
        interval = self.policy.interval
        while True:
            sent = False
            if time.monotonic() >= self._submit_after.get(name, 0.0):
                sent = await self._submit_pending(name, backend)
            active = self._work.submitted(name)
            if active:
                interval = await self._poll_if_due(name, backend, active, sent, interval)
            else:
                self._next_poll[name] = None
            await self._sleep(name)

    async def _poll_if_due(
        self, name: str, backend: Backend, active: list[Record], sent: bool, interval: float
    ) -> float:
        """Schedule the first look at new jobs and poll if due; the polling interval from now.

        The interval grows by ``factor`` while nothing changes and snaps back to
        the base one on any change, or when new jobs have just been sent.
        """
        due = self._next_poll[name]
        if due is None or sent:
            self._next_poll[name] = due = first_poll(due, time.monotonic(), self.policy)
            if sent:
                interval = self.policy.interval
        if time.monotonic() < due:
            return interval
        await self._check_credentials(name, backend, active)
        interval = next_interval(self.policy, interval, await self._refresh(name, active))
        self._next_poll[name] = time.monotonic() + interval + self._backoff(name)
        return interval

    def _deadline(self, name: str) -> float | None:
        """When the backend's task must wake by itself: the next poll or submission retry.

        A submission retry counts only while there is something left to submit.
        """
        retry_at = self._submit_after.get(name) if self._work.pending(name) else None
        return wake_by([self._next_poll.get(name), retry_at])

    async def _sleep(self, name: str) -> None:
        """Wait for the backend's deadline or a poke, then let a burst of pokes gather."""
        wake = self._wake[name]
        deadline = self._deadline(name)
        timeout = None if deadline is None else max(0.0, deadline - time.monotonic())
        # ``asyncio.timeout``, not ``wait_for``: on Python 3.11, ``wait_for`` returns the
        # event's result when a cancel and a poke land in the same loop turn, dropping the
        # cancel - and ``close`` then waits for this task forever.
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(timeout):
                await wake.wait()
        if wake.is_set():
            wake.clear()
            # Let a burst of submissions or pokes arrive before acting on it.
            await asyncio.sleep(self.policy.batch_delay)
            wake.clear()

    def _poke(self, name: str, *, poll: bool = False) -> None:
        """Wake a backend's task; with ``poll``, have it poll now rather than when due."""
        if poll and self._next_poll.get(name) is not None:
            self._next_poll[name] = time.monotonic()
        if name in self._wake:
            self._wake[name].set()

    def _backoff(self, name: str) -> float:
        """The extra pause before polling a backend whose last requests failed."""
        failures = self._errors[name]
        if not failures:
            return 0.0
        return retry.delay(failures, self.policy.interval, self.policy.backoff_cap)

    def _backend(self, name: str) -> Backend:
        """The backend called ``name``; :class:`ConfigError` if this manager has none."""
        try:
            return self.backends[name]
        except KeyError:
            raise ConfigError(f"No backend called {name!r} in this manager") from None

    # -- housekeeping --------------------------------------------------------------------

    async def _keep(self) -> None:
        """Renew the lease and take over orphans every lease/4; update foreign waiters often."""
        renew_every = self.policy.lease / 4
        watch_every = max(0.01, min(renew_every, self.policy.interval))
        renewed = time.monotonic()
        while True:
            await asyncio.sleep(watch_every)
            if time.monotonic() - renewed >= renew_every:
                renewed = time.monotonic()
                await self._housekeeping()
            self._work.watch_foreign()

    async def _housekeeping(self) -> None:
        """Renew our lease, act on cancellations asked of us, take over jobs nobody runs."""
        requested = self.journal.renew(self.owner, self._lease())
        mine = [key for key in requested if self._work.mine(key) is not None]
        if mine:
            await self.cancel(mine)
        for name in self.backends:
            self._adopt(self.journal.claim(name, self.owner, self._lease()))

    # -- submission -----------------------------------------------------------------------

    async def _submit_pending(self, name: str, backend: Backend) -> bool:
        """Send every job of ``name`` that has no handle yet; whether there were any."""
        records = self._work.pending(name)
        if not records:
            return False
        records = await self._recover(backend, records)
        staged = [await self._stage_in(name, backend, record) for record in records]
        records = [record for record in staged if record is not None]
        size = max(1, backend.capabilities.max_bulk)
        for start in range(0, len(records), size):
            await self._submit_chunk(name, backend, records[start : start + size])
        return True

    async def _recover(self, backend: Backend, records: list[Record]) -> list[Record]:
        """Reattach jobs a previous run was submitting when it stopped; the rest still to send."""
        interrupted = [r for r in records if r.state is JobState.SUBMITTING]
        if not interrupted or not backend.capabilities.find:
            return records
        try:
            found = await backend.find([r.attempt_key for r in interrupted])
        except Exception as exc:
            _log.warning("Could not look for interrupted submissions on %s: %s", backend.name, exc)
            return records
        remaining = []
        for record in records:
            handle = found.get(record.attempt_key)
            if handle is None:
                remaining.append(record)
                continue
            self._work.set(
                replace(record, state=JobState.QUEUED, handle=handle), "found after restart"
            )
        self._work.flush()
        return remaining

    async def _stage_in(self, name: str, backend: Backend, record: Record) -> Record | None:
        """The record ready to send: large local inputs uploaded to ``staging`` first.

        ``None`` when an upload failed; that counts as a failed submission
        (``DATA``, so retried while ``max_submit_tries`` allows).
        """
        if self.staging is None or not backend.capabilities.data_staging:
            return record
        base = Path(getattr(backend, "workdir", None) or Path.cwd())
        large = large_inputs(record.spec, base, backend.capabilities.sandbox_limit)
        if not large:
            return record
        current = self._work.owned[name].get(record.key)
        if current is None or current.handle is not None:
            return None  # cancelled or found meanwhile
        record = replace(current, state=JobState.STAGING_IN)
        self._work.set(record, "staging inputs")
        self._work.flush()
        replaced = await self._upload(name, record, base, large)
        return None if replaced is None else self._staged(name, record, len(large), replaced)

    async def _upload(
        self, name: str, record: Record, base: Path, large: list[Input]
    ) -> dict[str, Input] | None:
        """Copy each large input to the staging area; their URL inputs, or ``None`` on failure.

        The copies run in a thread (storage clients block). The first one that
        fails ends the attempt as a retryable ``DATA`` failure of the submission.
        """
        replaced = {}
        for item in large:
            url = f"{self.staging}/{record.key}/{item.name}"
            try:
                await asyncio.to_thread(self.storage.copy, str(base / item.source), url)
            except OSError as exc:
                failure = Failure(FailureKind.DATA, f"Could not stage {item.name} to {url}: {exc}")
                self._submitted(name, record, SubmitResult(record.attempt_key, failure=failure))
                self._work.flush()
                return None
            replaced[item.name] = Input(url, item.name)
        return replaced

    def _staged(
        self, name: str, record: Record, count: int, replaced: dict[str, Input]
    ) -> Record | None:
        """Record the staged spec in the journal and the working set; ``None`` if cancelled."""
        spec = staged_spec(record.spec, replaced)
        self.journal.update(
            record.key, spec=spec, message=f"staged {count} inputs to {self.staging}"
        )
        current = self._work.owned[name].get(record.key)
        if current is None:
            return None  # cancelled while its inputs went up
        record = replace(current, spec=spec)
        self._work.remember(record)
        return record

    async def _submit_chunk(self, name: str, backend: Backend, records: list[Record]) -> None:
        """Submit one bulk request's worth of jobs and take in the answers.

        Each job is marked ``SUBMITTING`` (and written) first, so a crash
        mid-request leaves a trace the next manager can look for
        (:meth:`_recover`).
        """
        records = self._mark_submitting(name, records)
        if not records:
            return
        answers = await _submit_answers(
            backend, [KeyedSpec(r.attempt_key, r.spec) for r in records]
        )
        arrived_cancelled = self._take_answers(name, records, answers)
        self._work.flush()
        for record, handle in arrived_cancelled:
            # Cancelled while the request was in flight: the job must not run.
            await backend.cancel([handle])
            self.journal.update(record.key, handle=handle, message="cancelled on arrival")

    def _mark_submitting(self, name: str, records: list[Record]) -> list[Record]:
        """Mark the chunk's jobs that are still ours and unsent ``SUBMITTING``; those jobs.

        The list was made before earlier chunks' requests were awaited: a job
        cancelled (or taken from us) meanwhile must not be brought back, so
        each is looked up afresh.
        """
        owned = self._work.owned[name]
        fresh = [owned[r.key] for r in records if r.key in owned and owned[r.key].handle is None]
        for record in fresh:
            self._work.set(replace(record, state=JobState.SUBMITTING))
        self._work.flush()
        return [owned[r.key] for r in fresh if r.key in owned]

    def _take_answers(
        self, name: str, records: list[Record], answers: dict[str, SubmitResult]
    ) -> list[tuple[Record, JobHandle]]:
        """Take a chunk's answers in; the jobs that arrived after they were cancelled.

        A job the backend did not answer for failed (``INFRA``, so retried).
        """
        arrived_cancelled = []
        for record in records:
            result = answers.get(record.attempt_key) or SubmitResult(
                record.attempt_key, failure=Failure(FailureKind.INFRA, "The backend did not answer")
            )
            stray = self._submitted(name, record, result)
            if stray is not None:
                arrived_cancelled.append((record, stray))
        return arrived_cancelled

    def _submitted(self, name: str, record: Record, result: SubmitResult) -> JobHandle | None:
        """Take a submission's answer into the working set.

        The job's handle when it had been cancelled while the request was in
        flight and arrived anyway: the caller must cancel it on the backend.
        """
        current = self._work.owned[name].get(record.key)
        if current is None:
            # Cancelled (or taken over) while the request was in flight.
            return result.handle
        if result.handle is not None:
            self._work.set(
                replace(current, state=JobState.QUEUED, handle=result.handle), "submitted"
            )
            return None
        self._refused(
            name, current, result.failure or Failure(FailureKind.INFRA, "No reason given")
        )
        return None

    def _refused(self, name: str, current: Record, failure: Failure) -> None:
        """A refused submission: tried again later while retryable and allowed, else failed.

        The backend's next submission waits (with the polling backoff) so a
        backend refusing everything is not asked again at once.
        """
        tries = current.submit_tries + 1
        if failure.kind.retryable and tries < self.policy.max_submit_tries:
            pause = retry.delay(tries, self.policy.interval, self.policy.backoff_cap)
            self._submit_after[name] = time.monotonic() + pause
            self._work.set(
                replace(current, state=JobState.NEW, submit_tries=tries), failure.message
            )
            return
        info = unsubmitted_failure(name, failure)
        self._work.set(
            replace(current, state=JobState.FAILED, info=info),
            f"submission failed: {failure.message}",
        )

    # -- polling -------------------------------------------------------------------------

    async def _refresh(self, name: str, records: list[Record]) -> bool:
        """One round of status for ``records``; whether anything changed.

        A failed request ends the round (the rest are asked next time) and
        counts towards the backend's backoff; a successful one resets it.
        """
        backend = self._backend(name)
        size = max(1, backend.capabilities.max_bulk)
        changed = False
        try:
            for start in range(0, len(records), size):
                chunk = records[start : start + size]
                infos = await self._status(name, backend, chunk)
                if infos is None:
                    return changed
                for record, info in zip(chunk, infos, strict=True):
                    changed = await self._apply(record, info) or changed
        finally:
            self._work.flush()
        return changed

    async def _status(
        self, name: str, backend: Backend, chunk: list[Record]
    ) -> list[JobInfo] | None:
        """One bulk status request; ``None`` (logged, counted) if it failed."""
        try:
            infos = await backend.status([handle_of(r) for r in chunk])
        except Exception as exc:
            self._errors[name] += 1
            _log.warning("Status from %s failed (%d in a row): %s", name, self._errors[name], exc)
            return None
        self._errors[name] = 0
        return infos

    async def _apply(self, record: Record, info: JobInfo) -> bool:
        """Take one status answer into the working set; whether the job's state changed."""
        current = self._work.owned.get(record.backend, {}).get(record.key)
        if info.transient or current is None or current.handle != info.handle:
            return False
        settled = self._settle_unknown(record.key, info)
        if settled is None:
            return False
        if settled.state is JobState.DONE:
            settled = await self._check_outputs(current, settled)
        if not is_news(current, settled):
            # No news worth a write: kept in memory only.
            self._work.remember(replace(current, info=settled))
            return False
        failure = retryable_failure(current, settled)
        if failure is not None:
            self._retry(current, failure)
        else:
            message = settled.native_state or str(settled.state)
            self._work.set(replace(current, state=settled.state, info=settled), message)
        return True

    def _settle_unknown(self, key: str, info: JobInfo) -> JobInfo | None:
        """``info``, or ``None`` while a job its backend does not know may yet turn up.

        A backend can briefly not know a job it has just accepted; only
        ``lost_after`` such answers in a row fail it (:func:`lost_info`).
        """
        if info.state is JobState.UNKNOWN:
            self._work.unknown[key] += 1
            if self._work.unknown[key] < self.policy.lost_after:
                return None
            info = lost_info(info)
        self._work.unknown.pop(key, None)
        return info

    def _retry(self, current: Record, failure: Failure) -> None:
        """Reset a retryably failed job to ``NEW`` for its next attempt, and wake its backend."""
        self._work.set(
            replace(
                current,
                state=JobState.NEW,
                handle=None,
                info=None,
                attempt=current.attempt + 1,
                submit_tries=0,
            ),
            f"attempt {current.attempt} failed, retrying: {failure.message}",
        )
        self._poke(current.backend)

    async def _check_outputs(self, record: Record, info: JobInfo) -> JobInfo:
        """``info``, or a ``DATA`` failure if a URL output of the job is missing."""
        if all(o.in_sandbox for o in record.spec.outputs):
            return info
        problems = await asyncio.to_thread(verify_outputs, record.spec, self.storage)
        if not problems:
            return info
        failure = Failure(FailureKind.DATA, "Missing outputs: " + "; ".join(problems))
        return replace(info, state=JobState.FAILED, failure=failure)

    # -- credentials ---------------------------------------------------------------------

    async def _check_credentials(self, name: str, backend: Backend, active: list[Record]) -> None:
        """Warn about an expiring credential; renew jobs' when a new one has appeared."""
        if self._watch is None:
            return
        # Keyed by backend: each holds its own delegated copy, so each renews once.
        sighting = self._watch.check(name, warn_below=self.policy.expiry_warning)
        if sighting is None:
            return
        if sighting.warn:
            _warn_expiry(sighting)
        if sighting.renewed and backend.capabilities.proxy_renewal:
            await _renew(name, backend, active)

    # -- waiting -------------------------------------------------------------------------

    async def _until(self, key: str, predicate: Predicate) -> Record:
        """The job's record once ``predicate`` holds; :class:`WMSError` once closed."""
        if self._closed:
            raise WMSError("The job manager was closed")
        return await self._work.until(key, predicate)


def _warn_expiry(sighting: Sighting) -> None:
    """One warning that the watched credential runs out soon, in whole minutes."""
    _log.warning(
        "The %s credential expires in %d minutes",
        sighting.credential.kind,
        (sighting.left or 0) // 60,
    )


async def _renew(name: str, backend: Backend, active: list[Record]) -> None:
    """Hand a new credential to the backend's running and queued jobs, in one request."""
    running = [r for r in active if r.state in (JobState.QUEUED, JobState.RUNNING)]
    if not running:
        return
    answers = await backend.renew([handle_of(r) for r in running])
    failed = [a for a in answers if not a.ok]
    _log.info("Renewed %d jobs on %s (%d failed)", len(answers) - len(failed), name, len(failed))


async def _submit_answers(backend: Backend, keyed: list[KeyedSpec]) -> dict[str, SubmitResult]:
    """The backend's answer to one bulk submission, by key; a failed request fails every job."""
    try:
        results = await backend.submit(keyed)
    except Exception as exc:
        failure = failure_for(exc)
        results = [SubmitResult(k.key, failure=failure) for k in keyed]
    return {result.key: result for result in results}
