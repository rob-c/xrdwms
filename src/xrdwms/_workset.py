"""A job manager's working set: the active jobs it owns, and who is waiting on them.

:class:`~xrdwms.manager.JobManager` talks to backends; this module is its
memory between journal writes. Three things live here, because each needs
the other two:

* **the jobs it owns** - the latest record of every active job this manager
  submitted or took over, by backend and key. Polling, retries and
  cancellations read and change these records in memory;
* **the pending writes** - every change since the last flush, written to the
  journal in *one* transaction (:meth:`WorkingSet.flush`), so a round that
  touches a thousand jobs costs one commit, not a thousand. Only after the
  write are waiters woken and subscribers told, so nobody ever sees a state
  the journal does not hold;
* **the waiters and subscribers** - futures resolved when a job reaches the
  state someone waits for, and queues that receive every change. A key
  waited on that another manager owns is *foreign*: it is read back from
  the journal (:meth:`WorkingSet.watch_foreign`), since no change of it
  passes through here.

Everything here runs on the manager's event loop; nothing is locked.
What a maintainer must not break: a record leaves :attr:`WorkingSet.owned`
when it ends (a terminal record is written, then forgotten), and a job the
journal says another manager took (:meth:`~xrdwms.journal.Journal.save`'s
"lost" keys) leaves it at once - two managers must never both poll a job.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import Counter
from collections.abc import AsyncIterator, Callable, Iterable
from dataclasses import dataclass

from xrdcore.log import get_logger

from .errors import WMSError
from .journal import Event, Journal, Record

__all__ = ["ManagerEvent", "Predicate", "WorkingSet"]

_log = get_logger("xrdwms")

#: What a waiter waits for: true of the record that ends its wait.
Predicate = Callable[[Record], bool]


@dataclass(frozen=True, slots=True)
class ManagerEvent:
    """A job changed: its record now, and what happened."""

    record: Record
    message: str = ""


class WorkingSet:
    """The owned jobs of one manager, their pending writes, waiters and subscribers.

    ``journal`` is where changes are written and where jobs this manager
    does not own are read; ``owner`` is the manager's name there, so a write
    of a job another manager has taken over is refused rather than clobbering
    theirs. ``backends`` are the names the manager starts with.
    """

    def __init__(self, journal: Journal, owner: str, backends: Iterable[str]) -> None:
        """Start empty: nothing owned, nothing pending, nobody waiting."""
        self.journal = journal
        self.owner = owner
        #: The active jobs this manager owns, by backend and key.
        self.owned: dict[str, dict[str, Record]] = {name: {} for name in backends}
        #: Consecutive "no such job" answers per key (``PollPolicy.lost_after``).
        self.unknown: Counter[str] = Counter()
        #: Who waits for which job, and for what.
        self.waiters: dict[str, list[tuple[Predicate, asyncio.Future[Record]]]] = {}
        #: Keys waited on that another manager owns: read back from the journal.
        self.foreign: set[str] = set()
        # Changes not yet written: the latest record of each job, and the events in order.
        self._dirty: dict[str, Record] = {}
        self._changes: list[tuple[Record, str | None]] = []
        self._subscribers: list[asyncio.Queue[ManagerEvent | None]] = []

    # -- reading ---------------------------------------------------------------------------

    def mine(self, key: str) -> Record | None:
        """The job's record if this manager owns it (and it is active), else ``None``."""
        for owned in self.owned.values():
            record = owned.get(key)
            if record is not None:
                return record
        return None

    def known(self, key: str) -> Record | None:
        """Our freshest record of a job: a change not yet written, or the working set's."""
        return self._dirty.get(key) or self.mine(key)

    def get(self, key: str) -> Record:
        """The freshest record of a job: ours from memory, anyone else's from the journal."""
        return self.known(key) or self.journal.get(key)

    def pending(self, backend: str) -> list[Record]:
        """The owned jobs of ``backend`` not yet accepted by it (no handle)."""
        return [r for r in self.owned[backend].values() if r.handle is None]

    def submitted(self, backend: str) -> list[Record]:
        """The owned jobs of ``backend`` it has accepted: the ones to poll."""
        return [r for r in self.owned[backend].values() if r.handle is not None]

    # -- changing --------------------------------------------------------------------------

    def adopt(self, records: Iterable[Record]) -> set[str]:
        """Make taken-over jobs part of the working set; the backends that gained jobs.

        The manager wakes those backends' tasks, so an adopted job that was
        interrupted mid-submission is looked at without waiting for a poll.
        """
        gained = set()
        for record in records:
            self.owned.setdefault(record.backend, {})[record.key] = record
            self.notify(record, "taken over")
            gained.add(record.backend)
        return gained

    def set(self, record: Record, message: str | None = "") -> None:
        """A job's new record: in the working set now, in the journal at the next flush.

        ``message`` (even empty) logs an event; ``None`` logs none. A terminal
        job leaves the working set, and its lost-job count with it.
        """
        owned = self.owned[record.backend]
        if record.terminal:
            owned.pop(record.key, None)
            self.unknown.pop(record.key, None)
        else:
            owned[record.key] = record
        self._dirty[record.key] = record
        self._changes.append((record, message))

    def remember(self, record: Record) -> None:
        """Keep a newer record in memory only: news not worth a journal write."""
        self.owned[record.backend][record.key] = record

    def flush(self) -> None:
        """Write the pending changes in one transaction, then tell waiters and subscribers.

        A job the journal refuses because another manager now owns it (our
        lease ran out) is dropped from the working set; anyone waiting on it
        keeps waiting, now on the journal (it becomes foreign).
        """
        if not self._dirty:
            return
        now = time.time()
        changes, self._changes = self._changes, []
        records, self._dirty = list(self._dirty.values()), {}
        events = [Event(r.key, now, r.state, m) for r, m in changes if m is not None]
        lost = self.journal.save(records, events, owner=self.owner)
        for key in lost:
            self._lose(key)
        for record, message in changes:
            if record.key not in lost:
                self.notify(record, message or "")

    def _lose(self, key: str) -> None:
        """Forget a job another manager took over; its waiters now read the journal."""
        for owned in self.owned.values():
            owned.pop(key, None)
        if key in self.waiters:
            self.foreign.add(key)
        _log.warning("Job %s was taken over by another manager; leaving it to them", key)

    # -- waiting and events ----------------------------------------------------------------

    async def until(self, key: str, predicate: Predicate) -> Record:
        """The job's record once ``predicate`` holds for it (at once, if it already does).

        A job nobody here owns is watched through the journal. The waiter is
        removed however the wait ends, so a cancelled wait leaves nothing behind.
        """
        known = self.known(key)
        record = known or self.journal.get(key)
        if predicate(record):
            return record
        future: asyncio.Future[Record] = asyncio.get_running_loop().create_future()
        self.waiters.setdefault(key, []).append((predicate, future))
        if known is None:
            self.foreign.add(key)
        try:
            return await future
        finally:
            self._forget_waiter(key, predicate, future)

    def _forget_waiter(
        self, key: str, predicate: Predicate, future: asyncio.Future[Record]
    ) -> None:
        """Remove one waiter (it may be gone already) and the key's entry once it has none."""
        waiters = self.waiters.get(key, [])
        with contextlib.suppress(ValueError):
            waiters.remove((predicate, future))
        if not waiters:
            self.waiters.pop(key, None)

    def notify(self, record: Record, message: str | None) -> None:
        """Wake the waiters ``record`` satisfies; tell subscribers (unless ``message`` is None)."""
        for predicate, future in list(self.waiters.get(record.key, ())):
            if not future.done() and predicate(record):
                future.set_result(record)
        if message is None:
            return
        event = ManagerEvent(record, message)
        for queue in self._subscribers:
            queue.put_nowait(event)

    def watch_foreign(self) -> None:
        """Wake waiters on jobs other managers own, from the journal.

        A key stops being foreign once nobody waits on it or it has become
        ours (taken over): either way the journal need not be read for it.
        """
        self.foreign = {k for k in self.foreign if k in self.waiters and self.known(k) is None}
        if self.foreign:
            for record in self.journal.get_many(sorted(self.foreign)).values():
                self.notify(record, None)

    async def events(self) -> AsyncIterator[ManagerEvent]:
        """Every change from now on, until :meth:`close`."""
        queue: asyncio.Queue[ManagerEvent | None] = asyncio.Queue()
        self._subscribers.append(queue)
        try:
            while (event := await queue.get()) is not None:
                yield event
        finally:
            self._subscribers.remove(queue)

    def close(self) -> None:
        """Fail every waiter still waiting, and end every subscription."""
        for waiters in self.waiters.values():
            for _predicate, future in waiters:
                if not future.done():
                    future.set_exception(WMSError("The job manager was closed"))
        self.waiters.clear()
        for queue in self._subscribers:
            queue.put_nowait(None)
