"""The journal: every job xrdwms manages, on disk, so nothing is lost when the client stops.

One SQLite file (WAL mode, so a ``xrdwms status`` can read while a run
writes) holds a row per job - its key, spec, backend, handle, state and last
:class:`~xrdwms.model.JobInfo` - and an event log of every state change.
The row is written *before* the backend is asked to submit: after a crash a
job is either in the journal without a handle (the manager asks the
backend's ``find`` whether the submission landed) or with one, never
nowhere.

A job's ``key`` is stable for its whole life; each submission attempt sends
``<key>-<attempt>`` to the backend, so a resubmission is never mistaken for
the job it replaces.

**Ownership.** Several managers may share one journal (dask workers, a CLI
beside a dask session). Each active job is *owned* by at most one manager,
and only its owner submits it, polls it or changes it. A manager holds one
*lease* (a row in ``owners``) for all its jobs: :meth:`Journal.renew` extends
it, :meth:`Journal.claim` takes jobs nobody holds or whose owner's lease ran
out (that manager having died), :meth:`Journal.release` gives everything up
on a clean exit, and every batched write (:meth:`Journal.save`) is
conditional on still being the owner, so a manager that lost a job cannot
overwrite it.
Anyone may ask for a job to be cancelled (:meth:`Journal.request_cancel`);
its owner acts on the request.

Durability: WAL with ``synchronous=NORMAL`` - every committed write survives
the process dying; only a power cut can lose the last few.

**Every statement is a constant.** A list of keys (or the terminal states)
travels as *one* JSON parameter, read back with SQLite's ``json_each`` (built
in since SQLite 3.38, and the JSON1 extension in every Python build before),
and an optional filter is ``(? IS NULL OR column = ?)``. So no SQL text is
ever built at run time - nothing for an injection to ride on - and a key list
has no host-parameter limit.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import UnknownJobError, WMSError
from .model import JobHandle, JobInfo, JobSpec, JobState

__all__ = ["Event", "Journal", "Record", "default_path"]

_SCHEMA_VERSION = 2
_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    key TEXT PRIMARY KEY,
    backend TEXT NOT NULL,
    grp TEXT,
    spec TEXT NOT NULL,
    state TEXT NOT NULL,
    handle TEXT,
    attempt INTEGER NOT NULL DEFAULT 0,
    submit_tries INTEGER NOT NULL DEFAULT 0,
    info TEXT,
    created REAL NOT NULL,
    updated REAL NOT NULL,
    owner TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS owners (
    owner TEXT PRIMARY KEY,
    lease REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS jobs_cancel ON jobs (owner) WHERE cancel_requested = 1;
CREATE INDEX IF NOT EXISTS jobs_backend ON jobs (backend, state);
CREATE INDEX IF NOT EXISTS jobs_grp ON jobs (grp);
CREATE INDEX IF NOT EXISTS jobs_owner ON jobs (owner);
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL,
    time REAL NOT NULL,
    state TEXT NOT NULL,
    message TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS events_key ON events (key);
"""
#: Version 1 had no ownership columns; they are added in place.
_V1_TO_V2 = (
    "ALTER TABLE jobs ADD COLUMN owner TEXT",
    "ALTER TABLE jobs ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0",
)
#: The terminal states, as the JSON array the statements below read with ``json_each``.
_TERMINAL = json.dumps([state.value for state in JobState if state.terminal])
#: :meth:`Journal.update`: ``NULL`` leaves a column as it is (``update`` never clears one).
_UPDATE = (
    "UPDATE jobs SET state = COALESCE(?, state), handle = COALESCE(?, handle), "
    "info = COALESCE(?, info), attempt = COALESCE(?, attempt), "
    "submit_tries = COALESCE(?, submit_tries), spec = COALESCE(?, spec), updated = ? "
    "WHERE key = ?"
)
#: :meth:`Journal.claim`: a backend's active jobs, optionally only the JSON list of keys,
#: that nobody holds or whose owner's lease has run out.
_CLAIM = (
    "SELECT * FROM jobs WHERE backend = ? "
    "AND state NOT IN (SELECT value FROM json_each(?)) "
    "AND (owner IS NULL OR (owner != ? AND owner NOT IN "
    "(SELECT owner FROM owners WHERE lease >= ?))) "
    "AND (? IS NULL OR key IN (SELECT value FROM json_each(?)))"
)
#: :meth:`Journal.records`: each filter is skipped when its parameter is ``NULL``;
#: ``active`` compares "is terminal" with 0 (active) or 1 (ended).
_RECORDS = (
    "SELECT * FROM jobs WHERE (?1 IS NULL OR backend = ?1) AND (?2 IS NULL OR grp = ?2) "
    "AND (?3 IS NULL OR (state IN (SELECT value FROM json_each(?4))) = ?3) "
    "ORDER BY created, key"
)
#: :meth:`Journal.save`: a job's whole working-set record, unconditionally - or, as
#: ``_SAVE_AS_OWNER``, only while the saving manager still owns it.
_SAVE = (
    "UPDATE jobs SET state = ?, handle = ?, info = ?, attempt = ?, submit_tries = ?, updated = ? "
    "WHERE key = ?"
)
_SAVE_AS_OWNER = _SAVE + " AND owner = ?"
#: Parsed specs by their JSON text: jobs submitted together usually share one.
_SPEC_CACHE_LIMIT = 100_000


def default_path(environ: dict[str, str] | None = None) -> Path:
    """``$XRDWMS_JOURNAL``, else ``$XDG_STATE_HOME/xrdwms/journal.sqlite`` (``~/.local/state``)."""
    env = os.environ if environ is None else environ
    if env.get("XRDWMS_JOURNAL"):
        return Path(env["XRDWMS_JOURNAL"])
    state = env.get("XDG_STATE_HOME") or os.path.join(Path.home(), ".local", "state")
    return Path(state, "xrdwms", "journal.sqlite")


@dataclass(frozen=True, slots=True)
class Record:
    """One job as the journal holds it."""

    #: The job's name for its whole life, across every attempt.
    key: str
    #: The manager's name of the backend that runs it.
    backend: str
    #: What to run - after staging, with large inputs swapped for their URLs.
    spec: JobSpec
    state: JobState
    #: The backend's handle for the current attempt; ``None`` until it is accepted.
    handle: JobHandle | None = None
    #: Which run this is: 0 first, one more for each retry after a retryable failure.
    attempt: int = 0
    #: Refused submissions of the current attempt (``PollPolicy.max_submit_tries``).
    submit_tries: int = 0
    #: What the backend last said, kept for ``xrdwms status`` and the failure's reason.
    info: JobInfo | None = None
    #: A label for jobs submitted together (a CLI batch, a dask graph), to list them by.
    group: str | None = None
    #: When the job was added and last written (epoch seconds).
    created: float = 0.0
    updated: float = 0.0
    #: The manager holding the job (see the module docstring), and whether someone has
    #: asked it to cancel the job.
    owner: str | None = None
    cancel_requested: bool = False

    @property
    def terminal(self) -> bool:
        """The job will not change again."""
        return self.state.terminal

    @property
    def attempt_key(self) -> str:
        """The key the backend sees for the current attempt: ``<key>-<attempt>``."""
        return f"{self.key}-{self.attempt}"


@dataclass(frozen=True, slots=True)
class Event:
    """A state change, as the journal logged it."""

    key: str
    time: float
    state: JobState
    message: str = ""


class Journal:
    """The SQLite journal at ``path`` (``":memory:"`` for one that lives with the process).

    Safe to share between threads: every statement runs under one lock.
    Several ``Journal`` objects (in several processes) may open the same file.
    """

    def __init__(self, path: str | os.PathLike[str] = ":memory:") -> None:
        """Open (creating it, and its directory, if need be) and bring the schema up to date.

        A connection that cannot be set up is closed again, so a failed open
        leaves no file handle behind.
        """
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._specs: dict[str, JobSpec] = {}
        self._db = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        try:
            self._setup()
        except BaseException:
            self._db.close()
            raise

    def _setup(self) -> None:
        """Set the connection up and create (or migrate) the schema, in one transaction.

        ``busy_timeout`` lets several processes share the file: a writer waits
        for another's transaction instead of failing at once. The schema work
        runs under ``BEGIN IMMEDIATE``, so two processes opening a new journal
        together cannot both create it.
        """
        with self._lock:
            self._db.execute("PRAGMA busy_timeout=30000")
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=NORMAL")
            self._db.execute("BEGIN IMMEDIATE")
            try:
                version = self._db.execute("PRAGMA user_version").fetchone()[0]
                if version not in (0, 1, _SCHEMA_VERSION):
                    raise WMSError(
                        f"The journal {self.path} has schema {version}; this xrdwms reads "
                        f"up to {_SCHEMA_VERSION}"
                    )
                if version == 1:
                    for statement in _V1_TO_V2:
                        self._db.execute(statement)
                for statement in filter(str.strip, _SCHEMA.split(";")):
                    self._db.execute(statement)
                self._db.execute(f"PRAGMA user_version={_SCHEMA_VERSION}")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            self._db.execute("COMMIT")

    def close(self) -> None:
        """Close the database; the journal cannot be used afterwards."""
        with self._lock:
            self._db.close()

    def __enter__(self) -> Journal:
        """The journal itself; leaving the block closes it."""
        return self

    def __exit__(self, *exc: object) -> None:
        """Close."""
        self.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        """One write transaction under the journal's lock; rolled back if the block raises.

        ``BEGIN IMMEDIATE`` takes SQLite's write lock up front, so a
        read-then-write in the block cannot be overtaken by another process.
        """
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield self._db
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            self._db.execute("COMMIT")

    # -- writing one job -----------------------------------------------------------------

    def add(self, key: str, backend: str, spec: JobSpec, *, group: str | None = None) -> Record:
        """A new job, ``NEW``, unowned and without a handle; ``WMSError`` if ``key`` is taken."""
        inserted, existing = self.add_many([(key, backend, spec, group)])
        if existing:
            raise WMSError(f"A job with key {key!r} is already in the journal")
        return inserted[0]

    def update(
        self,
        key: str,
        *,
        state: JobState | None = None,
        handle: JobHandle | None = None,
        info: JobInfo | None = None,
        attempt: int | None = None,
        submit_tries: int | None = None,
        spec: JobSpec | None = None,
        message: str = "",
    ) -> Record:
        """Change the fields given (``None`` leaves one as it is); log a change of state.

        A field is never cleared here: :meth:`reset` (or :meth:`save`) does that.
        """
        now = time.time()
        values = (
            None if state is None else state.value,
            None if handle is None else str(handle),
            None if info is None else json.dumps(info.to_dict()),
            attempt,
            submit_tries,
            None if spec is None else json.dumps(spec.to_dict()),
            now,
            key,
        )
        with self._transaction() as db:
            # Read inside the transaction: the event is logged only for a real change of
            # state (or with a message), judged against the row as it was just now.
            before = db.execute("SELECT state FROM jobs WHERE key = ?", (key,)).fetchone()
            if before is None:
                raise UnknownJobError(f"No job {key!r} in the journal")
            db.execute(_UPDATE, values)
            if state is not None and (state.value != before["state"] or message):
                _log_event(db, key, now, state, message)
        return self.get(key)

    def reset(self, key: str, *, attempt: int, message: str) -> Record:
        """Back to ``NEW`` for another attempt: no handle, no info, ``submit_tries`` zero."""
        now = time.time()
        with self._transaction() as db:
            cursor = db.execute(
                "UPDATE jobs SET state = ?, handle = NULL, info = NULL, attempt = ?, "
                "submit_tries = 0, updated = ? WHERE key = ?",
                (JobState.NEW.value, attempt, now, key),
            )
            if cursor.rowcount == 0:
                raise UnknownJobError(f"No job {key!r} in the journal")
            _log_event(db, key, now, JobState.NEW, message)
        return self.get(key)

    def remove(self, keys: list[str]) -> int:
        """Forget jobs (and their events); how many there were."""
        listed = (json.dumps(keys),)
        with self._transaction() as db:
            db.execute("DELETE FROM events WHERE key IN (SELECT value FROM json_each(?))", listed)
            return db.execute(
                "DELETE FROM jobs WHERE key IN (SELECT value FROM json_each(?))", listed
            ).rowcount

    # -- writing in bulk, as an owner ------------------------------------------------------

    def add_many(
        self,
        entries: Sequence[tuple[str, str, JobSpec, str | None]],
        *,
        owner: str | None = None,
    ) -> tuple[list[Record], list[str]]:
        """Add ``(key, backend, spec, group)`` jobs in one transaction, owned by ``owner``.

        Returns the records added and the keys that were already there (left
        untouched). The records are built from the specs given, not read back.
        """
        now = time.time()
        added: list[Record] = []
        existing: list[str] = []
        with self._transaction() as db:
            for key, backend, spec, group in entries:
                text = json.dumps(spec.to_dict())
                cursor = db.execute(
                    "INSERT OR IGNORE INTO jobs (key, backend, grp, spec, state, created, "
                    "updated, owner) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (key, backend, group, text, JobState.NEW.value, now, now, owner),
                )
                if cursor.rowcount == 0:
                    existing.append(key)
                    continue
                _log_event(db, key, now, JobState.NEW, "added")
                added.append(
                    Record(
                        key,
                        backend,
                        spec,
                        JobState.NEW,
                        group=group,
                        created=now,
                        updated=now,
                        owner=owner,
                    )
                )
        return added, existing

    def save(
        self, records: Sequence[Record], events: Sequence[Event] = (), *, owner: str | None = None
    ) -> set[str]:
        """Write each record's mutable fields, and the events, in one transaction.

        With ``owner``, a row is written only while ``owner`` still holds it;
        the keys of the rows it no longer holds come back (their events are
        dropped too). Handle and info are written as they are, ``None``
        clearing them.
        """
        now = time.time()
        lost: set[str] = set()
        statement = _SAVE if owner is None else _SAVE_AS_OWNER
        with self._transaction() as db:
            for record in records:
                values: tuple[Any, ...] = (
                    record.state.value,
                    str(record.handle) if record.handle else None,
                    json.dumps(record.info.to_dict()) if record.info else None,
                    record.attempt,
                    record.submit_tries,
                    now,
                    record.key,
                )
                cursor = db.execute(statement, (*values, owner) if owner is not None else values)
                # No row written: another manager has taken the job over (our lease
                # ran out while we were busy). Its state is theirs to write now.
                if cursor.rowcount == 0:
                    lost.add(record.key)
            db.executemany(
                "INSERT INTO events (key, time, state, message) VALUES (?, ?, ?, ?)",
                [(e.key, e.time, e.state.value, e.message) for e in events if e.key not in lost],
            )
        return lost

    def claim(
        self, backend: str, owner: str, lease: float, *, keys: Sequence[str] | None = None
    ) -> list[Record]:
        """Take ``backend``'s active jobs (or ``keys``) nobody holds, or whose owner died.

        A dead owner is one whose lease ran out. ``owner``'s own lease is
        renewed to ``lease`` (``time.time()`` seconds) first. Returns the
        records newly taken, not those ``owner`` held already.
        """
        now = time.time()
        listed = None if keys is None else json.dumps(list(keys))
        values = (backend, _TERMINAL, owner, now, listed, listed)
        with self._transaction() as db:
            # The lease first, in the same transaction: a job is never held by an owner
            # whose lease another manager could already consider expired.
            db.execute("INSERT OR REPLACE INTO owners (owner, lease) VALUES (?, ?)", (owner, lease))
            rows = db.execute(_CLAIM, values).fetchall()
            db.executemany(
                "UPDATE jobs SET owner = ? WHERE key = ?", [(owner, row["key"]) for row in rows]
            )
        return [self._record(row, owner=owner) for row in rows]

    def renew(self, owner: str, lease: float) -> list[str]:
        """Extend ``owner``'s lease to ``lease``; the keys whose cancellation someone asked for."""
        with self._transaction() as db:
            db.execute("INSERT OR REPLACE INTO owners (owner, lease) VALUES (?, ?)", (owner, lease))
            rows = db.execute(
                "SELECT key FROM jobs WHERE owner = ? AND cancel_requested = 1 "
                "AND state NOT IN (SELECT value FROM json_each(?))",
                (owner, _TERMINAL),
            ).fetchall()
            keys = [row["key"] for row in rows]
            # The requests are handed over once: cleared here, acted on by the caller.
            db.executemany(
                "UPDATE jobs SET cancel_requested = 0 WHERE key = ?", [(k,) for k in keys]
            )
        return keys

    def release(self, owner: str) -> int:
        """Give up ``owner``'s active jobs and lease, so another manager can take them at once."""
        with self._transaction() as db:
            db.execute("DELETE FROM owners WHERE owner = ?", (owner,))
            return db.execute(
                "UPDATE jobs SET owner = NULL WHERE owner = ? "
                "AND state NOT IN (SELECT value FROM json_each(?))",
                (owner, _TERMINAL),
            ).rowcount

    def request_cancel(self, keys: Sequence[str], message: str = "cancel requested") -> int:
        """Ask the owners of these active jobs to cancel them; how many were still active."""
        now = time.time()
        with self._transaction() as db:
            rows = db.execute(
                "SELECT key, state FROM jobs WHERE key IN (SELECT value FROM json_each(?)) "
                "AND state NOT IN (SELECT value FROM json_each(?))",
                (json.dumps(list(keys)), _TERMINAL),
            ).fetchall()
            db.executemany(
                "UPDATE jobs SET cancel_requested = 1 WHERE key = ?", [(r["key"],) for r in rows]
            )
            db.executemany(
                "INSERT INTO events (key, time, state, message) VALUES (?, ?, ?, ?)",
                [(r["key"], now, r["state"], message) for r in rows],
            )
        return len(rows)

    # -- reading -------------------------------------------------------------------------

    def find(self, key: str) -> Record | None:
        """The job with ``key``, or ``None``."""
        with self._lock:
            row = self._db.execute("SELECT * FROM jobs WHERE key = ?", (key,)).fetchone()
        return self._record(row) if row is not None else None

    def get(self, key: str) -> Record:
        """The job with ``key``; ``UnknownJobError`` if there is none."""
        record = self.find(key)
        if record is None:
            raise UnknownJobError(f"No job {key!r} in the journal")
        return record

    def get_many(self, keys: Sequence[str]) -> dict[str, Record]:
        """The jobs with these keys that exist, by key."""
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM jobs WHERE key IN (SELECT value FROM json_each(?))",
                (json.dumps(list(keys)),),
            ).fetchall()
        return {row["key"]: self._record(row) for row in rows}

    def records(
        self,
        *,
        backend: str | None = None,
        group: str | None = None,
        active: bool | None = None,
    ) -> list[Record]:
        """Jobs, oldest first; filtered by backend, group, and whether they are still going."""
        ended = None if active is None else int(not active)
        with self._lock:
            rows = self._db.execute(_RECORDS, (backend, group, ended, _TERMINAL)).fetchall()
        return [self._record(row) for row in rows]

    def events(self, key: str) -> list[Event]:
        """Every state change of a job, in order."""
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM events WHERE key = ? ORDER BY seq", (key,)
            ).fetchall()
        return [Event(r["key"], r["time"], JobState(r["state"]), r["message"]) for r in rows]

    def _spec(self, text: str) -> JobSpec:
        """The spec stored as ``text``, parsed once."""
        spec = self._specs.get(text)
        if spec is None:
            if len(self._specs) >= _SPEC_CACHE_LIMIT:
                self._specs.clear()
            spec = self._specs[text] = JobSpec.from_dict(json.loads(text))
        return spec

    def _record(self, row: sqlite3.Row, *, owner: str | None = None) -> Record:
        """A database row as a :class:`Record`; ``owner`` overrides the row's when given."""
        return Record(
            key=row["key"],
            backend=row["backend"],
            spec=self._spec(row["spec"]),
            state=JobState(row["state"]),
            handle=JobHandle.parse(row["handle"]) if row["handle"] else None,
            attempt=row["attempt"],
            submit_tries=row["submit_tries"],
            info=JobInfo.from_dict(json.loads(row["info"])) if row["info"] else None,
            group=row["grp"],
            created=row["created"],
            updated=row["updated"],
            owner=owner if owner is not None else row["owner"],
            cancel_requested=bool(row["cancel_requested"]),
        )


def _log_event(db: sqlite3.Connection, key: str, now: float, state: JobState, message: str) -> None:
    """Append one state change to the event log (inside the caller's transaction)."""
    db.execute(
        "INSERT INTO events (key, time, state, message) VALUES (?, ?, ?, ?)",
        (key, now, state.value, message),
    )
