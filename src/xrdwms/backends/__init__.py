"""Backends: the interface every workload manager is driven through, and where to find them.

A backend is a small object with async, bulk methods (:class:`Backend`).
The :class:`~xrdwms.manager.JobManager` owns pacing, retries and the
journal, so a backend only translates and calls its client. Clients that
are synchronous (``xrdarc``) subclass :class:`SyncBackend`, which runs each
call on a worker thread of the backend's own, one at a time.

Backends are found by *kind* (``arc``, ``local``): the built-in ones, then
the ``xrdwms.backends`` entry point group, so another package can add one
without xrdwms knowing of it. :func:`create` builds one from a kind, a name
and its options - what a configuration file holds.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import importlib
from collections.abc import Callable, Mapping, Sequence
from importlib import metadata
from typing import Any, ClassVar, Protocol, TypeVar, runtime_checkable

from xrdcore.fanout import Pool

from ..errors import ConfigError
from ..model import Capabilities, JobHandle, JobInfo, KeyedSpec, OpResult, Retrieved, SubmitResult

__all__ = ["BUILTIN", "Backend", "BackendClass", "SyncBackend", "create", "kinds", "load"]

T = TypeVar("T")

#: Kinds that ship with xrdwms, as ``module:attribute``; entry points can add more.
BUILTIN: dict[str, str] = {
    "arc": "xrdwms.backends.arc:ARCBackend",
    "local": "xrdwms.backends.local:LocalBackend",
    "fake": "xrdwms.testing:FakeBackend",
}

ENTRY_POINT_GROUP = "xrdwms.backends"


@runtime_checkable
class Backend(Protocol):
    """A workload manager, driven in bulk; see the module docstring.

    ``name`` is the configured name (it goes into every :class:`JobHandle`),
    ``kind`` the implementation (``arc``). Each method takes many jobs and
    answers one result per job, in order; a method raises only for what
    stops the whole request, and the manager turns that into per-job
    results.
    """

    kind: ClassVar[str]
    name: str
    capabilities: Capabilities

    async def submit(self, specs: Sequence[KeyedSpec]) -> list[SubmitResult]:
        """Submit each spec, attaching its key to the job so :meth:`find` can see it."""
        ...

    async def status(self, handles: Sequence[JobHandle]) -> list[JobInfo]:
        """The current state of each job (``transient`` where the backend could not ask)."""
        ...

    async def cancel(self, handles: Sequence[JobHandle]) -> list[OpResult]:
        """Ask for each job to be cancelled."""
        ...

    async def retrieve(self, handle: JobHandle, directory: str) -> Retrieved:
        """Download the job's sandbox into ``directory``."""
        ...

    async def clean(self, handles: Sequence[JobHandle]) -> list[OpResult]:
        """Remove finished jobs (and their sandboxes) from the backend."""
        ...

    async def renew(self, handles: Sequence[JobHandle]) -> list[OpResult]:
        """Refresh the credential each running job holds from the current one."""
        ...

    async def find(self, keys: Sequence[str]) -> dict[str, JobHandle]:
        """The jobs carrying these idempotency keys, for recovery after a crash."""
        ...

    async def close(self) -> None:
        """Release connections and threads."""
        ...


class SyncBackend:
    """Base for backends over a synchronous client: each call runs on the backend's own thread.

    Subclasses implement the ``_submit`` ... ``_find`` methods (plain,
    blocking); the async methods of :class:`Backend` run them through
    :meth:`call`, one at a time on a single-thread executor, so a client
    that is not thread-safe never sees two calls at once, and the event loop
    never blocks. The caller's :mod:`contextvars` go with each call.
    """

    kind: ClassVar[str] = "sync"
    capabilities: Capabilities = Capabilities()

    def __init__(self, name: str) -> None:
        """``name`` is the backend's name in the manager; it also names the backend's thread.

        One worker thread, not a pool of several: a blocking client (an ARC
        session, a gfal context) is then never used from two threads at once.
        The thread is :class:`xrdcore.fanout.Pool`'s, so it starts with the
        first call and a forked child gets one of its own.
        """
        self.name = name
        self._pool = Pool(f"xrdwms-{name}", 1)

    async def call(self, function: Callable[..., T], *args: Any) -> T:
        """Run ``function(*args)`` on the backend's thread and await its result."""
        loop = asyncio.get_running_loop()
        context = contextvars.copy_context()
        return await loop.run_in_executor(
            self._pool.executor(), functools.partial(context.run, function, *args)
        )

    async def submit(self, specs: Sequence[KeyedSpec]) -> list[SubmitResult]:
        """:meth:`Backend.submit`, through ``_submit``."""
        return await self.call(self._submit, list(specs))

    async def status(self, handles: Sequence[JobHandle]) -> list[JobInfo]:
        """:meth:`Backend.status`, through ``_status``."""
        return await self.call(self._status, list(handles))

    async def cancel(self, handles: Sequence[JobHandle]) -> list[OpResult]:
        """:meth:`Backend.cancel`, through ``_cancel``."""
        return await self.call(self._cancel, list(handles))

    async def retrieve(self, handle: JobHandle, directory: str) -> Retrieved:
        """:meth:`Backend.retrieve`, through ``_retrieve``."""
        return await self.call(self._retrieve, handle, directory)

    async def clean(self, handles: Sequence[JobHandle]) -> list[OpResult]:
        """:meth:`Backend.clean`, through ``_clean``."""
        return await self.call(self._clean, list(handles))

    async def renew(self, handles: Sequence[JobHandle]) -> list[OpResult]:
        """:meth:`Backend.renew`, through ``_renew``."""
        return await self.call(self._renew, list(handles))

    async def find(self, keys: Sequence[str]) -> dict[str, JobHandle]:
        """:meth:`Backend.find`, through ``_find``."""
        return await self.call(self._find, list(keys))

    async def close(self) -> None:
        """Run ``_close`` on the backend's thread, then stop the thread."""
        try:
            await self.call(self._close)
        finally:
            self._pool.shutdown(wait=False)

    # -- for subclasses ---------------------------------------------------------------

    def _submit(self, specs: list[KeyedSpec]) -> list[SubmitResult]:
        """Submit the specs; one :class:`SubmitResult` each, in order (blocking)."""
        raise NotImplementedError

    def _status(self, handles: list[JobHandle]) -> list[JobInfo]:
        """The state of each job; one :class:`JobInfo` per handle, in order (blocking)."""
        raise NotImplementedError

    def _cancel(self, handles: list[JobHandle]) -> list[OpResult]:
        """Ask for each job to be cancelled; one :class:`OpResult` per handle (blocking)."""
        raise NotImplementedError

    def _retrieve(self, handle: JobHandle, directory: str) -> Retrieved:
        """Download one job's sandbox into ``directory`` (blocking)."""
        raise NotImplementedError

    def _clean(self, handles: list[JobHandle]) -> list[OpResult]:
        """Remove ended jobs from the backend; one :class:`OpResult` per handle (blocking)."""
        raise NotImplementedError

    def _renew(self, handles: list[JobHandle]) -> list[OpResult]:
        """Renew the jobs' credentials; by default every job is refused, as most cannot."""
        return [OpResult(h, False, f"The {self.kind} backend cannot renew") for h in handles]

    def _find(self, keys: list[str]) -> dict[str, JobHandle]:
        """The jobs submitted under ``keys`` that the backend knows; by default none.

        A backend that cannot search (``Capabilities.find`` false) answers
        nothing, and the manager resubmits an interrupted submission instead.
        """
        return {}

    def _close(self) -> None:
        """Release the backend's resources (connections, clients); by default nothing."""
        return None


def kinds() -> dict[str, str]:
    """Every backend kind there is, and where it comes from (``module:attribute``)."""
    found = dict(BUILTIN)
    for entry in metadata.entry_points(group=ENTRY_POINT_GROUP):
        found.setdefault(entry.name, entry.value)
    return found


class BackendClass(Protocol):
    """What a backend entry point names: something built as ``cls(name, **options)``.

    Not ``type[Backend]``: :class:`Backend` is a protocol, and says nothing
    about construction; this is the constructor contract :func:`create` relies on.
    """

    def __call__(self, name: str, /, **options: Any) -> Backend:
        """A backend called ``name``, configured by ``options``."""
        ...


def load(kind: str) -> BackendClass:
    """The class implementing ``kind``; ``ConfigError`` for an unknown or broken one."""
    target = kinds().get(kind)
    if target is None:
        known = ", ".join(sorted(kinds()))
        raise ConfigError(f"No backend of kind {kind!r} (known: {known})")
    module_name, _, attribute = target.partition(":")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        hint = f"; install xrdwms[{kind}]" if kind in BUILTIN else ""
        raise ConfigError(f"The {kind} backend cannot be loaded: {exc}{hint}") from exc
    cls: BackendClass = getattr(module, attribute)
    return cls


def create(kind: str, name: str | None = None, **options: Any) -> Backend:
    """A backend of ``kind`` called ``name`` (default: the kind), built from ``options``."""
    cls = load(kind)
    try:
        return cls(name or kind, **options)
    except TypeError as exc:
        raise ConfigError(f"Bad options for the {kind} backend {name or kind!r}: {exc}") from exc


def from_mapping(name: str, config: Mapping[str, Any]) -> Backend:
    """A backend from a configuration table: ``kind`` plus that kind's options."""
    options = dict(config)
    kind = options.pop("kind", None)
    if not isinstance(kind, str):
        raise ConfigError(f"Backend {name!r} has no 'kind'")
    return create(kind, name, **options)
