"""Moving and checking a job's data: one ``Storage`` over xgfalclient's context.

Every scheme a grid site runs - ``root://``, ``https://``/``davs://``,
``s3://``, ``srm://``, ``gsiftp://``, ``dcap://``, local files - goes through
one gfal2-compatible context (:func:`xgfalclient.creat_context`), whose
plugin table already routes ``root://`` to xrdclient and the HTTP family to
its HTTP plugin. :class:`GridStorage` therefore does no routing of its own:
it turns local paths into ``file://`` URLs (gfal wants URLs) and gfal's
errors into the :class:`OSError` its callers catch. Credentials are found
the way ``gfal-copy`` finds them (the WLCG search orders; S3 keys from
gfal2's ``[S3]`` configuration groups).

:func:`verify_outputs` is what the manager runs after a backend says a job
is done: every output with a URL destination must be there, or the job
failed with ``FailureKind.DATA`` (which a retry can fix).
"""

from __future__ import annotations

import errno
import os
import threading
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, TypeVar

from .model import JobSpec, is_url

if TYPE_CHECKING:
    from xgfalclient import Gfal2Context

__all__ = ["Exists", "GridStorage", "Storage", "verify_outputs"]

_T = TypeVar("_T")


class Exists(Protocol):
    """The one question :func:`verify_outputs` asks of storage."""

    def exists(self, url: str) -> bool:
        """Whether anything is at ``url``."""
        ...


class Storage(Exists, Protocol):
    """What xrdwms needs from storage. Paths are local files, URLs remote ones."""

    def size(self, url: str) -> int:
        """The size of the file at ``url``."""
        ...

    def checksum(self, url: str, algorithm: str = "adler32") -> str:
        """The file's checksum, as the server computes it."""
        ...

    def copy(self, source: str, target: str) -> None:
        """Copy between any two of: a local path, a URL."""
        ...


class GridStorage:
    """:class:`Storage` over one xgfalclient context, made on first use.

    The context is thread-safe (as gfal2's is), so one is shared by the
    manager's output checks and a local backend's stage-in and stage-out
    threads. A failure is an :class:`OSError` whose ``errno`` is gfal's code.
    ``context`` is one of the caller's own - with credentials or options
    set (``cred_set``, ``set_opt_*``) - used instead of a default one.
    """

    def __init__(self, context: Gfal2Context | None = None) -> None:
        """Nothing is imported or connected until the first call."""
        self._gfal = context
        self._lock = threading.Lock()

    def _context(self) -> Gfal2Context:
        """The shared context; importing xgfalclient waits for the first data operation."""
        with self._lock:
            if self._gfal is None:
                import xgfalclient

                self._gfal = xgfalclient.creat_context()
            return self._gfal

    def _call(self, operation: Callable[[Gfal2Context], _T]) -> _T:
        """``operation(context)``, with a gfal error raised as the ``OSError`` callers catch."""
        context = self._context()
        from xgfalclient import GError

        try:
            return operation(context)
        except GError as exc:
            raise OSError(exc.code, str(exc)) from exc

    def exists(self, url: str) -> bool:
        """A ``stat`` that succeeds; ``False`` only for ``ENOENT``, any other failure raises."""
        try:
            self._call(lambda context: context.stat(_as_url(url)))
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                return False
            raise
        return True

    def size(self, url: str) -> int:
        """``stat().st_size``."""
        return int(self._call(lambda context: context.stat(_as_url(url))).st_size)

    def checksum(self, url: str, algorithm: str = "adler32") -> str:
        """The server's checksum value (just the digest, no ``algorithm:`` prefix)."""
        return str(self._call(lambda context: context.checksum(_as_url(url), algorithm.upper())))

    def copy(self, source: str, target: str) -> None:
        """gfal's ``filecopy``, overwriting, with the target's directories made first.

        Parents are created as a CE's stage-out (and ``xrdcp``) would, local
        or remote.
        """

        def filecopy(context: Gfal2Context) -> None:
            params = context.transfer_parameters()
            params.overwrite = True
            params.create_parent = True
            context.filecopy(params, _as_url(source), _as_url(target))

        self._call(filecopy)


def _as_url(location: str) -> str:
    """A local path as a ``file://`` URL (gfal wants URLs); a URL as it is."""
    return location if is_url(location) else Path(os.path.abspath(location)).as_uri()


def verify_outputs(spec: JobSpec, storage: Exists) -> list[str]:
    """Why each URL output of ``spec`` is not where it should be; empty when all are.

    Sandbox outputs are not checked here: they are the backend's to keep.
    """
    problems: list[str] = []
    for output in spec.outputs:
        if output.in_sandbox:
            continue
        try:
            if not storage.exists(output.destination):
                problems.append(f"{output.name}: nothing at {output.destination}")
        except OSError as exc:
            problems.append(f"{output.name}: cannot check {output.destination}: {exc}")
    return problems


def url_outputs(spec: JobSpec) -> Sequence[str]:
    """The URL destinations of ``spec``'s outputs."""
    return [o.destination for o in spec.outputs if not o.in_sandbox]
