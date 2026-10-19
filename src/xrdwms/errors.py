"""What xrdwms raises: one family, joined to xrdcore's vocabulary.

Every error is an :class:`xrdcore.errors.XrdError`, so code that already
catches the storage clients' errors catches these too. A failed *job* is not
an exception until someone waits on it: :class:`JobFailedError` carries the
job's last :class:`~xrdwms.model.JobInfo` so a dask task that raised it says
why the grid job failed, not just that it did.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from xrdcore.errors import XrdError

if TYPE_CHECKING:
    from .journal import Record

__all__ = [
    "BackendError",
    "ConfigError",
    "JobFailedError",
    "RemoteError",
    "UnknownJobError",
    "UnsupportedError",
    "WMSError",
]


class WMSError(XrdError):
    """Anything xrdwms itself refuses or reports."""


class ConfigError(WMSError, ValueError):
    """A backend or configuration that cannot be built as written."""


class UnsupportedError(WMSError, ValueError):
    """A ``JobSpec`` asks for something the backend cannot express.

    ``field`` names the spec field (``runtime.container``), so a message can
    say what to change rather than what went wrong inside a translator.
    """

    def __init__(self, backend: str, field: str, why: str = "") -> None:
        """Name the backend and field; ``why`` says what to use instead, when there is something."""
        self.backend = backend
        self.field = field
        detail = f": {why}" if why else ""
        super().__init__(f"The {backend} backend cannot express {field}{detail}")

    def __reduce__(self) -> tuple[type[UnsupportedError], tuple[str, str]]:
        """Pickle by the constructor's arguments (dask moves exceptions between processes)."""
        return type(self), (self.backend, self.field)


class BackendError(WMSError):
    """A backend failed a whole request (the CE was down, the answer made no sense)."""


class UnknownJobError(WMSError, KeyError):
    """No job with that key in the journal."""

    def __str__(self) -> str:
        """The message, not ``KeyError``'s quoted repr of it."""
        return str(self.args[0]) if self.args else ""


class JobFailedError(WMSError):
    """A grid job ended in ``FAILED`` or ``CANCELLED``; ``record`` is its last state."""

    def __init__(self, record: Record) -> None:
        """Keep the job's last record; the message carries its failure's reason."""
        self.record = record
        info = record.info
        why = info.failure.message if info is not None and info.failure else "no reason given"
        super().__init__(f"Grid job {record.key} ({record.spec.name}) {record.state.value}: {why}")

    def __reduce__(self) -> tuple[type[JobFailedError], tuple[Record]]:
        """Pickle with the record, so the reason survives the trip to the dask client."""
        return type(self), (self.record,)


class RemoteError(WMSError):
    """A Python function run as a grid job raised; ``remote_traceback`` is the worker node's."""

    def __init__(self, message: str, remote_traceback: str = "") -> None:
        """``message`` is the remote exception's; ``remote_traceback`` its formatted traceback."""
        self.message = message
        self.remote_traceback = remote_traceback
        super().__init__(message)

    def __reduce__(self) -> tuple[type[RemoteError], tuple[str, str]]:
        """Pickle by the constructor's arguments."""
        return type(self), (self.message, self.remote_traceback)

    def __str__(self) -> str:
        """The exception line, then the worker node's traceback when there is one."""
        if self.remote_traceback:
            return f"{self.message}\n\nRemote traceback:\n{self.remote_traceback}"
        return self.message
