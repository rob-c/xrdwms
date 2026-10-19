"""xrdwms: grid jobs as dask tasks, over ARC and other grid workload managers.

The pieces, bottom up:

* :mod:`xrdwms.model` - :class:`JobSpec` and the states, handles and results
  every backend shares;
* :mod:`xrdwms.backends` - the backend interface, and the ARC
  (:mod:`~xrdwms.backends.arc`) and local (:mod:`~xrdwms.backends.local`)
  backends;
* :mod:`xrdwms.manager` - the :class:`JobManager`: bulk submission and
  polling, retries, renewal, all kept in a :class:`Journal`;
* :mod:`xrdwms.dask` - grid jobs as dask tasks (``import xrdwms.dask``; it
  is not imported here, so the rest works without starting dask).

::

    import asyncio, xrdwms

    async def main():
        arc = xrdwms.backends.create("arc", "ce", ce="ce.example.org")
        async with xrdwms.JobManager([arc]) as manager:
            [key] = await manager.submit([xrdwms.JobSpec("hello", "/bin/hostname")], backend="ce")
            print((await manager.wait(key)).state)

    asyncio.run(main())
"""

from __future__ import annotations

from . import backends
from ._version import __version__
from .errors import (
    BackendError,
    ConfigError,
    JobFailedError,
    RemoteError,
    UnknownJobError,
    UnsupportedError,
    WMSError,
)
from .journal import Journal, Record
from .manager import JobManager, ManagerEvent, PollPolicy
from .model import (
    Capabilities,
    Failure,
    FailureKind,
    GridRef,
    Input,
    JobHandle,
    JobInfo,
    JobSpec,
    JobState,
    Output,
    Placement,
    Resources,
    Runtime,
)

__all__ = [
    "BackendError",
    "Capabilities",
    "ConfigError",
    "Failure",
    "FailureKind",
    "GridRef",
    "Input",
    "JobFailedError",
    "JobHandle",
    "JobInfo",
    "JobManager",
    "JobSpec",
    "JobState",
    "Journal",
    "ManagerEvent",
    "Output",
    "Placement",
    "PollPolicy",
    "Record",
    "RemoteError",
    "Resources",
    "Runtime",
    "UnknownJobError",
    "UnsupportedError",
    "WMSError",
    "__version__",
    "backends",
]
