"""Grid jobs as dask tasks.

A grid task is an ``async`` dask task that submits one grid job through a
:class:`~xrdwms.manager.JobManager` living on the dask worker, awaits it,
and returns its result. Waiting costs a coroutine, not a thread, so one
worker holds thousands of queued grid jobs; the manager polls their
backends in bulk. dask does the rest: dependencies, retries, cancellation
(cancelling the future cancels the grid job), the dashboard.

Grid tasks need the resource :data:`GRID` and a worker that has it with many
slots - the *gateway*. :func:`gateway_cluster` builds a local one, with a
small thread pool for ordinary tasks so that the gateway's large slot count
never becomes thousands of threads::

    import xrdwms.dask as gw
    from dask.distributed import Client

    cluster = gw.gateway_cluster(backends={"ce": {"kind": "arc", "ce": "ce.example.org"}})
    client = Client(cluster)

    @gw.grid_task(backend="ce", resources=gw.Resources(walltime_s=3600))
    def simulate(seed):
        ...                                 # runs on the grid's worker node
        return summary

    totals = sum_up([simulate(s) for s in range(100)])   # an ordinary dask.delayed
    totals.compute()

A job's key is fixed when the graph is built: for a ``pure`` task (the
default) it is dask's token of the call, so with an on-disk journal the same
graph computed again after a crash *re-attaches* to the jobs already
submitted instead of sending them twice. ``pure=False`` makes every call a
new job.

Plain executables run with :func:`submit_job`; Python functions with
:func:`grid_task` need a Python of the client's minor version, with
cloudpickle, on the worker node (see :mod:`xrdwms.payload`).
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Generic, ParamSpec, TypeVar

from dask.base import annotate, tokenize
from dask.delayed import Delayed, delayed
from distributed import Client, Future, LocalCluster, WorkerPlugin, get_worker
from xrdclient.fsspec_impl import register_fsspec
from xrdcore.fanout import Pool

from . import backends as _backends
from .errors import JobFailedError, WMSError
from .journal import Journal, Record
from .manager import JobManager, PollPolicy
from .model import GridRef, Input, JobSpec, JobState, Output, Placement, Resources, Runtime
from .payload import function_spec, read_result, write_call

__all__ = [
    "GRID",
    "GridPlugin",
    "GridRef",
    "GridTask",
    "Input",
    "JobResult",
    "JobSpec",
    "Output",
    "Placement",
    "Resources",
    "Runtime",
    "gateway_cluster",
    "grid_task",
    "install",
    "job_key",
    "submit_job",
]

#: The dask resource a grid task needs; the gateway worker has many of it.
GRID = "xrdwms-grid"
#: The name the plugin is registered under on workers.
PLUGIN = "xrdwms"
#: Seconds a cancelled grid task must stay cancelled before its grid job is cancelled.
CANCEL_GRACE = 1.0

P = ParamSpec("P")
R = TypeVar("R")

BackendConfigs = Mapping[str, Mapping[str, Any]]


@dataclass(frozen=True, slots=True)
class JobResult:
    """A finished grid job: its journal record and where its sandbox was downloaded."""

    key: str
    record: Record
    directory: str | None = None

    def path(self, name: str) -> Path:
        """A file of the downloaded sandbox; ``WMSError`` if nothing was downloaded."""
        if self.directory is None:
            raise WMSError(f"Job {self.key}'s sandbox was not retrieved")
        return Path(self.directory) / name


class GridPlugin(WorkerPlugin):
    """Runs a :class:`JobManager` on each worker it is installed on.

    It is built from plain data (backend configuration tables, a journal
    path, a :class:`PollPolicy`) so it pickles to remote workers, where each
    backend is created with :func:`xrdwms.backends.from_mapping`.
    """

    name = PLUGIN

    def __init__(
        self,
        backends: BackendConfigs,
        *,
        journal: str | None = None,
        policy: PollPolicy | None = None,
        workdir: str | None = None,
        staging: str | None = None,
    ) -> None:
        """Keep only plain, picklable settings; :meth:`setup` makes the backends and manager."""
        self.configs = {name: dict(config) for name, config in backends.items()}
        self.journal_path = journal
        self.policy = policy
        self.workdir = workdir
        self.staging = staging
        self.manager: JobManager | None = None
        self.root = Path(workdir) if workdir else None
        self.worker: Any = None
        self.loop: asyncio.AbstractEventLoop | None = None
        #: Job keys of cancelled grid tasks, with their dask keys, awaiting the next sweep.
        self._doomed: dict[str, str] = {}
        self._sweep_due = False

    async def setup(self, worker: Any) -> None:
        """Build the backends and start the manager, on the worker's event loop."""
        built = [_backends.from_mapping(name, cfg) for name, cfg in self.configs.items()]
        # Without a path the journal lives and dies with this worker: jobs survive a
        # client restart only when the cluster is given a journal file.
        journal = Journal(self.journal_path or ":memory:")
        self.manager = JobManager(built, journal=journal, policy=self.policy, staging=self.staging)
        register_fsspec()
        await self.manager.start()
        self.root = Path(self.workdir or worker.local_directory) / "xrdwms"
        self.worker = worker
        self.loop = asyncio.get_running_loop()

    def transition(self, key: Any, start: str, finish: str, **kwargs: Any) -> None:
        """Cancel the grid job of a grid task that dask has cancelled.

        distributed does not cancel a running async task when its future is
        released - it lets it run to the end and drops the result - so the
        grid job would run on unwanted. When one of our tasks (its key ends
        in ``-<job key>``) leaves ``executing`` that way and is still not wanted after
        :data:`CANCEL_GRACE` seconds (not resumed by a new request for the
        same key), its job is cancelled - all such jobs in one request.
        """
        # A released future shows up as executing -> released (the task itself is then
        # "cancelled" inside the worker until its coroutine returns).
        if start != "executing" or finish not in ("released", "cancelled"):
            return
        if not isinstance(key, str) or self.manager is None or self.loop is None:
            return
        # Decided after the grace period, not now: a task cancelled the moment it started may
        # not have submitted its job yet, and would then submit one nobody wants.
        self._doomed[key.rsplit("-", 1)[-1]] = key
        if not self._sweep_due:
            self._sweep_due = True
            self.loop.call_later(CANCEL_GRACE, self._sweep)

    def _sweep(self) -> None:
        """Cancel, in one request, the jobs of grid tasks that are still not wanted."""
        self._sweep_due = False
        doomed, self._doomed = self._doomed, {}
        assert self.manager is not None and self.loop is not None
        jobs = []
        for job, key in doomed.items():
            task = self.worker.state.tasks.get(key)
            if task is not None and task.state not in ("cancelled", "released", "forgotten"):
                continue  # asked for again meanwhile
            record = self.manager.journal.find(job)
            if record is not None and not record.terminal:
                jobs.append(job)
        if jobs:
            self.loop.create_task(self.manager.cancel(jobs))

    async def teardown(self, worker: Any) -> None:
        """Stop the manager and close its backends and journal."""
        if self.manager is not None:
            await self.manager.close()
            self.manager.journal.close()


def install(
    client: Client,
    backends: BackendConfigs,
    *,
    journal: str | os.PathLike[str] | None = None,
    policy: PollPolicy | None = None,
    workdir: str | None = None,
    staging: str | None = None,
) -> None:
    """Install the plugin (and so a manager) on the client's workers.

    ``staging`` is where large local inputs are uploaded before submission
    (see :class:`~xrdwms.manager.JobManager`). xrdclient's fsspec filesystems
    are registered here and on each worker, so ``root://`` and ``davs://``
    URLs - GridRefs among them - open the same way everywhere.
    """
    plugin = GridPlugin(
        backends,
        journal=os.fspath(journal) if journal else None,
        policy=policy,
        workdir=workdir,
        staging=staging,
    )
    client.register_plugin(plugin, name=PLUGIN)
    register_fsspec()


def gateway_cluster(
    *,
    backends: BackendConfigs | None = None,
    grid_slots: int = 10_000,
    threads: int | None = None,
    journal: str | os.PathLike[str] | None = None,
    policy: PollPolicy | None = None,
    workdir: str | None = None,
    staging: str | None = None,
    **kwargs: Any,
) -> LocalCluster:
    """A one-worker, in-process cluster that can hold ``grid_slots`` grid jobs at once.

    Ordinary tasks share ``threads`` threads (default: the CPU count), so the
    large slot count never turns into threads. With ``backends``, the
    plugin is installed as soon as the cluster is up.
    """
    # The cluster's executor for ordinary tasks: a long-lived pool, not a fan-out.
    executor = Pool("xrdwms", threads or os.cpu_count() or 1).executor()
    kwargs.setdefault("dashboard_address", ":8787")
    cluster = LocalCluster(
        n_workers=1,
        processes=False,
        threads_per_worker=grid_slots,
        resources={GRID: grid_slots},
        executor=executor,
        **kwargs,
    )
    if backends is not None:
        with Client(cluster) as client:
            install(
                client, backends, journal=journal, policy=policy, workdir=workdir, staging=staging
            )
    return cluster


# -- on the worker -----------------------------------------------------------------------


def _plugin() -> GridPlugin:
    """This worker's plugin with its manager running; :class:`WMSError` when it is not.

    A grid task scheduled on a worker without the plugin would otherwise
    fail with an attribute error deep in the manager; this says what to do.
    """
    worker = get_worker()
    plugin = worker.plugins.get(PLUGIN)
    if not isinstance(plugin, GridPlugin) or plugin.manager is None or plugin.root is None:
        raise WMSError(
            "xrdwms is not installed on this dask worker: call xrdwms.dask.install(client, ...) "
            "or use xrdwms.dask.gateway_cluster(backends=...)"
        )
    return plugin


async def _run(
    manager: JobManager, key: str, spec: JobSpec, backend: str, group: str | None
) -> Record:
    """Submit (or re-attach to) the job ``key`` and wait for it; cancel it if we are cancelled."""
    await manager.submit([spec], backend=backend, keys=[key], group=group)
    try:
        record = await manager.wait(key)
    except asyncio.CancelledError:
        # Shielded: the cancel request must reach the backend even though this very
        # coroutine is being cancelled; otherwise the grid job would run on, unwanted.
        await asyncio.shield(manager.cancel([key]))
        raise
    if record.state is not JobState.DONE:
        raise JobFailedError(record)
    return record


async def _fetch(plugin: GridPlugin, key: str) -> str:
    """The job's sandbox, downloaded once into the worker's xrdwms directory."""
    assert plugin.manager is not None and plugin.root is not None
    target = plugin.root / "results" / key
    # The marker is written only after a complete download, so a task rerun after a
    # worker restart reuses a finished download and redoes an interrupted one.
    marker = target / ".xrdwms-retrieved"
    if marker.is_file():
        return marker.read_text()
    target.mkdir(parents=True, exist_ok=True)
    retrieved = await plugin.manager.retrieve(key, str(target))
    marker.write_text(retrieved.path)
    return retrieved.path


async def run_job(
    spec: JobSpec, backend: str, key: str, group: str | None = None, fetch: bool = True
) -> JobResult:
    """The task :func:`submit_job` runs: one plain job, its sandbox downloaded if ``fetch``."""
    plugin = _plugin()
    assert plugin.manager is not None
    record = await _run(plugin.manager, key, spec, backend, group)
    directory = await _fetch(plugin, key) if fetch else None
    return JobResult(key, record, directory)


async def run_function(
    task: GridTask[Any, Any], key: str, args: tuple[Any, ...], kwargs: dict[str, Any]
) -> Any:
    """The task a :class:`GridTask` call runs: ``task.function(*args, **kwargs)`` on the grid.

    The whole :class:`GridTask` travels with the call (it is a small frozen
    dataclass: the function, its job template and where to run it), so the
    worker builds the job from the same description the client saw. The
    call is written to the worker's staging area, the job runs it there, and
    its pickled result comes back in the sandbox.
    """
    plugin = _plugin()
    assert plugin.manager is not None and plugin.root is not None
    staging = write_call(plugin.root / "calls" / key, task.function, args, kwargs)
    spec = function_spec(task.template, staging, task.python)
    await _run(plugin.manager, key, spec, task.backend, task.group)
    return read_result(await _fetch(plugin, key))


# -- on the client -----------------------------------------------------------------------


def job_key(*parts: Any, pure: bool = True) -> str:
    """A job key: dask's token of ``parts`` when ``pure``, else a fresh random one."""
    if not pure:
        return uuid.uuid4().hex[:16]
    return hashlib.sha256(tokenize(*parts).encode()).hexdigest()[:16]


def submit_job(
    client: Client,
    spec: JobSpec,
    *,
    backend: str,
    group: str | None = None,
    fetch: bool = True,
    pure: bool = False,
    **kwargs: Any,
) -> Future[Any]:
    """Run a plain job; a dask future of its :class:`JobResult`.

    ``kwargs`` go to ``client.submit`` (``retries=``, ``priority=``...).
    """
    key = job_key("job", spec.to_dict(), backend, group, pure=pure)
    return client.submit(
        run_job,
        spec,
        backend,
        key,
        group,
        fetch,
        resources={GRID: 1},
        pure=False,
        key=f"xrdwms-{spec.name}-{key}",
        **kwargs,
    )


@dataclass(frozen=True)
class GridTask(Generic[P, R]):
    """A Python function that runs as a grid job when called (lazily, as ``dask.delayed``)."""

    #: What runs on the worker node; it and its arguments must pickle.
    function: Callable[P, R]
    #: The manager's name of the backend that runs the jobs.
    backend: str
    #: The job every call becomes, before the call itself is added as an input.
    template: JobSpec
    #: The interpreter on the worker node that unpickles and runs the call.
    python: str = "python3"
    #: The journal group the jobs are listed under.
    group: str | None = None
    #: Same arguments, same job key: a recomputed graph reattaches to the jobs it had.
    pure: bool = True
    options: dict[str, Any] = field(default_factory=dict)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Delayed:
        """A delayed call: one grid job when computed."""
        key = self._key(args, kwargs)
        with annotate(resources={GRID: 1}):
            call: Delayed = delayed(run_function)(
                self, key, args, kwargs, dask_key_name=f"{self.template.name}-{key}"
            )
            return call

    def submit(self, client: Client, *args: P.args, **kwargs: P.kwargs) -> Future[Any]:
        """Run it now, as a future (``client.submit``), rather than lazily."""
        key = self._key(args, kwargs)
        return client.submit(
            run_function,
            self,
            key,
            args,
            kwargs,
            resources={GRID: 1},
            pure=False,
            key=f"{self.template.name}-{key}",
        )

    def _key(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
        """The job key of one call: the same arguments give the same key when ``pure``."""
        return job_key(
            self.function,
            args,
            kwargs,
            self.template.to_dict(),
            self.backend,
            self.group,
            pure=self.pure,
        )


def grid_task(
    function: Callable[P, R] | None = None,
    *,
    backend: str,
    name: str | None = None,
    resources: Resources | None = None,
    runtime: Runtime | None = None,
    placement: Placement | None = None,
    environment: Mapping[str, str] | None = None,
    max_retries: int = 0,
    python: str = "python3",
    group: str | None = None,
    pure: bool = True,
    native: Mapping[str, Mapping[str, Any]] | None = None,
) -> Any:
    """Decorate a function so that calling it makes a grid job (see :class:`GridTask`).

    ``python`` is the interpreter on the worker node; the other arguments are
    the :class:`~xrdwms.model.JobSpec` fields the job gets.
    """

    def wrap(target: Callable[P, R]) -> GridTask[P, R]:
        template = JobSpec(
            name=name or target.__name__,
            executable=python,
            environment=dict(environment or {}),
            resources=resources or Resources(),
            runtime=runtime or Runtime(),
            placement=placement or Placement(),
            max_retries=max_retries,
            native=dict(native or {}),
        )
        return GridTask(target, backend, template, python, group, pure)

    return wrap(function) if function is not None else wrap
