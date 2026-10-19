"""The local backend: jobs as processes on this machine, with the grid's sandbox discipline.

Each job gets a directory under ``root`` holding its inputs (local files
copied, URLs fetched through :mod:`xrdwms.staging`), its ``stdout.txt`` and
``stderr.txt``, and its outputs; outputs with URL destinations are uploaded
when it ends, as a CE would. Everything a job's state depends on is a file
in that directory (its pid, its exit code, a cancel mark), so jobs survive
the client being restarted and :meth:`LocalBackend.find` works from disk.

A job is started with :func:`os.posix_spawn` in a session of its own, and
from then on it is known only by the pid in its sandbox - the same whether
this backend object started it or an earlier run did. No ``Popen`` object
is kept: a job is meant to outlive the object that started it, and a
``Popen`` dropped while its process runs is a resource leak by Python's
account. :func:`_alive` reaps a job that was our child when it ends.

It is for trying a workflow before sending it to the grid, and for tests
that run real payloads (``xrdwms.dask`` function jobs) without a CE. Walltime
is not enforced; containers and runtime environments are not supported.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import time
import uuid
from pathlib import Path

from ..errors import UnsupportedError
from ..model import (
    Capabilities,
    Failure,
    FailureKind,
    JobHandle,
    JobInfo,
    JobSpec,
    JobState,
    KeyedSpec,
    OpResult,
    Retrieved,
    SubmitResult,
)
from ..staging import GridStorage, Storage
from . import SyncBackend

__all__ = ["LocalBackend"]

_META, _PID, _EXIT, _CANCEL, _STAGED, _BROKEN = (
    ".xrdwms-job.json",
    ".xrdwms-pid",
    ".xrdwms-exit",
    ".xrdwms-cancelled",
    ".xrdwms-staged",
    ".xrdwms-unstartable",
)
_WRAPPER = ".xrdwms-run.sh"
#: Runs the payload with its streams in the sandbox, then records how it ended.
#: The exit code is written aside and renamed, so a reader never sees it half-written.
#: Run by ``/bin/sh -c`` with the sandbox as ``$0``: ``posix_spawn`` has no working directory.
_LAUNCH = 'cd "$0" && exec /bin/sh ' + _WRAPPER + ' "$@"'
_SCRIPT = (
    '"$@" > stdout.txt 2> stderr.txt < /dev/null\n'
    "echo $? > .xrdwms-exit.part && mv .xrdwms-exit.part .xrdwms-exit\n"
)
_INTERNAL = frozenset(
    {_META, _PID, _EXIT, _CANCEL, _STAGED, _BROKEN, _WRAPPER, ".xrdwms-exit.part"}
)


def _default_root() -> Path:
    """Where job directories live by default: ``$XDG_STATE_HOME/xrdwms/local``.

    State, not cache: a job's directory is the only record of it, so it must
    survive a cache clean-up and a restart of the client.
    """
    state = os.environ.get("XDG_STATE_HOME") or os.path.join(Path.home(), ".local", "state")
    return Path(state, "xrdwms", "local")


def _alive(pid: int) -> bool:
    """Whether a process with ``pid`` exists (ours or not); a child of ours that ended is reaped."""
    try:
        if os.waitpid(pid, os.WNOHANG)[0] == pid:
            return False
    except ChildProcessError:
        pass  # not our child: ask the kernel instead
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class LocalBackend(SyncBackend):
    """Jobs as local processes; see the module docstring.

    ``root`` holds the job directories (default ``$XDG_STATE_HOME/xrdwms/local``),
    ``max_running`` caps how many run at once (the rest wait, ``QUEUED``),
    ``workdir`` is where relative input paths are read from, and ``storage``
    fetches URL inputs and uploads URL outputs (default :class:`GridStorage`).
    """

    kind = "local"
    capabilities = Capabilities(max_bulk=1000, data_staging=True, find=True)

    def __init__(
        self,
        name: str = "local",
        *,
        root: str | None = None,
        max_running: int | None = None,
        workdir: str | None = None,
        storage: Storage | None = None,
    ) -> None:
        super().__init__(name)
        self.root = Path(root) if root else _default_root()
        self.max_running = max_running or os.cpu_count() or 1
        self.workdir = workdir
        self.storage: Storage = storage or GridStorage()

    # -- submit ------------------------------------------------------------------------------

    def _submit(self, specs: list[KeyedSpec]) -> list[SubmitResult]:
        """Prepare every job's sandbox, then start as many as ``max_running`` allows.

        A job that cannot be prepared fails alone; the others still go in.
        """
        self.root.mkdir(parents=True, exist_ok=True)
        results = [self._submit_one(keyed) for keyed in specs]
        self._start_queued()
        return results

    def _submit_one(self, keyed: KeyedSpec) -> SubmitResult:
        """One job's sandbox: refused, failed to stage, or ready (``QUEUED``) with a handle.

        The sandbox name is random, so a resubmitted key never meets the debris
        of an earlier attempt. The metadata file is written last: a sandbox
        without it is not a job yet, and :meth:`_jobs` ignores it.
        """
        try:
            _check(keyed.spec)
        except UnsupportedError as exc:
            return SubmitResult(keyed.key, failure=Failure(FailureKind.PAYLOAD, str(exc)))
        sandbox = self.root / uuid.uuid4().hex
        sandbox.mkdir()
        try:
            self._stage_in(keyed.spec, sandbox)
        except OSError as exc:
            shutil.rmtree(sandbox, ignore_errors=True)
            return SubmitResult(keyed.key, failure=Failure(FailureKind.DATA, str(exc)))
        meta = {"key": keyed.key, "spec": keyed.spec.to_dict(), "submitted": time.time()}
        (sandbox / _META).write_text(json.dumps(meta))
        return SubmitResult(keyed.key, handle=JobHandle(self.name, sandbox.name))

    def _stage_in(self, spec: JobSpec, sandbox: Path) -> None:
        """Copy (or fetch) every input into the sandbox under its name.

        Relative paths are read from ``workdir`` (or the current directory), as
        the grid reads them from the submission directory.
        """
        base = Path(self.workdir) if self.workdir else Path.cwd()
        for item in spec.inputs:
            target = sandbox / item.name
            if item.is_url:
                self.storage.copy(item.source, str(target))
            else:
                shutil.copy2(base / item.source, target)

    # -- running -----------------------------------------------------------------------------

    def _jobs(self) -> list[Path]:
        """Every job directory under ``root``, sorted by its (random) name, so a scan is stable."""
        if not self.root.is_dir():
            return []
        return sorted(p for p in self.root.iterdir() if (p / _META).is_file())

    def _running(self) -> int:
        """How many jobs are running now; what ``max_running`` is checked against."""
        return sum(1 for job in self._jobs() if self._live(job))

    def _live(self, sandbox: Path) -> bool:
        """Whether the job's process is running: started, not yet ended, still there."""
        pid_file = sandbox / _PID
        if not pid_file.is_file() or (sandbox / _EXIT).is_file():
            return False
        # The exit file is looked for again: the job may have ended while it was asked.
        return _alive(int(pid_file.read_text())) and not (sandbox / _EXIT).is_file()

    def _start_queued(self) -> None:
        """Start waiting jobs while there are free slots.

        Called on every submission and status poll: there is no scheduler
        process, so a slot freed by an ending job is filled the next time
        anyone looks.
        """
        free = self.max_running - self._running()
        for sandbox in self._jobs():
            if free <= 0:
                return
            if any((sandbox / mark).is_file() for mark in (_PID, _CANCEL, _BROKEN)):
                continue
            try:
                self._start(sandbox)
            except OSError as exc:
                # This job cannot run; the others must not be held up by it.
                (sandbox / _BROKEN).write_text(f"Could not start the job: {exc}")
                continue
            free -= 1

    def _start(self, sandbox: Path) -> None:
        """Start the job's process in its sandbox and record its pid.

        Raises :class:`OSError` when the process cannot be spawned; the caller
        marks that job unstartable so the queue moves on.
        """
        spec = _spec(sandbox)
        executable = spec.executable
        # A relative executable shipped as an input is made runnable; an absolute one is the
        # node's own (``/bin/sh``) and is left alone.
        if not os.path.isabs(executable) and (sandbox / executable).is_file():
            (sandbox / executable).chmod(0o755)
            executable = f"./{executable}"
        (sandbox / _WRAPPER).write_text(_SCRIPT)
        environment = {**os.environ, **spec.environment}
        # A session of its own (so its pid is its process group's, for ``killpg``), and
        # nothing of ours on its standard streams: the wrapper redirects the payload's.
        pid = os.posix_spawn(
            "/bin/sh",
            ["/bin/sh", "-c", _LAUNCH, str(sandbox), executable, *spec.arguments],
            environment,
            setsid=True,
            file_actions=[(os.POSIX_SPAWN_OPEN, fd, os.devnull, os.O_RDWR, 0) for fd in (0, 1, 2)],
        )
        (sandbox / _PID).write_text(str(pid))

    # -- monitor -----------------------------------------------------------------------------

    def _status(self, handles: list[JobHandle]) -> list[JobInfo]:
        """Each job's state from its sandbox, after filling any free slots."""
        self._start_queued()
        return [self._info(handle) for handle in handles]

    def _info(self, handle: JobHandle) -> JobInfo:
        """What the sandbox says about one job.

        The marks are read in the order that decides: cancelled beats ended (a job
        killed by its cancel may still leave an exit code), and a job neither started
        nor ended is queued. A pid that is gone without an exit code is a lost
        job (killed from outside, or the machine rebooted).
        """
        sandbox = self.root / handle.id
        if not (sandbox / _META).is_file():
            return JobInfo(handle, JobState.UNKNOWN, native_state="gone")
        if (sandbox / _CANCEL).is_file():
            return JobInfo(handle, JobState.CANCELLED, native_state="cancelled", site="localhost")
        if (sandbox / _BROKEN).is_file():
            failure = Failure(FailureKind.INFRA, (sandbox / _BROKEN).read_text())
            return JobInfo(handle, JobState.FAILED, native_state="unstartable", failure=failure)
        if (sandbox / _EXIT).is_file():
            return self._ended(handle, sandbox)
        if not (sandbox / _PID).is_file():
            return JobInfo(handle, JobState.QUEUED, native_state="queued", site="localhost")
        if self._live(sandbox):
            return JobInfo(handle, JobState.RUNNING, native_state="running", site="localhost")
        if (sandbox / _EXIT).is_file():  # it ended between the two looks
            return self._ended(handle, sandbox)
        failure = Failure(FailureKind.INFRA, "The job's process vanished without an exit code")
        return JobInfo(handle, JobState.FAILED, native_state="lost", failure=failure)

    def _ended(self, handle: JobHandle, sandbox: Path) -> JobInfo:
        """The info of a job whose exit code is on disk: done, failed, or failed at stage-out.

        URL outputs are uploaded the first time an ended job is looked at,
        as a CE uploads them before reporting the job finished.
        """
        # The code is written aside and renamed, so an empty file should never be seen;
        # if one is anyway (emptied by hand), it counts as a failure, never a success.
        code = int((sandbox / _EXIT).read_text().strip() or "1")
        state, native, failure = JobState.DONE, "exited", None
        if code != 0:
            state, failure = (
                JobState.FAILED,
                Failure(FailureKind.PAYLOAD, f"The job exited with code {code}"),
            )
        elif problems := self._stage_out(sandbox):
            state, native = JobState.FAILED, "stage-out"
            failure = Failure(FailureKind.DATA, "; ".join(problems))
        return JobInfo(
            handle,
            state,
            native,
            exit_code=code,
            failure=failure,
            site="localhost",
            ended=(sandbox / _EXIT).stat().st_mtime,
        )

    def _stage_out(self, sandbox: Path) -> list[str]:
        """Upload URL outputs once; the reasons any could not be."""
        if (sandbox / _STAGED).is_file():
            remembered: list[str] = json.loads((sandbox / _STAGED).read_text())
            return remembered
        problems = []
        for output in _spec(sandbox).outputs:
            if output.in_sandbox:
                continue
            source = sandbox / output.name
            try:
                self.storage.copy(str(source), output.destination)
            except OSError as exc:
                problems.append(f"{output.name}: {exc}")
        (sandbox / _STAGED).write_text(json.dumps(problems))
        return problems

    # -- manage ------------------------------------------------------------------------------

    def _cancel(self, handles: list[JobHandle]) -> list[OpResult]:
        """Mark each job cancelled and signal it to stop; one result per handle.

        The mark is written before the signal, so a job that dies of it is
        reported cancelled rather than failed.
        """
        results = []
        for handle in handles:
            sandbox = self.root / handle.id
            if not (sandbox / _META).is_file():
                results.append(OpResult(handle, False, "No such job"))
                continue
            if (sandbox / _EXIT).is_file():
                results.append(OpResult(handle, False, "The job has already ended"))
                continue
            (sandbox / _CANCEL).touch()
            self._kill(sandbox, signal.SIGTERM)
            results.append(OpResult(handle, True))
        return results

    def _kill(self, sandbox: Path, signum: int) -> None:
        """Signal the job's process group, if it was started and is still there."""
        if not (sandbox / _PID).is_file():
            return
        pid = int((sandbox / _PID).read_text())
        try:
            os.killpg(pid, signum)
        except (ProcessLookupError, PermissionError):
            # Gone - or, on macOS, a group of nothing but zombies (EPERM).
            return
        if signum != signal.SIGKILL:
            return
        # Until it is gone (reaped, if it was our child), but not forever: a process in
        # uninterruptible sleep outlives even SIGKILL for a while.
        for _ in range(200):
            if not _alive(pid):
                return
            time.sleep(0.01)

    def _retrieve(self, handle: JobHandle, directory: str) -> Retrieved:
        """Copy the job's sandbox (without the backend's own files) into ``directory``."""
        sandbox = self.root / handle.id
        if not (sandbox / _META).is_file():
            raise FileNotFoundError(f"No such job: {handle}")
        target = Path(directory) / handle.id
        shutil.copytree(
            sandbox, target, dirs_exist_ok=True, ignore=lambda _d, names: _INTERNAL & set(names)
        )
        files = sorted(str(p.relative_to(target)) for p in target.rglob("*") if p.is_file())
        return Retrieved(handle, str(target), tuple(files))

    def _clean(self, handles: list[JobHandle]) -> list[OpResult]:
        """Delete ended (or cancelled) jobs' sandboxes; a running job is refused.

        A cancelled job is killed for good first: SIGTERM may not have been
        enough, and its directory must not be written to while it goes.
        """
        results = []
        for handle in handles:
            sandbox = self.root / handle.id
            if (sandbox / _CANCEL).is_file():
                self._kill(sandbox, signal.SIGKILL)  # a cancelled job may not have died yet
            elif self._live(sandbox):
                results.append(OpResult(handle, False, "The job is still running"))
                continue
            _remove(sandbox)
            results.append(OpResult(handle, True))
        return results

    def _find(self, keys: list[str]) -> dict[str, JobHandle]:
        """The jobs whose submission key is among ``keys``, read from their sandboxes.

        This is what lets a restarted manager reattach to a job it was
        submitting when it stopped.
        """
        wanted = set(keys)
        found = {}
        for sandbox in self._jobs():
            key = json.loads((sandbox / _META).read_text()).get("key")
            if key in wanted:
                found[key] = JobHandle(self.name, sandbox.name)
        return found


def _remove(sandbox: Path) -> None:
    """Delete a sandbox, again if a dying process wrote into it while it went."""
    for _ in range(5):
        shutil.rmtree(sandbox, ignore_errors=True)
        if not sandbox.exists():
            return
        time.sleep(0.05)


def _spec(sandbox: Path) -> JobSpec:
    """The job spec stored in a sandbox's metadata when it was submitted."""
    return JobSpec.from_dict(json.loads((sandbox / _META).read_text())["spec"])


def _check(spec: JobSpec) -> None:
    """Refuse what a local process cannot honour."""
    if spec.runtime.container or spec.runtime.environments or spec.runtime.platform:
        raise UnsupportedError("local", "runtime")
    if spec.placement.sites or spec.placement.banned_sites or spec.placement.tags:
        raise UnsupportedError("local", "placement")
