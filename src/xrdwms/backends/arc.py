"""The ARC backend: jobs on a NorduGrid ARC CE, through xrdarc's REST client.

One backend is one CE (and optionally a default queue). :func:`to_xrsl`
writes a :class:`~xrdwms.model.JobSpec` as user-dialect xRSL; xrdarc does
the rest exactly as ``xrdarc sub`` would - delegation, uploads of local
inputs, the job list - so jobs submitted through xrdwms also show up in
``xrdarc stat`` and ARC's own ``arcstat``.

The idempotency key travels in the job name (``<spec name>~<key>``): ARC has
no free-form job labels, and the name is what A-REX reports back, so
:meth:`ARCBackend.find` can recognise a job after a crash.

URL inputs and outputs are staged by A-REX itself (``data_staging``); the
delegated proxy is what it stages with, so ``renew`` re-delegates.
"""

from __future__ import annotations

import datetime as _dt
import math
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from xrdarc import ARCClient, Auth, Job, discover, load_config
from xrdarc import JobState as ARCState
from xrdarc.client import Outcome, Submission
from xrdarc.errors import ARCError, CredentialError, DescriptionError, NotAllowedError
from xrdarc.xrsl import Relation, Value, unparse
from xrdcore.errors import CredentialError as LibraryCredentialError

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
from . import SyncBackend

__all__ = ["GMLOG", "STDERR", "STDOUT", "ARCBackend", "classify", "job_name", "key_of", "to_xrsl"]

#: What every job's standard streams and A-REX log directory are called in its sandbox.
STDOUT, STDERR, GMLOG = "stdout.txt", "stderr.txt", "gmlog"

#: Separates the spec's name from the idempotency key in the ARC job name.
_KEY_MARK = "~"

#: ARC's generic states, as xrdwms states (FINISHED and FAILED are decided per job).
_STATES = {
    # A state A-REX could not (yet) report: counted towards "lost", not failed at once.
    ARCState.UNDEFINED: JobState.UNKNOWN,
    ARCState.ACCEPTED: JobState.QUEUED,
    # A-REX is downloading the job's inputs (its own data staging).
    ARCState.PREPARING: JobState.STAGING_IN,
    # ARC's SUBMITTING is the CE handing the job to its batch system, not ours:
    # from xrdwms's side the job is accepted and waiting.
    ARCState.SUBMITTING: JobState.QUEUED,
    ARCState.QUEUING: JobState.QUEUED,
    ARCState.HOLD: JobState.HELD,
    ARCState.RUNNING: JobState.RUNNING,
    # A batch-system state ARC has no name for: the job exists and has not ended.
    ARCState.OTHER: JobState.RUNNING,
    ARCState.FINISHING: JobState.FINISHING,
    ARCState.KILLED: JobState.CANCELLED,
}

#: Words in A-REX's failure text, and the kind of failure they mean; first match wins.
_CLASSES: tuple[tuple[FailureKind, tuple[str, ...]], ...] = (
    (FailureKind.AUTH, ("credential", "proxy", "expired", "authoriz", "authentic")),
    (FailureKind.LIMIT, ("wall time", "walltime", "time limit", "memory", "cpu time", "exceeded")),
    (FailureKind.DATA, ("stag", "download", "upload", "input file", "output file", "data")),
)


# -- the translator ----------------------------------------------------------------------


def job_name(spec: JobSpec, key: str) -> str:
    """The ARC job name for ``spec`` submitted under ``key``."""
    return f"{spec.name}{_KEY_MARK}{key}"


def key_of(name: str | None) -> str | None:
    """The idempotency key in an ARC job name, or ``None`` for a job xrdwms did not name."""
    if not name or _KEY_MARK not in name:
        return None
    return name.rsplit(_KEY_MARK, 1)[1] or None


def to_xrsl(spec: JobSpec, key: str) -> str:
    """``spec`` as one user-dialect xRSL job; ``UnsupportedError`` for what ARC cannot say.

    Memory is per core in xRSL, so the job's total is divided by its cores
    (rounded up). ``spec.native["arc"]`` attributes are added last and
    replace any of the same name.
    """
    relations: list[Relation] = []

    def put(attribute: str, *values: Value) -> None:
        relations.append(Relation(attribute, "=", values))

    put("executable", spec.executable)
    if spec.arguments:
        put("arguments", *spec.arguments)
    put("jobname", job_name(spec, key))
    put("stdout", STDOUT)
    put("stderr", STDERR)
    put("gmlog", GMLOG)
    if spec.inputs:
        put("inputfiles", *((i.name, i.source) for i in spec.inputs))
    if spec.outputs:
        put("outputfiles", *((o.name, "" if o.in_sandbox else o.destination) for o in spec.outputs))
    if spec.environment:
        put("environment", *((k, v) for k, v in spec.environment.items()))
    _resources(spec, put)
    _runtime_and_placement(spec, put)
    return unparse(_with_native(relations, spec.native.get("arc", {})))


def _resources(spec: JobSpec, put: Any) -> None:
    """``count``, ``memory`` (per core), ``walltime``, ``cputime`` and ``disk``."""
    res = spec.resources
    if res.gpus:
        raise UnsupportedError("arc", "resources.gpus", "xRSL has no GPU request; use native")
    if res.cores > 1:
        put("count", str(res.cores))
    if res.memory_mb is not None:
        put("memory", str(math.ceil(res.memory_mb / res.cores)))
    if res.walltime_s is not None:
        put("walltime", f"PT{res.walltime_s}S")
    if res.cputime_s is not None:
        put("cputime", f"PT{res.cputime_s}S")
    if res.disk_mb is not None:
        put("disk", str(res.disk_mb))


def _runtime_and_placement(spec: JobSpec, put: Any) -> None:
    """Runtime environments and the queue; containers, platforms and sites are not xRSL."""
    if spec.runtime.container:
        raise UnsupportedError(
            "arc", "runtime.container", "use the CE's container RTE in runtime.environments"
        )
    if spec.runtime.platform:
        raise UnsupportedError("arc", "runtime.platform")
    for environment in spec.runtime.environments:
        put("runtimeenvironment", environment)
    placement = spec.placement
    if placement.sites or placement.banned_sites or placement.tags:
        raise UnsupportedError("arc", "placement.sites/banned_sites/tags", "one backend is one CE")
    if placement.queue:
        put("queue", placement.queue)


def _with_native(relations: list[Relation], native: Mapping[str, Any]) -> list[Relation]:
    """``relations`` with the native attributes added, each replacing any of the same name."""
    extra = [
        Relation(_normalise(attribute), "=", _values(value)) for attribute, value in native.items()
    ]
    replaced = {relation.attribute for relation in extra}
    return [r for r in relations if r.attribute not in replaced] + extra


def _normalise(attribute: str) -> str:
    """ARC's attribute normalisation: lower case, no underscores."""
    return attribute.lower().replace("_", "")


def _values(value: Any) -> tuple[Value, ...]:
    """A native value as xRSL values: a scalar is one literal, a list one per item."""
    if isinstance(value, list | tuple):
        return tuple(
            _values(item) if isinstance(item, list | tuple) else str(item) for item in value
        )
    return (str(value),)


# -- states ------------------------------------------------------------------------------


def classify(errors: Sequence[str], exit_code: int | None) -> FailureKind:
    """The kind of an ARC failure, from A-REX's failure lines and the exit code."""
    text = " ".join(errors).lower()
    for kind, words in _CLASSES:
        if any(word in text for word in words):
            return kind
    return FailureKind.PAYLOAD if exit_code not in (None, 0) else FailureKind.INFRA


def _state(job: Job) -> tuple[JobState, Failure | None]:
    """The xrdwms state of an ARC job, and its failure if it failed."""
    if job.state is ARCState.FINISHED:
        if job.exit_code in (None, 0):
            return JobState.DONE, None
        return JobState.FAILED, Failure(
            FailureKind.PAYLOAD, f"The job exited with code {job.exit_code}"
        )
    if job.state is ARCState.FAILED:
        message = "; ".join(job.errors) or "A-REX gave no reason"
        return JobState.FAILED, Failure(classify(job.errors, job.exit_code), message)
    if job.state is ARCState.DELETED:
        return JobState.FAILED, Failure(FailureKind.INFRA, "The CE deleted the job")
    return _STATES[job.state], None


def _stamp(moment: _dt.datetime | None) -> float | None:
    """A datetime as epoch seconds (what :class:`~xrdwms.model.JobInfo` holds), ``None`` kept."""
    return moment.timestamp() if moment is not None else None


# -- the backend -------------------------------------------------------------------------


class ARCBackend(SyncBackend):
    """Jobs on one ARC CE; see the module docstring.

    ``ce`` is a host name, URL or ``client.conf`` alias; ``queue`` the
    default queue (a spec's ``placement.queue`` wins). ``auth`` is
    ``auto``, ``x509`` or ``token``, as for ``xrdarc``. ``joblist`` keeps
    jobs in ARC's job list too (``True``: the configured one; ``False``:
    none; or a path). ``workdir`` is where relative input paths are read
    from (default: the current directory at submission). ``verify=False``
    skips checking the CE's certificate - for a test CE reached by another
    name, never a real one. ``client`` replaces
    the ``ARCClient`` built from all of these, for tests and for scripts
    that already have one.
    """

    kind = "arc"
    capabilities = Capabilities(
        max_bulk=100,
        data_staging=True,
        proxy_renewal=True,
        x509_auth=True,
        token_auth=True,
        find=True,
    )

    def __init__(
        self,
        name: str = "arc",
        *,
        ce: str,
        queue: str | None = None,
        auth: str = "auto",
        joblist: bool | str = True,
        workdir: str | None = None,
        timeout: float | None = None,
        delegation_lifetime: float | None = None,
        verify: bool = True,
        client: ARCClient | None = None,
    ) -> None:
        super().__init__(name)
        self.ce = ce
        self.queue = queue
        self.auth = Auth[auth.upper()]
        self.joblist = joblist
        self.workdir = workdir
        self.timeout = timeout
        self.delegation_lifetime = delegation_lifetime
        self.verify = verify
        self._arc = client

    @property
    def client(self) -> ARCClient:
        """The ARC client, built (credentials discovered) on first use, on the backend's thread."""
        if self._arc is None:
            config = load_config()
            self._arc = ARCClient(
                config=config,
                credentials=discover(config, auth=self.auth, verify=self.verify),
                joblist=self.joblist,
                timeout=self.timeout,
                delegation_lifetime=self.delegation_lifetime,
            )
        return self._arc

    def _handle(self, job: Job) -> JobHandle:
        """The xrdwms handle of an ARC job: its job URL, which names CE and job at once."""
        return JobHandle(self.name, job.url)

    def _job(self, handle: JobHandle) -> Job:
        """The ARC job a handle names, rebuilt from its URL (no request is made)."""
        return Job.from_url(handle.id)

    # -- submit --------------------------------------------------------------------------

    def _submit(self, specs: list[KeyedSpec]) -> list[SubmitResult]:
        """Check every description first, so one bad spec fails alone; then submit the rest."""
        base = Path(self.workdir) if self.workdir else Path.cwd()
        results: list[SubmitResult | None] = [None] * len(specs)
        ready: list[tuple[int, str]] = []
        for index, keyed in enumerate(specs):
            try:
                text = to_xrsl(keyed.spec, keyed.key)
                self.client.prepare(text, base=base, queue=self.queue)
            except (UnsupportedError, DescriptionError) as exc:
                results[index] = SubmitResult(
                    keyed.key, failure=Failure(FailureKind.PAYLOAD, str(exc))
                )
            else:
                ready.append((index, text))
        if ready:
            for (index, _), result in zip(ready, self._send(specs, ready, base), strict=True):
                results[index] = result
        return [r for r in results if r is not None]

    def _send(
        self, specs: list[KeyedSpec], ready: list[tuple[int, str]], base: Path
    ) -> list[SubmitResult]:
        """Submit the checked descriptions as one multi-job xRSL: one POST for the batch.

        A-REX takes ``+(&...)(&...)`` in a single ``action=new`` request and
        answers per job, in order; xrdarc delegates once and uploads each job's
        inputs. A failure of the request itself fails every job in it.
        """
        keys = [specs[index].key for index, _ in ready]
        texts = [text for _, text in ready]
        description = texts[0] if len(texts) == 1 else "+" + "".join(f"({t})" for t in texts)
        try:
            submissions = self.client.submit(self.ce, description, base=base, queue=self.queue)
        except (CredentialError, NotAllowedError, LibraryCredentialError) as exc:
            return [SubmitResult(k, failure=Failure(FailureKind.AUTH, str(exc))) for k in keys]
        except (ARCError, OSError) as exc:
            return [SubmitResult(k, failure=Failure(FailureKind.INFRA, str(exc))) for k in keys]
        return [self._result(key, sub) for key, sub in zip(keys, submissions, strict=True)]

    def _result(self, key: str, sub: Submission) -> SubmitResult:
        """One job's answer: its handle, or why not - a 4xx is the job's fault, else the CE's."""
        if sub.job is not None:
            return SubmitResult(key, handle=self._handle(sub.job))
        kind = FailureKind.PAYLOAD if 400 <= sub.status < 500 else FailureKind.INFRA
        why = sub.reason or f"The CE refused the job (HTTP {sub.status})"
        return SubmitResult(key, failure=Failure(kind, why))

    # -- monitor ---------------------------------------------------------------------------

    def _status(self, handles: list[JobHandle]) -> list[JobInfo]:
        """One bulk status request for every handle (xrdarc batches it per CE)."""
        outcomes = self.client.status([self._job(h) for h in handles])
        return [self._info(h, o) for h, o in zip(handles, outcomes, strict=True)]

    def _info(self, handle: JobHandle, outcome: Outcome) -> JobInfo:
        """One ARC outcome as a ``JobInfo``: a 404 is a job gone, other failures no news."""
        if not outcome.ok:
            gone = outcome.status == 404 and not outcome.transient
            return JobInfo(
                handle, JobState.UNKNOWN, native_state=outcome.reason, transient=not gone
            )
        job = outcome.job
        state, failure = _state(job)
        return JobInfo(
            handle,
            state,
            native_state=job.rest_state or str(job.state),
            exit_code=job.exit_code,
            failure=failure,
            site=urlparse(job.ce).hostname,
            worker_nodes=job.execution_nodes,
            submitted=_stamp(job.submitted_on_ce or job.submitted),
            started=_stamp(job.started),
            ended=_stamp(job.ended),
        )

    # -- manage ------------------------------------------------------------------------------

    def _results(self, handles: list[JobHandle], outcomes: list[Outcome]) -> list[OpResult]:
        """Pair each handle with xrdarc's outcome for it, as the manager's ``OpResult``."""
        return [OpResult(h, o.ok, o.reason) for h, o in zip(handles, outcomes, strict=True)]

    def _cancel(self, handles: list[JobHandle]) -> list[OpResult]:
        """Ask A-REX to kill the jobs; the end shows up as ``KILLED`` on a later poll."""
        return self._results(handles, self.client.kill([self._job(h) for h in handles]))

    def _clean(self, handles: list[JobHandle]) -> list[OpResult]:
        """Ask A-REX to remove the ended jobs' session directories."""
        return self._results(handles, self.client.clean([self._job(h) for h in handles]))

    def _renew(self, handles: list[JobHandle]) -> list[OpResult]:
        """Re-delegate the current proxy to the jobs, so their data staging keeps working."""
        return self._results(handles, self.client.renew([self._job(h) for h in handles]))

    def _retrieve(self, handle: JobHandle, directory: str) -> Retrieved:
        """Download the job's session directory into ``directory/<job id>``; the files in it.

        ``force`` lets a second retrieve of the same job overwrite the first,
        which is what a rerun after a partial download needs.
        """
        job = self._job(handle)
        path = self.client.retrieve(job, directory, name=job.id, force=True)
        files = sorted(
            str(Path(root, name).relative_to(path))
            for root, _dirs, names in os.walk(path)
            for name in names
        )
        return Retrieved(handle, str(path), tuple(files))

    def _find(self, keys: list[str]) -> dict[str, JobHandle]:
        """List the CE's jobs for this user and match their names against ``keys``."""
        wanted = set(keys)
        rest = self.client.rest(self.ce)
        jobs = [Job(ce=rest.ce, id=job_id) for job_id, _state in rest.jobs()]
        found: dict[str, JobHandle] = {}
        for outcome in self.client.status(jobs) if jobs else []:
            key = key_of(outcome.job.name) if outcome.ok else None
            if key is not None and key in wanted:
                found[key] = self._handle(outcome.job)
        return found

    def _close(self) -> None:
        """Close the ARC client (its connections), if one was ever made."""
        if self._arc is not None:
            self._arc.close()
