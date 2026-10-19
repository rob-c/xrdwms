# xrdwms

Grid jobs as dask tasks. One job description (`JobSpec`) runs on any grid workload manager
xrdwms has a backend for; a `JobManager` submits, polls in bulk, retries, renews credentials and
keeps every job in an on-disk journal, so nothing is lost when the client stops. `xrdwms.dask`
makes each grid job a dask task: waiting costs a coroutine, not a thread, so one dask worker
holds thousands of queued jobs.

Status: **pre-alpha**. Backends today: **ARC** (through [xrdarc](https://github.com/rob-c/xrdarc),
X.509/VOMS proxies or WLCG tokens) and **local** (processes on this machine, for trying a
workflow before sending it to the grid). DIRAC/LHCbDIRAC (DiracX), PanDA and HTCondor
(including Fermilab's jobsub_lite) are planned; see `../plans/`.

Built on dask.distributed, [xrdcore](https://github.com/rob-c/xrdcore),
[xrdsec](https://github.com/rob-c/xrdsec) (credentials and their lifetimes, `xrdsec.lifetime`),
and xgfalclient (checking and moving the data jobs read and write, every scheme through one
gfal2-compatible context, which hands `root://` to xrdclient).

## From the shell

```console
$ xrdarc proxy                                   # or voms-proxy-init / a WLCG token
$ xrdwms submit -b arc:ce.example.org/main -t 30m -o out.txt -- /bin/sh -c 'hostname > out.txt'
3f2a9c1e0b7d4e55  queued      sh  arc:ce.example.org/main:https://ce.example.org:443/arex/rest/1.0/jobs/6Q5N...
$ xrdwms wait 3f2a                               # unique key prefixes work
3f2a9c1e0b7d4e55  done        sh  ...  (exit 0)
$ xrdwms get 3f2a -D results
$ xrdwms status --all --json                     # the storage-client report format
```

Backends can be named inline (`arc:<ce>[/<queue>]`, `local`) or in
`~/.config/xrdwms/config.toml`:

```toml
journal = "~/.local/state/xrdwms/journal.sqlite"   # the default
staging = "davs://se.example.org/user/me/xrdwms"   # where inputs too big for a sandbox go

[poll]
interval = 60          # seconds; grows to max_interval while nothing changes

[backends.gridlab]
kind = "arc"
ce = "arc-ce.arctest.local"
queue = "main"
auth = "auto"          # x509 | token
```

Jobs submitted through the ARC backend are also written to ARC's job list, so `xrdarc stat` and
`arcstat` see them.

## From Python, with dask

```python
import dask
from dask.distributed import Client
import xrdwms.dask as gw

cluster = gw.gateway_cluster(backends={"ce": {"kind": "arc", "ce": "ce.example.org"}},
                             journal="~/work/run1.sqlite")
client = Client(cluster)

# A plain executable: a future of a JobResult (record + downloaded sandbox).
spec = gw.JobSpec("hello", "/bin/hostname")
result = gw.submit_job(client, spec, backend="ce").result()
print(result.path("stdout.txt").read_text())

# A Python function as a grid job (needs the same Python minor version + cloudpickle on the node).
@gw.grid_task(backend="ce", resources=gw.Resources(cores=1, walltime_s=3600), max_retries=2)
def simulate(seed: int) -> float:
    import random
    return random.Random(seed).random()

total = dask.delayed(sum)([simulate(s) for s in range(100)])
print(total.compute())
```

Function jobs pickle the call with cloudpickle. The function's own module goes *by value* when it
is your code rather than an installed package, so script and notebook functions run without being
installed on the node; installed packages they import must be there. The runner checks the Python
version first and reports a mismatch as a clear `RemoteError` (`PythonVersionMismatch`).

Cancelling a grid task's dask future cancels its grid job (after a one-second grace period, in case
the same key is asked for again).

Calls of a `grid_task` are *pure* by default: the job key is dask's token of the call, so with an
on-disk journal the same graph computed again after a crash re-attaches to the jobs it had
already submitted rather than sending them twice.

Large results should go to storage: return a `GridRef(url)` and read it downstream with fsspec.
`xrdwms.dask` calls `xrdclient.fsspec_impl.register_fsspec()` on the client and every worker,
so xrdclient's filesystems for `root://`, `davs://` and the rest win even where `fsspec-xrootd`
is installed too.

## Without dask

```python
import asyncio, xrdwms

async def main():
    arc = xrdwms.backends.create("arc", "ce", ce="ce.example.org", queue="main")
    async with xrdwms.JobManager([arc], journal=xrdwms.Journal("jobs.sqlite")) as manager:
        keys = await manager.submit([xrdwms.JobSpec("hello", "/bin/hostname")] * 10, backend="ce")
        for record in await manager.wait_all(keys):
            print(record.key, record.state, record.info.exit_code)

asyncio.run(main())
```

## How it behaves

* **One bulk request per backend and round**, never one per job. A burst of submissions is
  batched (`PollPolicy.batch_delay`); polling starts at `interval` and backs off while nothing
  changes.
* **Crash-safe.** A job is in the journal before it is submitted. A job interrupted mid-
  submission is looked up on the backend by its key (`<key>-<attempt>`, carried in the ARC job
  name) before it is sent again.
* **Retries by failure kind.** `INFRA` and `DATA` failures are retried up to
  `JobSpec.max_retries`; `PAYLOAD`, `AUTH` and `LIMIT` failures are not.
* **Large inputs are staged.** With `staging` set, a local input bigger than the backend's
  sandbox limit is uploaded to `<staging>/<key>/<name>` first (through xgfalclient, whatever the
  scheme), and the job fetches it from there.
* **Outputs are checked.** A job the backend calls done whose URL outputs are missing is a `DATA`
  failure.
* **Credentials.** An expiring proxy or token is warned about once; when a fresh proxy appears,
  ARC jobs' delegations are renewed (`xrdsec.lifetime.CredentialWatch`).
* **Shared journals.** Several managers - dask workers, or `xrdwms wait` beside a dask session -
  can use one journal: each owns the jobs it submits (a lease, renewed while it runs), takes over
  jobs whose owner died once that lease lapses (`PollPolicy.lease`, default 2 minutes), and
  leaves everyone else's alone. Waiting on or cancelling another manager's job works through the
  journal.

## Development

```sh
python -m pip install -e '.[dev]'
python -m pytest -q --cov -n 4  # 100 % line and branch; every warning is an error
ruff check src tests tools && ruff format --check src tests && mypy   # mypy: src and tests
complexipy --quiet               # also run by tests/test_code_health.py
tools/interop-docker.sh          # against the docker ARC CE + SLURM cluster (ARC-test/cluster)
tools/stress.sh 1                # opt-in stress suite: tens of thousands of jobs, faults, threads
xrddevtools conformance --repo . # the chain's one configuration standard (from xrd-devtools)
```

The code standard, the banned APIs and how the gates find
[xrd-devtools](https://github.com/rob-c/xrd-devtools) are in
[CONTRIBUTING.md](CONTRIBUTING.md).

