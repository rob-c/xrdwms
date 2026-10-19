# Changelog

## [0.4.0] - Unreleased

- First version: JobSpec model, async JobManager with an SQLite journal, ARC and local
  backends, dask integration (gateway cluster, grid tasks, plain jobs), `xrdwms` command line.
- Stage-in: inputs above a backend's sandbox limit go to a `staging` storage URL first.
- `xrdwms.staging.register_fsspec()`, called by the dask layer: xrdclient's fsspec filesystems
  take precedence over other packages' for the same protocols.
- ARC backend `verify=False` for test CEs reached by another name.
- Journal schema 2 (migrated in place from 1): job ownership with one lease per manager, so
  managers can share a journal without submitting or polling each other's jobs; cancel requests
  for jobs another manager owns.
- JobManager keeps its jobs in memory and writes in batches: about 7x faster at 10k jobs, and
  linear rather than quadratic in the number of jobs.
- Fixed: a job cancelled while an earlier chunk of a bulk submission was in flight could be
  resubmitted and run.
- Opt-in stress suite (`tests/stress`, `tools/stress.sh`).
- CI (`.github/workflows/ci.yml`, the first): xrd-devtools' reusable
  workflows - ruff, mypy, complexipy and `xrddevtools conformance`; the
  pytest matrix with the 100 % coverage gate; the duplicate and conformance
  gates over all seven repositories of the chain - against candidate
  checkouts of the six packages xrdwms is built on. It needs
  `rob-c/xrd-devtools` to exist. xrdwms is not yet held to the take-over
  scorecard (worse than the corpus median on four of twelve measurements).

### Moved into the libraries (pre-0.4.0 consolidation)

- `staging.register_fsspec()` is `xrdclient.fsspec_impl.register_fsspec()` (public, documented
  and tested there); the dask layer imports it from xrdclient, and `xrdwms.staging` no longer
  has it (re-exporting it would have made `import xrdwms` import fsspec and xrdclient).
- `credentials.Credential`, `CredentialProvider`, `ProxyFile` and `BearerToken` are
  `xrdsec.lifetime`'s; `xrdwms.credentials` re-exports the same objects. The manager's expiry
  warning and renewal detection is `xrdsec.lifetime.CredentialWatch` (one warning per expiry,
  renewal per backend), and `JobManager.credentials` is a read-only property.
- `GridStorage` no longer routes by scheme: every operation goes through one xgfalclient
  context, whose plugins already send `root://` to xrdclient. `XRDCLIENT_SCHEMES` is removed.
  Changes: a gfal error is an `OSError` with gfal's errno (it escaped as `GError` before, so
  `exists` on a missing `srm://` file raised); copies create the target's parent directories
  remotely too (`create_parent`); `s3://` keys come from gfal2's `[S3]` configuration, as for
  `gfal-copy`; `GridStorage(context)` takes a configured context of the caller's.
- The `dev` extra pins `mypy>=2.3,<2.5`, as the other repositories do; xgfalclient is
  type-checked (it ships `py.typed`) instead of ignored.
- The command line's shared exit codes (0, 1, 2, 70, 130) are `xrdcore.cli.ExitCode`'s, and
  Ctrl-C and a closed pipe go through `xrdcore.cli.run`: `xrdwms status | head` now exits a
  quiet 141 (`ExitCode.BROKEN_PIPE`, new) instead of reporting a local error (6).
- Every other failure ends through `xrdcore.cli.Fatal` (was `cli._fatal`), with the exit
  codes as one ordered table read by `exit_code_for`; the line is now redacted, and under
  `--json` the line is one `message` record instead of two.
- New `-v`/`--verbose` on every subcommand (`-v` info, `-vv` debug, including an internal
  error's traceback), through `xrdcore.log.configure` on the `xrdwms` and `xrdarc` loggers;
  without it warnings are printed as `WARNING: message` on stderr.

### Code health

- Banned APIs (ruff `TID251`, `[tool.ruff.lint.flake8-tidy-imports.banned-api]`), the same
  list as the clients': src and tests may not use `http.client`, a third-party HTTP stack,
  `gssapi`/`krb5`, hand-built TLS contexts, `ThreadPoolExecutor`, the stdlib XML parsers or
  botocore; each message names the library replacement (README, *Banned APIs*).
  `SyncBackend`'s one thread and `gateway_cluster`'s dask executor are now
  `xrdcore.fanout.Pool`s (same names and sizes; the backend's thread starts with its first
  call, and a forked child gets its own), and the stress suite's concurrent callers run on
  `xrdcore.fanout.run_all`.
- Held to the family's take-over scorecard (`tests/test_scorecard.py`, xrd-devtools' plugin;
  `radon` joined the `dev` extra), and passing all twelve measurements: `JobManager`'s working
  set - owned jobs, pending journal writes, waiters, subscribers - is `xrdwms._workset.WorkingSet`,
  and the decisions it takes on a backend's answer (is it news, retry or not, the next poll
  interval, which inputs to stage) are plain functions in `xrdwms._policy`; `PollPolicy`,
  `ManagerEvent`, `failure_for`, `new_key` and `UNSUBMITTED` are the same objects, still
  importable from `xrdwms.manager`. The complex manager methods (`_apply`, `_stage_in`,
  `_submit_chunk`, `_check_credentials`, `cancel`) are split into named steps; the backends,
  journal, model and fake backend are documented field by field. `dask.run_function` (the task
  a `GridTask` call runs) takes the `GridTask` itself instead of its eight fields.
- The fake-CE fixtures (`ce`, `make_ce`, `credentials`, `make_arc`) and the interop gate come
  from xrdarc's new `xrdarc.testing.fixtures` plugin, shared with xrdarc's own suite, instead
  of copies in `tests/conftest.py`.
- `tests/conftest.py` loads xrdsec's `xrdsec.testing.fixtures` plugin
  (the hermetic environment, `home`, `pki`, `grid_env`) instead of its own
  copy; its `pytest_xrdsec_hermetic` hook clears xrdwms's own variables and
  hides ARC's system `client.conf`, and interop tests are marked `ambient`.
- The same standard as the other repositories, with nothing grandfathered: ruff's common rule
  set with no ignores (the three it had are fixed), mypy strict over `src` *and* `tests` (the
  worker-node runner `_runner.py` is typed instead of exempt), complexipy at 15 with no
  snapshot (`tests/test_code_health.py` runs it), the shared coverage `exclude_also`, 100 %
  line and branch coverage, and `filterwarnings = ["error"]`. No `noqa`, no `fmt: skip`; the
  only `type: ignore`s left are three in `tests/test_model.py` that pass loose values on
  purpose to pin `JobSpec`'s coercion.
- The journal's SQL is all constants: key lists and the terminal states travel as one JSON
  parameter read with SQLite's `json_each`, optional filters as `(? IS NULL OR ...)`, and
  `update` uses `COALESCE`. `get_many` no longer chunks its keys.
- The local backend starts jobs with `os.posix_spawn` in a session of their own and keeps no
  `Popen` objects: a job outlives the backend object by design, and a dropped `Popen` whose
  process still runs is a `ResourceWarning`. `_alive` reaps finished children by pid.
- `backends.load` returns a `BackendClass` (the `cls(name, **options)` constructor contract)
  rather than `type[Backend]`; `staging.verify_outputs` takes any `staging.Exists` (an object
  with `exists`), of which `Storage` is a subprotocol.
- Complexity: `ARCBackend._result`, `JobManager._poll_if_due`, `_clean_on_backends`,
  `_submit_answers` and `_clean_unasked` were split out of the functions over 15.
- Tests: optional fields are narrowed through `tests/_shared.py` helpers (`info_of`,
  `failure_of`, `handle_of`) instead of ignored; `tests/stress` is a package (`stress._load`,
  `stress.conftest`) so mypy can check both conftests; the dask gateway test that depended on
  earlier tests' journal is split into two self-contained tests.
- Release metadata is xrd-devtools' release gate:
  `tests/test_release_metadata.py` is `release_gate(__file__)` from
  `xrddevtools.pytest_plugin` - one version source (`_version.py`, read by
  hatch), a CHANGELOG led by exactly that version's well-formed section,
  project metadata and README that name this repository, and a release
  workflow built through the shared publish action.
- The standard's settings in `pyproject.toml` (ruff rules, the banned-API
  table, mypy's strictness and allowed overrides, complexipy, coverage,
  pytest's warnings filter) and CONTRIBUTING's *The code standard*, *Banned
  APIs* table and *xrd-devtools* sections are managed blocks, written by
  `xrddevtools sync` from the standard kept once in xrd-devtools;
  `xrddevtools conformance` fails a hand edit between the markers. The
  repository's own keys and notes sit beside the blocks.
- Version `0.4.0` (was `0.1.0.dev0`): xrdwms is released with the rest of
  the chain, last, after xrdarc, and its changelog heading is in the chain's
  form (`## [0.4.0] - Unreleased`), as the release gate requires;
  `[project.urls]` gains Issues and Changelog.
- `.github/workflows/publish.yml`, new: a `v*` tag builds with xrd-devtools'
  shared `publish` action (the pre-flight requires every package of the
  chain on PyPI first) and uploads to PyPI with trusted publishing
  (workflow `publish.yml`, environment `pypi`).
- `CONTRIBUTING.md`, new: the code standard, the banned APIs (moved from the
  README) and how the gates find xrd-devtools.
