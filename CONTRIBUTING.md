# Contributing to xrdwms

xrdwms is the jobs layer on top of the chain: one job model over grid
workload managers, built on xrdcore, xrdsec, xrdhttp and the clients.
Changes are welcome when they keep a job's record in the journal the truth
(nothing submitted twice, nothing lost when a manager stops) and leave
protocol work to the libraries and clients it is built on.

## Development setup

```console
$ python -m pip install -e '.[dev]'
$ python -m pytest -q --cov -n 4
```

The suite is hermetic: the ARC backend runs against xrdarc's fake A-REX.
`tools/interop-docker.sh` runs it against a real ARC CE and SLURM cluster
in docker, and `tools/stress.sh` is the opt-in stress suite (tens of
thousands of jobs, faults, threads).

## Definition of done

A change is ready when all of these pass, as CI runs them:

```console
$ ruff check src tests tools
$ ruff format --check src tests
$ mypy
$ complexipy --quiet
$ python -m pytest -q -p no:cacheprovider -n 4 --cov
$ xrddevtools conformance --repo .
```

- **Coverage is 100 % of lines and branches**, reached by a test that makes
  the case happen; every warning is an error.
- **[The code standard](#the-code-standard)** holds for every line.
- **The chain's gates** pass: the take-over scorecard
  (`tests/test_scorecard.py`), the release-metadata gate
  (`tests/test_release_metadata.py`), `xrddevtools conformance` and
  `xrddevtools dupes`, all from [xrd-devtools](#xrd-devtools). CI
  (`.github/workflows/ci.yml`) is xrd-devtools' reusable workflows, against
  candidate checkouts of the six packages xrdwms is built on.
- **Changelog.** Add user-visible changes to [CHANGELOG.md](CHANGELOG.md).

## The code standard

<!-- >>> xrddevtools sync: code-standard (generated from xrd-devtools/src/xrddevtools/standard) >>> -->
Every repository of the chain is held to one code standard, and every line
of it is held to the standard: old code passes the same checks as new code.

- **Ruff** selects `E`, `F`, `W`, `I`, `UP`, `B`, `C4`, `SIM`, `RUF`, the
  high-confidence security checks `S102`, `S307`, `S506`, `S602`, `S608`,
  `S609`, `S701` and the banned-API rule `TID251` (see *Banned APIs*), with
  `ignore = []` and no per-file ignores, over `src`, `tests`; `ruff format`
  formats it, 100 columns wide.
- **mypy** is strict (Python 3.11) over `src`, `tests`. A test function may
  leave its fixtures unannotated where `pyproject.toml` allows it, but its
  body is still checked; only a named third-party module that ships no types
  may be imported untyped, with the reason beside it.
- **complexipy** allows no function above 15, over `src`, `tests`, with no
  snapshot of old offenders.
- **Coverage** is 100 % of lines and branches, reached by a test that makes
  each case happen. The only exclusions are `if TYPE_CHECKING:`,
  `@overload`, `raise NotImplementedError`, `\.\.\.$`, `if __name__ ==
  .__main__.:`; there is no `omit`.
- **Every warning is an error** (`filterwarnings = ["error"]`): ours are
  fixed, not filtered.
- **No suppression comment**: no `# noqa`, `# pragma: no cover`, `# fmt:
  off`, `# complexipy: ignore` or `# nosec`. A `# type: ignore[code]` only
  where it cannot be avoided (a test passing a wrongly typed value on
  purpose), with its error code and the reason beside it or just above.
- **No snapshot or baseline** of old findings, for any tool.

The settings behind these rules are the managed blocks of `pyproject.toml`
(between `# >>> xrddevtools sync` markers), and this section is one of the
managed blocks of this file (between `<!-- >>> xrddevtools sync` markers).
`xrddevtools sync` writes them all from the standard, kept once in
xrd-devtools (`src/xrddevtools/standard`); `xrddevtools conformance`, which
CI runs, fails if one is edited by hand. To change the standard, change it
there and run `xrddevtools sync --workspace ..` over every repository.
<!-- <<< xrddevtools sync: code-standard <<< -->

## Banned APIs

<!-- >>> xrddevtools sync: banned-apis (generated from xrd-devtools/src/xrddevtools/standard) >>> -->
Ruff's `TID251` rule refuses the APIs whose job another part of the chain
does, in every tree ruff checks (`src`, `tests`), with no per-file ignore
and no `noqa`; the list is `[tool.ruff.lint.flake8-tidy-imports.banned-api]`
in `pyproject.toml`, and each entry's message names what to use instead. A
need for a banned API is met by the library that owns it growing an API,
with its tests - never by an exception here.

| Banned | Use instead | Why |
|---|---|---|
| `http.client` | HTTP is xrdhttp's: requests through xrdhttp.session.Session; connections, and the http.client types a client names (IncompleteRead, HTTPMessage, ...), through xrdhttp.connection | one pooled session with the chain's redirect, retry, `100-continue` and credential rules; `xrdhttp.connection` re-exports the stdlib's own `http.client` classes, so `except`/`isinstance` mean the same |
| `urllib3`, `requests`, `httpx` | the chain has no third-party HTTP stack: use xrdhttp.session.Session (pooling, redirects, retries; TLS from xrdsec.tls) | one HTTP stack for the whole chain, with its TLS, redirect and retry rules |
| `urllib.request` | use xrdhttp.session.Session: urllib's opener has none of the chain's TLS, redirect or retry rules | urllib's opener knows none of the grid's TLS (CAs, CRLs, proxies) and follows redirects with credentials attached |
| `gssapi` | Kerberos GSS-API is xrdsec.krb5.gss (the xrdsec[krb5] extra), one wrapper for every client | one GSS-API wrapper, behind the `xrdsec[krb5]` extra |
| `krb5` | pykrb5 is not a dependency: credential caches are xrdsec.krb5.caches / xrdsec.krb5.ccache, GSS-API is xrdsec.krb5.gss | the pure-Python ccache, KCM and keyring readers replaced it |
| `ssl.PROTOCOL_TLS`, `ssl.PROTOCOL_TLS_CLIENT`, `ssl.PROTOCOL_TLS_SERVER`, `ssl._create_unverified_context`, `ssl.create_default_context` | TLS contexts are built by xrdsec.tls.client_context (grid CAs, CRLs, proxies, caching); GSI data channels by xrdsec.gsi.gss.data_contexts; tests by xrdsec.testing.pki.PKI.client_context / server_context | grid CAs, CRLs, proxies and context caching in one builder. `ssl.SSLContext` itself is not banned, so annotations still name it: building one needs a `PROTOCOL_*` constant (without one the constructor warns, and warnings are errors), and that is what the ban catches |
| `concurrent.futures.ThreadPoolExecutor` | a fan-out is xrdcore.fanout.run_all (first failure raised, the rest stopped); a long-lived pool is xrdcore.fanout.Pool | the first failure raised as itself and the rest stopped; a long-lived pool made on first use and made again in a forked child |
| `xml.etree.ElementTree.XML`, `xml.etree.ElementTree.XMLParser`, `xml.etree.ElementTree.XMLPullParser`, `xml.etree.ElementTree.fromstring`, `xml.etree.ElementTree.fromstringlist`, `xml.etree.ElementTree.iterparse`, `xml.etree.ElementTree.parse`, `xml.dom.minidom`, `xml.dom.pulldom`, `xml.parsers.expat`, `xml.sax.make_parser`, `xml.sax.parse`, `xml.sax.parseString` | parse XML with xrdcore.safexml.fromstring, which refuses DOCTYPEs and entities | it refuses DOCTYPEs, so no entity is declared, expanded or fetched. Building and writing trees (`ET.Element`, `ET.tostring`) and `xml.sax.saxutils.escape` stay allowed |
| `botocore`, `boto3` | S3 signing and the S3 codec are xrdhttp.s3 (the xrdhttp[s3] extra); only xrdhttp.s3 imports botocore | S3 signing and the S3 codec, behind the `xrdhttp[s3]` extra |
<!-- <<< xrddevtools sync: banned-apis <<< -->

xrdwms bans no package of the chain: it sits on top and depends on every
one of them. Its pools are the two kinds the `ThreadPoolExecutor` entry
names: each `SyncBackend`'s single thread and the executor
`gateway_cluster` hands dask are long-lived `xrdcore.fanout.Pool`s; the
stress suite's "many callers at once" fan-outs are `xrdcore.fanout.run_all`.

## xrd-devtools

<!-- >>> xrddevtools sync: devtools (generated from xrd-devtools/src/xrddevtools/standard) >>> -->
The chain's gates and tools - the take-over scorecard and the
release-metadata and maintainability gates, `xrddevtools conformance`,
`dupes` and `sync`, the release pre-flight and the packaging tools - live in
[xrd-devtools](https://github.com/rob-c/xrd-devtools). It is never uploaded
to PyPI and cannot be a dependency (PyPI refuses direct-URL requirements),
so it is used from a checkout:

- **A developer** keeps it beside this repository (`../xrd-devtools`, the
  workspace layout). pytest's `pythonpath` lists `../xrd-devtools/src`, so
  the gate tests find it with nothing installed; `pip install -e
  ../xrd-devtools` once gives the `xrddevtools` command.
- **CI** checks it out under `.dependencies/xrd-devtools` (the shared
  `candidates` action always does), which `pythonpath` lists too. It loads
  the plugin with `pytest -p xrddevtools.pytest_plugin`, which fails at
  start-up if it cannot be imported, and sets `XRD_DEVTOOLS_REQUIRED=1`, so
  a missing radon or complexipy fails rather than skips. This repository's
  workflows are xrd-devtools' reusable workflows and actions: CI runs only
  once `rob-c/xrd-devtools` exists.
- **Anywhere else** - a wheel-only test environment, a distribution build -
  `pip install -e` a checkout or put its `src` on `PYTHONPATH`. Without one,
  the gate tests (`tests/test_scorecard.py`,
  `tests/test_release_metadata.py`) skip, saying why.
<!-- <<< xrddevtools sync: devtools <<< -->
