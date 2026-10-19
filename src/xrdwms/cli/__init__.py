"""``xrdwms``: submit and follow grid jobs from the shell, on any configured backend.

Every job goes into the journal, so ``status``, ``wait``, ``get`` and the
rest work on jobs from any earlier run - or from a dask session using the
same journal. Every subcommand takes ``--json``/``--xml`` for the report
format shared with xrdarc and the storage clients.

``-v`` logs what the manager and the backends do (``-vv`` in detail, with
the traceback of an internal error) on stderr; without it, only warnings.

Exit status: 0 when everything worked, 1 when some jobs failed, 2 for a
usage error, 3 for credentials, 4 for configuration, 5 for a backend that
failed, 6 for a local problem, 70 for a bug, 130 after Ctrl-C and 141 when
the reader of a pipe went away. 0-2, 70, 130 and 141 are
:class:`xrdcore.cli.ExitCode`'s, the same in every tool on the stack; 3-6
are xrdwms's own.
"""

from __future__ import annotations

import argparse
import asyncio
import enum
import re
import sys
from collections.abc import Callable, Coroutine, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

from xrdcore import cli as _cli
from xrdcore import log as _xrdlog
from xrdcore import report as output
from xrdcore.errors import AuthenticationError

from .. import backends as _backends
from .._version import __version__
from ..config import Config, load_config, resolve_backend
from ..errors import ConfigError, JobFailedError, UnknownJobError, WMSError
from ..journal import Journal, Record, default_path
from ..manager import JobManager
from ..model import Input, JobSpec, JobState, Output, Placement, Resources, Runtime

__all__ = ["ExitCode", "main"]


@_cli.extends(_cli.ExitCode)
class ExitCode(enum.IntEnum):
    """What ``xrdwms`` exits with: :class:`xrdcore.cli.ExitCode`'s codes, plus xrdwms's own.

    ``extends`` refuses, at import, a table that drops or renumbers a shared
    code; 3-6 are this tool's.
    """

    OK = _cli.ExitCode.OK
    #: Some jobs failed, or a named job is not in the journal.
    FAILED = _cli.ExitCode.FAILED
    USAGE = _cli.ExitCode.USAGE
    #: No usable credential, or a backend refused it.
    CREDENTIALS = 3
    #: The configuration file is wrong.
    CONFIG = 4
    #: A backend failed the request.
    BACKEND = 5
    #: A local problem: a file to read or write, a bad value.
    LOCAL = 6
    INTERNAL = _cli.ExitCode.INTERNAL
    INTERRUPTED = _cli.ExitCode.INTERRUPTED
    #: The reader of standard output went away (``xrdwms status | head``).
    BROKEN_PIPE = _cli.ExitCode.BROKEN_PIPE


class _Usage(WMSError):
    """The command line asks for something impossible."""


# -- the context every command gets -------------------------------------------------------


class Session:
    """The parsed options, the configuration, the journal and (on demand) a manager."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.config: Config = load_config(args.config)
        path = args.journal or self.config.journal or default_path()
        self.journal = Journal(path)
        self.policy = self.config.policy
        if getattr(args, "interval", None):
            self.policy = replace(self.policy, interval=args.interval)

    def manager(self, names: set[str]) -> JobManager:
        """A manager over the backends ``names`` (configured, or shorthands)."""
        built = [_backends.from_mapping(n, resolve_backend(n, self.config)) for n in sorted(names)]
        return JobManager(
            built, journal=self.journal, policy=self.policy, staging=self.config.staging
        )

    def selected(self) -> list[Record]:
        """The jobs named by ``KEYS``, else by ``--group``/``--all``, else the active ones."""
        keys = getattr(self.args, "keys", None) or []
        if keys:
            return [self.journal.get(_key(self.journal, k)) for k in keys]
        group = getattr(self.args, "group", None)
        active = None if getattr(self.args, "all", False) else True
        return self.journal.records(group=group, active=active)

    def close(self) -> None:
        self.journal.close()


def _key(journal: Journal, text: str) -> str:
    """A full key from a unique prefix of one (like git's short hashes)."""
    if journal.find(text) is not None:
        return text
    matches = [r.key for r in journal.records() if r.key.startswith(text)]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise UnknownJobError(f"No job {text!r} in the journal")
    raise _Usage(f"{text!r} matches {len(matches)} jobs; give more of the key")


def _run(coroutine: Coroutine[Any, Any, int]) -> int:
    return asyncio.run(coroutine)


def _say(text: str) -> None:
    print(text, flush=True)


def _line(record: Record) -> str:
    info = record.info
    detail = ""
    if info is not None and info.failure is not None and record.state is not JobState.DONE:
        detail = f"  ({info.failure.kind.value}: {info.failure.message})"
    elif info is not None and info.exit_code is not None:
        detail = f"  (exit {info.exit_code})"
    handle = str(record.handle) if record.handle else record.backend
    return f"{record.key}  {record.state.value:<11} {record.spec.name}  {handle}{detail}"


def _record(record: Record) -> None:
    output.record(
        "result",
        key=record.key,
        name=record.spec.name,
        backend=record.backend,
        state=record.state.value,
        handle=str(record.handle) if record.handle else None,
        attempt=record.attempt,
        group=record.group,
        exit_code=record.info.exit_code if record.info else None,
        failure=(record.info.failure.message if record.info and record.info.failure else None),
    )


def _report(records: Sequence[Record]) -> int:
    for record in records:
        _say(_line(record))
        _record(record)
    failed = any(r.state in (JobState.FAILED, JobState.CANCELLED) for r in records)
    return int(ExitCode.FAILED if failed else ExitCode.OK)


# -- submit --------------------------------------------------------------------------------


def _period(text: str) -> int:
    """Seconds from ``90``, ``90s``, ``30m``, ``2h`` or ``1d``."""
    match = re.fullmatch(r"\s*(\d+)\s*([smhd]?)\s*", text)
    if match is None:
        raise argparse.ArgumentTypeError(f"not a time period: {text!r} (use 90, 30m, 2h, 1d)")
    return int(match[1]) * {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}[match[2]]


def _pair(text: str, sep: str = "=") -> tuple[str, str]:
    left, found, right = text.partition(sep)
    return (left, right) if found else (left, "")


def _spec_from_args(args: argparse.Namespace) -> JobSpec:
    if args.spec:
        import json
        import tomllib

        path = Path(args.spec)
        text = path.read_text()
        data = tomllib.loads(text) if path.suffix == ".toml" else json.loads(text)
        return JobSpec.from_dict(data)
    if not args.command:
        raise _Usage("give the executable to run (after --), or --spec FILE")
    executable, *arguments = args.command
    native: dict[str, dict[str, str]] = {}
    for item in args.native:
        target, value = _pair(item)
        kind, _, attribute = target.partition(".")
        if not attribute:
            raise _Usage(f"--native wants KIND.ATTRIBUTE=VALUE, not {item!r}")
        native.setdefault(kind, {})[attribute] = value
    return JobSpec(
        name=args.name or Path(executable).name,
        executable=executable,
        arguments=tuple(arguments),
        environment=dict(_pair(e) for e in args.env),
        inputs=tuple(Input(src, name) for src, name in (_pair(i) for i in args.input)),
        outputs=tuple(
            Output(name, dest or "sandbox") for name, dest in (_pair(o) for o in args.output)
        ),
        resources=Resources(
            cores=args.cores,
            memory_mb=args.memory,
            walltime_s=args.walltime,
            cputime_s=args.cputime,
            disk_mb=args.disk,
        ),
        runtime=Runtime(environments=tuple(args.rte)),
        placement=Placement(queue=args.queue),
        max_retries=args.retries,
        native=native,
    )


async def _submit(session: Session) -> int:
    args = session.args
    spec = _spec_from_args(args)
    async with session.manager({args.backend}) as manager:
        keys = await manager.submit([spec] * args.count, backend=args.backend, group=args.group)
        records = await manager.submitted(keys)
        if args.wait:
            records = await manager.wait_all(keys)
        # Reported inside the block: an ``if`` as the block's last statement
        # leaves Python 3.11 a jump to after it that coverage cannot see taken.
        return _report(records)


def _setup_submit(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-b", "--backend", required=True, help="configured name, arc:CE[/QUEUE] or local"
    )
    parser.add_argument("--spec", help="a job file (TOML or JSON JobSpec) instead of a command")
    parser.add_argument("-n", "--name", help="job name (default: the executable's)")
    parser.add_argument("-c", "--cores", type=int, default=1)
    parser.add_argument("-m", "--memory", type=int, metavar="MB", help="memory for the whole job")
    parser.add_argument("-t", "--walltime", type=_period, help="e.g. 90, 30m, 2h")
    parser.add_argument("--cputime", type=_period)
    parser.add_argument("--disk", type=int, metavar="MB")
    parser.add_argument("-i", "--input", action="append", default=[], metavar="SRC[=NAME]")
    parser.add_argument("-o", "--output", action="append", default=[], metavar="NAME[=URL]")
    parser.add_argument("-e", "--env", action="append", default=[], metavar="NAME=VALUE")
    parser.add_argument("-q", "--queue")
    parser.add_argument("--rte", action="append", default=[], help="runtime environment")
    parser.add_argument("--retries", type=int, default=0, help="resubmissions on grid failures")
    parser.add_argument("--native", action="append", default=[], metavar="KIND.ATTR=VALUE")
    parser.add_argument("--count", type=int, default=1, help="submit this many copies")
    parser.add_argument("-g", "--group", help="tag the jobs with a group name")
    parser.add_argument("-w", "--wait", action="store_true", help="wait for the jobs to end")
    parser.add_argument("--interval", type=float, help="seconds between polls while waiting")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="-- EXECUTABLE [ARGS...]")


# -- the job commands ----------------------------------------------------------------------


def _selection(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("keys", nargs="*", metavar="KEY", help="job keys (unique prefixes do)")
    parser.add_argument("-g", "--group", help="the jobs of this group")
    parser.add_argument("-a", "--all", action="store_true", help="finished jobs too")


async def _status(session: Session) -> int:
    records = session.selected()
    if session.args.refresh and records:
        async with session.manager({r.backend for r in records}) as manager:
            records = await manager.refresh([r.key for r in records])
    return _report(records)


async def _wait(session: Session) -> int:
    records = session.selected()
    if not records:
        return int(ExitCode.OK)
    async with session.manager({r.backend for r in records}) as manager:
        manager.resume()
        done = await manager.wait_all([r.key for r in records])
    return _report(done)


async def _cancel(session: Session) -> int:
    records = session.selected()
    async with session.manager({r.backend for r in records}) as manager:
        answers = await manager.cancel([r.key for r in records])
    failed = False
    for record, answer in zip(records, answers, strict=True):
        _say(f"{record.key}  {'cancel requested' if answer.ok else answer.reason}")
        output.record("result", key=record.key, ok=answer.ok, reason=answer.reason)
        failed = failed or not answer.ok
    return int(ExitCode.FAILED if failed else ExitCode.OK)


async def _get(session: Session) -> int:
    records = session.selected()
    directory = session.args.directory
    failed = False
    async with session.manager({r.backend for r in records}) as manager:
        for record in records:
            try:
                got = await manager.retrieve(record.key, directory)
            except (OSError, WMSError) as exc:
                _say(f"{record.key}  {exc}")
                output.record("result", key=record.key, ok=False, reason=str(exc))
                failed = True
                continue
            _say(f"{record.key}  {got.path}")
            output.record("result", key=record.key, ok=True, path=got.path, files=list(got.files))
    return int(ExitCode.FAILED if failed else ExitCode.OK)


async def _clean(session: Session) -> int:
    records = session.selected()
    async with session.manager({r.backend for r in records}) as manager:
        answers = await manager.clean([r.key for r in records], forget=session.args.forget)
    failed = False
    for record, answer in zip(records, answers, strict=True):
        _say(f"{record.key}  {'cleaned' if answer.ok else answer.reason}")
        output.record("result", key=record.key, ok=answer.ok, reason=answer.reason)
        failed = failed or not answer.ok
    return int(ExitCode.FAILED if failed else ExitCode.OK)


async def _forget(session: Session) -> int:
    keys = [r.key for r in session.selected()]
    removed = session.journal.remove(keys) if keys else 0
    _say(f"forgot {removed} jobs")
    output.record("result", forgotten=removed)
    return int(ExitCode.OK)


async def _events(session: Session) -> int:
    import datetime as dt

    for record in session.selected():
        for event in session.journal.events(record.key):
            when = dt.datetime.fromtimestamp(event.time).isoformat(timespec="seconds")
            _say(f"{record.key}  {when}  {event.state.value:<11} {event.message}")
            output.record(
                "event",
                key=record.key,
                time=event.time,
                state=event.state.value,
                message=event.message,
            )
    return int(ExitCode.OK)


async def _backends_cmd(session: Session) -> int:
    for name, table in sorted(session.config.backends.items()):
        options = ", ".join(f"{k}={v}" for k, v in table.items() if k != "kind")
        _say(f"{name}  {table.get('kind', '?')}  {options}")
        output.record(
            "result",
            name=name,
            backend_kind=table.get("kind"),
            options={k: v for k, v in table.items() if k != "kind"},
        )
    _say("kinds: " + ", ".join(sorted(_backends.kinds())))
    return int(ExitCode.OK)


_Runner = Callable[[Session], Coroutine[Any, Any, int]]

#: subcommand -> (runner, extra-options setup, help)
_COMMANDS: dict[str, tuple[_Runner, Callable[[argparse.ArgumentParser], None] | None, str]] = {
    "submit": (_submit, _setup_submit, "submit a job"),
    "status": (_status, None, "show jobs from the journal (--refresh asks the backends)"),
    "wait": (_wait, None, "wait for jobs to finish, polling their backends"),
    "cancel": (_cancel, None, "cancel jobs"),
    "get": (_get, None, "download jobs' sandboxes"),
    "clean": (_clean, None, "remove jobs from their backends (--forget: and the journal)"),
    "forget": (_forget, None, "remove jobs from the journal only"),
    "events": (_events, None, "show every state change of jobs"),
    "backends": (_backends_cmd, None, "list configured backends and known kinds"),
}


def build_parser() -> argparse.ArgumentParser:
    """The whole command line."""
    parser = output.Parser(prog="xrdwms", description=__doc__.split("\n\n")[0])
    parser.add_argument("-V", "--version", action="version", version=f"xrdwms {__version__}")
    sub = parser.add_subparsers(dest="command_name", metavar="COMMAND", parser_class=output.Parser)
    for name, (_runner, setup, help_text) in _COMMANDS.items():
        child = sub.add_parser(name, help=help_text, description=help_text)
        if setup is not None:
            setup(child)
        elif name != "backends":
            _selection(child)
        if name == "status":
            child.add_argument("-r", "--refresh", action="store_true", help="ask the backends now")
        if name == "wait":
            child.add_argument("--interval", type=float, help="seconds between polls")
        if name == "get":
            child.add_argument("-D", "--directory", default=".", help="where to put them")
        if name == "clean":
            child.add_argument("--forget", action="store_true", help="also forget them")
        child.add_argument("--config", help="configuration file (default: $XRDWMS_CONFIG)")
        child.add_argument("--journal", help="journal file (default: $XRDWMS_JOURNAL)")
        child.add_argument(
            "-v", "--verbose", action="count", default=0, help="more logging (-v info, -vv debug)"
        )
        output.flags(child)
    output.flags(parser)
    return parser


#: Exception classes and their exit codes, most specific first
#: (:func:`xrdcore.cli.exit_code` takes the first that matches; anything
#: unlisted is a bug, 70). ``_Usage`` is a ``WMSError`` too, and a job that
#: is not in the journal or failed is 1, like a failed job in a listing.
_EXIT_CODES: tuple[_cli.ExitRule, ...] = (
    (_Usage, ExitCode.USAGE),
    (UnknownJobError, ExitCode.FAILED),
    (AuthenticationError, ExitCode.CREDENTIALS),
    (ConfigError, ExitCode.CONFIG),
    (JobFailedError, ExitCode.FAILED),
    (WMSError, ExitCode.BACKEND),
    (OSError, ExitCode.LOCAL),
    (ValueError, ExitCode.LOCAL),
)

#: The loggers ``-v`` turns up: the manager's, and the ARC client's under
#: the ``arc`` backend, which is where a job's requests are made.
_LOGGERS = ("xrdwms", "xrdarc")


def exit_code_for(exc: BaseException) -> ExitCode:
    """The exit code for what ended a command (see :data:`_EXIT_CODES`)."""
    return ExitCode(_cli.exit_code(exc, _EXIT_CODES))


def _main(argv: Sequence[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command_name is None:
        parser.print_help()
        return int(ExitCode.USAGE)
    if args.command_name == "submit" and args.command[:1] == ["--"]:
        args.command = args.command[1:]
    _xrdlog.configure(_xrdlog.verbosity_level(args.verbose), loggers=_LOGGERS)
    output.identify(args.command_name)
    runner = _COMMANDS[args.command_name][0]
    sessions: list[Session] = []

    def body() -> int:
        """Open the session (it reads the configuration, so it may fail) and run the command."""
        sessions.append(Session(args))
        return _run(runner(sessions[0]))

    # Ctrl-C (130) and a closed pipe (a quiet 141) are xrdcore.cli.run's;
    # every other failure is xrdcore.cli.Fatal's: one line, its code from
    # the table, and a bug's traceback logged at -vv rather than printed.
    fatal = _cli.Fatal("xrdwms", _EXIT_CODES, logger="xrdwms")
    try:
        return _cli.run(body, interrupted=fatal.interrupted, fatal=fatal)
    finally:
        for session in sessions:
            session.close()


def main(argv: Sequence[str] | None = None) -> int:
    """The ``xrdwms`` console script."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    return output.run_cli("xrdwms", arguments, lambda: _main(arguments))
