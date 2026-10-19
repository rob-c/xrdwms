"""The xrdwms command line, run in process against the local backend and a temporary journal."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from xrdwms import JobState, Journal, cli
from xrdwms.cli import ExitCode, _period, exit_code_for
from xrdwms.errors import ConfigError, JobFailedError, UnknownJobError, WMSError
from xrdwms.journal import Record
from xrdwms.model import JobSpec


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    journal = tmp_path / "journal.sqlite"
    config = tmp_path / "config.toml"
    config.write_text(
        f"[poll]\ninterval = 0.01\nmax_interval = 0.05\nbatch_delay = 0.0\n"
        f'[backends.here]\nkind = "local"\nroot = "{tmp_path / "jobs"}"\n'
    )
    monkeypatch.setenv("XRDWMS_JOURNAL", str(journal))
    monkeypatch.setenv("XRDWMS_CONFIG", str(config))
    monkeypatch.chdir(tmp_path)
    return {"journal": journal, "tmp": tmp_path}


def run(capsys: pytest.CaptureFixture[str], *argv: str) -> tuple[int, str, str]:
    code = cli.main(list(argv))
    out, err = capsys.readouterr()
    return code, out, err


def run_json(capsys: pytest.CaptureFixture[str], *argv: str) -> tuple[int, dict[str, Any]]:
    code = cli.main([*argv, "--json"])
    out, _err = capsys.readouterr()
    return code, json.loads(out)


def keys(env: dict[str, Any]) -> list[str]:
    with Journal(env["journal"]) as journal:
        return [r.key for r in journal.records()]


def test_submit_wait_get_events_clean(
    env: dict[str, Any], capsys: pytest.CaptureFixture[str]
) -> None:
    (env["tmp"] / "in.txt").write_text("data")
    code, out, err = run(
        capsys,
        "submit",
        "-b",
        "here",
        "-n",
        "greet",
        "-i",
        "in.txt",
        "-o",
        "out.txt",
        "-e",
        "WHO=grid",
        "-t",
        "5m",
        "-c",
        "1",
        "-m",
        "100",
        "--cputime",
        "1h",
        "--disk",
        "10",
        "--retries",
        "1",
        "-g",
        "g1",
        "--wait",
        "--interval",
        "0.01",
        "--",
        "/bin/sh",
        "-c",
        'cat in.txt > out.txt; echo "hi $WHO"',
    )
    assert code == 0, err
    assert "done" in out and "greet" in out and "(exit 0)" in out
    [key] = keys(env)
    code, out, _ = run(capsys, "status", key[:6], "--all")
    assert code == 0 and key in out
    code, out, _ = run(capsys, "status", "-g", "g1", "--all")
    assert key in out
    code, out, _ = run(capsys, "events", key)
    assert "added" in out and "submitted" in out
    code, out, _ = run(capsys, "get", key, "-D", str(env["tmp"] / "res"))
    assert code == 0
    [job_dir] = (env["tmp"] / "res").iterdir()
    assert (job_dir / "out.txt").read_text() == "data"
    assert (job_dir / "stdout.txt").read_text() == "hi grid\n"
    code, out, _ = run(capsys, "clean", key, "--forget")
    assert code == 0 and "cleaned" in out and keys(env) == []


def test_submit_without_waiting_then_wait_and_json(
    env: dict[str, Any], capsys: pytest.CaptureFixture[str]
) -> None:
    code, out, _ = run(capsys, "submit", "-b", "here", "--count", "2", "--", "/bin/echo", "x")
    assert code == 0 and out.count("queued") == 2
    code, doc = run_json(capsys, "wait")
    assert code == 0
    states = {r["state"] for r in doc["records"] if r["kind"] == "result"}
    assert states == {"done"}
    code, doc = run_json(capsys, "status", "--all", "--refresh")
    assert code == 0 and len([r for r in doc["records"] if r["kind"] == "result"]) == 2
    code, out, _ = run(capsys, "wait")  # nothing active any more
    assert code == 0 and out == ""


def test_a_failed_job_exits_1(env: dict[str, Any], capsys: pytest.CaptureFixture[str]) -> None:
    code, out, _ = run(capsys, "submit", "-b", "here", "-w", "--", "/bin/sh", "-c", "exit 4")
    assert code == ExitCode.FAILED and "(payload: The job exited with code 4)" in out


def test_spec_files(env: dict[str, Any], capsys: pytest.CaptureFixture[str]) -> None:
    toml = env["tmp"] / "job.toml"
    toml.write_text('name = "t"\nexecutable = "/bin/echo"\narguments = ["toml"]\n')
    jsn = env["tmp"] / "job.json"
    jsn.write_text(json.dumps({"name": "j", "executable": "/bin/echo"}))
    for path in (toml, jsn):
        code, out, _ = run(capsys, "submit", "-b", "here", "--spec", str(path), "-w")
        assert code == 0, out


def test_cancel_and_forget(env: dict[str, Any], capsys: pytest.CaptureFixture[str]) -> None:
    run(capsys, "submit", "-b", "here", "--", "/bin/sleep", "30")
    [key] = keys(env)
    code, out, _ = run(capsys, "cancel", key)
    assert code == 0 and "cancel requested" in out
    run(capsys, "wait", key)
    code, out, _ = run(capsys, "cancel", key)
    assert code == ExitCode.FAILED and "cancelled" in out
    code, out, _ = run(capsys, "forget", key)
    assert "forgot 1" in out and keys(env) == []
    code, out, _ = run(capsys, "forget")
    assert "forgot 0" in out


def test_failures_of_get_and_clean(env: dict[str, Any], capsys: pytest.CaptureFixture[str]) -> None:
    run(capsys, "submit", "-b", "here", "--", "/bin/sleep", "30")
    [key] = keys(env)
    code, out, _ = run(capsys, "clean", key)
    assert code == ExitCode.FAILED and "cancel it first" in out
    run(capsys, "cancel", key)
    run(capsys, "wait", key)
    code, out, err = run(capsys, "clean", key)
    assert code == 0, (out, err)
    code, out, _ = run(capsys, "get", key, "-D", str(env["tmp"]))
    assert code == ExitCode.FAILED and "No such job" in out


def test_backends_listing(env: dict[str, Any], capsys: pytest.CaptureFixture[str]) -> None:
    code, out, _ = run(capsys, "backends")
    assert code == 0 and "here  local" in out and "kinds: arc, fake, local" in out


def test_usage_and_errors(env: dict[str, Any], capsys: pytest.CaptureFixture[str]) -> None:
    code, out, _ = run(capsys)
    assert code == ExitCode.USAGE and "submit" in out
    code, _, err = run(capsys, "submit", "-b", "here")
    assert code == ExitCode.USAGE and "give the executable" in err
    code, _, err = run(capsys, "submit", "-b", "here", "--native", "rerun=2", "--", "x")
    assert code == ExitCode.USAGE and "KIND.ATTRIBUTE" in err
    code, _, err = run(capsys, "submit", "-b", "nowhere", "--", "x")
    assert code == ExitCode.CONFIG and "No backend 'nowhere'" in err
    code, _, err = run(capsys, "status", "nosuchkey")
    assert code == ExitCode.FAILED and "No job" in err
    run(
        capsys, "submit", "-b", "here", "--count", "2", "--native", "arc.rerun=1", "--", "/bin/echo"
    )
    code, _, err = run(capsys, "status", "")
    assert code == ExitCode.USAGE and "matches 2 jobs" in err


def test_interrupt_and_internal_errors(
    env: dict[str, Any], capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def interrupted(_session: object) -> Any:
        raise KeyboardInterrupt

    monkeypatch.setitem(cli._COMMANDS, "forget", (interrupted, None, ""))
    code, _, err = run(capsys, "forget")
    assert code == ExitCode.INTERRUPTED and "interrupted" in err

    def broken(_session: object) -> Any:
        raise RuntimeError("bug")

    monkeypatch.setitem(cli._COMMANDS, "forget", (broken, None, ""))
    code, _, err = run(capsys, "forget")
    assert code == ExitCode.INTERNAL and "internal error: RuntimeError: bug" in err

    def closed(_session: object) -> Any:
        raise BrokenPipeError

    # ``xrdwms status | head``: xrdcore.cli.run's quiet 141, not a local fault (6).
    monkeypatch.setitem(cli._COMMANDS, "forget", (closed, None, ""))
    code, _, err = run(capsys, "forget")
    assert code == ExitCode.BROKEN_PIPE and int(ExitCode.BROKEN_PIPE) == 141 and err == ""


def test_verbose_logs_the_traceback_of_an_internal_error(
    env: dict[str, Any], capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(_session: object) -> Any:
        raise RuntimeError("bug")

    monkeypatch.setitem(cli._COMMANDS, "forget", (broken, None, ""))
    code, _, err = run(capsys, "forget", "-vv")
    assert code == ExitCode.INTERNAL
    assert err.startswith("DEBUG: internal error\nTraceback (most recent call last):\n")
    assert err.endswith("RuntimeError: bug\nxrdwms: internal error: RuntimeError: bug\n")
    code, _, err = run(capsys, "forget")
    assert err == "xrdwms: internal error: RuntimeError: bug\n"


def test_exit_codes() -> None:
    from xrdcore.errors import CredentialError

    record = Record("k", "b", JobSpec("x", "y"), JobState.FAILED)
    assert exit_code_for(cli._Usage("x")) is ExitCode.USAGE
    assert exit_code_for(UnknownJobError("x")) is ExitCode.FAILED
    assert exit_code_for(CredentialError("x")) is ExitCode.CREDENTIALS
    assert exit_code_for(ConfigError("x")) is ExitCode.CONFIG
    assert exit_code_for(JobFailedError(record)) is ExitCode.FAILED
    assert exit_code_for(WMSError("x")) is ExitCode.BACKEND
    assert exit_code_for(OSError("x")) is ExitCode.LOCAL
    assert exit_code_for(RuntimeError("x")) is ExitCode.INTERNAL


@pytest.mark.parametrize(
    ("text", "seconds"), [("90", 90), ("90s", 90), ("30m", 1800), ("2h", 7200), ("1d", 86400)]
)
def test_periods(text: str, seconds: int) -> None:
    assert _period(text) == seconds


def test_bad_period() -> None:
    import argparse

    with pytest.raises(argparse.ArgumentTypeError):
        _period("soon")
