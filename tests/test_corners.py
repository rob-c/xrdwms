"""Corner cases of the backends, the dask plugin and the payload that other tests miss."""

from __future__ import annotations

import asyncio
import json
import os
import signal
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest
from xrdarc import ARCClient

import xrdwms.dask as gw
from _shared import FAST, failure_of, handle_of
from xrdwms import JobManager, JobSpec
from xrdwms.backends import local as local_module
from xrdwms.backends.arc import ARCBackend
from xrdwms.backends.local import LocalBackend, _remove
from xrdwms.errors import WMSError
from xrdwms.model import FailureKind, JobHandle, JobState, KeyedSpec, Resources
from xrdwms.payload import _by_value, _installed
from xrdwms.testing import FakeBackend

# -- ARC ---------------------------------------------------------------------------------------


async def test_arc_with_nothing_valid_sends_nothing(ce: Any, make_arc: Any) -> None:
    backend = ARCBackend("ce", ce=ce.url, client=make_arc())
    [result] = await backend.submit(
        [KeyedSpec("k", JobSpec("g", "true", resources=Resources(gpus=1)))]
    )
    assert failure_of(result).kind is FailureKind.PAYLOAD
    assert not [r for r in ce.requests if r.method == "POST"]
    await backend.close()


# -- local ------------------------------------------------------------------------------------


@pytest.fixture
def local(tmp_path: Path) -> LocalBackend:
    return LocalBackend(root=str(tmp_path / "jobs"), workdir=str(tmp_path))


async def test_a_job_that_cannot_start(
    local: LocalBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(*args: Any, **kwargs: Any) -> Any:
        raise OSError("exec format error")

    monkeypatch.setattr("xrdwms.backends.local.os.posix_spawn", refuse)
    [result] = await local.submit([KeyedSpec("k", JobSpec("x", "true"))])
    [info] = await local.status([handle_of(result)])
    assert info.state is JobState.FAILED and "exec format error" in failure_of(info).message
    await local.close()


async def test_ending_between_the_two_looks(
    local: LocalBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    [result] = await local.submit([KeyedSpec("k", JobSpec("x", "true"))])
    sandbox = local.root / handle_of(result).id
    for name in (".xrdwms-pid", ".xrdwms-exit"):
        (sandbox / name).unlink(missing_ok=True)
    (sandbox / ".xrdwms-pid").write_text("1")
    monkeypatch.setattr(local, "_start_queued", lambda: None)

    def ends(_sandbox: Path) -> bool:
        (sandbox / ".xrdwms-exit").write_text("0\n")
        return False

    monkeypatch.setattr(local, "_live", ends)
    [info] = await local.status([handle_of(result)])
    assert info.state is JobState.DONE
    await local.close()


async def test_cleaning_a_cancelled_job_that_ignores_sigterm(local: LocalBackend) -> None:
    stubborn = JobSpec("stubborn", "/bin/sh", ("-c", "trap '' TERM; sleep 30"))
    [result] = await local.submit([KeyedSpec("k", stubborn)])
    handle = result.handle
    assert handle is not None
    for _ in range(100):
        [info] = await local.status([handle])
        if info.state is JobState.RUNNING:
            break
        await asyncio.sleep(0.02)
    await asyncio.sleep(0.2)  # let the trap be set
    [cancelled] = await local.cancel([handle])
    assert cancelled.ok
    [cleaned] = await local.clean([handle])  # SIGKILL, then wait for our own child
    assert cleaned.ok and not (local.root / handle.id).exists()
    await local.close()


def test_killing_a_process_by_its_pid_file(local: LocalBackend, tmp_path: Path) -> None:
    sandbox = tmp_path / "sb"
    sandbox.mkdir()
    pid = os.posix_spawn("/bin/sleep", ["/bin/sleep", "30"], dict(os.environ), setsid=True)
    (sandbox / ".xrdwms-pid").write_text(str(pid))
    local._kill(sandbox, signal.SIGKILL)  # killed as a group, and reaped by waitpid
    assert not local_module._alive(pid)
    local._kill(sandbox, signal.SIGKILL)  # already gone
    local._kill(tmp_path, signal.SIGKILL)  # never started


def test_killing_gives_up_on_a_process_that_will_not_die(
    local: LocalBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".xrdwms-pid").write_text(str(os.getpid()))
    monkeypatch.setattr("xrdwms.backends.local.os.killpg", lambda pid, sig: None)
    monkeypatch.setattr("xrdwms.backends.local.time.sleep", lambda s: None)
    local._kill(tmp_path, signal.SIGKILL)


def test_remove_gives_up_on_a_directory_that_comes_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("xrdwms.backends.local.shutil.rmtree", lambda *a, **k: None)
    monkeypatch.setattr("xrdwms.backends.local.time.sleep", lambda s: None)
    _remove(tmp_path)
    assert tmp_path.exists()


# -- payload ----------------------------------------------------------------------------------


def test_installed_and_by_value() -> None:
    assert _installed(json)  # the standard library
    assert _installed(ModuleType("built_in_like"))  # no __file__
    import cloudpickle

    assert _installed(cloudpickle)  # site-packages
    with _by_value(json.dumps):  # installed: left by reference
        pass
    with _by_value(lambda: None):  # this test module: user code, by value
        pass

    def main_like() -> None:
        """A function as a script's ``__main__`` defines it: user code by definition."""

    main_like.__module__ = "__main__"
    with _by_value(main_like):
        pass


# -- the fake backend -------------------------------------------------------------------------


async def test_fake_backend_corners(tmp_path: Path) -> None:
    fake = FakeBackend(steps=("exit:0",))
    [result] = await fake.submit([KeyedSpec("k", JobSpec("x", "y"))])
    handle = result.handle
    assert handle is not None
    [done] = await fake.status([handle])
    assert done.state is JobState.DONE and done.exit_code == 0
    await fake.clean([handle])
    [gone] = await fake.status([handle])
    assert gone.state is JobState.UNKNOWN
    nobody = JobHandle("fake", "nope")
    with pytest.raises(FileNotFoundError):
        await fake.retrieve(nobody, str(tmp_path))
    assert [r.ok for r in await fake.clean([nobody])] == [False]
    assert [r.ok for r in await fake.renew([nobody])] == [False]
    assert [r.ok for r in await fake.cancel([nobody])] == [False]
    assert fake.job(handle.id) is fake.job(handle)


# -- the dask plugin --------------------------------------------------------------------------


async def test_the_plugin_ignores_what_is_not_its_own(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(gw, "CANCEL_GRACE", 0.01)
    plugin = gw.GridPlugin({"fake": {"kind": "fake"}}, policy=FAST, workdir=str(tmp_path))
    plugin.transition("x-abc", "executing", "released")  # not set up: nothing to do
    worker = SimpleNamespace(local_directory=str(tmp_path), state=SimpleNamespace(tasks={}))
    await plugin.setup(worker)
    assert plugin.manager is not None
    slow = FakeBackend("fake", steps=("queued",) * 1000)
    plugin.manager.backends["fake"] = slow  # jobs that stay queued until cancelled
    keep, stop, done = await plugin.manager.submit(
        [JobSpec("x", "y")] * 3, backend="fake", keys=["keep", "stop", "done"]
    )
    await plugin.manager.cancel([done])  # already over by the time of the sweep
    plugin.transition("t-stop", "waiting", "ready")  # not a cancellation
    plugin.transition(("tuple", 1), "executing", "released")  # not one of our keys
    assert plugin._doomed == {}
    for job in (keep, stop, done, "nojob"):
        plugin.transition(f"t-{job}", "executing", "released")
    plugin.transition("t-again", "executing", "released")  # one sweep for all of them
    worker.state.tasks["t-keep"] = SimpleNamespace(state="executing")  # asked for again
    assert (await asyncio.wait_for(plugin.manager.wait(stop), 10)).state is JobState.CANCELLED
    assert plugin.manager.journal.get(keep).state is not JobState.CANCELLED
    plugin.transition("t-nothing", "executing", "released")  # a sweep with nothing to cancel
    await asyncio.sleep(0.1)
    assert not plugin._sweep_due and plugin._doomed == {}
    await plugin.teardown(worker)
    await gw.GridPlugin({}).teardown(worker)  # never set up


async def test_a_cancelled_waiter_cancels_its_job() -> None:
    fake = FakeBackend(steps=("running",))
    async with JobManager([fake], policy=FAST) as manager:
        task = asyncio.create_task(gw._run(manager, "k1", JobSpec("x", "y"), "fake", None))
        await manager.submitted(["k1"])
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert (await manager.wait("k1")).state is JobState.CANCELLED


async def test_run_job_without_a_plugin_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    worker = SimpleNamespace(plugins={})
    monkeypatch.setattr(gw, "get_worker", lambda: worker)
    with pytest.raises(WMSError, match="not installed"):
        await gw.run_job(JobSpec("x", "y"), "fake", "k")


def test_job_result_paths(tmp_path: Path) -> None:
    from xrdwms.journal import Record

    record = Record("k", "b", JobSpec("x", "y"), JobState.DONE)
    assert gw.JobResult("k", record, str(tmp_path)).path("f") == tmp_path / "f"


def test_arc_client_property_is_cached(ce: Any, make_arc: Any) -> None:
    client: ARCClient = make_arc()
    backend = ARCBackend("ce", ce=ce.url, client=client)
    assert backend.client is client
