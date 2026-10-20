"""A backend's own storage scheme: staging, output checks with a grace period, clean-up."""

from __future__ import annotations

import asyncio
import errno
import logging
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from _shared import FAST, failure_of
from xrdwms import JobManager, JobSpec, Output
from xrdwms.model import Capabilities, FailureKind, JobState
from xrdwms.staging import GridStorage, Routed, remove
from xrdwms.testing import FakeBackend

SPEC = JobSpec("hello", "/bin/echo")


class MemoryStorage:
    """Files by URL, in memory; ``remove`` refusable."""

    def __init__(self, *present: str, refuse_remove: bool = False) -> None:
        self.present = set(present)
        self.copies: list[tuple[str, str]] = []
        self.removed: list[str] = []
        self.refuse_remove = refuse_remove

    def exists(self, url: str) -> bool:
        return url in self.present

    def size(self, url: str) -> int:
        return 7

    def checksum(self, url: str, algorithm: str = "adler32") -> str:
        return f"{algorithm}:{url}"

    def copy(self, source: str, target: str) -> None:
        self.copies.append((source, target))
        self.present.add(target)

    def remove(self, url: str) -> None:
        if self.refuse_remove:
            raise OSError(errno.EACCES, "not yours")
        self.removed.append(url)
        self.present.discard(url)


class WithoutRemove:
    def exists(self, url: str) -> bool:
        return False

    def size(self, url: str) -> int:
        return 0

    def checksum(self, url: str, algorithm: str = "adler32") -> str:
        return ""

    def copy(self, source: str, target: str) -> None:
        return None


class OwnSchemeBackend(FakeBackend):
    """A fake backend that owns ``own:`` URLs, as DIRAC owns ``lfn:``."""

    def __init__(self, own: MemoryStorage, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.own = own

    def url_storage(self, url: str) -> MemoryStorage | None:
        return self.own if url.startswith("own:") else None


def test_routed() -> None:
    default, own = MemoryStorage("davs://se/a"), MemoryStorage("own:/b")
    routed = Routed(default, lambda url: own if url.startswith("own:") else None)
    assert routed.exists("own:/b") and routed.exists("davs://se/a") and not routed.exists("/local")
    assert routed.size("own:/b") == 7 and routed.checksum("own:/b") == "adler32:own:/b"
    routed.copy("/local/f", "own:/f")
    routed.copy("own:/b", "/local/g")
    routed.copy("/local/h", "davs://se/h")
    assert own.copies == [("/local/f", "own:/f"), ("own:/b", "/local/g")]
    assert default.copies == [("/local/h", "davs://se/h")]
    routed.remove("own:/f")
    assert own.removed == ["own:/f"]
    with pytest.raises(OSError, match="cannot remove") as raised:
        remove(WithoutRemove(), "davs://se/x")
    assert raised.value.errno == errno.ENOTSUP


def test_grid_storage_remove(tmp_path: Path) -> None:
    doomed = tmp_path / "doomed"
    doomed.write_text("x")
    GridStorage().remove(str(doomed))
    assert not doomed.exists()
    with pytest.raises(OSError):
        GridStorage().remove(str(doomed))


async def test_staging_and_outputs_use_the_backends_storage(tmp_path: Path) -> None:
    (tmp_path / "big.dat").write_bytes(b"x" * 100)
    own = MemoryStorage()
    fake = OwnSchemeBackend(own, capabilities=Capabilities(data_staging=True, sandbox_limit=10))
    default = MemoryStorage()
    spec = SPEC.with_(inputs=(str(tmp_path / "big.dat"),), outputs=(Output("o", "own:/out/o"),))
    async with JobManager([fake], policy=FAST, storage=default, staging="own:/stage") as jm:
        [key] = await jm.submit([spec], backend="fake")
        own.present.add("own:/out/o")
        record = await jm.wait(key)
        assert record.state is JobState.DONE
        assert own.copies == [(str(tmp_path / "big.dat"), f"own:/stage/{key}/big.dat")]
        assert default.copies == [] and fake.jobs["fake-1"].spec.inputs[0].source.startswith("own:")
        [cleaned] = await jm.clean([key])
        assert cleaned.ok and own.removed == [f"own:/stage/{key}/big.dat"]


async def test_outputs_registered_late_count_within_the_grace(tmp_path: Path) -> None:
    own = MemoryStorage()
    fake = OwnSchemeBackend(own, capabilities=Capabilities(output_grace=60))
    spec = SPEC.with_(outputs=(Output("o", "own:/out/o"),))
    async with JobManager([fake], policy=FAST) as jm:
        [key] = await jm.submit([spec], backend="fake")
        for _ in range(500):
            if jm.journal.get(key).state is JobState.STAGING_OUT:
                break
            await asyncio.sleep(0.01)
        assert "waiting for outputs" in (jm.journal.get(key).info.native_state or "")  # type: ignore[union-attr]  # polled
        own.present.add("own:/out/o")
        assert (await jm.wait(key)).state is JobState.DONE


async def test_outputs_still_missing_after_the_grace_fail(tmp_path: Path) -> None:
    fake = OwnSchemeBackend(MemoryStorage(), capabilities=Capabilities(output_grace=0.05))
    spec = SPEC.with_(outputs=(Output("o", "own:/out/o"),))
    policy = replace(FAST, max_submit_tries=1)
    async with JobManager([fake], policy=policy) as jm:
        [key] = await jm.submit([spec], backend="fake")
        record = await jm.wait(key)
    assert record.state is JobState.FAILED and failure_of(record).kind is FailureKind.DATA
    assert "Missing outputs: o: nothing at own:/out/o" in failure_of(record).message


async def test_staged_inputs_that_cannot_be_removed_are_only_logged(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (tmp_path / "big.dat").write_bytes(b"x" * 100)
    own = MemoryStorage(refuse_remove=True)
    fake = OwnSchemeBackend(own, capabilities=Capabilities(data_staging=True, sandbox_limit=10))
    spec = SPEC.with_(inputs=(str(tmp_path / "big.dat"), "own:/elsewhere/kept.dat"))
    async with JobManager([fake], policy=FAST, staging="own:/stage") as jm:
        [key] = await jm.submit([spec], backend="fake")
        await jm.wait(key)
        with caplog.at_level(logging.WARNING):
            [cleaned] = await jm.clean([key])
    assert cleaned.ok and "Could not remove the staged input" in caplog.text
    async with JobManager([FakeBackend()], policy=FAST, staging="davs://se/s") as plain:
        [other] = await plain.submit([SPEC], backend="fake")
        await plain.wait(other)
        assert plain.storage_for("fake") is plain.storage
        assert [r.ok for r in await plain.clean([other])] == [True]
