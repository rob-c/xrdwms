"""The supporting modules: storage, credentials, the payload runner, the registry, config."""

from __future__ import annotations

import errno
import json
import pickle
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

from xrdwms import _runner, backends, credentials
from xrdwms.backends import SyncBackend
from xrdwms.config import Config, config_path, load_config, resolve_backend
from xrdwms.errors import ConfigError, RemoteError
from xrdwms.model import JobHandle, JobSpec, Output
from xrdwms.payload import CALL, META, RESULT, RUNNER, function_spec, read_result, write_call
from xrdwms.staging import GridStorage, _as_url, url_outputs, verify_outputs
from xrdwms.testing import FakeBackend

if TYPE_CHECKING:
    from xgfalclient import Gfal2Context

# -- storage ---------------------------------------------------------------------------------


class FakeGfal:
    """Enough of a gfal2 context to see what GridStorage asks of it."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {"srm://se/f": b"abc"}
        self.copies: list[tuple[str, str]] = []

    class _Stat:
        def __init__(self, size: int) -> None:
            self.st_size = size

    def stat(self, url: str) -> Any:
        from xgfalclient import GError

        if url == "srm://se/broken":
            raise GError("I/O error", errno.EIO)
        if url not in self.files:
            raise GError("No such file", errno.ENOENT)
        return self._Stat(len(self.files[url]))

    def checksum(self, url: str, algorithm: str) -> str:
        return f"{algorithm}-sum"

    def transfer_parameters(self) -> Any:
        return type("P", (), {"overwrite": False, "create_parent": False})()

    def filecopy(self, params: Any, source: str, target: str) -> None:
        assert params.overwrite and params.create_parent
        self.copies.append((source, target))


def test_grid_storage_with_local_files(tmp_path: Path) -> None:
    storage = GridStorage()
    source = tmp_path / "a.txt"
    source.write_bytes(b"hello world\n")
    assert storage.exists(str(source)) and not storage.exists(str(tmp_path / "nope"))
    assert storage.size(source.as_uri()) == 12
    assert storage.checksum(str(source), "adler32") == "1e720467"
    storage.copy(str(source), str(tmp_path / "deeper" / "b.txt"))
    assert (tmp_path / "deeper" / "b.txt").read_bytes() == b"hello world\n"


def test_grid_storage_hands_every_scheme_to_gfal(tmp_path: Path) -> None:
    fake = FakeGfal()
    storage = GridStorage(cast("Gfal2Context", fake))
    assert storage.exists("srm://se/f") and not storage.exists("srm://se/none")
    with pytest.raises(OSError, match="I/O") as raised:
        storage.exists("srm://se/broken")
    assert raised.value.errno == errno.EIO
    assert storage.size("srm://se/f") == 3
    assert storage.checksum("srm://se/f") == "ADLER32-sum"
    storage.copy(str(tmp_path / "x"), "root://se//x")
    assert fake.copies == [((tmp_path / "x").as_uri(), "root://se//x")]


def test_grid_storage_builds_one_gfal_context() -> None:
    storage = GridStorage()
    assert storage._context() is storage._context()


def test_verify_outputs() -> None:
    class Some:
        def exists(self, url: str) -> bool:
            if url == "srm://bad":
                raise OSError("down")
            return url == "srm://ok"

    spec = JobSpec(
        "x",
        "y",
        outputs=(
            Output("a", "srm://ok"),
            Output("b", "srm://missing"),
            Output("c", "srm://bad"),
            Output("d"),
        ),
    )
    problems = verify_outputs(spec, Some())
    assert problems == ["b: nothing at srm://missing", "c: cannot check srm://bad: down"]
    assert url_outputs(spec) == ["srm://ok", "srm://missing", "srm://bad"]
    assert _as_url("root://h//f") == "root://h//f"


# -- credentials -----------------------------------------------------------------------------


def test_the_credential_classes_are_xrdsecs() -> None:
    """Re-exported, not copied: their behaviour is tested in xrdsec's suite."""
    from xrdsec import lifetime

    assert credentials.__all__ == ["BearerToken", "Credential", "CredentialProvider", "ProxyFile"]
    for name in credentials.__all__:
        assert getattr(credentials, name) is getattr(lifetime, name)


# -- the payload -----------------------------------------------------------------------------


def double(x: int) -> int:
    return 2 * x


def explode() -> None:
    raise ValueError("kaboom")


def run_runner(directory: Path) -> int:
    return _runner.main(
        ["runner", str(directory / META), str(directory / CALL), str(directory / RESULT)]
    )


def test_a_call_round_trip(tmp_path: Path) -> None:
    write_call(tmp_path, double, (21,))
    assert (tmp_path / RUNNER).read_text().startswith('"""xrdwms')
    assert run_runner(tmp_path) == 0
    assert read_result(tmp_path) == 42


def test_a_raising_call(tmp_path: Path) -> None:
    write_call(tmp_path, explode)
    run_runner(tmp_path)
    with pytest.raises(RemoteError, match="ValueError: kaboom") as caught:
        read_result(tmp_path)
    assert "Traceback" in caught.value.remote_traceback


def test_the_runner_reports_what_it_cannot_do(tmp_path: Path) -> None:
    write_call(tmp_path, double, (1,))
    meta = json.loads((tmp_path / META).read_text())
    (tmp_path / META).write_text(json.dumps({**meta, "python": [2, 7]}))
    run_runner(tmp_path)
    with pytest.raises(
        RemoteError, match=r"PythonVersionMismatch: the call was pickled on Python 2\.7"
    ):
        read_result(tmp_path)
    (tmp_path / META).write_text(json.dumps(meta))
    (tmp_path / CALL).write_bytes(b"not a pickle")
    run_runner(tmp_path)
    with pytest.raises(RemoteError, match="PayloadError: cannot load the call"):
        read_result(tmp_path)
    write_call(tmp_path, lambda: (_ for _ in ()))  # a generator does not pickle
    run_runner(tmp_path)
    with pytest.raises(RemoteError, match="ResultError"):
        read_result(tmp_path)
    (tmp_path / RESULT).unlink()
    with pytest.raises(RemoteError, match="left no"):
        read_result(tmp_path)


def test_the_runner_without_cloudpickle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "cloudpickle", None)
    assert pickle.loads(_runner._dump(("ok", 1))) == ("ok", 1)


def test_function_spec(tmp_path: Path) -> None:
    template = JobSpec("f", "python3", outputs=(Output("extra"),))
    spec = function_spec(template, tmp_path, "/usr/bin/python3.12")
    assert spec.executable == "/usr/bin/python3.12"
    assert spec.arguments == (RUNNER, META, CALL, RESULT)
    assert [i.name for i in spec.inputs] == [RUNNER, META, CALL]
    assert [o.name for o in spec.outputs] == ["extra", RESULT]


# -- the registry ----------------------------------------------------------------------------


def test_kinds_load_create() -> None:
    assert {"arc", "local", "fake"} <= set(backends.kinds())
    loaded: object = backends.load("fake")
    assert loaded is FakeBackend
    made = backends.create("fake", "mine", steps=("done",))
    assert isinstance(made, FakeBackend) and made.name == "mine"
    assert backends.create("fake").name == "fake"
    with pytest.raises(ConfigError, match="No backend of kind 'nope'"):
        backends.load("nope")
    with pytest.raises(ConfigError, match="Bad options for the fake backend"):
        backends.create("fake", colour="red")
    assert isinstance(backends.from_mapping("x", {"kind": "fake"}), FakeBackend)
    with pytest.raises(ConfigError, match="has no 'kind'"):
        backends.from_mapping("x", {})


def test_a_kind_that_cannot_be_imported(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(backends.BUILTIN, "broken", "xrdwms.no_such_module:X")
    with pytest.raises(ConfigError, match=r"install xrdwms\[broken\]"):
        backends.load("broken")


async def test_sync_backend_defaults() -> None:
    class Bare(SyncBackend):
        kind = "bare"

    bare = Bare("b")
    handle = JobHandle("b", "1")
    for call in (bare.submit([]), bare.status([]), bare.cancel([]), bare.clean([])):
        with pytest.raises(NotImplementedError):
            await call
    with pytest.raises(NotImplementedError):
        await bare.retrieve(handle, ".")
    [renewed] = await bare.renew([handle])
    assert not renewed.ok and "cannot renew" in renewed.reason
    assert await bare.find(["k"]) == {}
    assert isinstance(bare, backends.Backend)
    await bare.close()


# -- configuration ---------------------------------------------------------------------------


def test_config_file(tmp_path: Path, home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert config_path() == home / ".config/xrdwms/config.toml"
    assert config_path({"XDG_CONFIG_HOME": "/c"}) == Path("/c/xrdwms/config.toml")
    assert load_config() == Config()
    path = tmp_path / "c.toml"
    path.write_text(
        'journal = "~/j.sqlite"\nstaging = "davs://se/s"\n[poll]\ninterval = 5\n'
        '[backends.ce]\nkind = "arc"\nce = "ce.example.org"\n'
    )
    monkeypatch.setenv("XRDWMS_CONFIG", str(path))
    config = load_config()
    assert config.journal == str(home / "j.sqlite") and config.policy.interval == 5
    assert config.staging == "davs://se/s"
    assert resolve_backend("ce", config) == {"kind": "arc", "ce": "ce.example.org"}
    with pytest.raises(ConfigError, match="No configuration file"):
        load_config(tmp_path / "missing.toml")


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("this is = = not toml", "c.toml"),
        ("backends = 3", "must be tables"),
        ("[backends]\nce = 3", "must be tables"),
        ("poll = 3", r"\[poll\] must be a table"),
        ("[poll]\nspeed = 3", "unknown"),
    ],
)
def test_bad_config(tmp_path: Path, text: str, message: str) -> None:
    path = tmp_path / "c.toml"
    path.write_text(text)
    with pytest.raises(ConfigError, match=message):
        load_config(path)


def test_shorthands() -> None:
    assert resolve_backend("arc:ce.example.org") == {"kind": "arc", "ce": "ce.example.org"}
    assert resolve_backend("arc:ce/short") == {"kind": "arc", "ce": "ce", "queue": "short"}
    assert resolve_backend("local") == {"kind": "local"}
    assert resolve_backend("local:/scratch") == {"kind": "local", "root": "/scratch"}
    assert resolve_backend("fake") == {"kind": "fake"}
    with pytest.raises(ConfigError, match="configured are none"):
        resolve_backend("arc")
    with pytest.raises(ConfigError, match="configured are a"):
        resolve_backend("zz", Config(backends={"a": {"kind": "fake"}}))
