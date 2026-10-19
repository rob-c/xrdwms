"""Python functions as grid jobs: the call pickled into the sandbox, the result pickled back.

:func:`write_call` cloudpickles ``(function, args, kwargs)`` into a staging
directory with the runner (:mod:`xrdwms._runner`) and a JSON note of the
client's Python version; :func:`function_spec` makes the job that runs
them; :func:`read_result` turns the result file into the value, or raises
:class:`~xrdwms.errors.RemoteError` with the worker node's traceback.

The worker node needs a Python of the same minor version with cloudpickle
(and whatever installed packages the function imports). The function's own
module goes by value when it is user code rather than an installed package,
so a script's or notebook's functions run without being installed on the
node. The runner checks the Python version first and says so, rather than
failing to unpickle.
"""

from __future__ import annotations

import json
import pickle
import sys
import sysconfig
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from importlib import resources
from pathlib import Path
from types import ModuleType
from typing import Any

import cloudpickle

from .errors import RemoteError
from .model import Input, JobSpec, Output

__all__ = ["CALL", "META", "RESULT", "RUNNER", "function_spec", "read_result", "write_call"]

RUNNER = "xrdwms_run.py"
META = "xrdwms-call.json"
CALL = "xrdwms-call.pkl"
RESULT = "xrdwms-result.pkl"


def write_call(
    directory: Path,
    function: Callable[..., Any],
    args: Sequence[Any] = (),
    kwargs: Mapping[str, Any] | None = None,
) -> Path:
    """Write the runner, the pickled call and its note into ``directory``; returns it."""
    directory.mkdir(parents=True, exist_ok=True)
    runner = resources.files("xrdwms").joinpath("_runner.py").read_text()
    (directory / RUNNER).write_text(runner)
    meta = {"python": list(sys.version_info[:2]), "cloudpickle": cloudpickle.__version__}
    (directory / META).write_text(json.dumps(meta))
    call = (function, tuple(args), dict(kwargs or {}))
    with _by_value(function):
        (directory / CALL).write_bytes(cloudpickle.dumps(call))
    return directory


def _installed(module: ModuleType) -> bool:
    """Whether ``module`` is the standard library's or an installed package's, not user code."""
    path = getattr(module, "__file__", None)
    if path is None:
        return True  # built in
    resolved = str(Path(path).resolve())
    roots = {sysconfig.get_paths()[k] for k in ("stdlib", "platstdlib", "purelib", "platlib")}
    return any(resolved.startswith(str(Path(r).resolve())) for r in roots) or (
        "site-packages" in resolved or "dist-packages" in resolved
    )


@contextmanager
def _by_value(function: Callable[..., Any]) -> Iterator[None]:
    """Pickle the function's own module by value when it is user code, not an installed package.

    cloudpickle pickles functions of importable modules *by reference*, which
    the worker node could only load if the module were installed there; a
    user's own module usually is not. Installed packages stay by reference
    (they must be on the node, as in any environment).
    """
    module = sys.modules.get(getattr(function, "__module__", "") or "")
    if module is None or module.__name__ == "__main__" or _installed(module):
        yield
        return
    cloudpickle.register_pickle_by_value(module)
    try:
        yield
    finally:
        cloudpickle.unregister_pickle_by_value(module)


def function_spec(template: JobSpec, directory: Path, python: str = "python3") -> JobSpec:
    """``template`` made into the job that runs the call staged in ``directory``."""
    staged = tuple(Input(str(directory / name), name) for name in (RUNNER, META, CALL))
    return template.with_(
        executable=python,
        arguments=(RUNNER, META, CALL, RESULT),
        inputs=(*template.inputs, *staged),
        outputs=(*template.outputs, Output(RESULT)),
    )


def read_result(directory: str | Path) -> Any:
    """The value the function returned, from a retrieved sandbox; ``RemoteError`` if it raised."""
    path = Path(directory) / RESULT
    if not path.is_file():
        raise RemoteError(f"The job left no {RESULT}: the runner could not run (see stderr.txt)")
    outcome = pickle.loads(path.read_bytes())
    if outcome[0] == "ok":
        return outcome[1]
    _status, kind, message, trace = outcome
    raise RemoteError(f"{kind}: {message}", trace)
