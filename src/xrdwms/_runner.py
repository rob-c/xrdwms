"""xrdwms's worker-node runner: call one pickled Python function, pickle what it returns.

Shipped in each function job's sandbox and run as::

    python3 xrdwms_run.py xrdwms-call.json xrdwms-call.pkl xrdwms-result.pkl

It needs only the standard library and cloudpickle (to unpickle the call),
and the Python minor version the client pickled with: a different one could
not load the function's code. Whatever happens, it writes the result file -
``("ok", value)`` or ``("error", type name, message, traceback)`` - and
exits 0, so the client always learns what went wrong; a non-zero exit means
the runner itself could not write anything.
"""

import json
import pickle
import sys
import traceback
from typing import Any

#: What the result file holds: ``("ok", value)`` or ``("error", kind, message, trace)``.
Outcome = tuple[Any, ...]


def _error(kind: str, message: str, trace: str = "") -> Outcome:
    """The ``("error", ...)`` outcome the client turns back into an exception."""
    return ("error", kind, message, trace)


def _call(meta_path: str, call_path: str) -> Outcome:
    """Load and run the call, if this interpreter can; every failure is an outcome, not a raise."""
    with open(meta_path) as handle:
        meta = json.load(handle)
    here = list(sys.version_info[:2])
    if list(meta["python"]) != here:
        return _error(
            "PythonVersionMismatch",
            "the call was pickled on Python {}.{} but this node runs {}.{}".format(
                meta["python"][0], meta["python"][1], here[0], here[1]
            ),
        )
    try:
        with open(call_path, "rb") as handle:
            function, args, kwargs = pickle.load(handle)
    except Exception as exc:
        return _error("PayloadError", f"cannot load the call: {exc}", traceback.format_exc())
    try:
        return ("ok", function(*args, **kwargs))
    except BaseException as exc:
        return _error(type(exc).__name__, str(exc), traceback.format_exc())


def _dump(outcome: Outcome) -> bytes:
    """``outcome`` pickled: by cloudpickle where the node has it (a value may need it)."""
    try:
        import cloudpickle
    except ImportError:
        return pickle.dumps(outcome)
    data: bytes = cloudpickle.dumps(outcome)
    return data


def main(argv: list[str]) -> int:
    """Run ``argv[1:4]`` = meta, call and result paths; always 0 once the result is written."""
    meta_path, call_path, result_path = argv[1:4]
    outcome = _call(meta_path, call_path)
    try:
        data = _dump(outcome)
    except Exception as exc:
        data = pickle.dumps(_error("ResultError", f"cannot pickle the result: {exc}"))
    with open(result_path, "wb") as handle:
        handle.write(data)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
