"""The complexity gate, run with the suite: xrdwms has no CI of its own to run it.

``[tool.complexipy]`` in pyproject.toml holds the limit (15) and the paths;
there is no snapshot, so old code meets the same limit as new code.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_every_function_is_within_the_complexity_limit() -> None:
    # Next to this interpreter first: the dev extra installs it into the same environment.
    found = Path(sys.executable).with_name("complexipy")
    tool = str(found) if found.is_file() else shutil.which("complexipy")
    if tool is None:
        pytest.skip(
            "complexipy is not installed (an optional dev dependency: pip install -e '.[dev]')"
        )
    run = subprocess.run([tool, "--quiet"], cwd=ROOT, capture_output=True, text=True)
    assert run.returncode == 0, run.stdout + run.stderr
