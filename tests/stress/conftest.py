"""The stress suite: opt in with XRDWMS_STRESS=1; size it with XRDWMS_STRESS_SCALE.

    XRDWMS_STRESS=1 XRDWMS_STRESS_SCALE=5 python -m pytest tests/stress -s -p no:randomly

Each scenario prints one measurement line and appends it, as JSON, to
``$XRDWMS_STRESS_REPORT`` (default ``stress-report.jsonl`` in the pytest rootdir's
parent temp directory) so runs can be compared.
"""

from __future__ import annotations

import dataclasses
import json
import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from stress._load import Report

collect_ignore: list[str] = []


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if os.environ.get("XRDWMS_STRESS") == "1":
        return
    skip = pytest.mark.skip(reason="stress test; set XRDWMS_STRESS=1")
    for item in items:
        if "stress" in str(item.fspath):
            item.add_marker(skip)


@pytest.fixture
def record_report(tmp_path_factory: pytest.TempPathFactory) -> Iterator[list[Report]]:
    reports: list[Report] = []
    yield reports
    target = Path(
        os.environ.get("XRDWMS_STRESS_REPORT")
        or tmp_path_factory.getbasetemp().parent / "stress-report.jsonl"
    )
    with target.open("a") as handle:
        for report in reports:
            handle.write(json.dumps({**dataclasses.asdict(report), "rate": report.rate}) + "\n")
