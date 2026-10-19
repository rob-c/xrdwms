"""Stress: thousands of jobs through the real xrdarc client against the in-process fake A-REX."""

from __future__ import annotations

import threading
import time
from collections import Counter
from collections.abc import Callable
from pathlib import Path

from xrdarc import ARCClient
from xrdarc.testing.arex import DEFAULT_PROGRESSION, FakeARex

from stress._load import Report, max_rss_mb, scaled
from xrdwms import JobManager, JobSpec, Journal, PollPolicy
from xrdwms.backends.arc import ARCBackend

POLICY = PollPolicy(interval=0.05, max_interval=0.3, batch_delay=0.05, lease=10)


async def test_arc_at_scale(
    make_ce: Callable[..., FakeARex],
    make_arc: Callable[..., ARCClient],
    tmp_path: Path,
    record_report: list[Report],
) -> None:
    n = scaled(2_000)
    ce = make_ce(progression=DEFAULT_PROGRESSION)  # one state further on each poll
    backend = ARCBackend("ce", ce=ce.url, client=make_arc(joblist=None))
    threads, started = threading.active_count(), time.monotonic()
    specs = [JobSpec(f"a{i}", "/bin/true") for i in range(n)]
    with Journal(tmp_path / "j.sqlite") as journal:
        async with JobManager([backend], journal=journal, policy=POLICY) as jm:
            keys = await jm.submit(specs, backend="ce")
            records = await jm.wait_all(keys)
        states = Counter(r.state.value for r in records)
    elapsed = time.monotonic() - started
    posts: Counter[str] = Counter()
    for request in ce.requests:
        action = request.query.get("action", "")
        posts[f"{request.method} {action or request.path.rsplit('/', 1)[-1]}"] += 1
    report = Report(
        "arc-fake-arex",
        n,
        elapsed,
        dict(states),
        len(ce.jobs) - n,
        dict(posts.most_common(6)),
        max_rss_mb(),
        threads,
        threading.active_count(),
    )
    print("\n" + report.line())
    record_report.append(report)
    assert states == {"done": n}
    assert len(ce.jobs) == n, "every job submitted exactly once"
    info_calls = sum(c for k, c in posts.items() if "info" in k)
    assert info_calls < n, "status goes in bulk"
