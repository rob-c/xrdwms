#!/bin/bash
# Run the stress suite (tests/stress) and summarise what it measured.
#
#   tools/stress.sh [SCALE] [pytest args...]      e.g. tools/stress.sh 5 -k dask
#
# SCALE multiplies every scenario's job count (default 1: ~20k jobs in the largest).
# XRDWMS_STRESS_SEED replays the same injected faults. Each scenario appends one JSON
# line to the report; the summary at the end is read from it.
set -euo pipefail
SCALE=${1:-1}
shift || true
HERE=$(cd "$(dirname "$0")/.." && pwd)
PY=${PYTHON:-"$HERE/../.venv/bin/python"}
REPORT=${XRDWMS_STRESS_REPORT:-"$(mktemp -t xrdwms-stress).jsonl"}
cd "$HERE"
status=0
XRDWMS_STRESS=1 XRDWMS_STRESS_SCALE="$SCALE" XRDWMS_STRESS_REPORT="$REPORT" \
  "$PY" -m pytest tests/stress -s -v -p no:cacheprovider -o addopts= --timeout 3600 "$@" \
  || status=$?
echo
echo "== stress summary (scale $SCALE, report $REPORT) =="
"$PY" - "$REPORT" <<'EOF'
import json, sys
for line in open(sys.argv[1]):
    r = json.loads(line)
    states = " ".join(f"{k}={v}" for k, v in sorted(r["states"].items()))
    print(f"{r['name']:<22} {r['jobs']:>7} jobs {r['seconds']:>7.1f}s {r['rate']:>7.0f}/s"
          f"  dup={r['duplicates']}  rss={r['max_rss_mb']:.0f}MB  {states}")
EOF
exit $status
