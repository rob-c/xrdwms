#!/bin/bash
# Run the interop suite against the docker ARC CE + SLURM cluster in ARC-test/cluster.
#
#   tools/interop-docker.sh [/path/to/ARC-test/cluster] [pytest args...]
#
# As for xrdarc: the CE's host certificate names arc-ce.arctest.local, which only
# resolves inside the compose network, so the client runs in a throwaway python
# container on that network, with xrdwms and its siblings installed from this
# checkout. The test CA and the 'tester' user certificate are copied out of the CE;
# the proxy is made by `xrdarc proxy`.
#
# XRDWMS_INTEROP_HOST=1 runs the suite from the host instead (no container, so no
# network access needed to install anything), against the CE's published port.
set -euo pipefail
CLUSTER=${1:-$(cd "$(dirname "$0")/../../../ARC-test/cluster" && pwd)}
shift || true
GFAL=$(cd "$(dirname "$0")/../.." && pwd)
WORK=$(mktemp -d)
NAME=xrdwms-interop-$$
trap 'docker rm -f "$NAME" >/dev/null 2>&1 || true; rm -rf "$WORK"' EXIT

cd "$CLUSTER"
docker compose --profile arc up -d >/dev/null
CE=$(docker compose ps -q arc-ce)
until docker exec "$CE" test -f /etc/grid-security/.e2e-ca; do sleep 2; done

mkdir -p "$WORK/certificates" "$WORK/globus"
for f in $(docker exec "$CE" sh -c 'cd /etc/grid-security/certificates && ls *.0 *.signing_policy'); do
  docker exec "$CE" cat "/etc/grid-security/certificates/$f" > "$WORK/certificates/$f"
done
docker exec "$CE" cat /home/tester/.globus/usercert.pem > "$WORK/globus/usercert.pem"
docker exec "$CE" cat /home/tester/.globus/userkey.pem > "$WORK/globus/userkey.pem"
chmod 600 "$WORK/globus/userkey.pem"

if [ "${XRDWMS_INTEROP_HOST:-0}" = "1" ]; then
  # No container: run from this checkout's interpreter against the CE's published port
  # (127.0.0.1:8443). Its certificate names arc-ce.arctest.local, so verification is off.
  PY=${PYTHON:-"$GFAL/.venv/bin/python"}
  X509_USER_CERT="$WORK/globus/usercert.pem" X509_USER_KEY="$WORK/globus/userkey.pem" \
    X509_CERT_DIR="$WORK/certificates" X509_USER_PROXY="$WORK/proxy.pem" \
    "$PY" -m xrdarc.cli proxy >/dev/null
  cd "$GFAL/xrdwms"
  X509_USER_PROXY="$WORK/proxy.pem" X509_CERT_DIR="$WORK/certificates" XRDWMS_INTEROP=1 \
    XRDWMS_INTEROP_CE=https://127.0.0.1:8443/arex XRDWMS_INTEROP_VERIFY=0 \
    XRDWMS_INTEROP_TIMEOUT=600 \
    "$PY" -m pytest tests/test_interop.py -v -p no:cacheprovider -o addopts= --timeout 900 "$@"
  exit
fi

NETWORK=$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$CE")
docker run -d --name "$NAME" --network "$NETWORK" -v "$GFAL:/gfal:ro" -v "$WORK:/creds:ro" \
  python:3.12-slim sleep infinity >/dev/null
docker exec "$NAME" bash -c '
  set -e
  apt-get -qq update >/dev/null && apt-get -qq install -y openssl >/dev/null
  for p in xrdcore xrdsec xrdhttp xrdclient xgfalclient xrdarc xrdwms; do
    cp -r /gfal/$p /tmp/$p; rm -rf /tmp/$p/.venv
  done
  pip install -q --root-user-action=ignore /tmp/xrdcore /tmp/xrdsec /tmp/xrdhttp \
    /tmp/xrdclient /tmp/xgfalclient /tmp/xrdarc /tmp/xrdwms pytest pytest-asyncio pytest-timeout
  pip list 2>/dev/null | grep -E "^(xrd|xgfal|dask|distributed) "
  X509_USER_CERT=/creds/globus/usercert.pem X509_USER_KEY=/creds/globus/userkey.pem \
    X509_CERT_DIR=/creds/certificates xrdarc proxy >/dev/null'
docker exec -e X509_CERT_DIR=/creds/certificates -e XRDWMS_INTEROP=1 \
  -e XRDWMS_INTEROP_CE=arc-ce.arctest.local -e XRDWMS_INTEROP_TIMEOUT=600 "$NAME" \
  bash -c "cd /tmp/xrdwms && python -m pytest tests/test_interop.py -v -p no:cacheprovider -o addopts= --timeout 900 $*"
