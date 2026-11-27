#!/usr/bin/env bash
# Realtime end-to-end: the real firebase-js SDK (Node build, gRPC) against kord.
#
#   FIREBASE_NODE_MODULES=/path/to/a/node_modules/with/firebase e2e/js/run.sh
#
# Needs Go, Node >= 20 and Postgres binaries (initdb, pg_ctl).
set -euo pipefail
: "${FIREBASE_NODE_MODULES:?point at a node_modules that has firebase >= 10}"
HERE=$(cd "$(dirname "$0")" && pwd); REPO=$(cd "$HERE/../.." && pwd)
W=$(mktemp -d /tmp/korjs.XXXXXX); PORT=$((20000 + RANDOM % 20000))
cleanup() { pkill -f "$W/kord" 2>/dev/null || true; pg_ctl -D "$W/pg" stop -m immediate >/dev/null 2>&1 || true; rm -rf "$W"; }
trap cleanup EXIT
(cd "$REPO" && go build -o "$W/kord" ./cmd/kord)
initdb -D "$W/pg" -U postgres -A trust >/dev/null
pg_ctl -D "$W/pg" -o "-p $PORT -k '' -h 127.0.0.1" -l "$W/pg.log" start >/dev/null; sleep 1
psql -h 127.0.0.1 -p "$PORT" -U postgres -qc "create database kor"
"$W/kord" -listen 127.0.0.1:$((PORT+1)) -metrics "" -pg-dsn "postgres://postgres@127.0.0.1:$PORT/kor?sslmode=disable" >"$W/kord.log" 2>&1 &
for i in $(seq 1 30); do nc -z 127.0.0.1 $((PORT+1)) && break; sleep 0.5; done
node "$HERE/sdk.cjs" $((PORT+1)) || { tail -40 "$W/kord.log"; exit 1; }
