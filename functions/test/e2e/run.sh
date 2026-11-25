#!/usr/bin/env bash
# End-to-end: real firebase-functions + firebase-admin against two kords.
#
#   FIREBASE_NODE_MODULES=/path/to/a/functions/node_modules functions/test/e2e/run.sh
#
# Needs Go, Postgres binaries (initdb, pg_ctl) and a node_modules that has
# firebase-functions >= 5 and firebase-admin >= 12 (any Firebase Functions
# project's will do). Kord #1 is "Kor"; kord #2 stands in for Google Firestore
# so the hybrid router can be checked without credentials.
set -euo pipefail
: "${FIREBASE_NODE_MODULES:?point at a node_modules with firebase-functions + firebase-admin}"
HERE=$(cd "$(dirname "$0")" && pwd); REPO=$(cd "$HERE/../../.." && pwd)
W=$(mktemp -d /tmp/korfn.XXXXXX); PORT=$((20000 + RANDOM % 20000))
cleanup() { pkill -f "$W/kord" 2>/dev/null || true; pkill -f "kor-functions.js --source $W" 2>/dev/null || true; pg_ctl -D "$W/pg" stop -m immediate >/dev/null 2>&1 || true; rm -rf "$W"; }
trap cleanup EXIT
(cd "$REPO" && go build -o "$W/kord" ./cmd/kord)
initdb -D "$W/pg" -U postgres -A trust >/dev/null
pg_ctl -D "$W/pg" -o "-p $PORT -k '' -h 127.0.0.1" -l "$W/pg.log" start >/dev/null; sleep 1
for db in kor kor2; do psql -h 127.0.0.1 -p "$PORT" -U postgres -qc "create database $db"; done
"$W/kord" -listen 127.0.0.1:$((PORT+1)) -metrics 127.0.0.1:$((PORT+2)) -pg-dsn "postgres://postgres@127.0.0.1:$PORT/kor?sslmode=disable" >"$W/kord.log" 2>&1 &
"$W/kord" -listen 127.0.0.1:$((PORT+3)) -metrics 127.0.0.1:$((PORT+4)) -pg-dsn "postgres://postgres@127.0.0.1:$PORT/kor2?sslmode=disable" >"$W/kord2.log" 2>&1 &
sleep 2
export KOR_FUNCTIONS_LIB="$HERE/../../lib" KOR_GRPC=127.0.0.1:$((PORT+1)) GOOGLE_STANDIN=127.0.0.1:$((PORT+3))

cp -r "$HERE/fixture/." "$W/fx"; ln -s "$FIREBASE_NODE_MODULES" "$W/fx/node_modules"
cp "$HERE/drive.js" "$HERE/hybrid.js" "$W/fx/"
node "$HERE/../../bin/kor-functions.js" --source "$W/fx" --project demo-p --feed "http://127.0.0.1:$((PORT+2))" \
  --kor "$KOR_GRPC" --hybrid posts,audit,counters,ticks --except api >"$W/rt.log" 2>&1 &
sleep 2
(cd "$W/fx" && node drive.js) || { cat "$W/rt.log"; exit 1; }
(cd "$W/fx" && node hybrid.js)
