#!/usr/bin/env bash
# Browser end-to-end: the real firebase-js WEB SDK in headless Chromium, talking
# to kord only over WebChannel + REST (-client-web-listen), cross-origin, with
# security rules on. Runs streaming and forced long-polling.
#
#   FIREBASE_NODE_MODULES=/path/to/node_modules e2e/js/browser.sh
#
# That node_modules needs firebase >= 10, esbuild and playwright (with Chromium
# installed). Also needs Go and Postgres binaries (initdb, pg_ctl).
set -euo pipefail
: "${FIREBASE_NODE_MODULES:?point at a node_modules with firebase, esbuild and playwright}"
HERE=$(cd "$(dirname "$0")" && pwd); REPO=$(cd "$HERE/../.." && pwd)
W=$(mktemp -d /tmp/korweb.XXXXXX); PORT=$((20000 + RANDOM % 20000))
cleanup() { pkill -f "$W/kord" 2>/dev/null || true; pg_ctl -D "$W/pg" stop -m immediate >/dev/null 2>&1 || true; rm -rf "$W"; }
trap cleanup EXIT
(cd "$REPO" && go build -o "$W/kord" ./cmd/kord)
NODE_PATH="$FIREBASE_NODE_MODULES" "$FIREBASE_NODE_MODULES/.bin/esbuild" "$HERE/browser-page.mjs" \
  --bundle --format=iife --platform=browser --log-level=warning --outfile="$W/b.js"
initdb -D "$W/pg" -U postgres -A trust >/dev/null
pg_ctl -D "$W/pg" -o "-p $PORT -k '' -h 127.0.0.1" -l "$W/pg.log" start >/dev/null; sleep 1
psql -h 127.0.0.1 -p "$PORT" -U postgres -qc "create database kor"
"$W/kord" -listen 127.0.0.1:$((PORT+1)) -metrics "" -pg-dsn "postgres://postgres@127.0.0.1:$PORT/kor?sslmode=disable" \
  -client-listen 127.0.0.1:$((PORT+2)) -rules "$HERE/browser.rules" -project demo-kor -accept-unsigned-test-tokens \
  -client-web-listen 127.0.0.1:$((PORT+3)) -client-web-origins http://127.0.0.1:9 >"$W/kord.log" 2>&1 &
for i in $(seq 1 30); do nc -z 127.0.0.1 $((PORT+3)) && break; sleep 0.5; done
node "$HERE/browser.cjs" $((PORT+3)) "$W/b.js" || { grep -v "level=DEBUG" "$W/kord.log" | tail -40; [ -n "${KORD_LOG:-}" ] && grep -i -E "err|denied|unauth" "$W/kord.log" | head -40; exit 1; }
