#!/usr/bin/env bash
# Runs this SDK's integration tests against a scratch `hot dev` instance.
#
# Requires the `hot` CLI (https://hot.dev) and python3 on PATH. Starts a
# dedicated dev server for the fixture project on HOT_TEST_PORT (default
# 4725), mints a local API key directly in the fixture's sqlite database,
# exports HOT_TEST_BASE_URL / HOT_TEST_API_KEY, and runs the suite.
set -euo pipefail
cd "$(dirname "$0")"

PORT="${HOT_TEST_PORT:-4725}"
APP_PORT=$((PORT + 50))

command -v hot >/dev/null || { echo "error: hot CLI not found on PATH" >&2; exit 1; }
command -v python3 >/dev/null || { echo "error: python3 not found on PATH" >&2; exit 1; }

echo "starting hot dev on :$PORT (app :$APP_PORT)..."
(cd fixture && exec env HOT_API_PORT="$PORT" HOT_APP_PORT="$APP_PORT" hot dev --log.target file > dev.out 2>&1) &
DEV_PID=$!
trap 'kill "$DEV_PID" 2>/dev/null || true' EXIT

up=""
for _ in $(seq 1 60); do
  if curl -sf -m 2 "http://localhost:$PORT/status" > /dev/null 2>&1; then up=1; break; fi
  kill -0 "$DEV_PID" 2>/dev/null || { echo "error: hot dev exited early:" >&2; tail -20 fixture/dev.out >&2; exit 1; }
  sleep 2
done
[ -n "$up" ] || { echo "error: hot dev did not become ready" >&2; tail -20 fixture/dev.out >&2; exit 1; }

export HOT_TEST_BASE_URL="http://127.0.0.1:$PORT"
HOT_TEST_API_KEY="$(python3 mint_key.py fixture/.hot/db/hot.sqlite.db)"
export HOT_TEST_API_KEY

echo "hot dev ready; running integration tests..."
cd ..
PYTHON="${PYTHON:-python3}"; [ -x .venv/bin/python ] && PYTHON=.venv/bin/python; "$PYTHON" -m pytest tests/test_integration.py -v
