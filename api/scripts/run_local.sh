#!/usr/bin/env bash
set -euo pipefail

# Run from api/ root. Auto-load local env files once per launch.
cd "$(dirname "$0")/.."

if [[ -f ".env.local" ]]; then
  set -a
  source ".env.local"
  set +a
elif [[ -f ".env" ]]; then
  set -a
  source ".env"
  set +a
fi

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8765}"

exec uvicorn app.main:app --host "$HOST" --port "$PORT" --reload

