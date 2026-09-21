#!/usr/bin/env bash
# Run the SignalNow Console app locally against the mock backend.
#   ./scripts/dev-app.sh            -> http://localhost:8000
#   PORT=8010 ./scripts/dev-app.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -x .venv/bin/uvicorn ]; then
  echo "App deps missing. Install them into the dev venv:"
  echo "  PIP_USER=0 .venv/bin/python -m pip install -r app/requirements.txt"
  exit 1
fi

export USE_MOCK_BACKEND="${USE_MOCK_BACKEND:-true}"
exec .venv/bin/uvicorn --app-dir app app:app --host 127.0.0.1 --port "${PORT:-8000}"
