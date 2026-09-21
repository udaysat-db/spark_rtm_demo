#!/usr/bin/env bash
# Run the local unit tests (no cluster / Kafka needed).
# Requires: a JDK 17 (brew install openjdk@17) and a .venv with requirements-dev.txt.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -x .venv/bin/python ]; then
  echo "No .venv found. Create it with:"
  echo "  python3.10 -m venv .venv && PIP_USER=0 .venv/bin/python -m pip install -r requirements-dev.txt"
  exit 1
fi

if command -v brew >/dev/null && brew --prefix openjdk@17 >/dev/null 2>&1; then
  export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
fi
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"

exec .venv/bin/python -m pytest "$@"
