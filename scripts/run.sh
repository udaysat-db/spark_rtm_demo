#!/usr/bin/env bash
# Run one SignalNow job. These are streaming jobs — each runs continuously, so start
# them in separate terminals (producer first, then the consumers).
#   ./scripts/run.sh producer|rtm_consumer|microbatch_consumer [target]
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f config.yaml ] || { echo "config.yaml not found — cp config.template.yaml config.yaml and fill it in."; exit 1; }
cfg() { grep -E "^$1:" config.yaml | head -1 | sed -E "s/^$1:[[:space:]]*//; s/[[:space:]]*#.*$//" | tr -d "\"'" | xargs; }

JOB="${1:-}"
TARGET="${2:-dev}"
PROFILE="$(cfg profile)"

case "$JOB" in
  producer|rtm_consumer|microbatch_consumer)
    echo "==> databricks bundle run $JOB (target=$TARGET, profile=$PROFILE)"
    exec databricks bundle run "$JOB" -t "$TARGET" --profile "$PROFILE" ;;
  *)
    echo "usage: $0 <producer|rtm_consumer|microbatch_consumer> [target]"
    exit 1 ;;
esac
