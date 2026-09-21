#!/usr/bin/env bash
# Run one SignalNow job. These are streaming jobs — each runs continuously, so start
# them in separate terminals (producer first, then the consumers).
#   ./scripts/run.sh producer|rtm_consumer|microbatch_consumer [target]
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f config.yaml ] || { echo "config.yaml not found — cp config.template.yaml config.yaml and fill it in."; exit 1; }
cfg() { { grep -E "^$1:" config.yaml || true; } | head -1 | sed -E "s/^$1:[[:space:]]*//; s/[[:space:]]*#.*$//" | tr -d "\"'" | xargs; }

JOB="${1:-}"
TARGET="${2:-dev}"
PROFILE="$(cfg profile)"

# Match the deploy-time variables so `bundle run` resolves the same bundle — in
# particular workspace.profile = ${var.profile}, else it conflicts with --profile.
setvar() { local v; v="$(cfg "$1")"; [ -n "$v" ] && export "BUNDLE_VAR_$1=$v"; return 0; }
for k in profile catalog schema spark_version node_type_id rtm_workers shuffle_partitions \
         kafka_bootstrap_servers input_topic output_topic metrics_topic \
         kafka_topic_partitions kafka_secret_scope \
         events_per_second scenario processing_time_interval rtm_trigger_interval; do setvar "$k"; done

case "$JOB" in
  producer|rtm_consumer|microbatch_consumer)
    echo "==> databricks bundle run $JOB (target=$TARGET, profile=$PROFILE)"
    # --no-wait: streaming jobs run continuously; trigger and detach (job keeps running).
    exec databricks bundle run "$JOB" -t "$TARGET" --profile "$PROFILE" --no-wait ;;
  *)
    echo "usage: $0 <producer|rtm_consumer|microbatch_consumer> [target]"
    exit 1 ;;
esac
