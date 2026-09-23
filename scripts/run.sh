#!/usr/bin/env bash
# Run one SignalNow job. These are streaming jobs — each runs continuously, so start
# them in separate terminals (producer first, then the consumers).
#   ./scripts/run.sh producer|producer_rtm|rtm_consumer|microbatch_consumer [target]
# (producer_rtm is the RTM+ForeachWriter prototype — run it INSTEAD of producer, never
#  alongside; both write the input topic. See src/producer_rtm/main.py.)
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
  producer|producer_rtm|rtm_consumer|microbatch_consumer)
    # Start from scratch: clear this job's checkpoint before launching. The Kafka source
    # uses startingOffsets=latest (pipeline.py), but a SURVIVING checkpoint overrides that
    # and resumes from old committed offsets — so the consumers would reprocess the whole
    # backlog and skew latency/lag (alert_now − event_from_ages_ago). Clearing the
    # checkpoint makes `latest` take effect, so they only ever process live events.
    # Set RESET_CHECKPOINT=0 to keep the checkpoint (exactly-once resume) instead.
    if [ "${RESET_CHECKPOINT:-1}" = "1" ]; then
      CATALOG="$(cfg catalog)"
      SCHEMA_DEPLOYED="$(databricks bundle summary -t "$TARGET" --profile "$PROFILE" -o json 2>/dev/null \
        | python3 -c 'import sys,json; print(json.load(sys.stdin)["resources"]["schemas"]["signalnow"]["name"])' 2>/dev/null)"
      [ -n "$SCHEMA_DEPLOYED" ] || SCHEMA_DEPLOYED="$(cfg schema)"
      CKPT="/Volumes/$CATALOG/$SCHEMA_DEPLOYED/checkpoints/$JOB"
      echo "==> resetting checkpoint $CKPT (RESET_CHECKPOINT=1; set 0 to resume)"
      databricks fs rm -r "dbfs:$CKPT" --profile "$PROFILE" 2>/dev/null || true
    fi
    echo "==> databricks bundle run $JOB (target=$TARGET, profile=$PROFILE)"
    # --no-wait: streaming jobs run continuously; trigger and detach (job keeps running).
    exec databricks bundle run "$JOB" -t "$TARGET" --profile "$PROFILE" --no-wait ;;
  *)
    echo "usage: $0 <producer|producer_rtm|rtm_consumer|microbatch_consumer> [target]"
    exit 1 ;;
esac
