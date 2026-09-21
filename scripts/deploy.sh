#!/usr/bin/env bash
# Deploy the SignalNow bundle and upload the enrichment CSVs.
# Reads workspace/Kafka/compute values from config.yaml (copy from config.template.yaml).
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f config.yaml ] || { echo "config.yaml not found — cp config.template.yaml config.yaml and fill it in."; exit 1; }

# Read a top-level scalar from config.yaml (strips inline comments and quotes).
cfg() { grep -E "^$1:" config.yaml | head -1 | sed -E "s/^$1:[[:space:]]*//; s/[[:space:]]*#.*$//" | tr -d "\"'" | xargs; }

PROFILE="$(cfg profile)"
CATALOG="$(cfg catalog)"
SCHEMA="$(cfg schema)"
TARGET="${1:-dev}"
[ -n "$PROFILE" ] || { echo "config.yaml: 'profile' is required."; exit 1; }

# Pass config values through as bundle variables (only those actually set).
VARS=()
addvar() { local v; v="$(cfg "$1")"; [ -n "$v" ] && VARS+=("--var=$1=$v"); }
for k in profile catalog schema spark_version node_type_id rtm_workers shuffle_partitions \
         kafka_bootstrap_servers input_topic output_topic metrics_topic \
         kafka_topic_partitions kafka_secret_scope \
         events_per_second scenario processing_time_interval rtm_trigger_interval; do addvar "$k"; done

echo "==> databricks bundle deploy (target=$TARGET, profile=$PROFILE)"
databricks bundle deploy -t "$TARGET" --profile "$PROFILE" "${VARS[@]}"

echo "==> uploading enrichment CSVs to /Volumes/$CATALOG/$SCHEMA/static"
for f in data/static/*.csv; do
  databricks fs cp "$f" "dbfs:/Volumes/$CATALOG/$SCHEMA/static/$(basename "$f")" --overwrite --profile "$PROFILE"
done

cat <<EOF

Deployed. Next:
  1) Confirm the feeder-provisioned secret scope '$(cfg kafka_secret_scope)' exists and is
     populated (sasl_jaas_config [+ sasl_mechanism]). The bundle does NOT create it.
  2) Start the jobs (each streams continuously; run in separate terminals):
       ./scripts/run.sh producer
       ./scripts/run.sh rtm_consumer
       ./scripts/run.sh microbatch_consumer
EOF
