#!/usr/bin/env bash
# Deploy the SignalNow bundle and upload the enrichment CSVs.
# Reads workspace/Kafka/compute values from config.yaml (copy from config.template.yaml).
set -euo pipefail
cd "$(dirname "$0")/.."

# The wheel build (`python -m build`) spins up an isolated env; a --user-forcing pip
# profile breaks its install ("Can not perform a '--user' install"). Neutralize it.
export PIP_USER=0

[ -f config.yaml ] || { echo "config.yaml not found — cp config.template.yaml config.yaml and fill it in."; exit 1; }

# Read a top-level scalar from config.yaml (strips inline comments and quotes).
cfg() { { grep -E "^$1:" config.yaml || true; } | head -1 | sed -E "s/^$1:[[:space:]]*//; s/[[:space:]]*#.*$//" | tr -d "\"'" | xargs; }

PROFILE="$(cfg profile)"
CATALOG="$(cfg catalog)"
SCHEMA="$(cfg schema)"
TARGET="${1:-dev}"
[ -n "$PROFILE" ] || { echo "config.yaml: 'profile' is required."; exit 1; }

# Pass config values as bundle variables via BUNDLE_VAR_* env (only those actually set).
# Env avoids `--var`'s comma-splitting, which breaks values like a multi-broker
# kafka_bootstrap_servers (host1:9096,host2:9096).
setvar() { local v; v="$(cfg "$1")"; [ -n "$v" ] && export "BUNDLE_VAR_$1=$v"; return 0; }
for k in profile catalog schema spark_version node_type_id instance_pool_id rtm_workers shuffle_partitions \
         kafka_bootstrap_servers input_topic output_topic metrics_topic \
         kafka_topic_partitions kafka_secret_scope \
         events_per_second scenario processing_time_interval rtm_trigger_interval; do setvar "$k"; done

echo "==> databricks bundle deploy (target=$TARGET, profile=$PROFILE)"
databricks bundle deploy -t "$TARGET" --profile "$PROFILE"

# In dev mode DAB may prefix the schema name — resolve the real one from the deployment.
SCHEMA_DEPLOYED="$(databricks bundle summary -t "$TARGET" --profile "$PROFILE" -o json 2>/dev/null \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["resources"]["schemas"]["signalnow"]["name"])' 2>/dev/null)"
[ -n "$SCHEMA_DEPLOYED" ] || SCHEMA_DEPLOYED="$SCHEMA"
STATIC="/Volumes/$CATALOG/$SCHEMA_DEPLOYED/static"
echo "==> uploading enrichment CSVs to $STATIC"
for f in data/static/*.csv; do
  databricks fs cp "$f" "dbfs:$STATIC/$(basename "$f")" --overwrite --profile "$PROFILE"
done

# Seed the live-control DIRECTORY the producer reads each batch. It is APPEND-ONLY —
# the console (and this seed) write a NEW timestamped file each time; the producer
# reads the whole dir and takes the latest by `ts`. Never overwrite a control file:
# the producer reads it every second and an overwrite races the read (FAILED_READ_FILE
# kills the query). Seed with the configured scenario at the current ts so a fresh
# deploy sets the baseline; a later console write (higher ts) wins.
CONTROL="/Volumes/$CATALOG/$SCHEMA_DEPLOYED/control"
SCN="$(cfg scenario)"; [ -n "$SCN" ] || SCN="normal"
TS="$(python3 -c 'import time;print(int(time.time()*1000))')"
TMPCTL="$(mktemp)"; printf '{"scenario": "%s", "ts": %s}\n' "$SCN" "$TS" > "$TMPCTL"
echo "==> seeding control $CONTROL/seed_$TS.json (scenario=$SCN, ts=$TS)"
databricks fs cp "$TMPCTL" "dbfs:$CONTROL/seed_$TS.json" --overwrite --profile "$PROFILE"
rm -f "$TMPCTL"

cat <<EOF

Deployed. Next:
  1) Confirm the feeder-provisioned secret scope '$(cfg kafka_secret_scope)' exists and is
     populated (sasl_jaas_config [+ sasl_mechanism]). The bundle does NOT create it.
  2) Start the jobs (each streams continuously; run in separate terminals):
       ./scripts/run.sh producer
       ./scripts/run.sh rtm_consumer
       ./scripts/run.sh microbatch_consumer
EOF
