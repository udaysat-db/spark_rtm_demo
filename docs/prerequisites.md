# Prerequisites

This repo deploys **itself**: `scripts/deploy.sh` runs `databricks bundle deploy` (jobs, schema, volumes, app) and `scripts/run.sh` starts the jobs (see the [README](../README.md) Quickstart). It provisions **no infrastructure**, though. You bring:

- a **Databricks workspace** with Unity Catalog, and a **workspace-admin** login to deploy with ([Workspace auth](#1-workspace-auth));
- a filled **`config.yaml`** with the non-secret values ([Config values](#2-config-values--configyaml));
- a **Kafka cluster** with the topics created, reachable from the workspace ([Kafka](#4-kafka-network-and-topics));
- a **Databricks secret scope** holding the Kafka credentials ([Kafka secrets](#3-kafka-secrets--databricks-secret-scope)).

This page says exactly what each of those must contain, in the format the code reads it. Kafka is the only sink (see [alerting-logic.md](./alerting-logic.md) and [data-model.md](./data-model.md)); no database or SQL warehouse is needed.

> Config files with real values are **never committed**. Copy [`config.template.yaml`](../config.template.yaml) → `config.yaml` (gitignored) and fill it in; put secrets only in the Databricks secret scope.

## 1. Workspace auth

Used by `databricks bundle deploy` / `run`. **The deployer must be a workspace admin**, a deliberate simplification for the demo (see *Why admin* below). Any auth form works for that identity: your own login for a human deploy, a service principal for unattended / CI runs.

| Form | Supply | Where it goes |
|---|---|---|
| **User OAuth** (interactive; simplest for a human deploy) | `host` — run `databricks auth login --host <url> --profile <name>` | writes the `.databrickscfg` profile for you |
| **OAuth M2M** (service principal; unattended / CI) | `host`, `client_id`, `client_secret` | `.databrickscfg` profile, or env `DATABRICKS_HOST` / `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` |
| **PAT** | `host`, `token` | `.databrickscfg` profile, or env `DATABRICKS_HOST` / `DATABRICKS_TOKEN` |

The bundle references a **profile name** (`config.yaml: profile`). A service-principal profile looks like this (keep it in `~/.databrickscfg`, never in the repo):

```ini
[signalnow]
host          = https://<workspace>.cloud.databricks.com
client_id     = <sp-client-id>
client_secret = <sp-client-secret>
```

**Why admin.** One identity has to create the schema, volumes, jobs and app, read the Kafka secret for the jobs, and **grant the app's service principal `READ` on the secret scope**. That last grant can only happen at deploy time, because the app's service principal is created together with the app. Workspace admin covers all of it in one step. *(Least-privilege alternative: `USE CATALOG` + `CREATE SCHEMA` + `CREATE VOLUME` on the catalog, `MANAGE` on the scope, the Apps entitlement, and a dedicated `run_as` service principal.)*

## 2. Config values — `config.yaml`

Maps 1:1 to the bundle `variables` in [`databricks.yml`](../databricks.yml). A filled example (no secrets):

```yaml
profile: signalnow
catalog: field_demos                  # deploy principal has CREATE SCHEMA/VOLUME here
schema: signalnow_rtm

spark_version: 18.1.x-scala2.13       # DBR 18.1+
node_type_id: i3.2xlarge              # cloud-specific; Standard_E8ds_v5 on Azure
rtm_workers: 2                        # worker vCPUs >= Σ partitions across stages (slot math)
shuffle_partitions: 8                 # spark.sql.shuffle.partitions for the stateful shuffle (keep small for RTM)

kafka_bootstrap_servers: pkc-abc12.us-east-1.aws.confluent.cloud:9092
input_topic: freezer_sensor_events
output_topic: freezer_alerts_enriched
metrics_topic: freezer_pipeline_metrics   # StreamingQueryListener → app (1 partition is enough)
kafka_topic_partitions: 4             # MUST equal the real input/output topic partition count

kafka_secret_scope: signalnow_kafka

events_per_second: 200
scenario: normal                      # normal|door_open|compressor_failure|power_outage|fleet_hot_zone
processing_time_interval: "5 seconds" # micro-batch trigger
rtm_trigger_interval: "5 minutes"     # RTM long-batch/checkpoint duration (trigger realTime); 1 min in dev
```

**Slot-math note:** RTM runs all stages simultaneously, and worker vCPUs must be ≥ the sum of partitions across stages. The pipeline is Kafka source (`kafka_topic_partitions`) → broadcast enrichment (no shuffle) → `transformWithState` (one shuffle, sized by `shuffle_partitions`, set into `spark.sql.shuffle.partitions` on the cluster — default **8**, not Spark's 200). So size `rtm_workers` × the node's vCPUs to cover `kafka_topic_partitions + shuffle_partitions`.

## 3. Kafka secrets — Databricks secret scope

Create the scope named by `kafka_secret_scope` and populate it **before** deploying; the bundle does not create it. The **pipeline** (Spark, `shared/kafka_io.py`) and the **live app** (`kafka-python`) use the same credential in two forms, so the scope holds both:

| Scope key | Used by | Value format | Required |
|---|---|---|---|
| `sasl_jaas_config` | pipeline (Spark) | full JAAS line. PLAIN: `org.apache.kafka.common.security.plain.PlainLoginModule required username="<key>" password="<secret>";` — SCRAM: `kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="<user>" password="<pw>";` (the `kafkashaded.` prefix is required on Databricks, whose Spark Kafka connector is shaded) | if the broker uses SASL |
| `sasl_mechanism` | pipeline + app | SASL mechanism, e.g. `SCRAM-SHA-512`. Defaults to `PLAIN` when absent | non-PLAIN SASL |
| `sasl_username` | live app | SASL username (same credential as in the JAAS line); `kafka-python` needs it as a separate field | live app only |
| `sasl_password` | live app | SASL password | live app only |
| `kafka_bootstrap` | live app | broker endpoint(s), the **same value** as `config.yaml: kafka_bootstrap_servers`. Kept in the scope so the broker endpoint stays out of the committed `app.yaml` | live app only |

```bash
databricks secrets create-scope signalnow_kafka
databricks secrets put-secret  signalnow_kafka sasl_jaas_config
databricks secrets put-secret  signalnow_kafka sasl_mechanism   # e.g. SCRAM-SHA-512 (omit for PLAIN)
databricks secrets put-secret  signalnow_kafka sasl_username    # live app
databricks secrets put-secret  signalnow_kafka sasl_password    # live app
databricks secrets put-secret  signalnow_kafka kafka_bootstrap  # live app
```

When `sasl_jaas_config` is present, the code uses **SASL_SSL** with the mechanism from `sasl_mechanism` (default **PLAIN**); when it's absent, the connection is plaintext.

**Supported broker security:** plaintext, SASL_SSL + PLAIN, and SASL_SSL + SCRAM (e.g. Amazon MSK SASL/SCRAM, Confluent Cloud). mTLS and cloud IAM auth (e.g. Amazon MSK IAM) need `kafka_options()` in `shared/kafka_io.py` extended.

**Scope permissions.** Whoever owns the Kafka credentials can own the scope; the deployer never needs the raw values. Because the deployer is a workspace admin, the deploy grants the app's service principal `READ` on the scope and reads `sasl_jaas_config` for the jobs. The jobs run as the deployer unless a `run_as` is set.

## 4. Kafka network and topics

These aren't credentials, but the run fails without them:

- **Network reachability.** Two *separate* paths must reach the brokers:
  1. **The job clusters** (producer + consumers, in the workspace VPC), for the pipeline. RTM breaks on interruption, so keep this path stable.
  2. **The Databricks App compute**, if the live console is used. Apps run on serverless compute with their *own* egress, **not** the cluster's VPC path, so a working pipeline does **not** mean the app can reach Kafka. Enable serverless egress to the brokers (e.g. a network connectivity config with private endpoints, plus a broker security group that allows it). Symptom when missing: the app logs `KafkaTimeoutError: Unable to bootstrap from [...]` while the jobs run fine. Mock mode needs no network.
- **Topics created up front.** `input_topic` and `output_topic` each need `kafka_topic_partitions` partitions (this count feeds the RTM slot math, so pick it deliberately). `metrics_topic` needs 1 partition: both consumers' `StreamingQueryListener` write per-batch metrics to it (input rate, offset lag, trigger duration, RTM latency percentiles), and the live app reads them.
  - **Set `input_topic` and `output_topic` to `message.timestamp.type = LogAppendTime`.** The console computes business latency from Kafka record timestamps (alert queued − event in Kafka). With LogAppendTime both ends come from the broker's clock, so the number is free of clock skew. With the Kafka default (`CreateTime`) it still works, but producer↔consumer clock skew leaks in.
- **Volumes.** `/Volumes/<catalog>/<schema>/static`, `.../checkpoints` and `.../control` are created by the bundle. The deploy script uploads the enrichment CSVs to `static` and seeds `control`. RTM checkpoints must be persistent (UC Volume, v2+ format). `control` holds the append-only live-scenario files the console writes and the producer reads ([The app](#5-the-app)).

## 5. The app

The console is a Databricks App and a **bundle resource** (`resources/signalnow_console.app.yml`), so `databricks bundle deploy` creates it alongside the jobs, schema and volumes. Its env lives in `app/app.yaml`.

| Mode | `USE_MOCK_BACKEND` | Needs |
|---|---|---|
| **Mock** (default) | `true` | nothing; deploys fully demoable, no secrets |
| **Live** (tails Kafka via `KafkaDataSource`) | `false` | the steps below |

**Going live.** The app reads the broker endpoint and SASL credentials from the secret scope, so nothing sensitive lands in the repo:

1. **Uncomment** the three `secret` bindings in [`resources/signalnow_console.app.yml`](../resources/signalnow_console.app.yml). They map the scope keys `kafka_bootstrap` / `sasl_username` / `sasl_password` to the app resources `kafka-bootstrap` / `kafka-sasl-username` / `kafka-sasl-password`. When the admin deployer applies them, the app's service principal gets `READ` on the scope.
2. In `app/app.yaml`, set `USE_MOCK_BACKEND` to `"false"` and **uncomment** the `KAFKA_BOOTSTRAP` / `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` entries (they use `valueFrom` with those resource names). Check that `KAFKA_SASL_MECHANISM` matches your broker.
3. Run `databricks bundle deploy` again.

The store roster ships with the app (`app/fleet_roster.csv`), so the grid shows every store even when nothing is alerting.

**Live control (console drives the scenario).** The console's scenario selector and burst button change the *live producer's* scenario with **no restart or redeploy**. The app appends a JSON file to the bundle-created **`control` volume**, and the producer (a micro-batch job) re-reads that directory every batch in `foreachBatch`, where the latest `ts` wins. A stream-static join wouldn't work here: it caches the file listing at query start and never sees files added later, so the read has to be re-issued every batch. The app's service principal needs `WRITE_VOLUME` on `control`. The **`control-volume` `uc_securable` binding** in [`signalnow_console.app.yml`](../resources/signalnow_console.app.yml) grants it in both modes, and also passes the deployed volume path to the app through `valueFrom` (`KAFKA_CONTROL_PATH`). Never overwrite a control file: the producer reads it every second, so an overwrite races the read. Only append new timestamped files.

## 6. Running the deploy from an assistant or CI

When a coding assistant or a CI job runs the deploy, keep **raw secrets out of the transcript and out of the repo**. Put each credential where the CLI already reads it, and give the session only *names*:

| Need | Where it lives | What the session uses |
|---|---|---|
| **Auth** | a workspace-admin profile in `~/.databrickscfg` (a human's `databricks auth login`, or a service principal's `client_secret` for CI), or the `DATABRICKS_*` env vars | `--profile <name>`; the secret stays in `~/.databrickscfg` |
| **Non-secret config** | a filled `config.yaml` at the repo root (gitignored) | `./scripts/deploy.sh` reads it |
| **Kafka SASL creds** | the secret scope, populated by whoever owns the creds | the scope **name** only |

Then the session runs:

```bash
./scripts/deploy.sh                                   # bundle deploy (+ upload CSVs)
./scripts/run.sh producer rtm_consumer microbatch_consumer
```

Notes:
- A human's own login is fine; use a service principal only for unattended runs. Interactive-login tokens expire, so re-authenticate if a run fails on auth.
- For interactive OAuth, the human runs `databricks auth login --host <url> --profile <name>` themselves, then tells the session which profile to use. An assistant shouldn't pick a profile on its own.

## Checklist

Before deploying:
- [ ] a **workspace-admin** profile in `~/.databrickscfg` (or `DATABRICKS_*` env vars) — [1](#1-workspace-auth)
- [ ] filled `config.yaml` (no secrets) — [2](#2-config-values--configyaml)
- [ ] secret scope created and populated: `sasl_jaas_config` (+ `sasl_mechanism` for non-PLAIN), plus `sasl_username` / `sasl_password` / `kafka_bootstrap` for the live app — [3](#3-kafka-secrets--databricks-secret-scope)
- [ ] brokers reachable from the job clusters (and from serverless, for the live app); `input_topic` / `output_topic` / `metrics_topic` created with the right partitions — [4](#4-kafka-network-and-topics)

Deploy:
- [ ] `./scripts/deploy.sh`, then `./scripts/run.sh producer rtm_consumer microbatch_consumer` — [6](#6-running-the-deploy-from-an-assistant-or-ci)
- [ ] (optional, live app) uncomment the app secret bindings and the `KAFKA_*` `valueFrom` entries, set `USE_MOCK_BACKEND=false`, and redeploy — [5](#5-the-app)
