# Deploy contract

**SignalNow deploys itself.** Running `databricks bundle deploy` (jobs, schema, volumes,
secret scope, app) and starting the jobs is *this* repo's responsibility — via `scripts/deploy.sh`
/ `scripts/run.sh` (see the [README](../README.md) Quickstart). This repo provisions **no
infrastructure**, though: the Databricks workspace and the Kafka cluster are bring-your-own,
and secrets live in a Databricks secret scope, not the repo.

A separate **infra / feeder repo** owns those **prerequisites** — the workspace, the Kafka
cluster + topics, network reachability, and the catalog + grants — and populates the secret
scope. It hands this project an auth profile and the non-secret config values below; it does
**not** run SignalNow's deployment. This document is the contract between the two: exactly
what the feeder must provide, in the format the code and bundle read it.

Kafka is the only sink (see [alerting-logic.md](./alerting-logic.md) and
[data-model.md](./data-model.md)). **Do not provision Lakebase or a SQL warehouse** —
that path was removed. DBR **18.1+** is sufficient.

> Config files with real values are **never committed**. Copy
> [`config.template.yaml`](../config.template.yaml) → `config.yaml` (gitignored) and
> fill it in; put secrets only in the Databricks secret scope.

## 1. Databricks workspace auth

Used by `databricks bundle deploy` / `run`. **Any profile that authenticates to the target
workspace with the grants below works — your own user login is fine.** Use a service principal
only for **unattended / CI** deploys, or when you want the jobs and app to run as a stable
non-human identity. Forms:

| Form | Supply | Where it goes |
|---|---|---|
| **User OAuth** (interactive; simplest for a human deploy) | `host` — run `databricks auth login --host <url> --profile <name>` | writes the `.databrickscfg` profile for you |
| **OAuth M2M** (service principal; unattended / CI) | `host`, `client_id`, `client_secret` | `.databrickscfg` profile, or env `DATABRICKS_HOST` / `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` |
| **PAT** | `host`, `token` | `.databrickscfg` profile, or env `DATABRICKS_HOST` / `DATABRICKS_TOKEN` |

The bundle references a **profile name** (`config.yaml: profile`). For the SP/CI case the
handoff is a `.databrickscfg` stanza (gitignored in-repo; normally at `~/.databrickscfg`):

```ini
[signalnow]                                          # service-principal example
host          = https://<workspace>.cloud.databricks.com
client_id     = <sp-client-id>
client_secret = <sp-client-secret>
```

**Grants the deploy principal needs:** `USE CATALOG` + `CREATE SCHEMA` + `CREATE VOLUME`
on the target catalog; workspace rights to create jobs, secret scopes, and (if deployed)
the Databricks App.

## 2. Non-secret config values — `config.yaml`

Maps 1:1 to the bundle `variables` in [`databricks.yml`](../databricks.yml). A filled
example (no secrets — safe to share):

```yaml
profile: signalnow
catalog: field_demos                  # deploy principal has CREATE SCHEMA/VOLUME here
schema: signalnow_rtm

spark_version: 18.1.x-scala2.13       # DBR 18.1+ (NOT 18.3 — Lakebase is gone)
node_type_id: i3.2xlarge              # cloud-specific; Standard_E8ds_v5 on Azure
rtm_workers: 2                        # worker vCPUs >= Σ partitions across stages (slot math)

kafka_bootstrap_servers: pkc-abc12.us-east-1.aws.confluent.cloud:9092
input_topic: freezer_sensor_events
output_topic: freezer_alerts_enriched
metrics_topic: freezer_pipeline_metrics   # StreamingQueryListener → app (1 partition is enough)
kafka_topic_partitions: 4             # MUST equal the real input/output topic partition count

kafka_secret_scope: signalnow_kafka

events_per_second: 200
scenario: normal                      # normal|door_open|compressor_failure|power_outage|fleet_hot_zone
processing_time_interval: "5 seconds"
```

**Slot-math note:** RTM runs all stages simultaneously and worker vCPUs must be ≥ the sum
of partitions across stages. The pipeline is Kafka source (`kafka_topic_partitions`) →
broadcast enrichment (no shuffle) → `transformWithState` (one shuffle,
`spark.sql.shuffle.partitions`). Size `rtm_workers` × the node's vCPUs to cover both.

## 3. Kafka secrets — Databricks secret scope

The bundle creates the **empty** scope named by `kafka_secret_scope`; infra populates it. The
**pipeline** (Spark, `shared/kafka_io.py`) and the **live app** (`kafka-python`) authenticate
the same credential in two different forms, so the scope holds both:

| Scope key | Used by | Value format | Required |
|---|---|---|---|
| `sasl_jaas_config` | pipeline (Spark) | full JAAS line. PLAIN: `org.apache.kafka.common.security.plain.PlainLoginModule required username="<key>" password="<secret>";` — SCRAM: `kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="<user>" password="<pw>";` (the `kafkashaded.` prefix is required on Databricks — the Spark Kafka connector is shaded) | if the broker uses SASL |
| `sasl_mechanism` | pipeline + app | SASL mechanism, e.g. `SCRAM-SHA-512`. Defaults to `PLAIN` when absent | non-PLAIN SASL |
| `sasl_username` | **live app** | SASL username (same credential as in the JAAS line) — `kafka-python` needs it as a discrete field | only if the **live** app is deployed (§5) |
| `sasl_password` | **live app** | SASL password | only if the **live** app is deployed (§5) |

```bash
databricks secrets create-scope signalnow_kafka
databricks secrets put-secret  signalnow_kafka sasl_jaas_config
databricks secrets put-secret  signalnow_kafka sasl_mechanism   # e.g. SCRAM-SHA-512 (omit for PLAIN)
# Only if deploying the LIVE app (mock app needs none of these):
databricks secrets put-secret  signalnow_kafka sasl_username
databricks secrets put-secret  signalnow_kafka sasl_password
```

When `sasl_jaas_config` is present the code uses **SASL_SSL** with the mechanism from
`sasl_mechanism` (default **PLAIN**); absent, it's plaintext. SCRAM (Amazon MSK SASL/SCRAM,
Confluent Cloud SCRAM) is supported by setting `sasl_mechanism` + a shaded SCRAM
`sasl_jaas_config`. mTLS / cloud-IAM (MSK IAM) still need `kafka_options()` extending.

**Scope permissions:**
- The **job-cluster run identity** needs **`READ`** — the streaming jobs read `sasl_jaas_config`
  at runtime. `SINGLE_USER` clusters run as the deploying principal unless a `run_as` is set.
- The **SignalNow deployer** needs **`MANAGE`** (for the live app): the app-resource binding
  grants the app's SP `READ` *during deploy*, and adding that ACL is a MANAGE-level action.
  `MANAGE` also implies read — so a deployer who can wire the app can read the Kafka creds
  (unavoidable in this model, and the run identity needs READ anyway).
- Simplest path: **the bundle creates the scope**, so the deployer owns it (MANAGE)
  automatically. If the **feeder** pre-creates the scope, it must grant the deployer MANAGE.
  The deployer is **not** required to be a workspace admin (see §1 grants).

## 4. Preconditions the infra repo owns

Not credentials, but they block the run:

- **Network reachability** — the workspace/cluster must reach the brokers (VPC peering /
  PrivateLink / security-group rules). RTM breaks on interruption, so keep it stable.
- **Topics pre-created** — `input_topic` and `output_topic`, each with
  `kafka_topic_partitions` partitions (this count feeds RTM slot math, so make it
  intentional), plus `metrics_topic` (1 partition is enough) — both consumers'
  `StreamingQueryListener` write per-batch metrics here (real input rate, offset lag,
  trigger duration, and RTM latency percentiles). The app consumes it once `backend_kafka`
  lands; provision it now with the other topics.
- **Volumes** — `/Volumes/<catalog>/<schema>/static` and `.../checkpoints` are created by
  the bundle; the deploy script uploads the enrichment CSVs to `static`. RTM checkpoints
  must be persistent (UC Volume, v2+ format).

## 5. The app (deployed with the bundle)

The console is a Databricks App and a **bundle resource**
(`resources/signalnow_console.app.yml`), so `databricks bundle deploy` creates it alongside
the jobs, schema, volumes, and secret scope. Its env lives in `app/app.yaml`.

| Mode | `USE_MOCK_BACKEND` | Needs |
|---|---|---|
| **Mock** (default) | `true` | nothing — deploys fully demoable, no secrets |
| **Live** (tails Kafka via `KafkaDataSource`) | `false` | the four steps below |

**Going live** (the app uses `kafka-python`, which needs the SASL username/password as
discrete fields — not the pipeline's JAAS blob):

1. **Feeder** adds `sasl_username` + `sasl_password` to the scope (§3).
2. **Uncomment** the two `secret` bindings in
   [`resources/signalnow_console.app.yml`](../resources/signalnow_console.app.yml) — they map
   scope keys `sasl_username`/`sasl_password` to the app-resource keys
   `kafka-sasl-username`/`kafka-sasl-password` (and grant the app's SP `READ`). They ship
   commented so the mock deploy needs no secrets.
3. In `app/app.yaml` set `USE_MOCK_BACKEND=false`, the non-secret `KAFKA_BOOTSTRAP` /
   `KAFKA_ALERTS_TOPIC` / `KAFKA_METRICS_TOPIC` / `KAFKA_SOURCE_MODE` / `KAFKA_SASL_MECHANISM`,
   and `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` via `valueFrom: kafka-sasl-username` /
   `kafka-sasl-password`.
4. `databricks bundle deploy` again.

The store roster ships in the app (`app/fleet_roster.csv`) so the grid renders every store at
rest. There is **no `PGHOST/PGUSER/...`** — that was the removed Lakebase path.

## 6. Feeding a session (assistant / automation)

When a coding assistant or CI job runs the deploy, the handoff must keep **raw secrets out
of the chat/transcript and out of the repo**. The rule: land each credential where the CLI
already reads it, and pass only *names* to the session.

| Need | Infra lands it at | The session uses |
|---|---|---|
| **Auth** | a working profile in `~/.databrickscfg` (a human's `databricks auth login`, or an SP's `client_secret` for CI), or the `DATABRICKS_*` env vars | `--profile <name>` — any secret stays in `~/.databrickscfg`, never pasted |
| **Non-secret config** | a filled `config.yaml` at the repo root (gitignored; from `config.template.yaml`) | `./scripts/deploy.sh` reads it |
| **Kafka SASL creds** | infra **populates the secret scope itself** (`databricks secrets put-secret <scope> sasl_jaas_config` [+ `sasl_mechanism`]) with its own access | referenced by scope **name** only — the session never handles the raw JAAS/password |

Then the session runs:

```bash
./scripts/deploy.sh                                   # bundle deploy (+ upload CSVs)
./scripts/run.sh producer rtm_consumer microbatch_consumer
```

**What to hand the session:** the **profile name**, confirmation that **`config.yaml` is in
place**, and that the **secret scope is populated and the topics exist**. Nothing secret needs
to appear in the conversation.

Notes:
- Any profile with the deploy grants (§1) works — a human's own login is fine. Reach for a
  service principal only for unattended / CI runs. Interactive-login tokens do expire, so
  re-auth if a run fails on auth.
- For interactive OAuth, a human runs `databricks auth login --host <url> --profile <name>`
  themselves (in Claude Code, prefix with `!` so it runs in-session), then tells the session
  to use that profile. Assistants must **never auto-select a profile**.

## Handoff checklist (what the infra repo provides)

- [ ] `.databrickscfg` profile stanza (or the equivalent env vars) — §1
- [ ] filled `config.yaml` (no secrets) — §2
- [ ] secret scope `signalnow_kafka` populated with `sasl_jaas_config` (+ `sasl_mechanism` for SCRAM) — §3
- [ ] network reachable + `input_topic`/`output_topic`/`metrics_topic` created with the right partitions — §4
- [ ] (if app is live) scope also holds `sasl_username` + `sasl_password`; the two `secret`
      bindings uncommented in the app resource; `app/app.yaml` set to `USE_MOCK_BACKEND=false` + `KAFKA_*` — §5

## Open item

Broker security mechanism: plaintext, SASL_SSL + **PLAIN**, and SASL_SSL + **SCRAM**
(via the `sasl_mechanism` scope key, §3) are now supported by `shared/kafka_io.py`.
Still unsupported without extending `kafka_options()`: **mTLS** and **cloud-IAM** (Amazon
MSK IAM). Confirm the broker's mechanism so the right scope keys are populated.

> Wired against an Amazon **MSK SASL/SCRAM** feeder (SCRAM-SHA-512): scope `signalnow_kafka`
> holds `sasl_mechanism=SCRAM-SHA-512` + a shaded-ScramLoginModule `sasl_jaas_config`.
