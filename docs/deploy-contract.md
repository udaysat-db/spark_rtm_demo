# Deploy contract

Everything the deploy needs, in the exact format the code and bundle read it — so a
separate infra repo can provision it and hand it over. **This repo provisions no
infrastructure**: the Kafka cluster and the Databricks workspace are bring-your-own,
and secrets go into a Databricks secret scope out of band.

Kafka is the only sink (see [alerting-logic.md](./alerting-logic.md) and
[data-model.md](./data-model.md)). **Do not provision Lakebase or a SQL warehouse** —
that path was removed. DBR **18.1+** is sufficient.

> Config files with real values are **never committed**. Copy
> [`config.template.yaml`](../config.template.yaml) → `config.yaml` (gitignored) and
> fill it in; put secrets only in the Databricks secret scope.

## 1. Databricks workspace auth

Used by `databricks bundle deploy` / `run`. A service principal is best for an infra
repo. Any one form:

| Form | Supply | Where it goes |
|---|---|---|
| **OAuth M2M** (preferred) | `host` (https workspace URL), `client_id`, `client_secret` | `.databrickscfg` profile, or env `DATABRICKS_HOST` / `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` |
| PAT | `host`, `token` | `.databrickscfg` profile, or env `DATABRICKS_HOST` / `DATABRICKS_TOKEN` |

The bundle references a **profile name** (`config.yaml: profile`). Cleanest handoff is a
`.databrickscfg` stanza (this file is gitignored in-repo; normally it lives at `~/.databrickscfg`):

```ini
[signalnow]
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
kafka_topic_partitions: 4             # MUST equal the real topic partition count

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

The bundle creates the **empty** scope named by `kafka_secret_scope`; infra populates it.
Exact keys the code reads ([`shared/kafka_io.py`](../shared/kafka_io.py)):

| Scope key | Value format | Required |
|---|---|---|
| `sasl_jaas_config` | full JAAS line for the broker's mechanism. PLAIN: `org.apache.kafka.common.security.plain.PlainLoginModule required username="<key>" password="<secret>";` — SCRAM: `kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="<user>" password="<pw>";` (the `kafkashaded.` prefix is required on Databricks — the Spark Kafka connector is shaded) | only if the broker uses SASL |
| `sasl_mechanism` | SASL mechanism, e.g. `SCRAM-SHA-512`. Optional — defaults to `PLAIN` when absent | only for non-PLAIN SASL (SCRAM, etc.) |

```bash
databricks secrets create-scope signalnow_kafka
databricks secrets put-secret  signalnow_kafka sasl_jaas_config
databricks secrets put-secret  signalnow_kafka sasl_mechanism   # e.g. SCRAM-SHA-512 (omit for PLAIN)
```

When `sasl_jaas_config` is present the code uses **SASL_SSL** with the mechanism from
`sasl_mechanism` (default **PLAIN**); absent, it's plaintext. SCRAM (Amazon MSK SASL/SCRAM,
Confluent Cloud SCRAM) is supported by setting `sasl_mechanism` + a shaded SCRAM
`sasl_jaas_config`. mTLS / cloud-IAM (MSK IAM) still need `kafka_options()` extending.

The **deploy principal and the job-cluster run principal both need `READ`** on this scope.

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
| **Mock** (default) | `true` | nothing — deploys fully demoable |
| **Live** (tails Kafka via `KafkaDataSource`) | `false` | set the `KAFKA_*` env in `app/app.yaml` (bootstrap, topics, source mode); SASL creds via `valueFrom` from `${var.kafka_secret_scope}`; grant the app's service principal `READ` on that scope |

The store roster ships in the app (`app/fleet_roster.csv`) so the grid renders every store at
rest. There is **no `PGHOST/PGUSER/...`** — that was the removed Lakebase path.

## 6. Feeding a session (assistant / automation)

When a coding assistant or CI job runs the deploy, the handoff must keep **raw secrets out
of the chat/transcript and out of the repo**. The rule: land each credential where the CLI
already reads it, and pass only *names* to the session.

| Need | Infra lands it at | The session uses |
|---|---|---|
| **Auth** | a service-principal OAuth profile in `~/.databrickscfg` (`host` + `client_id` + `client_secret`), or `DATABRICKS_HOST` / `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` exported in the shell | `--profile <name>` — the secret stays in `~/.databrickscfg`, never pasted |
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
- Prefer a **fresh SP profile** with deploy grants (§1) — interactive-login tokens expire.
- For interactive OAuth instead of an SP, a human runs `databricks auth login --host <url>
  --profile <name>` themselves (in Claude Code, prefix with `!` so it runs in-session), then
  tells the session to use that profile. Assistants must **never auto-select a profile**.

## Handoff checklist (what the infra repo provides)

- [ ] `.databrickscfg` profile stanza (or the equivalent env vars) — §1
- [ ] filled `config.yaml` (no secrets) — §2
- [ ] secret scope `signalnow_kafka` populated with `sasl_jaas_config` (+ `sasl_mechanism` for SCRAM) — §3
- [ ] network reachable + `input_topic`/`output_topic`/`metrics_topic` created with the right partitions — §4
- [ ] (if app is live) app SP granted `READ` on the scope — §5

## Open item

Broker security mechanism: plaintext, SASL_SSL + **PLAIN**, and SASL_SSL + **SCRAM**
(via the `sasl_mechanism` scope key, §3) are now supported by `shared/kafka_io.py`.
Still unsupported without extending `kafka_options()`: **mTLS** and **cloud-IAM** (Amazon
MSK IAM). Confirm the broker's mechanism so the right scope keys are populated.

> Wired against an Amazon **MSK SASL/SCRAM** feeder (SCRAM-SHA-512): scope `signalnow_kafka`
> holds `sasl_mechanism=SCRAM-SHA-512` + a shaded-ScramLoginModule `sasl_jaas_config`.
