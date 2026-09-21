# SignalNow RTM

A demo of **sub-second operational alerting with Spark Real-Time Mode (RTM)**, on a
grocery cold-chain (freezer failure) use case. The same alerting logic runs as both a
**Real-Time Mode** stream and a **micro-batch** stream so you can watch latency diverge
under load — and because RTM is now in **open-source Apache Spark 4.1**, the same code is
portable across OSS Spark, EMR, and Databricks.

See [`docs/business-case.md`](docs/business-case.md) for the why.

## What it does

A synthetic IoT producer streams freezer telemetry to Kafka. Consumers enrich each event
with static metadata, apply deterministic alerting rules, and emit enriched alerts — one
pipeline in RTM (sub-second), one in micro-batch (the contrast). A Databricks App reads the
results and shows a live **Business** and **Tech** view.

```
                       rate source (RTM)
                              │
                   producer  ▼  synthetic telemetry
                     Kafka: freezer_sensor_events
                              │
          broadcast static ───┤  (freezer / site / thresholds CSVs)
          enrichment          │
             ┌────────────────┴────────────────┐
             ▼                                  ▼
   rtm_consumer (RTM)                 microbatch_consumer
   trigger(realTime)                  trigger(processingTime)
             │                                  │
             └──────────┬───────────────────────┘
                        ▼
            Kafka: freezer_alerts_enriched  ──►  SignalNow Console (app tails Kafka)
```

Kafka is the only sink — the app tails the `freezer_alerts_enriched` topic (and a
`freezer_pipeline_metrics` topic) and aggregates in memory; there is no database in the path.
The alerting logic is shared by both consumers unchanged — only the trigger and output mode
differ. It runs as a **stateful incident engine** (`shared/incident_engine.py`, wired into a
stream by `shared/stateful.py`), which shapes the five rules into OPENED/ESCALATED/RESOLVED
incidents; `shared/rules.py` keeps the equivalent stateless rules as a readable reference. See
[`docs/alerting-logic.md`](docs/alerting-logic.md).

## Repo layout

```
shared/            engine-agnostic core: schemas, rules, incident_engine, stateful, latency,
                   enrichment, kafka, metrics_listener, pipeline, scenarios
src/               streaming entrypoints: producer, rtm_consumer, microbatch_consumer
resources/         DAB resources: jobs, schema + volumes, Kafka secret scope
data/static/       enrichment CSVs (the fleet + thresholds)
app/               Databricks App (FastAPI) — SignalNow Console, Business + Tech tabs
tests/             pytest suite (local Spark, no cluster/Kafka)
docs/              business-case, data-model, alerting-logic, deploy-contract
databricks.yml     the bundle
config.template.yaml   copy to config.yaml (gitignored) and fill in
scripts/           test.sh, deploy.sh, run.sh, dev-app.sh
```

## Prerequisites

- **Databricks CLI** and a workspace with Unity Catalog. RTM needs **DBR 18.1+**.
- A **Kafka** cluster reachable from the workspace (bring your own — this repo provisions no infra).
- For local tests: **Python 3.10–3.12** and a **JDK 17**.

> This is a public demo repo and contains **no infrastructure-provisioning code** — no Terraform
> for the broker/workspace/network, no discovery scripts, and no secrets. Kafka and the workspace
> are bring-your-own; credentials go into a Databricks secret scope out of band.

## Quickstart

### 1. Run the tests locally (no cluster or Kafka)

```bash
python3.10 -m venv .venv
PIP_USER=0 .venv/bin/python -m pip install -r requirements-dev.txt
brew install openjdk@17          # JDK 17 for local Spark
./scripts/test.sh
```

### 2. Try the app locally (mock data)

```bash
PIP_USER=0 .venv/bin/python -m pip install -r app/requirements.txt
./scripts/dev-app.sh             # http://localhost:8000
```

The app serves realistic mock data via `MockDataSource`, so it's fully demoable before the
pipeline runs. **Inject burst** and the **RTM / Micro-batch** toggle simulate the scenarios.

### 3. Deploy and run on Databricks

```bash
cp config.template.yaml config.yaml   # fill in profile, catalog, Kafka, DBR, node type

# Create the Kafka secret scope values out of band (the bundle creates the empty scope):
databricks secrets put-secret <scope> sasl_jaas_config --profile <profile>

./scripts/deploy.sh                    # deploy the bundle + upload enrichment CSVs
./scripts/run.sh producer              # start each job (streaming, runs continuously)
./scripts/run.sh rtm_consumer
./scripts/run.sh microbatch_consumer
```

Confirm the RTM query is truly in Real-Time Mode by checking the physical plan shows
`RealTimeStreamScan` (not `MicroBatchScan`).

## The app and real data

`app/` is a real Databricks App. It reads only through the `DataSource` interface
(`app/backend.py`). To point it at live data, set `USE_MOCK_BACKEND=false` and implement a
`KafkaDataSource` that tails the `freezer_alerts_enriched` topic (plus the metrics topic) and
aggregates in memory — the API and UI don't change. Kafka is the only sink; there is no
database in the path. See [`app/README.md`](app/README.md).

## Roadmap

1. **Databricks classic jobs + RTM** — this repo.
2. **Databricks SDP (Lakeflow Declarative Pipelines) on RTM** — same logic, declarative.
3. **OSS Spark / EMR** — the same code on Spark 4.1+, for the portability story.
