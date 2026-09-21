# SignalNow Console (Databricks App)

Single-pipeline, business-first monitoring console for the SignalNow RTM demo. Two
tabs — **Business** (fleet status, live alert feed, alerts by type/severity, the
"detected in real-time window" KPI) and **Tech** (latency over time with business
bands, percentiles, window split, throughput, RTM built-in metrics). Compare engines
by running one instance per pipeline, side by side.

## Data source (swap, don't rewrite)

The API and UI only call `DataSource` (`backend.py`):

- **`MockDataSource`** (default, `USE_MOCK_BACKEND=true`) — a simulated stream, so the
  app is fully demoable before the pipeline runs.
- **`KafkaDataSource`** (`USE_MOCK_BACKEND=false`) — tails the `freezer_alerts_enriched`
  and `freezer_pipeline_metrics` topics and aggregates in memory (the pure logic lives
  in `aggregator.py`). Kafka is the only sink — no database in the path. The `snapshot()`
  contract is identical, so the API and frontend don't change. Both engines share the
  topics (tagged by `source_mode`), so the **RTM / Micro-batch** toggle switches which
  tag's view is shown. Config is via env (see `app.yaml` and `docs/deploy-contract.md`).

## Run locally

```bash
# from the repo root, using the dev venv
PIP_USER=0 .venv/bin/python -m pip install -r app/requirements.txt
.venv/bin/uvicorn --app-dir app app:app --port 8000
# open http://localhost:8000
```

Controls (mock only): **Inject burst** drives a fleet-hot-zone surge; the **RTM /
Micro-batch** toggle previews the same UI under each latency profile.

## Deploy (live data)

Add an app resource to the bundle (or `databricks apps deploy`), set
`USE_MOCK_BACKEND=false`, and set the `KAFKA_*` env in `app.yaml` (bootstrap, topics,
source mode, and SASL creds via `valueFrom` from the Kafka secret scope). The app SP
needs `READ` on that scope. No database is attached — the app reads only Kafka.
