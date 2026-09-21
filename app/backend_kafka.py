"""Kafka-backed data source — tails the alerts + metrics topics and feeds the pure
`FleetAggregator`; `snapshot()` returns the same dict as `MockDataSource`, so the API
and UI don't change. Kafka is the only sink — there is no database in the path.

A background thread polls Kafka and updates the in-memory aggregator; `snapshot()`
reads the current view. Both engines write to the shared topics tagged by
`source_mode`, so one instance can toggle RTM ↔ micro-batch with `set_mode`.

Config via env (injected by app.yaml on Databricks Apps; see docs/deploy-contract.md):
  KAFKA_BOOTSTRAP          host:port[,host:port]
  KAFKA_ALERTS_TOPIC       default freezer_alerts_enriched
  KAFKA_METRICS_TOPIC      default freezer_pipeline_metrics
  KAFKA_SOURCE_MODE        engine to show first: rtm | mb   (default rtm)
  KAFKA_SASL_MECHANISM     PLAIN | SCRAM-SHA-512 | ...       (omit for plaintext)
  KAFKA_SASL_USERNAME      SASL user   (secret; omit for plaintext)
  KAFKA_SASL_PASSWORD      SASL secret (secret; omit for plaintext)
  KAFKA_SECURITY_PROTOCOL  default SASL_SSL when a username is set, else PLAINTEXT
  STORE_ROSTER_PATH        optional override; defaults to the shipped app/fleet_roster.csv
                           (a site_id CSV) so the grid shows every store at rest
"""
from __future__ import annotations

import csv
import json
import os
import threading

from aggregator import FleetAggregator, _store_num


def _load_roster(path):
    if not path or not os.path.exists(path):
        return None
    stores = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("site_id"):
                stores.append(_store_num({"site_id": row["site_id"], "site_name": row.get("site_name")}))
    return stores or None


def _consumer_kwargs():
    kwargs = {
        "bootstrap_servers": os.environ["KAFKA_BOOTSTRAP"].split(","),
        "value_deserializer": lambda b: json.loads(b.decode("utf-8")),
        "auto_offset_reset": "latest",     # live tail; history isn't needed
        "enable_auto_commit": False,       # ephemeral view, no offset tracking
        "consumer_timeout_ms": 1000,
    }
    user = os.getenv("KAFKA_SASL_USERNAME")
    if user:
        kwargs.update({
            "security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
            "sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            "sasl_plain_username": user,
            "sasl_plain_password": os.getenv("KAFKA_SASL_PASSWORD", ""),
        })
    else:
        kwargs["security_protocol"] = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    return kwargs


class KafkaDataSource:
    is_mock = False

    def __init__(self):
        self.mode = "mb" if os.getenv("KAFKA_SOURCE_MODE") == "mb" else "rtm"
        self.alerts_topic = os.getenv("KAFKA_ALERTS_TOPIC", "freezer_alerts_enriched")
        self.metrics_topic = os.getenv("KAFKA_METRICS_TOPIC", "freezer_pipeline_metrics")
        # Roster ships with the app (app/fleet_roster.csv) so the grid renders every
        # store at rest; STORE_ROSTER_PATH overrides. Falls back to deriving stores
        # from alerts if neither is present.
        default_roster = os.path.join(os.path.dirname(__file__), "fleet_roster.csv")
        roster_path = os.getenv("STORE_ROSTER_PATH") or (
            default_roster if os.path.exists(default_roster) else None)
        self.agg = FleetAggregator(roster=_load_roster(roster_path))
        self._lock = threading.Lock()
        t = threading.Thread(target=self._run, daemon=True)
        t.start()

    # ---- controls -------------------------------------------------------
    def set_burst(self, burst: bool) -> None:
        pass  # real data reflects the live producer; no-op

    def set_mode(self, mode: str) -> None:
        # Both engines are in the shared topics (tagged), so this just switches which
        # tag's view we render.
        if mode in ("rtm", "mb"):
            self.mode = mode

    # ---- consume loop ---------------------------------------------------
    def _run(self):
        from kafka import KafkaConsumer  # imported here so the module loads without kafka installed
        consumer = KafkaConsumer(self.alerts_topic, self.metrics_topic, **_consumer_kwargs())
        for msg in consumer:                       # times out every 1s, then loops
            rec = msg.value
            if not isinstance(rec, dict):
                continue
            with self._lock:
                if msg.topic == self.alerts_topic:
                    self.agg.ingest_alert(rec)
                else:
                    self.agg.ingest_metric(rec)

    # ---- snapshot -------------------------------------------------------
    def snapshot(self) -> dict:
        with self._lock:
            return self.agg.snapshot(self.mode)
