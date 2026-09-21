"""StreamingQueryListener that relays per-batch pipeline metrics to a Kafka topic.

Both consumers attach this so the app can show the REAL input rate, offset lag,
trigger duration, and — in RTM — the built-in latency percentiles, read straight off
a Kafka topic with no workspace queries. One JSON record per StreamingQueryProgress
event, tagged with `source_mode` (the shared metrics topic, like the alerts topic,
separates engines by tag).

Publishing uses a driver-side Java KafkaProducer via py4j, NOT a Spark write. Under
RTM the query holds every executor slot continuously, so a Spark batch write would
contend for slots; producing from the driver JVM sidesteps that and needs no extra
Python dependency — kafka-clients is already on the classpath because the pipeline
reads and writes Kafka. Any publish failure is swallowed: a metrics hiccup must never
take down the streaming query.

Record fields (JSON value; key = source_mode):
  source_mode, query_name, query_id, run_id, batch_id, timestamp, emit_ts,
  num_input_rows, input_rows_per_second, processed_rows_per_second,
  trigger_duration_ms, offsets_behind_latest_max, offsets_behind_latest_avg, is_rtm,
  and (RTM only) proc/queue/e2e latency p50/p99 in ms.
"""
from __future__ import annotations

import json
import time

from pyspark.sql.streaming import StreamingQueryListener

from shared.kafka_io import kafka_options

# Reference list of the record fields (for the app-side consumer / docs).
METRICS_RECORD_FIELDS = [
    "source_mode", "query_name", "query_id", "run_id", "batch_id", "timestamp",
    "emit_ts", "num_input_rows", "input_rows_per_second", "processed_rows_per_second",
    "trigger_duration_ms", "offsets_behind_latest_max", "offsets_behind_latest_avg",
    "is_rtm", "proc_latency_p50_ms", "proc_latency_p99_ms", "queue_latency_p50_ms",
    "queue_latency_p99_ms", "e2e_latency_p50_ms", "e2e_latency_p99_ms",
]


def _f(x):
    """Coerce to float, or None."""
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _pctile(dist, key):
    """Pull a percentile (e.g. 'p50') out of an RTM latencies sub-object, tolerating
    key spellings ('p50' / 'P50' / '50')."""
    if not isinstance(dist, dict):
        return None
    for k in (key, key.upper(), key.lstrip("pP")):
        if k in dist:
            return _f(dist[k])
    return None


def _progress_json(progress) -> str:
    """StreamingQueryProgress → JSON string, across PySpark versions where `json` is
    either a property or a method."""
    j = getattr(progress, "json", None)
    if callable(j):
        return j()
    if isinstance(j, str):
        return j
    return str(progress)


class MetricsListener(StreamingQueryListener):
    def __init__(self, spark, bootstrap, secret_scope, metrics_topic, source_mode):
        self._spark = spark
        self._topic = metrics_topic
        self._source_mode = source_mode
        # Raw Kafka client props: strip the Spark "kafka." prefix kafka_options uses.
        self._props = {
            (k[6:] if k.startswith("kafka.") else k): v
            for k, v in kafka_options(bootstrap, secret_scope, spark).items()
        }
        self._producer = None

    # -- lifecycle ---------------------------------------------------------
    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        try:
            self._publish(self._build(event.progress))
        except Exception:
            pass  # a metrics hiccup must never take down the query

    def onQueryIdle(self, event):        # newer PySpark; harmless if never called
        pass

    def onQueryTerminated(self, event):
        self._close()

    # -- build a metrics record from StreamingQueryProgress ----------------
    def _build(self, progress) -> dict:
        p = json.loads(_progress_json(progress))
        sources = p.get("sources") or [{}]
        src_metrics = (sources[0] or {}).get("metrics") or {}
        lat = p.get("latencies") or {}          # present in RTM only
        rec = {
            "source_mode": self._source_mode,
            "query_name": p.get("name"),
            "query_id": p.get("id"),
            "run_id": p.get("runId"),
            "batch_id": p.get("batchId"),
            "timestamp": p.get("timestamp"),
            "emit_ts": int(time.time() * 1000),
            "num_input_rows": p.get("numInputRows"),
            "input_rows_per_second": _f(p.get("inputRowsPerSecond")),
            "processed_rows_per_second": _f(p.get("processedRowsPerSecond")),
            "trigger_duration_ms": (p.get("durationMs") or {}).get("triggerExecution"),
            "offsets_behind_latest_max": _f(src_metrics.get("maxOffsetsBehindLatest")),
            "offsets_behind_latest_avg": _f(src_metrics.get("avgOffsetsBehindLatest")),
            "is_rtm": bool(lat),
        }
        if lat:
            rec.update({
                "proc_latency_p50_ms": _pctile(lat.get("processingLatencyMs"), "p50"),
                "proc_latency_p99_ms": _pctile(lat.get("processingLatencyMs"), "p99"),
                "queue_latency_p50_ms": _pctile(lat.get("sourceQueuingLatencyMs"), "p50"),
                "queue_latency_p99_ms": _pctile(lat.get("sourceQueuingLatencyMs"), "p99"),
                "e2e_latency_p50_ms": _pctile(lat.get("e2eLatencyMs"), "p50"),
                "e2e_latency_p99_ms": _pctile(lat.get("e2eLatencyMs"), "p99"),
            })
        return rec

    # -- driver-side Java KafkaProducer via py4j ---------------------------
    def _get_producer(self):
        if self._producer is not None:
            return self._producer
        jvm = self._spark._jvm
        props = jvm.java.util.Properties()
        ser = "org.apache.kafka.common.serialization.StringSerializer"
        props.put("key.serializer", ser)
        props.put("value.serializer", ser)
        for k, v in self._props.items():
            props.put(k, str(v))
        self._producer = jvm.org.apache.kafka.clients.producer.KafkaProducer(props)
        return self._producer

    def _publish(self, record: dict) -> None:
        jvm = self._spark._jvm
        producer = self._get_producer()
        rec = jvm.org.apache.kafka.clients.producer.ProducerRecord(
            self._topic, self._source_mode, json.dumps(record))
        producer.send(rec)

    def _close(self) -> None:
        try:
            if self._producer is not None:
                self._producer.flush()
                self._producer.close()
        except Exception:
            pass
        finally:
            self._producer = None
