"""Unit tests for the metrics listener's PARSING logic — pure, no Spark/JVM/Kafka.

The py4j Java-producer publish path is on-cluster glue and isn't exercised here;
what's tested is `_build` turning a StreamingQueryProgress JSON into the metrics
record, plus the percentile extraction. The listener is constructed with spark=None
(the JVM producer is created lazily, only on publish).
"""
import json

from shared.metrics_listener import MetricsListener, _pctile


class FakeProgress:
    """Stands in for StreamingQueryProgress — only `json()` is used by `_build`."""
    def __init__(self, d):
        self._d = d

    def json(self):
        return json.dumps(self._d)


def _listener(source_mode="rtm"):
    return MetricsListener(spark=None, bootstrap="host:9092", secret_scope=None,
                           metrics_topic="freezer_pipeline_metrics", source_mode=source_mode)


RTM_PROGRESS = {
    "name": "signalnow-rtm-consumer", "id": "q-1", "runId": "run-1", "batchId": 5,
    "timestamp": "2026-09-21T00:00:00.000Z",
    "numInputRows": 1000, "inputRowsPerSecond": 200.0, "processedRowsPerSecond": 205.0,
    "durationMs": {"triggerExecution": 120},
    "sources": [{"metrics": {"maxOffsetsBehindLatest": "3", "avgOffsetsBehindLatest": "1"}}],
    "latencies": {
        "processingLatencyMs": {"p50": 40.0, "p99": 120.0},
        "sourceQueuingLatencyMs": {"p50": 5.0, "p99": 30.0},
        "e2eLatencyMs": {"p50": 60.0, "p99": 180.0},
    },
}

MB_PROGRESS = {
    "name": "signalnow-microbatch-consumer", "id": "q-2", "runId": "run-2", "batchId": 7,
    "timestamp": "2026-09-21T00:00:05.000Z",
    "numInputRows": 900, "inputRowsPerSecond": 180.0, "processedRowsPerSecond": 178.0,
    "durationMs": {"triggerExecution": 4200},
    "sources": [{"metrics": {"maxOffsetsBehindLatest": "1200", "avgOffsetsBehindLatest": "600"}}],
    # no "latencies" — RTM-only field
}


def test_build_rtm_record_has_latency_percentiles():
    rec = _listener("rtm")._build(FakeProgress(RTM_PROGRESS))
    assert rec["source_mode"] == "rtm" and rec["is_rtm"] is True
    assert rec["batch_id"] == 5 and rec["num_input_rows"] == 1000
    assert rec["input_rows_per_second"] == 200.0
    assert rec["trigger_duration_ms"] == 120
    assert rec["offsets_behind_latest_max"] == 3.0
    assert rec["proc_latency_p99_ms"] == 120.0
    assert rec["queue_latency_p50_ms"] == 5.0
    assert rec["e2e_latency_p50_ms"] == 60.0 and rec["e2e_latency_p99_ms"] == 180.0


def test_build_microbatch_record_has_no_latencies():
    rec = _listener("microbatch")._build(FakeProgress(MB_PROGRESS))
    assert rec["source_mode"] == "microbatch" and rec["is_rtm"] is False
    assert rec["offsets_behind_latest_max"] == 1200.0     # lag shows up here
    assert rec["trigger_duration_ms"] == 4200
    assert "e2e_latency_p50_ms" not in rec                 # RTM-only fields absent


def test_build_tolerates_sparse_progress():
    rec = _listener()._build(FakeProgress({"batchId": 0}))
    assert rec["is_rtm"] is False
    assert rec["input_rows_per_second"] is None
    assert rec["offsets_behind_latest_max"] is None


def test_pctile_key_spellings_and_missing():
    assert _pctile({"p50": 12.0}, "p50") == 12.0
    assert _pctile({"P99": 30.0}, "p99") == 30.0    # upper-case
    assert _pctile({"50": 9.0}, "p50") == 9.0       # bare number
    assert _pctile({}, "p50") is None
    assert _pctile(None, "p50") is None
