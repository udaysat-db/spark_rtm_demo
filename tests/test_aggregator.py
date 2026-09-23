"""Unit tests for the console's rolling aggregator — pure Python, no Kafka/Spark.

Exercises the alert/metric ingest and the snapshot contract: per-mode routing,
incident lifecycle, cells/roster, latency stats, freshness, and the metrics-fed
Tech fields.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from aggregator import FleetAggregator  # noqa: E402

T = 1_000_000  # base now_ms


def alert(**over):
    # event_ts / input_kafka_ts are the latency START references; the app computes
    # latency = alert_kafka_ts (passed to ingest_alert) − start. Defaults put both at T.
    rec = dict(
        source_mode="rtm", freezer_id="FZ-0142-01", site_id="S0142",
        site_name="Store 0142 Austin TX", alert_type="TEMP_HIGH", alert_severity="warning",
        lifecycle_event="OPENED", event_ts=T, input_kafka_ts=T,
        incident_id="FZ-0142-01:1", scenario_name="normal",
    )
    rec.update(over)
    return rec


def metric(**over):
    rec = dict(source_mode="rtm", input_rows_per_second=200.0, num_input_rows=1000,
               offsets_behind_latest_max=3.0, is_rtm=True,
               proc_latency_p50_ms=40.0, proc_latency_p99_ms=120.0,
               queue_latency_p50_ms=5.0, queue_latency_p99_ms=30.0,
               e2e_latency_p50_ms=60.0, e2e_latency_p99_ms=180.0)
    rec.update(over)
    return rec


def test_opened_alert_populates_business():
    agg = FleetAggregator()
    agg.ingest_alert(alert(alert_type="DOOR_OPEN_TOO_LONG", alert_severity="serious"), now_ms=T)
    b = agg.snapshot("rtm", now_ms=T)["business"]
    assert b["alerts"] == 1 and b["units_in_alert"] == 1
    assert b["by_severity"]["serious"] == 1
    assert next(t["n"] for t in b["by_type"] if t["key"] == "DOOR_OPEN_TOO_LONG") == 1
    assert b["feed"][0]["device"] == "FZ-0142-01" and b["feed"][0]["severity"] == "serious"


def test_source_mode_is_routed_and_microbatch_maps_to_mb():
    agg = FleetAggregator()
    agg.ingest_alert(alert(source_mode="rtm"), now_ms=T)
    agg.ingest_alert(alert(source_mode="microbatch", freezer_id="FZ-0331-02",
                           site_id="S0331", incident_id="FZ-0331-02:1"), now_ms=T)
    assert agg.snapshot("rtm", now_ms=T)["business"]["alerts"] == 1
    assert agg.snapshot("mb", now_ms=T)["business"]["alerts"] == 1
    # views are independent
    assert agg.snapshot("rtm", now_ms=T)["business"]["units_in_alert"] == 1


def test_resolved_closes_the_unit():
    agg = FleetAggregator()
    agg.ingest_alert(alert(), now_ms=T)
    assert agg.snapshot("rtm", now_ms=T)["business"]["units_in_alert"] == 1
    agg.ingest_alert(alert(lifecycle_event="RESOLVED"), now_ms=T + 1000)
    assert agg.snapshot("rtm", now_ms=T + 1000)["business"]["units_in_alert"] == 0


def test_critical_is_deduped_by_incident_id():
    agg = FleetAggregator()
    agg.ingest_alert(alert(alert_severity="critical", incident_id="i1"), now_ms=T)
    agg.ingest_alert(alert(lifecycle_event="ESCALATED", alert_severity="critical",
                           incident_id="i1"), now_ms=T + 500)
    b = agg.snapshot("rtm", now_ms=T + 500)["business"]
    assert b["alerts"] == 1        # only OPENED counts as a new alert
    assert b["critical"] == 1      # escalation didn't double-count the incident


def test_fresh_pct_from_within_250ms():
    agg = FleetAggregator()
    # Freshness is judged on B latency = alert_kafka_ts − input_kafka_ts; <=250ms is fresh.
    agg.ingest_alert(alert(input_kafka_ts=T - 100, incident_id="a"), now_ms=T, alert_kafka_ts=T)
    agg.ingest_alert(alert(freezer_id="FZ-2", input_kafka_ts=T - 500, incident_id="b"),
                     now_ms=T, alert_kafka_ts=T)
    assert agg.snapshot("rtm", now_ms=T)["business"]["fresh_pct"] == 50.0


def test_latency_percentiles_and_buckets():
    agg = FleetAggregator()
    # B latency per alert = alert_kafka_ts − input_kafka_ts.
    for i, lat in enumerate([100, 300, 2000, 8000]):
        agg.ingest_alert(alert(freezer_id=f"FZ-{i}", incident_id=f"i{i}",
                               input_kafka_ts=T - lat), now_ms=T, alert_kafka_ts=T)
    tech = agg.snapshot("rtm", now_ms=T)["tech"]
    assert tech["max"] == 8000
    assert tech["buckets"] == [1, 1, 1, 1]     # <=250, <=1000, <=5000, >5000
    assert tech["segments"]["pipeline"]["p50"] == 2000   # pipeline (B) p50


def test_cells_use_roster_and_worst_severity():
    agg = FleetAggregator(roster=["0142", "0331"])
    agg.ingest_alert(alert(site_id="S0142", alert_severity="critical"), now_ms=T)
    cells = {c["n"]: c["s"] for c in agg.snapshot("rtm", now_ms=T)["business"]["cells"]}
    assert cells == {"0142": 3, "0331": 0}   # critical store hot, the other OK


def test_metric_feeds_tech_lag_builtins():
    # The consumer's per-batch metric feeds lag + RTM-internal builtins (NOT evps — that's
    # now the app-tailed live input rate, see test_input_arrivals_feed_live_evps).
    agg = FleetAggregator()
    agg.ingest_metric(metric())
    tech = agg.snapshot("rtm", now_ms=T)["tech"]
    assert tech["lag_ms"] == 3.0
    assert tech["builtins"]["e50"] == 60.0 and tech["builtins"]["proc99"] == 120.0


def test_input_arrivals_feed_live_evps():
    # events/sec is computed app-side from input-topic arrivals over EVPS_WINDOW_MS (5s),
    # independent of the consumer's per-batch metric — so it stays live at any trigger.
    agg = FleetAggregator()
    for i in range(1000):                       # 1000 events across the 5s window → 200/s
        agg.ingest_input(T - i * 5)             # spread over the last ~5s
    snap = agg.snapshot("rtm", now_ms=T)
    assert snap["evps"] == 200 and snap["tech"]["evps"] == 200


def test_microbatch_metric_has_no_builtins():
    agg = FleetAggregator()
    agg.ingest_metric(metric(source_mode="microbatch", is_rtm=False,
                             proc_latency_p50_ms=None, e2e_latency_p50_ms=None))
    assert agg.snapshot("mb", now_ms=T)["tech"]["builtins"] is None


def test_stale_units_are_evicted():
    agg = FleetAggregator(roster=["0142"])
    agg.ingest_alert(alert(), now_ms=T)
    late = T + 60_001   # past ACTIVE_MS
    snap = agg.snapshot("rtm", now_ms=late)
    assert snap["business"]["units_in_alert"] == 0
    assert snap["business"]["cells"][0]["s"] == 0


def test_snapshot_shape_matches_contract():
    agg = FleetAggregator()
    agg.ingest_alert(alert(), now_ms=T)
    agg.ingest_metric(metric())
    s = agg.snapshot("rtm", now_ms=T)
    assert set(s) == {"mode", "engine", "scenario_name", "burst", "evps",
                      "business", "tech"}
    assert set(s["business"]) == {"units_monitored", "units_in_alert", "alerts", "critical",
                                  "fresh_pct", "cells", "top_sites", "by_type", "by_severity", "feed"}
    assert set(s["tech"]) == {"evps", "alps", "p50", "p95", "p99", "max", "segments",
                              "lag_ms", "vol_in", "vol_out", "lat_series", "ev_series",
                              "al_series", "buckets", "builtins"}
