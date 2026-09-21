"""Unit tests for the shared alerting rules — run on a local Spark session, no
cluster or Kafka required. Each scenario builds a small enriched DataFrame, runs
`business_logic`, and asserts the alert type/severity/count and output schema.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.schemas import enriched_schema, ALERT_OUTPUT_COLUMNS
from shared.rules import business_logic
from shared.latency_utils import add_latency_fields

SCHEMA = enriched_schema()
FIELDS = [f.name for f in SCHEMA.fields]

# A healthy freezer: frozen well below its -15°C limit, door shut, compressor
# healthy, on mains power. Overridden per scenario.
DEFAULTS = dict(
    event_id="e1", event_ts=1000, device_id="FRZ-001", freezer_id="FZ1", site_id="S1",
    temperature_c=-18.0, humidity_pct=40.0, door_open=False, door_open_seconds=0,
    compressor_on=True, compressor_health=0.95, power_state="normal", battery_pct=100.0,
    ambient_temp_c=22.0, defrost_cycle_active=False, scenario_name="normal",
    synthetic_severity="none", producer_ts=1000,
    site_name="Store 0142", site_region="TX", freezer_type="reach_in",
    temperature_upper_limit=-15.0, door_open_limit_seconds=120,
    maintenance_priority="low", inventory_value_band="low",
)


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder
         .master("local[1]")
         .appName("rules-tests")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


def _df(spark, *rows):
    data = []
    for r in rows:
        row = dict(DEFAULTS)
        row.update(r)
        data.append(tuple(row[f] for f in FIELDS))
    return spark.createDataFrame(data, SCHEMA)


def _alerts(spark, *rows):
    return {r["alert_type"]: r for r in business_logic(_df(spark, *rows)).collect()}


def test_normal_raises_no_alert(spark):
    assert business_logic(_df(spark, {})).count() == 0


def test_temp_high_warning_vs_critical(spark):
    warn = business_logic(_df(spark, {"temperature_c": -14.0})).collect()  # 1°C over
    assert len(warn) == 1 and warn[0]["alert_type"] == "TEMP_HIGH" and warn[0]["alert_severity"] == "warning"
    crit = business_logic(_df(spark, {"temperature_c": -12.0})).collect()  # 3°C over
    assert crit[0]["alert_type"] == "TEMP_HIGH" and crit[0]["alert_severity"] == "critical"


def test_door_open_too_long(spark):
    warn = business_logic(_df(spark, {"door_open": True, "door_open_seconds": 200})).collect()
    assert warn[0]["alert_type"] == "DOOR_OPEN_TOO_LONG" and warn[0]["alert_severity"] == "warning"
    crit = business_logic(_df(spark, {"door_open": True, "door_open_seconds": 300})).collect()  # > 2x limit
    assert crit[0]["alert_severity"] == "critical"


def test_compressor_failure_risk(spark):
    rows = business_logic(_df(spark, {
        "compressor_on": False, "compressor_health": 0.3, "temperature_c": -15.5,
    })).collect()
    assert len(rows) == 1 and rows[0]["alert_type"] == "COMPRESSOR_FAILURE_RISK"
    assert rows[0]["alert_severity"] == "serious"  # near limit but not yet over


def test_power_and_warming(spark):
    rows = business_logic(_df(spark, {
        "power_state": "outage", "battery_pct": 10.0, "temperature_c": -15.5,
    })).collect()
    assert len(rows) == 1 and rows[0]["alert_type"] == "POWER_AND_WARMING"
    assert rows[0]["alert_severity"] == "critical"


def test_multi_signal_takes_precedence(spark):
    # Temp high AND door too long AND high-value asset -> reported as MULTI, not TEMP/DOOR.
    rows = business_logic(_df(spark, {
        "temperature_c": -12.0, "door_open": True, "door_open_seconds": 300,
        "inventory_value_band": "high",
    })).collect()
    assert len(rows) == 1
    assert rows[0]["alert_type"] == "MULTI_SIGNAL_CRITICAL" and rows[0]["alert_severity"] == "critical"


def test_temp_high_alone_is_not_multi(spark):
    # Temp high but low-value, door shut, compressor healthy -> plain TEMP_HIGH.
    rows = business_logic(_df(spark, {"temperature_c": -12.0})).collect()
    assert rows[0]["alert_type"] == "TEMP_HIGH"


def test_mixed_batch_counts(spark):
    # One normal + one temp-high + one door -> exactly two alerts.
    out = business_logic(_df(spark,
        {},
        {"temperature_c": -13.0},
        {"door_open": True, "door_open_seconds": 500},
    ))
    assert out.count() == 2


def test_output_has_full_alert_schema_after_latency(spark):
    alerts = business_logic(_df(spark, {"temperature_c": -12.0}))
    stamped = add_latency_fields(alerts, source_mode="rtm", now_ms=1200)
    for col in ALERT_OUTPUT_COLUMNS:
        assert col in stamped.columns, f"missing output column: {col}"


def test_latency_fields_and_buckets(spark):
    alerts = business_logic(_df(spark, {"temperature_c": -12.0, "producer_ts": 1000}))
    fast = add_latency_fields(alerts, "rtm", now_ms=1200).collect()[0]  # 200ms
    assert fast["end_to_end_latency_ms"] == 200
    assert fast["within_250ms"] and fast["within_1s"] and fast["within_5s"]
    slow = add_latency_fields(alerts, "microbatch", now_ms=4000).collect()[0]  # 3000ms
    assert slow["end_to_end_latency_ms"] == 3000
    assert (not slow["within_250ms"]) and (not slow["within_1s"]) and slow["within_5s"]
