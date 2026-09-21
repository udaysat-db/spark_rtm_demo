"""Event, enrichment, and alert schemas — the single source of truth for the
SignalNow cold-chain pipelines.

The same schemas are used by the producer, both consumers (RTM and micro-batch),
and the tests, so every stage of the demo agrees on the contract.
"""
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    LongType,
    BooleanType,
)

# ---------------------------------------------------------------------------
# Sensor event — the JSON payload published to Kafka `freezer_sensor_events`.
# Timestamps are epoch milliseconds (LongType) so latency math is unit-safe.
# ---------------------------------------------------------------------------
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_ts", LongType(), False),
    StructField("device_id", StringType(), False),
    StructField("freezer_id", StringType(), False),
    StructField("site_id", StringType(), False),
    StructField("temperature_c", DoubleType(), False),
    StructField("humidity_pct", DoubleType(), True),
    StructField("door_open", BooleanType(), False),
    StructField("door_open_seconds", IntegerType(), False),
    StructField("compressor_on", BooleanType(), False),
    StructField("compressor_health", DoubleType(), False),
    StructField("power_state", StringType(), False),
    StructField("battery_pct", DoubleType(), False),
    StructField("ambient_temp_c", DoubleType(), True),
    StructField("defrost_cycle_active", BooleanType(), True),
    StructField("scenario_name", StringType(), False),
    StructField("synthetic_severity", StringType(), True),
    StructField("producer_ts", LongType(), False),
])

# ---------------------------------------------------------------------------
# Static enrichment — joined in from the reference CSVs (broadcast stream-static
# join). Keyed by freezer_id / site_id at runtime.
# ---------------------------------------------------------------------------
ENRICHMENT_SCHEMA = StructType([
    StructField("site_name", StringType(), True),
    StructField("site_region", StringType(), True),
    StructField("freezer_type", StringType(), True),
    StructField("temperature_upper_limit", DoubleType(), False),
    StructField("door_open_limit_seconds", IntegerType(), False),
    StructField("maintenance_priority", StringType(), True),
    StructField("inventory_value_band", StringType(), True),
])


def enriched_schema() -> StructType:
    """Event schema + enrichment columns — the input `business_logic` expects."""
    return StructType(EVENT_SCHEMA.fields + ENRICHMENT_SCHEMA.fields)


# Columns written to Kafka `freezer_alerts_enriched` (the only sink). All consumers
# emit this identical shape so results are comparable by `source_mode`. The stateful
# path adds the six incident columns (incident_id … temp_trend); the stateless
# reference path leaves them null (filled by `latency_utils.add_latency_fields`).
ALERT_OUTPUT_COLUMNS = [
    "event_id",
    "source_mode",
    "event_ts",
    "producer_ts",
    "processing_start_ts",
    "alert_emit_ts",
    "end_to_end_latency_ms",
    "device_id",
    "freezer_id",
    "site_id",
    "site_name",
    "site_region",
    "alert_type",
    "alert_severity",
    "alert_reason",
    # incident lifecycle (stateful path)
    "incident_id",
    "lifecycle_event",
    "incident_started_ts",
    "escalation_level",
    "peak_temp_c",
    "temperature_c",
    "temperature_upper_limit",
    "door_open",
    "door_open_seconds",
    "compressor_on",
    "compressor_health",
    "power_state",
    "battery_pct",
    "scenario_name",
    "temp_trend",
    "within_250ms",
    "within_1s",
    "within_5s",
]

# The six columns the stateful incident engine adds; the stateless reference path
# doesn't produce them, so they're null-filled downstream to keep one output shape.
INCIDENT_COLUMNS = [
    ("incident_id", "string"),
    ("lifecycle_event", "string"),
    ("incident_started_ts", "long"),
    ("escalation_level", "int"),
    ("peak_temp_c", "double"),
    ("temp_trend", "array<struct<ts:bigint,temp:double>>"),
]
