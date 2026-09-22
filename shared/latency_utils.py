"""End-to-end latency stamping, shared by all pipelines so every `source_mode`
measures latency the same way.

Kept separate from `rules.business_logic` because it reads the wall clock — the
rules stay pure and unit-testable, and this adds the timing/`source_mode` columns
after the rules have run. Pass `now_ms` to make the stamp deterministic in tests.
"""
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from shared.schemas import INCIDENT_COLUMNS

# Business KPI thresholds (milliseconds).
WITHIN_250MS = 250
WITHIN_1S = 1000
WITHIN_5S = 5000


def add_latency_fields(df: DataFrame, source_mode: str, now_ms: Optional[int] = None) -> DataFrame:
    """Stamp `source_mode`, processing/emit timestamps, end-to-end latency, and
    the within-window booleans onto an alert DataFrame.

    `now_ms` (epoch millis) fixes the emit time for tests; in a stream it is left
    None and the current wall clock is used.
    """
    # Keep one output shape: the stateless reference path doesn't emit the incident
    # columns, so fill them with typed nulls when absent (the stateful path already
    # has them). Harmless no-op when they're present.
    for name, sql_type in INCIDENT_COLUMNS:
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast(sql_type))
    # input_kafka_ts rides through the stateful path (read_events → enriched_schema);
    # null-fill it for the stateless reference path so the output shape is identical.
    if "input_kafka_ts" not in df.columns:
        df = df.withColumn("input_kafka_ts", F.lit(None).cast("long"))

    # NOTE: these emit-time / end_to_end_latency_ms fields use current_timestamp(), which
    # is batch-fixed in RTM and therefore NOT a valid latency there. They are kept only
    # for schema continuity; the console computes real latency in the app from the alert's
    # Kafka timestamp minus event_ts (A) / input_kafka_ts (B). See app/aggregator.py.
    emit = F.lit(now_ms).cast("long") if now_ms is not None else F.expr("unix_millis(current_timestamp())")
    latency = emit - F.col("producer_ts")
    return (
        df.withColumn("source_mode", F.lit(source_mode))
        .withColumn("processing_start_ts", emit)
        .withColumn("alert_emit_ts", emit)
        .withColumn("end_to_end_latency_ms", latency)
        .withColumn("within_250ms", latency <= F.lit(WITHIN_250MS))
        .withColumn("within_1s", latency <= F.lit(WITHIN_1S))
        .withColumn("within_5s", latency <= F.lit(WITHIN_5S))
    )
