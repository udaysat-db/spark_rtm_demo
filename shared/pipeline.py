"""The consumer pipeline, identical for RTM and micro-batch — only the trigger and
output mode differ between the two entrypoints. This is the "same code" the demo
is about."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from shared.schemas import EVENT_SCHEMA, ALERT_OUTPUT_COLUMNS
from shared.rules import business_logic
from shared.latency_utils import add_latency_fields
from shared.enrichment import enrich
from shared.kafka_io import kafka_options


def read_events(spark: SparkSession, bootstrap: str, secret_scope: str, topic: str) -> DataFrame:
    raw = (spark.readStream.format("kafka")
           .options(**kafka_options(bootstrap, secret_scope, spark))
           .option("subscribe", topic)
           .option("startingOffsets", "latest")
           .option("failOnDataLoss", "false")         # survive retention/offset gaps
           .option("kafka.fetch.max.wait.ms", "50")   # low-latency polling
           .load())
    return raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e")).select("e.*")


def alerts_stream(events: DataFrame, fleet: DataFrame, source_mode: str) -> DataFrame:
    """STATELESS reference path: events -> broadcast enrich -> per-row rules -> latency
    stamp -> alert output schema. Batch-safe (used by the unit tests). The consumers
    run the stateful path below; this stays as the readable, testable equivalent."""
    return (add_latency_fields(business_logic(enrich(events, fleet)), source_mode)
            .select(*ALERT_OUTPUT_COLUMNS))


def alerts_stream_stateful(events: DataFrame, fleet: DataFrame, source_mode: str) -> DataFrame:
    """STATEFUL streaming path used by both consumers: events -> broadcast enrich (no
    shuffle) -> transformWithState incident engine (RTM's one shuffle) -> latency stamp
    -> alert output schema. Streaming-only (transformWithState needs a streaming query);
    the enrichment stays a broadcast join so the query keeps a single shuffle. See
    docs/alerting-logic.md."""
    from shared.stateful import apply_business_logic   # imported lazily (streaming-only)
    incidents = apply_business_logic(enrich(events, fleet))
    return add_latency_fields(incidents, source_mode).select(*ALERT_OUTPUT_COLUMNS)


def to_kafka(df: DataFrame) -> DataFrame:
    return df.select(F.col("event_id").alias("key"), F.to_json(F.struct("*")).alias("value"))
