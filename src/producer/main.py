"""SignalNow producer.

A micro-batch streaming job: a rate source drives synthetic freezer telemetry
(shaped by the current scenario) that is published to the sensor-events Kafka topic.

The producer is a data generator, NOT the Real-Time Mode showcase — that's the
consumers, where alerting latency is the story. Running the producer in plain
micro-batch (rather than RTM) is deliberate: it lets the scenario be driven LIVE
from the console via a stream-static join to a control file (RTM's operator
allowlist forbids the broadcast-nested-loop join that attaches a refreshing 1-row
control to every event). The control file is re-read every micro-batch, so a
console change takes effect within a trigger (~1-2s).
"""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from shared.enrichment import load_fleet, materialize
from shared.scenarios import build_events
from shared.kafka_io import kafka_options


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--kafka-bootstrap", required=True)
    p.add_argument("--secret-scope", default=None)
    p.add_argument("--topic", required=True)
    p.add_argument("--static-path", required=True)
    p.add_argument("--checkpoint", required=True)
    p.add_argument("--events-per-second", type=int, default=200)
    p.add_argument("--scenario", default="normal",
                   help="Fallback scenario when no control file is present.")
    p.add_argument("--control-path", default=None,
                   help="UC Volume DIRECTORY of append-only control files the console writes "
                        "to drive the scenario LIVE (re-read every batch via a stream-static "
                        "join; latest by `ts` wins). Absent → fixed --scenario.")
    p.add_argument("--num-partitions", type=int, default=4)
    p.add_argument("--trigger-interval", default="1 second",
                   help="Micro-batch trigger (processingTime); also the max latency for a "
                        "live control-file change to take effect.")
    return p.parse_args()


def _scenario_source(spark, control_dir, default_scenario):
    """A 1-row broadcastable DataFrame with a single `scenario` column, read fresh from
    the control DIRECTORY so a stream-static join re-evaluates it each batch — that's
    what makes the scenario switchable live without restarting the query.

    The directory is APPEND-ONLY: each write is a new timestamped file, never an
    overwrite, so the per-batch read never races a writer (overwriting one file would
    make the read fail with FAILED_READ_FILE and kill the query). `max_by(scenario, ts)`
    picks the newest write; `agg` (no groupBy) always yields exactly one row even when
    the dir is empty, so the join never drops the whole stream; null → the default."""
    schema = T.StructType([
        T.StructField("scenario", T.StringType()),
        T.StructField("ts", T.LongType()),
    ])
    control = spark.read.schema(schema).json(control_dir)
    return control.agg(F.expr("max_by(scenario, ts)").alias("scenario")).select(
        F.coalesce(F.col("scenario"), F.lit(default_scenario)).alias("scenario"))


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("signalnow-producer").getOrCreate()

    # The fleet the producer draws from, with a 0-based index for round-robin assignment.
    fleet = load_fleet(spark, args.static_path).withColumn(
        "idx", (F.row_number().over(Window.orderBy("freezer_id")) - 1)
    )
    fleet = materialize(fleet)   # collapse the window too — the RTM broadcast side must be a plain relation
    n = fleet.count()

    rate = (spark.readStream.format("rate")
            .option("rowsPerSecond", args.events_per_second)
            .option("numPartitions", args.num_partitions)
            .load())

    assigned = rate.withColumn("idx", (F.col("value") % F.lit(n)).cast("int"))
    enriched = assigned.join(F.broadcast(fleet), "idx")

    if args.control_path:
        # Cross-join the current control row (broadcast) onto every event so the
        # scenario arrives as a live column. This is a stream-static join whose static
        # side (a 1-row JSON file scan) is re-evaluated each micro-batch, so a console
        # edit to the control file switches the scenario within one trigger — no
        # restart. (This broadcast-nested-loop join is why the producer is micro-batch,
        # not RTM: RTM's operator allowlist rejects it.)
        control = _scenario_source(spark, args.control_path, args.scenario)
        events = build_events(enriched.crossJoin(F.broadcast(control)), F.col("scenario"))
    else:
        events = build_events(enriched, args.scenario)

    payload = events.select(F.col("freezer_id").alias("key"), F.to_json(F.struct("*")).alias("value"))

    query = (payload.writeStream.format("kafka")
             .options(**kafka_options(args.kafka_bootstrap, args.secret_scope, spark))
             .option("topic", args.topic)
             .option("checkpointLocation", args.checkpoint)
             .outputMode("update")
             .trigger(processingTime=args.trigger_interval)   # micro-batch (see module docstring)
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
