"""SignalNow producer.

A micro-batch streaming job: a rate source drives synthetic freezer telemetry
(shaped by the current scenario) that is published to the sensor-events Kafka topic.

The producer is a data generator, NOT the Real-Time Mode showcase — that's the
consumers, where alerting latency is the story. It runs plain micro-batch.

**Live scenario control.** The console writes append-only control files to a UC
Volume directory; each micro-batch the producer re-reads that directory and shapes
events by the newest scenario, so a console change takes effect within a trigger —
no restart. This MUST be done in `foreachBatch`: a stream-static join caches the
file listing at query start and never sees files added later (that bug made the
scenario appear stuck). Reading the directory FRESH each batch re-lists it and
picks up new writes.
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
                        "to drive the scenario LIVE (re-read fresh every batch in foreachBatch; "
                        "latest by `ts` wins). Absent → fixed --scenario.")
    p.add_argument("--num-partitions", type=int, default=4)
    p.add_argument("--trigger-interval", default="1 second",
                   help="Micro-batch trigger (processingTime); also the max latency for a "
                        "live control-file change to take effect.")
    return p.parse_args()


_CONTROL_SCHEMA = T.StructType([
    T.StructField("scenario", T.StringType()),
    T.StructField("ts", T.LongType()),
])


def read_scenario(spark, control_dir, default_scenario):
    """Return the newest scenario from the append-only control DIRECTORY, or the
    default. Reads the directory FRESH (a new DataFrame re-lists it), which is why this
    is called per-batch inside foreachBatch and NOT set up once as a stream-static join
    (that caches the listing and never sees files the console adds later). Any read
    hiccup (e.g. a file mid-upload) falls back to the default rather than failing."""
    if not control_dir:
        return default_scenario
    try:
        row = (spark.read.schema(_CONTROL_SCHEMA).json(control_dir)
               .agg(F.expr("max_by(scenario, ts)").alias("scenario")).first())
        return row["scenario"] if row and row["scenario"] else default_scenario
    except Exception:
        return default_scenario


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("signalnow-producer").getOrCreate()

    # The fleet the producer draws from, with a 0-based index for round-robin assignment.
    fleet = load_fleet(spark, args.static_path).withColumn(
        "idx", (F.row_number().over(Window.orderBy("freezer_id")) - 1)
    )
    fleet = materialize(fleet)   # collapse the window into a plain relation for broadcast
    n = fleet.count()

    rate = (spark.readStream.format("rate")
            .option("rowsPerSecond", args.events_per_second)
            .option("numPartitions", args.num_partitions)
            .load())

    kopts = kafka_options(args.kafka_bootstrap, args.secret_scope, spark)
    # Keep the last good scenario so a transient control-read miss doesn't flip the demo.
    state = {"scenario": args.scenario}

    def publish_batch(batch_df, _batch_id):
        # Re-read the control dir FRESH each batch → picks up console writes live.
        state["scenario"] = read_scenario(spark, args.control_path, state["scenario"])
        assigned = batch_df.withColumn("idx", (F.col("value") % F.lit(n)).cast("int"))
        events = build_events(assigned.join(F.broadcast(fleet), "idx"), state["scenario"])
        payload = events.select(F.col("freezer_id").alias("key"),
                                F.to_json(F.struct("*")).alias("value"))
        (payload.write.format("kafka").options(**kopts)
         .option("topic", args.topic).mode("append").save())

    query = (rate.writeStream.foreachBatch(publish_batch)
             .option("checkpointLocation", args.checkpoint)
             .trigger(processingTime=args.trigger_interval)
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
