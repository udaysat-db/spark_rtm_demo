"""SignalNow RTM producer.

A Real-Time Mode streaming job: a rate source drives synthetic freezer telemetry
(shaped by the chosen scenario) that is published to the sensor-events Kafka topic.
The producer itself runs in RTM, so the whole demo is one streaming engine.
"""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

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
    p.add_argument("--scenario", default="normal")
    p.add_argument("--num-partitions", type=int, default=4)
    p.add_argument("--rtm-trigger", default="5 minutes",
                   help="RTM long-running batch / checkpoint duration (trigger realTime).")
    return p.parse_args()


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
    events = build_events(assigned.join(F.broadcast(fleet), "idx"), args.scenario)
    payload = events.select(F.col("freezer_id").alias("key"), F.to_json(F.struct("*")).alias("value"))

    query = (payload.writeStream.format("kafka")
             .options(**kafka_options(args.kafka_bootstrap, args.secret_scope, spark))
             .option("topic", args.topic)
             .option("checkpointLocation", args.checkpoint)
             .outputMode("update")
             .trigger(realTime=args.rtm_trigger)     # Real-Time Mode
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
