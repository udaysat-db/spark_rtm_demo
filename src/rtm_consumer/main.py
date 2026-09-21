"""SignalNow RTM consumer.

Reads sensor events from Kafka, enriches via a broadcast stream-static join, applies
the shared alerting rules, stamps latency, and writes enriched alerts back to Kafka
in Real-Time Mode (`trigger(realTime=...)`, `outputMode("update")`). Identical logic
to the micro-batch consumer — only the trigger/output mode differ.
"""
import argparse

from pyspark.sql import SparkSession

from shared.enrichment import load_fleet
from shared.pipeline import read_events, alerts_stream_stateful, to_kafka
from shared.kafka_io import kafka_options
from shared.metrics_listener import MetricsListener


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--kafka-bootstrap", required=True)
    p.add_argument("--secret-scope", default=None)
    p.add_argument("--input-topic", required=True)
    p.add_argument("--output-topic", required=True)
    p.add_argument("--static-path", required=True)
    p.add_argument("--checkpoint", required=True)
    p.add_argument("--source-mode", default="rtm")
    p.add_argument("--metrics-topic", default=None,
                   help="If set, relay per-batch pipeline metrics to this Kafka topic.")
    return p.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("signalnow-rtm-consumer").getOrCreate()

    if args.metrics_topic:
        spark.streams.addListener(MetricsListener(
            spark, args.kafka_bootstrap, args.secret_scope, args.metrics_topic, args.source_mode))

    fleet = load_fleet(spark, args.static_path)
    events = read_events(spark, args.kafka_bootstrap, args.secret_scope, args.input_topic)
    alerts = alerts_stream_stateful(events, fleet, args.source_mode)

    query = (to_kafka(alerts).writeStream.format("kafka")
             .options(**kafka_options(args.kafka_bootstrap, args.secret_scope, spark))
             .option("topic", args.output_topic)
             .option("checkpointLocation", args.checkpoint)
             .outputMode("update")
             .trigger(realTime="5 minutes")     # Real-Time Mode
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
