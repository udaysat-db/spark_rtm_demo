"""SignalNow micro-batch consumer.

The deliberate contrast to the RTM consumer: the SAME enrichment + rules + latency
pipeline, but with a processing-time micro-batch trigger. Its latency climbs under
burst while the RTM consumer stays sub-second — the whole point of the demo.
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
    p.add_argument("--source-mode", default="microbatch")
    p.add_argument("--trigger-interval", default="5 seconds")
    p.add_argument("--metrics-topic", default=None,
                   help="If set, relay per-batch pipeline metrics to this Kafka topic.")
    return p.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("signalnow-microbatch-consumer").getOrCreate()

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
             .outputMode("update")   # transformWithState emits incident updates
             .trigger(processingTime=args.trigger_interval)   # micro-batch
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
