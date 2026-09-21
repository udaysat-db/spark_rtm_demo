"""Static enrichment — the broadcast side of the stream-static join (RTM-supported).

Loads the three reference CSVs and produces one fleet table keyed by freezer_id,
carrying the thresholds and metadata `rules.business_logic` needs."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

_ENRICH_COLS = [
    "freezer_id", "freezer_type", "temperature_upper_limit", "door_open_limit_seconds",
    "maintenance_priority", "inventory_value_band", "site_name", "site_region",
]


def load_fleet(spark: SparkSession, static_path: str) -> DataFrame:
    """Join freezer_metadata + device_thresholds + site_metadata into one table.
    Also the pool the producer draws freezers from."""
    fm = spark.read.option("header", True).csv(f"{static_path}/freezer_metadata.csv")
    dt = (spark.read.option("header", True).csv(f"{static_path}/device_thresholds.csv")
          .withColumn("temperature_upper_limit", F.col("temperature_upper_limit").cast("double"))
          .withColumn("door_open_limit_seconds", F.col("door_open_limit_seconds").cast("int")))
    sm = spark.read.option("header", True).csv(f"{static_path}/site_metadata.csv")
    return fm.join(dt, "freezer_type", "left").join(sm, "site_id", "left")


def enrich(events: DataFrame, fleet: DataFrame) -> DataFrame:
    """Broadcast stream-static join of live events to the fleet table by freezer_id.
    `events` keeps its own site_id; only the enrichment columns are pulled in."""
    return events.join(F.broadcast(fleet.select(*_ENRICH_COLS)), "freezer_id", "left")
