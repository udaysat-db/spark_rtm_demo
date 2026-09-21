"""Synthetic freezer telemetry for the producer.

`build_events(df, scenario)` turns rows that already carry a freezer's static
attributes (freezer_id, site_id, thresholds, …) into full sensor events shaped by
a demo scenario. It is expressed entirely in Spark column functions so it runs as
part of a Real-Time Mode streaming query (rate source → events → Kafka) with no
Python row loops.

Scenarios control how often a unit is "stressed" and how its telemetry deviates,
so that downstream `rules.business_logic` produces a believable alert mix:

  normal            mostly healthy; rare, mild excursions
  door_open         many units with doors open past their limit
  compressor_failure compressors off + degraded health + warming
  power_outage      units on battery/outage, low battery, warming
  fleet_hot_zone    a large share of units warming over their limit (burst)
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SCENARIOS = ["normal", "door_open", "compressor_failure", "power_outage", "fleet_hot_zone"]

# fraction of units "stressed" for the scenario's characteristic failure
_STRESS = {
    "normal": 0.02,
    "door_open": 0.45,
    "compressor_failure": 0.30,
    "power_outage": 0.30,
    "fleet_hot_zone": 0.65,
}


def build_events(df: DataFrame, scenario: str) -> DataFrame:
    """`df` must carry: freezer_id, site_id, freezer_type, temperature_upper_limit,
    door_open_limit_seconds, maintenance_priority, inventory_value_band.
    Returns a DataFrame with the sensor-event columns (see schemas.EVENT_SCHEMA)."""
    if scenario not in SCENARIOS:
        raise ValueError(f"unknown scenario {scenario!r}; expected one of {SCENARIOS}")
    p = _STRESS[scenario]

    stressed = F.rand() < F.lit(p)
    noise = (F.rand() - F.lit(0.5)) * F.lit(2.0)          # ±1.0 °C jitter
    upper = F.col("temperature_upper_limit")
    nominal = upper - F.lit(3.0)                           # healthy set point

    warming = scenario in ("compressor_failure", "power_outage", "fleet_hot_zone")
    # temperature: healthy near nominal; stressed units climb over the limit
    temperature_c = F.when(stressed & F.lit(warming), upper + F.rand() * F.lit(3.0)) \
        .when(stressed & F.lit(scenario == "door_open"), upper - F.lit(1.0) + F.rand() * F.lit(2.0)) \
        .otherwise(nominal + noise)

    door_open = stressed & F.lit(scenario in ("door_open", "fleet_hot_zone")) & (F.rand() < F.lit(0.8))
    door_open_seconds = F.when(door_open, (F.col("door_open_limit_seconds") * (F.lit(1.2) + F.rand() * F.lit(1.5))).cast("int")) \
        .otherwise((F.rand() * F.lit(20)).cast("int"))

    compressor_off = stressed & F.lit(scenario == "compressor_failure")
    compressor_on = ~compressor_off
    compressor_health = F.when(compressor_off, F.rand() * F.lit(0.45)) \
        .otherwise(F.lit(0.8) + F.rand() * F.lit(0.2))

    on_power_event = stressed & F.lit(scenario == "power_outage")
    power_state = F.when(on_power_event, F.when(F.rand() < F.lit(0.5), F.lit("outage")).otherwise(F.lit("battery"))) \
        .otherwise(F.lit("normal"))
    battery_pct = F.when(on_power_event, F.rand() * F.lit(25.0)).otherwise(F.lit(100.0))

    return df.select(
        F.expr("uuid()").alias("event_id"),
        F.expr("unix_millis(current_timestamp())").alias("event_ts"),
        F.col("freezer_id").alias("device_id"),
        F.col("freezer_id"),
        F.col("site_id"),
        F.round(temperature_c, 2).alias("temperature_c"),
        F.round(F.lit(35.0) + F.rand() * F.lit(20.0), 1).alias("humidity_pct"),
        door_open.alias("door_open"),
        door_open_seconds.alias("door_open_seconds"),
        compressor_on.alias("compressor_on"),
        F.round(compressor_health, 2).alias("compressor_health"),
        power_state.alias("power_state"),
        F.round(battery_pct, 1).alias("battery_pct"),
        F.round(F.lit(20.0) + F.rand() * F.lit(8.0), 1).alias("ambient_temp_c"),
        (F.rand() < F.lit(0.05)).alias("defrost_cycle_active"),
        F.lit(scenario).alias("scenario_name"),
        F.when(stressed, F.lit("elevated")).otherwise(F.lit("none")).alias("synthetic_severity"),
        F.expr("unix_millis(current_timestamp())").alias("producer_ts"),
    )
