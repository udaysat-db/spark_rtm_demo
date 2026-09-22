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
from pyspark.sql import Column, DataFrame
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


def _stress_fraction(sc: Column) -> Column:
    """Map the scenario column to its stress fraction, entirely in SQL so it can be a
    live, per-batch value (the control file) rather than a plan-time constant."""
    expr = F.lit(_STRESS["normal"])
    for name, frac in _STRESS.items():
        if name == "normal":
            continue
        expr = F.when(sc == F.lit(name), F.lit(frac)).otherwise(expr)
    return expr


def build_events(df: DataFrame, scenario) -> DataFrame:
    """`df` must carry: freezer_id, site_id, freezer_type, temperature_upper_limit,
    door_open_limit_seconds, maintenance_priority, inventory_value_band.

    `scenario` may be a **string** (fixed, as before) or a **Column** (a live value,
    e.g. from the control file joined in per batch) — everything below is expressed
    in SQL so the shaping switches on the value at runtime, not at plan time. This is
    what lets the console change the scenario without restarting the producer.
    Returns a DataFrame with the sensor-event columns (see schemas.EVENT_SCHEMA)."""
    if isinstance(scenario, str):
        if scenario not in SCENARIOS:
            raise ValueError(f"unknown scenario {scenario!r}; expected one of {SCENARIOS}")
        sc = F.lit(scenario)
    else:
        sc = scenario  # a Column carrying the (live) scenario name per row

    p = _stress_fraction(sc)
    # Stress is decided PER FREEZER (deterministic on freezer_id), not per event: a unit
    # that is "failing" for the scenario stays failed across ALL its readings, so its
    # temperature/compressor breach is SUSTAINED — which is what the incident engine's
    # EWMA + consecutive-reading gate needs to latch. Per-event randomness interleaves
    # healthy readings and the EWMA never crosses the limit (no incidents ever fire).
    # A higher scenario stress fraction crosses more freezers, monotonically, so
    # switching normal → compressor_failure adds a wave of new incidents.
    freezer_r = F.pmod(F.hash(F.col("freezer_id")), F.lit(10000)) / F.lit(10000.0)
    stressed = freezer_r < p
    noise = (F.rand() - F.lit(0.5)) * F.lit(2.0)          # ±1.0 °C jitter
    upper = F.col("temperature_upper_limit")
    nominal = upper - F.lit(3.0)                           # healthy set point

    warming = sc.isin("compressor_failure", "power_outage", "fleet_hot_zone")
    # temperature: healthy near nominal; stressed units climb over the limit
    temperature_c = F.when(stressed & warming, upper + F.rand() * F.lit(3.0)) \
        .when(stressed & (sc == F.lit("door_open")), upper - F.lit(1.0) + F.rand() * F.lit(2.0)) \
        .otherwise(nominal + noise)

    door_open = stressed & sc.isin("door_open", "fleet_hot_zone") & (F.rand() < F.lit(0.8))
    door_open_seconds = F.when(door_open, (F.col("door_open_limit_seconds") * (F.lit(1.2) + F.rand() * F.lit(1.5))).cast("int")) \
        .otherwise((F.rand() * F.lit(20)).cast("int"))

    compressor_off = stressed & (sc == F.lit("compressor_failure"))
    compressor_on = ~compressor_off
    compressor_health = F.when(compressor_off, F.rand() * F.lit(0.45)) \
        .otherwise(F.lit(0.8) + F.rand() * F.lit(0.2))

    on_power_event = stressed & (sc == F.lit("power_outage"))
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
        sc.alias("scenario_name"),
        F.when(stressed, F.lit("elevated")).otherwise(F.lit("none")).alias("synthetic_severity"),
        F.expr("unix_millis(current_timestamp())").alias("producer_ts"),
    )
