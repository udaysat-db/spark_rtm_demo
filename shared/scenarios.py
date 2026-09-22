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

# Fraction of units affected at any moment for each scenario's characteristic failure.
# Kept to a realistic slice (fleet_hot_zone is the deliberately dramatic one) so the
# fleet grid reads mostly-healthy with a moving scatter of alerts, not a solid block.
_STRESS = {
    "normal": 0.03,
    "door_open": 0.18,
    "compressor_failure": 0.15,
    "power_outage": 0.15,
    "fleet_hot_zone": 0.40,
}

# Each freezer re-rolls its "affected" status on its own staggered window of this length,
# so the affected SET churns over time (units fail and recover) while still staying put
# long enough (>> the incident sustained-gate) for an incident to latch.
_BUCKET_MS = 120_000


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
    # WHICH freezers are affected CHURNS over time: each freezer re-rolls `r` on its own
    # staggered ~_BUCKET_MS window (offset by its hash), so the affected set moves
    # continuously instead of being a fixed block — the fleet grid actually changes. `r`
    # is stable within a window (so breaches SUSTAIN and the engine latches), and its
    # position in [0, p) sets a per-unit INTENSITY (mild → severe) so incident severity
    # SPREADS: most affected units just warm (→ warning), only the worst trip the
    # scenario's hard signal (compressor off / power outage → serious/critical).
    now_ms = F.expr("unix_millis(current_timestamp())")
    bucket = F.floor(
        (now_ms + F.pmod(F.hash(F.col("freezer_id"), F.lit(9)), F.lit(_BUCKET_MS)))
        / F.lit(float(_BUCKET_MS)))
    r = F.pmod(F.hash(F.col("freezer_id"), bucket), F.lit(10000)) / F.lit(10000.0)
    stressed = r < p
    frac = F.when(p > F.lit(0.0), (p - r) / p).otherwise(F.lit(0.0))   # 0 (mild) … 1 (severe)
    intensity = frac * frac                                            # skew toward mild
    hard = stressed & (intensity > F.lit(0.72))    # only the worst ~15% trip the hard signal

    noise = (F.rand() - F.lit(0.5)) * F.lit(1.0)          # ±0.5 °C jitter
    upper = F.col("temperature_upper_limit")
    nominal = upper - F.lit(3.0)                           # healthy set point
    dlimit = F.col("door_open_limit_seconds")

    warming = sc.isin("compressor_failure", "power_outage", "fleet_hot_zone")
    door_scn = sc.isin("door_open", "fleet_hot_zone")

    # Temperature: affected warming units climb over the limit by an amount scaled by
    # intensity — mild ≈ just over (TEMP_HIGH warning), only the severe tail ≥ +2 °C
    # (critical). Gentle ramp so most sit in the warning band.
    over = intensity * F.lit(2.6) - F.lit(0.2)
    temperature_c = F.when(stressed & warming, upper + over) \
        .when(stressed & door_scn, upper - F.lit(1.0) + intensity * F.lit(1.5)) \
        .otherwise(nominal + noise)

    # Door held open; seconds scale with intensity (mild < 2×limit → warning, severe > 2× → critical).
    door_open = stressed & door_scn
    door_open_seconds = F.when(door_open, (dlimit * (F.lit(1.0) + intensity * F.lit(2.4))).cast("int")) \
        .otherwise((F.rand() * F.lit(15)).cast("int"))

    # Compressor cuts out only for the worst compressor_failure units (→ COMPRESSOR
    # serious/critical); milder ones keep running but degraded and surface as warnings.
    compressor_off = hard & (sc == F.lit("compressor_failure"))
    compressor_on = ~compressor_off
    compressor_health = F.when(compressor_off, F.rand() * F.lit(0.4)) \
        .when(stressed & (sc == F.lit("compressor_failure")), F.lit(0.55) + F.rand() * F.lit(0.2)) \
        .otherwise(F.lit(0.85) + F.rand() * F.lit(0.15))

    # Power failure (outage + low battery) only for the worst power_outage units
    # (→ POWER critical); milder ones just warm.
    on_power_event = hard & (sc == F.lit("power_outage"))
    power_state = F.when(on_power_event, F.when(F.rand() < F.lit(0.5), F.lit("outage")).otherwise(F.lit("battery"))) \
        .otherwise(F.lit("normal"))
    battery_pct = F.when(on_power_event, F.rand() * F.lit(20.0)).otherwise(F.lit(100.0))

    return df.select(
        F.expr("uuid()").alias("event_id"),
        now_ms.alias("event_ts"),
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
        now_ms.alias("producer_ts"),
    )
