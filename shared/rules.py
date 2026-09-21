"""Freezer failure alerting rules — STATELESS reference implementation.

`business_logic(df)` is the pure, per-row form of the rules: it takes an enriched
sensor-events DataFrame (see `schemas.enriched_schema`), annotates each row with an
alert type/severity/reason, and returns only the rows that raised an alert. It has
no time or streaming calls, so it runs identically in a unit test and in a stream.

The demo runs the **stateful** form instead — `shared.stateful.apply_business_logic`,
built on the incident state machine in `shared.incident_engine` — which shapes the
same five rules into OPENED/ESCALATED/RESOLVED/HEARTBEAT incidents. This module is
kept as the readable, one-glance statement of the rules and as the fixture for the
classifier unit tests; the two share their constants and precedence below so they
cannot drift. See `docs/alerting-logic.md`.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Precedence and rule thresholds live in the engine so the stateless and stateful
# forms stay identical. An event that trips several rules is reported once, as the
# highest-priority type — one row per event, comparable across pipelines.
from shared.incident_engine import (
    ALERT_PRECEDENCE,
    DEGRADED_HEALTH,   # compressor_health below this is "degraded"
    LOW_BATTERY_PCT,   # battery below this is "low"
    NEAR_LIMIT_C,      # within this many °C of the limit counts as "warming"
)

__all__ = ["business_logic", "ALERT_PRECEDENCE", "DEGRADED_HEALTH", "LOW_BATTERY_PCT", "NEAR_LIMIT_C"]


def business_logic(df: DataFrame) -> DataFrame:
    over = F.col("temperature_c") - F.col("temperature_upper_limit")
    temp_high = F.col("temperature_c") > F.col("temperature_upper_limit")
    near_limit = F.col("temperature_c") >= (F.col("temperature_upper_limit") - F.lit(NEAR_LIMIT_C))
    degraded = F.col("compressor_health") < F.lit(DEGRADED_HEALTH)

    door_too_long = F.col("door_open") & (F.col("door_open_seconds") > F.col("door_open_limit_seconds"))
    compressor_risk = (~F.col("compressor_on")) & degraded & near_limit
    power_warming = (
        F.col("power_state").isin("battery", "outage")
        & (F.col("battery_pct") < F.lit(LOW_BATTERY_PCT))
        & near_limit
    )
    high_value = (F.col("inventory_value_band") == F.lit("high")) | (F.col("maintenance_priority") == F.lit("high"))
    multi = temp_high & (door_too_long | degraded) & high_value

    alert_type = (
        F.when(multi, F.lit("MULTI_SIGNAL_CRITICAL"))
        .when(power_warming, F.lit("POWER_AND_WARMING"))
        .when(compressor_risk, F.lit("COMPRESSOR_FAILURE_RISK"))
        .when(door_too_long, F.lit("DOOR_OPEN_TOO_LONG"))
        .when(temp_high, F.lit("TEMP_HIGH"))
        .otherwise(F.lit(None).cast("string"))
    )

    alert_severity = (
        F.when(multi, F.lit("critical"))
        .when(power_warming, F.lit("critical"))
        .when(compressor_risk, F.when(temp_high, F.lit("critical")).otherwise(F.lit("serious")))
        .when(
            door_too_long,
            F.when(F.col("door_open_seconds") > (2 * F.col("door_open_limit_seconds")), F.lit("critical"))
            .otherwise(F.lit("warning")),
        )
        .when(temp_high, F.when(over >= F.lit(2.0), F.lit("critical")).otherwise(F.lit("warning")))
        .otherwise(F.lit(None).cast("string"))
    )

    alert_reason = (
        F.when(multi, F.format_string(
            "Multiple risk factors on high-value asset: temp %.1f°C over %.1f°C limit",
            F.col("temperature_c"), F.col("temperature_upper_limit")))
        .when(power_warming, F.format_string(
            "Power %s, battery %.0f%%, temp %.1f°C warming toward %.1f°C limit",
            F.col("power_state"), F.col("battery_pct"), F.col("temperature_c"), F.col("temperature_upper_limit")))
        .when(compressor_risk, F.format_string(
            "Compressor off, health %.2f, temp %.1f°C near %.1f°C limit",
            F.col("compressor_health"), F.col("temperature_c"), F.col("temperature_upper_limit")))
        .when(door_too_long, F.format_string(
            "Door open %ds (limit %ds)", F.col("door_open_seconds"), F.col("door_open_limit_seconds")))
        .when(temp_high, F.format_string(
            "Temperature %.1f°C exceeds %.1f°C limit", F.col("temperature_c"), F.col("temperature_upper_limit")))
        .otherwise(F.lit(None).cast("string"))
    )

    return (
        df.withColumn("alert_type", alert_type)
        .withColumn("alert_severity", alert_severity)
        .withColumn("alert_reason", alert_reason)
        .where(F.col("alert_type").isNotNull())
    )
