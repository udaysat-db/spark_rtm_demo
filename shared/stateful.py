"""Spark wrapper around the pure incident engine.

`apply_business_logic(enriched_df)` keys the enriched stream by `freezer_id` and
runs the `FreezerIncidentProcessor` via the **row-based** `transformWithState`
API — the only stateful surface RTM supports (no `transformWithStateInPandas`; in
RTM `handleInputRows` is called once per row, and timers are processing-time only).
All alerting logic lives in `shared/incident_engine.py`, which is pure Python and
unit-tested without Spark; this file is thin glue and is exercised on-cluster
(DBR 18.1+ / Spark 4.1), not by the local test suite.

The enrichment (broadcast stream-static join) happens upstream and adds no shuffle,
so this `transformWithState` keying is the query's single streaming shuffle —
within RTM's one-shuffle budget.

State is persisted as one JSON string in a single-field `ValueState`. The state is
small and the round-trip is trivial, which sidesteps nested-struct state schemas;
JSON turns the trend tuples into lists, so we restore them on load.
"""
from __future__ import annotations

import json
from typing import Iterator

from pyspark.sql import DataFrame, Row
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

from shared import incident_engine as eng
from shared.schemas import enriched_schema

# Incident columns added on top of the enriched passthrough. `add_latency_fields`
# appends source_mode/timing/within_* after this.
_INCIDENT_FIELDS = [
    StructField("alert_type", StringType(), True),
    StructField("alert_severity", StringType(), True),
    StructField("alert_reason", StringType(), True),
    StructField("incident_id", StringType(), True),
    StructField("lifecycle_event", StringType(), True),
    StructField("incident_started_ts", LongType(), True),
    StructField("escalation_level", IntegerType(), True),
    StructField("peak_temp_c", DoubleType(), True),
    StructField("temp_trend", ArrayType(StructType([
        StructField("ts", LongType(), True),
        StructField("temp", DoubleType(), True),
    ])), True),
]

# transformWithState output = every enriched field (passthrough) + incident fields.
INCIDENT_OUTPUT_SCHEMA = StructType(enriched_schema().fields + _INCIDENT_FIELDS)

# ValueState schema: the whole engine state as one JSON blob.
_STATE_SCHEMA = StructType([StructField("json", StringType(), True)])


# Row FACTORIES with explicit field names, in schema order. A bare `Row(*values)` is
# positional and carries NO field names, so when the transformWithState serializer does
# `row.asDict(True)` on an emitted row it raises "Cannot convert Row into dict". Naming
# the fields (and the nested temp_trend struct's) makes asDict work while keeping the
# positional, schema-ordered construction (no Row(**kwargs) alphabetizing).
_OUT_ROW = Row(*[f.name for f in INCIDENT_OUTPUT_SCHEMA.fields])
_TREND_ROW = Row("ts", "temp")


def _emit_to_row(emit: dict) -> Row:
    """Build an output Row in INCIDENT_OUTPUT_SCHEMA field order, with named fields so
    the Arrow serializer can convert it (and its nested temp_trend structs)."""
    values = []
    for f in INCIDENT_OUTPUT_SCHEMA.fields:
        if f.name == "temp_trend":
            trend = emit.get("temp_trend") or []
            values.append([_TREND_ROW(pt["ts"], pt["temp"]) for pt in trend])
        else:
            values.append(emit.get(f.name))
    return _OUT_ROW(*values)


class FreezerIncidentProcessor(StatefulProcessor):
    """Row-based stateful processor: delegates every reading and timer to the pure
    engine, persists engine state as JSON."""

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.handle = handle
        self.state = handle.getValueState("incident", _STATE_SCHEMA)

    def _load(self) -> dict:
        if self.state.exists():
            s = json.loads(self.state.get()[0])
            s["trend"] = [list(pt) for pt in s.get("trend", [])]
            return s
        return eng.new_state()

    def _save(self, s: dict) -> None:
        self.state.update((json.dumps(s),))

    def _reconcile_timers(self, res: eng.EngineResult) -> None:
        if res.clear_timers:
            for t in self.handle.listTimers():
                self.handle.deleteTimer(t)
        if res.timer_at is not None:
            self.handle.registerTimer(res.timer_at)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        now = timerValues.getCurrentProcessingTimeInMs()
        s = self._load()
        emits = []
        for row in rows:                       # RTM: exactly one row; micro-batch: many
            res = eng.on_reading(s, row.asDict(), now)
            self._reconcile_timers(res)
            emits.extend(res.emits)
        self._save(s)
        for e in emits:
            yield _emit_to_row(e)

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo) -> Iterator[Row]:
        now = timerValues.getCurrentProcessingTimeInMs()
        s = self._load()
        res = eng.on_timer(s, now)
        self._reconcile_timers(res)
        self._save(s)
        for e in res.emits:
            yield _emit_to_row(e)

    def close(self) -> None:
        pass


def apply_business_logic(enriched_df: DataFrame) -> DataFrame:
    """Enriched stream → incident stream. Same call for both consumers; only the
    trigger differs. Output carries `lifecycle_event` and the incident columns;
    stamp latency with `latency_utils.add_latency_fields` next."""
    return (
        enriched_df.groupBy("freezer_id")
        .transformWithState(
            statefulProcessor=FreezerIncidentProcessor(),
            outputStructType=INCIDENT_OUTPUT_SCHEMA,
            outputMode="Update",
            timeMode="ProcessingTime",
        )
    )
