"""Stateful incident engine — the pure, framework-agnostic core of the alerting
business logic.

This module holds the ALERTING RULES and the INCIDENT STATE MACHINE as plain
Python operating on dicts, so the whole thing is unit-testable without Spark, a
cluster, or Kafka. `shared/stateful.py` wraps it in a Spark row-based
`transformWithState` processor (RTM calls `handleInputRows` once per row); both
consumers share that one path and differ only in the streaming trigger — the crux
of the demo's "same code, sub-second or not" story.

The full specification lives in `docs/alerting-logic.md`. Keep the two in sync.

Design decisions (see docs/alerting-logic.md → "Decisions"):
  1. MULTI_SIGNAL_CRITICAL evaluates temperature on the RAW reading, not the EWMA —
     it is already gated by a second robust signal + a high-value asset, and it is
     the alert we most want to fire immediately.
  2. HEARTBEAT rows are keep-alive snapshots: they carry the LAST reading's
     temperature/trend (no new data has arrived), tagged via `lifecycle_event` so a
     consumer reads them as "still bad", not "new reading".
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

# --- rule constants (single source of truth; rules.py imports these) ---------
DEGRADED_HEALTH = 0.5      # compressor_health below this is "degraded"
LOW_BATTERY_PCT = 30.0     # battery below this is "low"
NEAR_LIMIT_C = 1.0         # within this many °C of the limit counts as "warming"

# --- tunables (single knobs; safe to retune at rehearsal) --------------------
EWMA_ALPHA = 0.4               # weight on the newest sample when smoothing temp
TEMP_WINDOW_SECS = 15          # temp must stay over limit this long to open TEMP_HIGH
TEMP_WINDOW_READINGS = 3       # ...or this many consecutive over-limit readings
COMPRESSOR_WINDOW_SECS = 10    # sustained window for COMPRESSOR_FAILURE_RISK
DEFROST_GRACE_SECS = 120       # suppress TEMP_HIGH this long after a defrost cycle
RECOVERY_SECS = 30             # conditions must stay clear this long to RESOLVE
HEARTBEAT_SECS = 20            # re-emit cadence while an incident is open
STALE_SECS = 90                # close an incident if no readings arrive for this long
TREND_SAMPLES = 16             # readings kept in the trend ring buffer

# Alert types in precedence order (highest first) — same order as the legacy
# column rules. Lower index = higher priority.
ALERT_PRECEDENCE = [
    "MULTI_SIGNAL_CRITICAL",
    "POWER_AND_WARMING",
    "COMPRESSOR_FAILURE_RISK",
    "DOOR_OPEN_TOO_LONG",
    "TEMP_HIGH",
]
PREC = {t: i for i, t in enumerate(ALERT_PRECEDENCE)}
SEV_RANK = {"warning": 0, "serious": 1, "critical": 2}
RANK_SEV = {v: k for k, v in SEV_RANK.items()}

# Lifecycle events emitted on the alert stream.
OPENED = "OPENED"
ESCALATED = "ESCALATED"
RESOLVED = "RESOLVED"
HEARTBEAT = "HEARTBEAT"


# ---------------------------------------------------------------------------
# Rule 0 — the pure classifier
# ---------------------------------------------------------------------------
def classify(reading, ewma, slope, over_sustained, comp_sustained, in_defrost):
    """A single reading + state summary → (alert_type, severity) or (None, None).

    No state writes, no clock — the same predicates and severity ladders as the
    legacy `rules.business_logic`, with two changes: the temperature comparison
    for TEMP_HIGH uses `ewma`, and the two slow rules take a `*_sustained` gate.
    MULTI still compares the raw temperature (decision 1).
    """
    r = reading
    limit = r["temperature_upper_limit"]
    dlimit = r["door_open_limit_seconds"]
    temp_c = r["temperature_c"]

    near_limit = temp_c >= limit - NEAR_LIMIT_C
    degraded = r["compressor_health"] < DEGRADED_HEALTH
    temp_over = ewma > limit                       # smoothed, for TEMP_HIGH + comp severity
    warming = slope > 0 or near_limit

    door_too_long = bool(r["door_open"]) and r["door_open_seconds"] > dlimit
    compressor_bad = (not r["compressor_on"]) and degraded and near_limit
    power_bad = (
        r["power_state"] in ("battery", "outage")
        and r["battery_pct"] < LOW_BATTERY_PCT
        and warming
    )
    high_value = r["inventory_value_band"] == "high" or r["maintenance_priority"] == "high"
    multi = (temp_c > limit) and (door_too_long or degraded) and high_value  # raw temp — decision 1

    temp_high_active = temp_over and over_sustained and not in_defrost
    comp_active = compressor_bad and comp_sustained

    if multi:
        return "MULTI_SIGNAL_CRITICAL", "critical"
    if power_bad:
        return "POWER_AND_WARMING", "critical"
    if comp_active:
        return "COMPRESSOR_FAILURE_RISK", ("critical" if temp_over else "serious")
    if door_too_long:
        return "DOOR_OPEN_TOO_LONG", ("critical" if r["door_open_seconds"] > 2 * dlimit else "warning")
    if temp_high_active:
        return "TEMP_HIGH", ("critical" if temp_c - limit >= 2.0 else "warning")
    return None, None


def reason(alert_type, ctx):
    """Human-readable alert reason — mirrors the format strings in rules.py."""
    t = ctx.get("temperature_c", 0.0)
    lim = ctx.get("temperature_upper_limit", 0.0)
    if alert_type == "MULTI_SIGNAL_CRITICAL":
        return f"Multiple risk factors on high-value asset: temp {t:.1f}°C over {lim:.1f}°C limit"
    if alert_type == "POWER_AND_WARMING":
        return (f"Power {ctx.get('power_state', '')}, battery {ctx.get('battery_pct', 0):.0f}%, "
                f"temp {t:.1f}°C warming toward {lim:.1f}°C limit")
    if alert_type == "COMPRESSOR_FAILURE_RISK":
        return (f"Compressor off, health {ctx.get('compressor_health', 0):.2f}, "
                f"temp {t:.1f}°C near {lim:.1f}°C limit")
    if alert_type == "DOOR_OPEN_TOO_LONG":
        return f"Door open {ctx.get('door_open_seconds', 0)}s (limit {ctx.get('door_open_limit_seconds', 0)}s)"
    if alert_type == "TEMP_HIGH":
        return f"Temperature {t:.1f}°C exceeds {lim:.1f}°C limit"
    return ""


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------
def new_state() -> dict:
    """A fresh per-freezer state. All incident_* fields are empty until one opens."""
    return {
        # reading-derived
        "last_event_ts": 0, "last_seen_proc_ms": 0, "last_temp": 0.0,
        "temp_ewma": 0.0, "slope_c_per_min": 0.0,
        # sustained-window anchors (0 = not currently tripping)
        "over_limit_since": 0, "consecutive_over": 0, "health_bad_since": 0,
        "cleared_since": 0, "defrost_until_ts": 0,
        # open incident
        "incident_open": False, "incident_id": "", "incident_type": "",
        "incident_severity": "", "escalation_level": 0, "incident_started_ts": 0,
        "peak_temp_c": 0.0,
        # cached last reading + trend ring buffer (list of [ts, temp])
        "ctx": None, "trend": [],
    }


@dataclass
class EngineResult:
    """What the wrapper must do after a call: emit these rows, and reconcile timers."""
    emits: list = field(default_factory=list)
    timer_at: Optional[int] = None   # register a processing-time timer at this ms
    clear_timers: bool = False       # delete pending timers (incident closed)


def on_reading(state: dict, reading: dict, now_proc_ms: int) -> EngineResult:
    """Process one reading (RTM: one row per call). Mutates `state`, returns the
    rows to emit and any timer directive."""
    res = EngineResult()
    r = reading
    seeded = state["last_event_ts"] != 0

    # (a) smoothing, slope, trend
    prev_ewma = state["temp_ewma"] if seeded else r["temperature_c"]
    dt_min = max((r["event_ts"] - state["last_event_ts"]) / 60000.0, 1 / 60.0) if seeded else 0.0
    ewma = EWMA_ALPHA * r["temperature_c"] + (1 - EWMA_ALPHA) * prev_ewma
    slope = (ewma - prev_ewma) / dt_min if dt_min else 0.0
    state["temp_ewma"] = ewma
    state["slope_c_per_min"] = slope
    state["last_event_ts"] = r["event_ts"]
    state["last_temp"] = r["temperature_c"]
    state["last_seen_proc_ms"] = now_proc_ms
    state["ctx"] = dict(r)
    _push_trend(state, r["event_ts"], r["temperature_c"])

    # (b) defrost window
    if r.get("defrost_cycle_active"):
        state["defrost_until_ts"] = r["event_ts"] + DEFROST_GRACE_SECS * 1000
    in_defrost = r["event_ts"] < state["defrost_until_ts"]

    # (c) sustained anchors
    limit = r["temperature_upper_limit"]
    if ewma > limit:
        state["over_limit_since"] = state["over_limit_since"] or r["event_ts"]
        state["consecutive_over"] += 1
    else:
        state["over_limit_since"] = 0
        state["consecutive_over"] = 0
    over_sustained = bool(state["over_limit_since"]) and (
        (r["event_ts"] - state["over_limit_since"]) >= TEMP_WINDOW_SECS * 1000
        or state["consecutive_over"] >= TEMP_WINDOW_READINGS)

    comp_bad_now = (
        (not r["compressor_on"])
        and r["compressor_health"] < DEGRADED_HEALTH
        and r["temperature_c"] >= limit - NEAR_LIMIT_C
    )
    state["health_bad_since"] = (state["health_bad_since"] or r["event_ts"]) if comp_bad_now else 0
    comp_sustained = bool(state["health_bad_since"]) and (
        (r["event_ts"] - state["health_bad_since"]) >= COMPRESSOR_WINDOW_SECS * 1000)

    # (d) classify + lifecycle
    typ, sev = classify(r, ewma, slope, over_sustained, comp_sustained, in_defrost)

    if typ is None:
        if state["incident_open"]:
            state["cleared_since"] = state["cleared_since"] or r["event_ts"]
            if (r["event_ts"] - state["cleared_since"]) >= RECOVERY_SECS * 1000:
                res.emits.append(_build_emit(state, RESOLVED))
                _close(state)
                res.clear_timers = True
    else:
        state["cleared_since"] = 0
        if not state["incident_open"]:
            _open(state, r, typ, sev)
            res.emits.append(_build_emit(state, OPENED))
            res.timer_at = now_proc_ms + HEARTBEAT_SECS * 1000
        else:
            state["peak_temp_c"] = max(state["peak_temp_c"], r["temperature_c"])
            escalated = PREC[typ] < PREC[state["incident_type"]] or SEV_RANK[sev] > state["escalation_level"]
            if escalated:
                if PREC[typ] < PREC[state["incident_type"]]:
                    state["incident_type"] = typ            # ratchet type up in priority
                state["escalation_level"] = max(state["escalation_level"], SEV_RANK[sev])
                state["incident_severity"] = RANK_SEV[state["escalation_level"]]  # never downgrades
                res.emits.append(_build_emit(state, ESCALATED))
            # else: no per-reading emit — the heartbeat timer keeps the cell warm
    return res


def on_timer(state: dict, now_proc_ms: int) -> EngineResult:
    """Fired by the processing-time heartbeat timer. Emits HEARTBEAT while open, or
    RESOLVED (stale) if readings have stopped."""
    res = EngineResult()
    if not state["incident_open"]:
        return res
    if (now_proc_ms - state["last_seen_proc_ms"]) >= STALE_SECS * 1000:
        res.emits.append(_build_emit(state, RESOLVED))
        _close(state)
        res.clear_timers = True
        return res
    res.emits.append(_build_emit(state, HEARTBEAT))
    res.timer_at = now_proc_ms + HEARTBEAT_SECS * 1000
    return res


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _push_trend(state, ts, temp):
    state["trend"].append([ts, temp])
    if len(state["trend"]) > TREND_SAMPLES:
        del state["trend"][:-TREND_SAMPLES]


def _open(state, r, typ, sev):
    state["incident_open"] = True
    state["incident_started_ts"] = r["event_ts"]
    state["incident_id"] = f'{r["freezer_id"]}:{r["event_ts"]}'
    state["incident_type"] = typ
    state["incident_severity"] = sev
    state["escalation_level"] = SEV_RANK[sev]
    state["peak_temp_c"] = r["temperature_c"]
    state["cleared_since"] = 0


def _close(state):
    state["incident_open"] = False
    state["incident_id"] = ""
    state["incident_type"] = ""
    state["incident_severity"] = ""
    state["escalation_level"] = 0
    state["incident_started_ts"] = 0
    state["peak_temp_c"] = 0.0
    state["cleared_since"] = 0


def _build_emit(state, lifecycle):
    """One output record: all enriched fields (from the cached last reading) plus
    the alert + incident columns. `add_latency_fields` stamps timing downstream."""
    ctx = state["ctx"] or {}
    out = dict(ctx)                                  # enriched fields pass through
    out["alert_type"] = state["incident_type"]
    out["alert_severity"] = state["incident_severity"]
    out["alert_reason"] = reason(state["incident_type"], ctx)
    out["incident_id"] = state["incident_id"]
    out["lifecycle_event"] = lifecycle
    out["incident_started_ts"] = state["incident_started_ts"]
    out["escalation_level"] = state["escalation_level"]
    out["peak_temp_c"] = state["peak_temp_c"]
    out["temp_trend"] = [{"ts": ts, "temp": temp} for ts, temp in state["trend"]]
    return out
