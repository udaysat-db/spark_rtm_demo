"""Unit tests for the stateful incident engine — pure Python, no Spark/JDK/Kafka.

Two layers, matching docs/alerting-logic.md:
  * `classify` — the pure reading→(type, severity) rules (sustained gates passed in).
  * the state machine — driven by ordered synthetic sequences, asserting the
    OPENED / ESCALATED / RESOLVED / HEARTBEAT stream and the sustained windows.
"""
from shared import incident_engine as eng

# A healthy walk-in freezer: -18°C, well under its -15°C limit, door shut,
# compressor healthy, on mains power, low-value asset. Overridden per case.
DEFAULTS = dict(
    event_id="e1", event_ts=0, device_id="FRZ-001", freezer_id="FZ1", site_id="S1",
    temperature_c=-18.0, humidity_pct=40.0, door_open=False, door_open_seconds=0,
    compressor_on=True, compressor_health=0.95, power_state="normal", battery_pct=100.0,
    ambient_temp_c=22.0, defrost_cycle_active=False, scenario_name="normal",
    synthetic_severity="none", producer_ts=0,
    site_name="Store 0142", site_region="TX", freezer_type="walk_in_freezer",
    temperature_upper_limit=-15.0, door_open_limit_seconds=120,
    maintenance_priority="low", inventory_value_band="low",
)


def r(**over):
    row = dict(DEFAULTS)
    row.update(over)
    return row


def drive(seq):
    """Feed (reading-dict, now_proc_ms) pairs; return (state, all_emits)."""
    st = eng.new_state()
    emits = []
    for reading, now in seq:
        emits.extend(eng.on_reading(st, reading, now).emits)
    return st, emits


# ---------------------------------------------------------------------------
# Rule 0 — the pure classifier (sustained gates forced so we test classification)
# ---------------------------------------------------------------------------
def _classify(reading, ewma=None, slope=0.0, over=True, comp=True, defrost=False):
    ewma = reading["temperature_c"] if ewma is None else ewma
    return eng.classify(reading, ewma, slope, over, comp, defrost)


def test_normal_is_no_alert():
    assert _classify(r()) == (None, None)


def test_temp_high_warning_vs_critical():
    assert _classify(r(temperature_c=-14.0)) == ("TEMP_HIGH", "warning")   # 1°C over
    assert _classify(r(temperature_c=-12.0)) == ("TEMP_HIGH", "critical")  # 3°C over


def test_temp_high_suppressed_during_defrost():
    assert _classify(r(temperature_c=-12.0), defrost=True) == (None, None)


def test_temp_high_needs_sustained_gate():
    # Over the limit but the sustained window hasn't been met yet.
    assert _classify(r(temperature_c=-12.0), over=False) == (None, None)


def test_door_open_warning_vs_critical():
    assert _classify(r(door_open=True, door_open_seconds=200)) == ("DOOR_OPEN_TOO_LONG", "warning")
    assert _classify(r(door_open=True, door_open_seconds=300)) == ("DOOR_OPEN_TOO_LONG", "critical")  # >2x


def test_compressor_risk_serious_then_critical():
    near = r(compressor_on=False, compressor_health=0.3, temperature_c=-15.5)  # near, not over
    assert _classify(near) == ("COMPRESSOR_FAILURE_RISK", "serious")
    over = r(compressor_on=False, compressor_health=0.3, temperature_c=-12.0)  # over → critical
    assert _classify(over) == ("COMPRESSOR_FAILURE_RISK", "critical")


def test_power_and_warming():
    out = _classify(r(power_state="outage", battery_pct=10.0, temperature_c=-15.5))
    assert out == ("POWER_AND_WARMING", "critical")


def test_multi_takes_precedence_on_high_value():
    out = _classify(r(temperature_c=-12.0, door_open=True, door_open_seconds=300,
                      inventory_value_band="high"))
    assert out == ("MULTI_SIGNAL_CRITICAL", "critical")


def test_multi_uses_raw_temp_even_if_ewma_below_limit():
    # Decision 1: MULTI keys off the raw reading, so a low EWMA doesn't gate it out.
    out = _classify(r(temperature_c=-12.0, door_open=True, door_open_seconds=300,
                      inventory_value_band="high"), ewma=-20.0)
    assert out == ("MULTI_SIGNAL_CRITICAL", "critical")


# ---------------------------------------------------------------------------
# State machine — sustained windows and lifecycle
# ---------------------------------------------------------------------------
def test_temp_high_opens_only_after_consecutive_readings():
    # Three consecutive over-limit readings trip TEMP_WINDOW_READINGS.
    seq = [(r(event_ts=t, temperature_c=-13.0), t) for t in (1000, 6000, 11000)]
    st, emits = drive(seq)
    assert [e["lifecycle_event"] for e in emits] == ["OPENED"]
    assert emits[0]["alert_type"] == "TEMP_HIGH"
    # It opened on the 3rd reading, not the 1st or 2nd.
    assert emits[0]["event_ts"] == 11000


def test_single_spike_is_rejected_by_ewma():
    # Steady cold, one lone warm reading: smoothed temp never crosses the limit.
    st, emits = drive([
        (r(event_ts=1000, temperature_c=-20.0), 1000),
        (r(event_ts=6000, temperature_c=-14.0), 6000),   # raw over, but ewma≈-17.6
        (r(event_ts=11000, temperature_c=-20.0), 11000),
    ])
    assert emits == []
    assert not st["incident_open"]


def test_door_opens_immediately_fast_rule():
    st, emits = drive([(r(event_ts=1000, door_open=True, door_open_seconds=200), 1000)])
    assert len(emits) == 1 and emits[0]["lifecycle_event"] == "OPENED"
    assert emits[0]["alert_type"] == "DOOR_OPEN_TOO_LONG" and emits[0]["alert_severity"] == "warning"


def test_escalation_ratchets_type_and_severity_up():
    st, emits = drive([
        (r(event_ts=1000, door_open=True, door_open_seconds=200), 1000),          # OPEN door/warning
        (r(event_ts=2000, door_open=True, door_open_seconds=300,                   # → MULTI/critical
           temperature_c=-12.0, inventory_value_band="high"), 2000),
    ])
    assert [e["lifecycle_event"] for e in emits] == ["OPENED", "ESCALATED"]
    assert emits[1]["alert_type"] == "MULTI_SIGNAL_CRITICAL"
    assert emits[1]["alert_severity"] == "critical" and emits[1]["escalation_level"] == 2
    # Same incident throughout.
    assert emits[0]["incident_id"] == emits[1]["incident_id"]


def test_no_emit_on_steady_open_incident():
    # After opening, unchanged bad readings don't re-fire — the heartbeat does that.
    seq = [(r(event_ts=t, door_open=True, door_open_seconds=200), t)
           for t in (1000, 2000, 3000, 4000)]
    st, emits = drive(seq)
    assert [e["lifecycle_event"] for e in emits] == ["OPENED"]


def test_resolves_only_after_recovery_window():
    st = eng.new_state()
    emits = []
    emits += eng.on_reading(st, r(event_ts=1000, door_open=True, door_open_seconds=200), 1000).emits
    # cleared, but not yet for RECOVERY_SECS
    emits += eng.on_reading(st, r(event_ts=5000, door_open=False, door_open_seconds=0), 5000).emits
    assert [e["lifecycle_event"] for e in emits] == ["OPENED"]
    assert st["incident_open"]
    # cleared long enough (>30s after cleared_since=5000)
    res = eng.on_reading(st, r(event_ts=40000, door_open=False, door_open_seconds=0), 40000)
    assert [e["lifecycle_event"] for e in res.emits] == ["RESOLVED"]
    assert not st["incident_open"] and res.clear_timers


def test_reopens_after_resolve_with_new_incident_id():
    st = eng.new_state()
    first = eng.on_reading(st, r(event_ts=1000, door_open=True, door_open_seconds=200), 1000).emits
    eng.on_reading(st, r(event_ts=40000, door_open=False, door_open_seconds=0), 40000)   # clear starts
    resolved = eng.on_reading(st, r(event_ts=75000, door_open=False, door_open_seconds=0), 75000)  # +35s → RESOLVED
    assert [e["lifecycle_event"] for e in resolved.emits] == ["RESOLVED"] and not st["incident_open"]
    reopen = eng.on_reading(st, r(event_ts=80000, door_open=True, door_open_seconds=200), 80000).emits
    assert reopen[0]["lifecycle_event"] == "OPENED"
    assert reopen[0]["incident_id"] != first[0]["incident_id"]


def test_heartbeat_timer_re_emits_and_rearms():
    st = eng.new_state()
    eng.on_reading(st, r(event_ts=1000, door_open=True, door_open_seconds=200), 1000)
    res = eng.on_timer(st, 1000 + eng.HEARTBEAT_SECS * 1000)
    assert len(res.emits) == 1 and res.emits[0]["lifecycle_event"] == "HEARTBEAT"
    assert res.timer_at is not None and st["incident_open"]


def test_stale_timer_resolves_when_readings_stop():
    st = eng.new_state()
    eng.on_reading(st, r(event_ts=1000, door_open=True, door_open_seconds=200), 1000)
    res = eng.on_timer(st, 1000 + eng.STALE_SECS * 1000)
    assert res.emits[0]["lifecycle_event"] == "RESOLVED"
    assert not st["incident_open"] and res.clear_timers


def test_timer_noop_when_no_incident():
    st = eng.new_state()
    res = eng.on_timer(st, 999999)
    assert res.emits == [] and res.timer_at is None


def test_emit_carries_incident_fields_and_trend():
    st, emits = drive([(r(event_ts=1000, door_open=True, door_open_seconds=200), 1000)])
    e = emits[0]
    for k in ("incident_id", "lifecycle_event", "incident_started_ts", "escalation_level",
              "peak_temp_c", "temp_trend", "alert_type", "alert_severity", "alert_reason",
              "freezer_id", "site_id", "temperature_c"):
        assert k in e, f"missing {k}"
    assert e["temp_trend"] == [{"ts": 1000, "temp": -18.0}]
    assert e["alert_reason"] == "Door open 200s (limit 120s)"


def test_trend_buffer_is_capped():
    seq = [(r(event_ts=t * 1000, temperature_c=-20.0), t * 1000)
           for t in range(1, eng.TREND_SAMPLES + 6)]
    st, _ = drive(seq)
    assert len(st["trend"]) == eng.TREND_SAMPLES
