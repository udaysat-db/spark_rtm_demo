"""Pure rolling aggregator for the SignalNow console — Kafka records in, the UI
`snapshot()` dict out.

`KafkaDataSource` (backend_kafka.py) tails the alerts and metrics topics and feeds
parsed JSON records here; this class holds a rolling in-memory view and produces the
exact dict shape `MockDataSource.snapshot()` returns, so the API and frontend don't
change. It is framework-free (no Kafka, no Spark), so it is unit-tested directly.

Both engines write to the shared topics tagged by `source_mode`, so this keeps a
separate view per app-mode (`rtm` / `mb`) and `snapshot(mode)` returns the selected
one — one app instance can toggle between RTM and micro-batch. Incoming
`source_mode="microbatch"` maps to app-mode `mb`.
"""
from __future__ import annotations

import time
from collections import defaultdict, deque

# Alert type → display label + a stable ordering for the "by type" list.
TYPES = [
    ("TEMP_HIGH", "Temperature high"),
    ("DOOR_OPEN_TOO_LONG", "Door open too long"),
    ("COMPRESSOR_FAILURE_RISK", "Compressor failure risk"),
    ("POWER_AND_WARMING", "Power + warming"),
    ("MULTI_SIGNAL_CRITICAL", "Multi-signal critical"),
]
TYPE_LABEL = {k: lbl for k, lbl in TYPES}
SEV_STATUS = {"warning": 1, "serious": 2, "critical": 3}

LAT_WINDOW_MS = 120_000     # latency percentiles / buckets look back this far
ACTIVE_MS = 60_000          # a freezer counts as "in alert" if seen this recently
SERIES_LEN = 60             # points kept in the time-series sparklines
FEED_LEN = 60
_OPENING = ("OPENED", "ESCALATED")   # lifecycle events that count as a fresh alert


def _now_ms():
    return int(time.time() * 1000)


def _store_num(rec: dict) -> str:
    """Short store number for a cell label, from site_id (e.g. 'S0142' → '0142')."""
    sid = rec.get("site_id") or ""
    digits = "".join(c for c in sid if c.isdigit())
    return digits or (rec.get("site_name") or "?")


class _ModeState:
    """Rolling view for one app-mode."""

    def __init__(self):
        self.alerts = 0
        self.vol_out = 0
        self.vol_in = 0
        self.by_type = {k: 0 for k, _ in TYPES}
        self.by_sev = {"warning": 0, "serious": 0, "critical": 0}
        self.critical_ids = set()           # incident_ids that reached critical (dedup)
        self.site_hits = defaultdict(int)   # site_name → opened count
        self.alert_ts = deque()             # opened-event timestamps (for alps)
        self.feed = deque(maxlen=FEED_LEN)
        # freezer_id → {"sev","status","site","store","ts","open"}
        self.units = {}
        self.scenario_name = "Live"
        # latest metrics
        self.evps = 0
        self.lag_ms = 0.0
        self.builtins = None
        self.seen_freezers = set()
        # series
        self.lat_series = deque(maxlen=SERIES_LEN)
        self.ev_series = deque(maxlen=SERIES_LEN)
        self.al_series = deque(maxlen=SERIES_LEN)
        self._last_series_s = 0


def _app_mode(source_mode: str) -> str:
    return "mb" if source_mode == "microbatch" else "rtm"


class FleetAggregator:
    def __init__(self, roster=None):
        # roster: optional list of store-number strings for a stable full grid.
        self.roster = list(roster) if roster else None
        self.modes = defaultdict(_ModeState)
        self.t0 = time.time()

    # ---- ingest ---------------------------------------------------------
    def ingest_alert(self, rec: dict, now_ms=None, alert_kafka_ts=None) -> None:
        now_ms = now_ms or _now_ms()
        mode = _app_mode(rec.get("source_mode", "rtm"))
        st = self.modes[mode]
        fid = rec.get("freezer_id") or "?"
        sev = rec.get("alert_severity") or "warning"
        typ = rec.get("alert_type") or "TEMP_HIGH"
        lifecycle = rec.get("lifecycle_event") or "OPENED"
        st.seen_freezers.add(fid)
        st.vol_out += 1
        st.alert_ts.append(now_ms)   # every alert record → "Alerts out" throughput (al/s)
        st.scenario_name = rec.get("scenario_name") or st.scenario_name

        # Latency = alert's Kafka ts (ending) − a carried start (no current_timestamp).
        #   A = alert_kafka_ts − event_ts        (event → alert; cross-clock, fuller)
        #   B = alert_kafka_ts − input_kafka_ts  (in-Kafka → alert; broker-clock, clean)
        # DETECTION metric: measure only on fresh OPENED/ESCALATED events. HEARTBEAT
        # keep-alives re-emit a stale reading every ~20s and would inflate latency.
        # Negatives (missing/odd clocks) are dropped rather than shown.
        lat_a = lat_b = None
        if alert_kafka_ts and lifecycle in _OPENING:
            ev, ik = rec.get("event_ts"), rec.get("input_kafka_ts")
            if ev is not None and alert_kafka_ts - ev >= 0:
                lat_a = float(alert_kafka_ts - ev)
            if ik is not None and alert_kafka_ts - ik >= 0:
                lat_b = float(alert_kafka_ts - ik)

        if lifecycle == "RESOLVED":
            u = st.units.get(fid)
            if u:
                u["open"] = False
                u["ts"] = now_ms
        else:  # OPENED / ESCALATED / HEARTBEAT — the unit is (still) in alert. Store
               # everything the feed, by-type and latency views need, so those are rebuilt
               # each snapshot from the CURRENT open set (below) rather than from one-shot
               # events. That keeps them correct across app restarts, when we only ever
               # see an already-open unit's HEARTBEATs, not its original OPENED.
            prev = st.units.get(fid) or {}
            st.units[fid] = {
                "sev": sev, "status": SEV_STATUS.get(sev, 1), "open": True,
                "site": rec.get("site_name") or "", "store": _store_num(rec),
                "ts": now_ms, "typ": typ,
                # Detection latency at OPEN (A and B), preserved across HEARTBEATs (lat is
                # None then) so it reflects how fast the incident was FIRST detected.
                "lat_a": lat_a if lat_a is not None else prev.get("lat_a"),
                "lat_b": lat_b if lat_b is not None else prev.get("lat_b"),
                "incident_id": rec.get("incident_id"),
            }

        # Cumulative session KPIs (total alerts, sites) count genuine fresh opens only;
        # current-state views (units_in_alert/by_type/feed/latency) come from st.units.
        if lifecycle == "OPENED":
            st.alerts += 1
            st.by_sev[sev] = st.by_sev.get(sev, 0) + 1
            st.site_hits[rec.get("site_name") or ""] += 1
        if lifecycle in _OPENING and sev == "critical":
            st.critical_ids.add(rec.get("incident_id") or f"{fid}:{now_ms}")

    def ingest_metric(self, rec: dict) -> None:
        mode = _app_mode(rec.get("source_mode", "rtm"))
        st = self.modes[mode]
        ips = rec.get("input_rows_per_second")
        if ips is not None:
            st.evps = int(ips)
        nin = rec.get("num_input_rows")
        if nin:
            st.vol_in += int(nin)
        lag = rec.get("offsets_behind_latest_max")
        if lag is not None:
            st.lag_ms = float(lag)
        if rec.get("is_rtm"):
            st.builtins = {
                "proc50": rec.get("proc_latency_p50_ms"), "proc99": rec.get("proc_latency_p99_ms"),
                "q50": rec.get("queue_latency_p50_ms"), "q99": rec.get("queue_latency_p99_ms"),
                "e50": rec.get("e2e_latency_p50_ms"), "e95": rec.get("e2e_latency_p95_ms"),
                "e99": rec.get("e2e_latency_p99_ms"),
            }

    # ---- snapshot -------------------------------------------------------
    def snapshot(self, mode: str = "rtm", now_ms=None) -> dict:
        now_ms = now_ms or _now_ms()
        mode = "mb" if mode in ("mb", "microbatch") else "rtm"
        st = self.modes[mode]
        self._evict(st, now_ms)

        # Current open incidents drive everything — units_in_alert, by_type, feed, AND the
        # latency percentiles — so they reflect the live fleet regardless of when this app
        # process started (a fresh window would sit empty once the fleet is stably open).
        open_units = [(fid, u) for fid, u in st.units.items() if u["open"]]
        units_in_alert = len(open_units)
        open_by_type = {}
        for _, u in open_units:
            open_by_type[u["typ"]] = open_by_type.get(u["typ"], 0) + 1

        # Business latency = detection latency of currently-open incidents. Two views:
        #   B (in-Kafka → alert): clean single-clock pipeline latency — the headline.
        #   A (event → alert): fuller, cross-clock — shown alongside.
        # Spark internals stay in their own "Real-Time Mode metrics" panel (builtins).
        lats_a = [u["lat_a"] for _, u in open_units if u.get("lat_a") is not None]
        lats_b = [u["lat_b"] for _, u in open_units if u.get("lat_b") is not None]
        pa = (_pct(lats_a, 50), _pct(lats_a, 95), _pct(lats_a, 99))
        pb = (_pct(lats_b, 50), _pct(lats_b, 95), _pct(lats_b, 99))
        p50, p95, p99 = pb
        pmax = max(lats_b) if lats_b else 0
        buckets = [0, 0, 0, 0]
        for v in lats_b:
            buckets[0 if v <= 250 else 1 if v <= 1000 else 2 if v <= 5000 else 3] += 1
        fresh_pct = (sum(1 for v in lats_b if v <= 250) / len(lats_b) * 100) if lats_b else 0.0
        self._advance_series(st, now_ms, pb[0])

        feed = [{
            "label": TYPE_LABEL.get(u["typ"], u["typ"]), "severity": u["sev"],
            "site": u["site"], "device": fid,
            "lat_ms": round(u["lat_b"]) if u.get("lat_b") is not None else None,   # B
            "ts": u["ts"] / 1000.0, "lifecycle": "OPEN",
            "ago_s": max(0, round(now_ms / 1000.0 - u["ts"] / 1000.0)),
        } for fid, u in sorted(open_units, key=lambda kv: -kv[1]["ts"])[:FEED_LEN]]
        top = sorted(st.site_hits.items(), key=lambda kv: -kv[1])[:4]
        el = int(time.time() - self.t0)
        return {
            "mode": mode,
            "engine": "Databricks · RTM" if mode == "rtm" else "Databricks · Micro-batch",
            "scenario_name": st.scenario_name,
            "burst": False,   # real data reflects the live producer; no simulated burst
            "evps": st.evps,
            "clock": f"{el // 60:02d}:{el % 60:02d}",
            "business": {
                "units_monitored": len(st.seen_freezers),
                "units_in_alert": units_in_alert,
                "alerts": st.alerts,
                "critical": len(st.critical_ids),
                "fresh_pct": fresh_pct,
                "cells": self._cells(st),
                "top_sites": [list(t) for t in top],
                "by_type": [{"key": k, "label": lbl, "n": open_by_type.get(k, 0)} for k, lbl in TYPES],
                "by_severity": dict(st.by_sev),
                "feed": feed[:30],
            },
            "tech": {
                "evps": st.evps, "alps": self._alps(st, now_ms),
                "p50": p50, "p95": p95, "p99": p99, "max": pmax,
                # Two business-latency views (see snapshot): A = event→alert, B = in-Kafka→alert.
                "lat_a": {"p50": pa[0], "p95": pa[1], "p99": pa[2]},
                "lat_b": {"p50": pb[0], "p95": pb[1], "p99": pb[2]},
                "lag_ms": st.lag_ms,
                "vol_in": st.vol_in, "vol_out": st.vol_out,
                "lat_series": list(st.lat_series), "ev_series": list(st.ev_series),
                "al_series": list(st.al_series), "buckets": buckets,
                "builtins": st.builtins,
            },
        }

    # ---- helpers --------------------------------------------------------
    def _evict(self, st, now_ms):
        acut = now_ms - ACTIVE_MS
        while st.alert_ts and st.alert_ts[0] < acut:
            st.alert_ts.popleft()
        # A unit that hasn't been seen within ACTIVE_MS drops out of the alert view.
        for fid, u in list(st.units.items()):
            if u["ts"] < acut:
                del st.units[fid]

    def _alps(self, st, now_ms):
        recent = [t for t in st.alert_ts if t >= now_ms - 5000]
        return round(len(recent) / 5.0, 1)

    def _advance_series(self, st, now_ms, latb_p50):
        sec = now_ms // 1000
        if sec == st._last_series_s:
            return
        st._last_series_s = sec
        st.lat_series.append(latb_p50)           # B (in-Kafka → alert) p50 over time
        st.ev_series.append(st.evps)
        st.al_series.append(self._alps(st, now_ms))

    def _cells(self, st):
        # Worst active severity per store. With a roster, every store shows (mostly OK);
        # without one, only stores seen so far appear.
        worst = defaultdict(int)
        for u in st.units.values():
            if u["open"]:
                worst[u["store"]] = max(worst[u["store"]], u["status"])
        stores = self.roster if self.roster is not None else sorted(worst.keys())
        return [{"n": s, "s": worst.get(s, 0)} for s in stores]


def _pct(sorted_or_not, p):
    if not sorted_or_not:
        return 0.0
    a = sorted(sorted_or_not)
    return a[min(len(a) - 1, int(p / 100 * len(a)))]
