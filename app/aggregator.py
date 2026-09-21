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
        self.fresh_hits = 0
        self.fresh_total = 0
        self.site_hits = defaultdict(int)   # site_name → opened count
        self.lat_win = deque()              # (ts, latency_ms) within LAT_WINDOW
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
    def ingest_alert(self, rec: dict, now_ms=None) -> None:
        now_ms = now_ms or _now_ms()
        mode = _app_mode(rec.get("source_mode", "rtm"))
        st = self.modes[mode]
        fid = rec.get("freezer_id") or "?"
        sev = rec.get("alert_severity") or "warning"
        typ = rec.get("alert_type") or "TEMP_HIGH"
        lifecycle = rec.get("lifecycle_event") or "OPENED"
        st.seen_freezers.add(fid)
        st.vol_out += 1
        st.scenario_name = rec.get("scenario_name") or st.scenario_name

        lat = rec.get("end_to_end_latency_ms")
        if lat is not None:
            st.lat_win.append((now_ms, float(lat)))

        if lifecycle == "RESOLVED":
            u = st.units.get(fid)
            if u:
                u["open"] = False
                u["ts"] = now_ms
        else:  # OPENED / ESCALATED / HEARTBEAT — the unit is (still) in alert
            st.units[fid] = {
                "sev": sev, "status": SEV_STATUS.get(sev, 1), "open": True,
                "site": rec.get("site_name") or "", "store": _store_num(rec), "ts": now_ms,
            }

        if lifecycle in _OPENING:
            if lifecycle == "OPENED":
                st.alerts += 1
                st.by_type[typ] = st.by_type.get(typ, 0) + 1
                st.by_sev[sev] = st.by_sev.get(sev, 0) + 1
                st.site_hits[rec.get("site_name") or ""] += 1
                st.alert_ts.append(now_ms)
                st.fresh_total += 1
                if rec.get("within_250ms"):
                    st.fresh_hits += 1
            if sev == "critical":
                st.critical_ids.add(rec.get("incident_id") or f"{fid}:{now_ms}")
            st.feed.appendleft({
                "label": TYPE_LABEL.get(typ, typ), "severity": sev,
                "site": rec.get("site_name") or "", "device": fid,
                "lat_ms": round(float(lat)) if lat is not None else None,
                "ts": now_ms / 1000.0, "lifecycle": lifecycle,
            })

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
                "e50": rec.get("e2e_latency_p50_ms"), "e99": rec.get("e2e_latency_p99_ms"),
            }

    # ---- snapshot -------------------------------------------------------
    def snapshot(self, mode: str = "rtm", now_ms=None) -> dict:
        now_ms = now_ms or _now_ms()
        mode = "mb" if mode in ("mb", "microbatch") else "rtm"
        st = self.modes[mode]
        self._evict(st, now_ms)
        self._advance_series(st, now_ms)

        lats = [v for _, v in st.lat_win]
        p50, p95, p99 = _pct(lats, 50), _pct(lats, 95), _pct(lats, 99)
        buckets = [0, 0, 0, 0]
        for v in lats:
            buckets[0 if v <= 250 else 1 if v <= 1000 else 2 if v <= 5000 else 3] += 1

        units_in_alert = sum(1 for u in st.units.values() if u["open"])
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
                "fresh_pct": (st.fresh_hits / st.fresh_total * 100) if st.fresh_total else 0.0,
                "cells": self._cells(st),
                "top_sites": [list(t) for t in top],
                "by_type": [{"key": k, "label": lbl, "n": st.by_type.get(k, 0)} for k, lbl in TYPES],
                "by_severity": dict(st.by_sev),
                "feed": [{**f, "ago_s": max(0, round(now_ms / 1000.0 - f["ts"]))}
                         for f in list(st.feed)[:30]],
            },
            "tech": {
                "evps": st.evps, "alps": self._alps(st, now_ms),
                "p50": p50, "p95": p95, "p99": p99, "max": max(lats) if lats else 0,
                "lag_ms": st.lag_ms,
                "vol_in": st.vol_in, "vol_out": st.vol_out,
                "lat_series": list(st.lat_series), "ev_series": list(st.ev_series),
                "al_series": list(st.al_series), "buckets": buckets,
                "builtins": st.builtins,
            },
        }

    # ---- helpers --------------------------------------------------------
    def _evict(self, st, now_ms):
        cutoff = now_ms - LAT_WINDOW_MS
        while st.lat_win and st.lat_win[0][0] < cutoff:
            st.lat_win.popleft()
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

    def _advance_series(self, st, now_ms):
        sec = now_ms // 1000
        if sec == st._last_series_s:
            return
        st._last_series_s = sec
        lats = [v for _, v in st.lat_win]
        st.lat_series.append(_pct(lats, 50) if lats else 0.0)
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
