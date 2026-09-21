"""Simulated cold-chain stream for the console — the same data shape the live Kafka
backend serves, generated in-process so the app is fully demoable before the pipeline
runs.

A background thread advances the simulation ~2x/second; `snapshot()` returns the
current state. Swap this out for `KafkaDataSource` by setting USE_MOCK_BACKEND=false.
"""
from __future__ import annotations

import random
import threading
import time
from collections import deque, defaultdict

TYPES = [
    ("TEMP_HIGH", "Temperature high", 42),
    ("DOOR_OPEN_TOO_LONG", "Door open too long", 24),
    ("COMPRESSOR_FAILURE_RISK", "Compressor failure risk", 16),
    ("POWER_AND_WARMING", "Power + warming", 10),
    ("MULTI_SIGNAL_CRITICAL", "Multi-signal critical", 8),
]
TYPE_LABEL = {k: lbl for k, lbl, _ in TYPES}
SITES = [
    "Store 0142 Austin TX", "Store 0331 Dallas TX", "Store 0518 Phoenix AZ",
    "Store 0724 Miami FL", "Store 0910 Atlanta GA", "Store 1187 Denver CO",
    "Store 1444 Sacramento CA", "Store 1620 Seattle WA",
]
CASES = ["FRZ", "WLK", "REACH", "CASE"]
NCELL = 500      # 20 cols x 25 rows — fits the business-tab layout


def _weighted(items, weights):
    return random.choices(items, weights=weights, k=1)[0]


class MockDataSource:
    is_mock = True

    def __init__(self):
        self.mode = "rtm"           # 'rtm' | 'mb'
        self.burst = False
        self.burst_start = 0.0
        self.t0 = time.time()
        self.units_alert = 22.0
        self.alerts = 0
        self.critical = 0
        self.by_type = {k: 0 for k, _, _ in TYPES}
        self.by_sev = {"warning": 0, "serious": 0, "critical": 0}
        self.fresh_hits = 0
        self.fresh_total = 0
        self.vol_in = 0
        self.vol_out = 0
        self.lat_win = deque(maxlen=500)
        self.lat_series = deque(maxlen=60)
        self.ev_series = deque(maxlen=60)
        self.al_series = deque(maxlen=60)
        self.feed = deque(maxlen=60)
        self.cells = [0] * NCELL
        # Stable store number per cell (mostly 4-digit, some 3-digit). Labels never change.
        self.cell_nums = [str(random.randint(100, 999)) if random.random() < 0.25
                          else str(random.randint(100, 1999)).zfill(4) for _ in range(NCELL)]
        self.site_hits = defaultdict(int)
        self.evps = 0
        self.alps = 0
        self.lag = 0.0
        self._lock = threading.Lock()
        for _ in range(50):          # seed history so first paint is populated
            self._tick(0.5)
        t = threading.Thread(target=self._run, daemon=True)
        t.start()

    # ---- controls -------------------------------------------------------
    def set_burst(self, burst: bool) -> None:
        with self._lock:
            self.burst = burst
            if burst:
                self.burst_start = time.time()

    def set_mode(self, mode: str) -> None:
        with self._lock:
            if mode in ("rtm", "mb"):
                self.mode = mode
                self.lat_win.clear()
                self.lat_series.clear()

    # ---- simulation -----------------------------------------------------
    def _run(self):
        while True:
            time.sleep(0.5)
            with self._lock:
                self._tick(0.5)

    def _lat_mean(self):
        if self.mode == "rtm":
            return random.uniform(85, 150) if self.burst else random.uniform(55, 105)
        if not self.burst:
            return random.uniform(2200, 3100)
        secs = time.time() - self.burst_start
        return min(2800 + secs * 380, 9200) + random.uniform(-300, 300)

    def _sample_lat(self):
        m = self._lat_mean()
        if self.mode == "rtm":
            v = m + random.uniform(-35, 55)
            if random.random() < 0.012:
                v = random.uniform(240, 340)
            return max(18.0, v)
        return max(400.0, m + random.uniform(-900, 1400))

    def _tick(self, dt):
        ev_rate, al_rate = (820, 24) if self.burst else (205, 2.1)
        self.evps = int(ev_rate * random.uniform(0.9, 1.1))
        self.alps = int(al_rate * random.uniform(0.8, 1.2))
        self.vol_in += int(self.evps * dt)
        n_alerts = max(0, round(al_rate * dt * random.uniform(0.6, 1.5)))

        for _ in range(n_alerts):
            k = _weighted([t[0] for t in TYPES], [t[2] for t in TYPES])
            if k == "MULTI_SIGNAL_CRITICAL":
                sev = "critical"
            elif k in ("POWER_AND_WARMING", "COMPRESSOR_FAILURE_RISK"):
                sev = _weighted(["serious", "critical"], [6, 4])
            else:
                sev = _weighted(["warning", "serious", "critical"],
                                [5, 4, 3] if self.burst else [7, 3, 1.2])
            lat = self._sample_lat()
            site = random.choice(SITES)
            dev = f"{random.choice(CASES)}-{random.randint(100, 999)}"
            self.alerts += 1
            self.vol_out += 1
            self.by_type[k] += 1
            self.by_sev[sev] += 1
            if sev == "critical":
                self.critical += 1
            self.site_hits[site] += 1
            self.fresh_total += 1
            if lat <= 250:
                self.fresh_hits += 1
            self.lat_win.append(lat)
            self.feed.appendleft({"label": TYPE_LABEL[k], "severity": sev, "site": site,
                                  "device": dev, "lat_ms": round(lat), "ts": time.time()})

        target = random.uniform(620, 880) if self.burst else random.uniform(14, 42)
        self.units_alert += (target - self.units_alert) * 0.12
        lag_target = (random.uniform(60, 180) if self.burst else random.uniform(20, 70)) \
            if self.mode == "rtm" else \
            (random.uniform(9000, 26000) if self.burst else random.uniform(1500, 3600))
        self.lag += (lag_target - self.lag) * 0.15

        self.lat_series.append(self._pct(50) or self._lat_mean())
        self.ev_series.append(self.evps)
        self.al_series.append(self.alps)
        self._update_cells()

    def _update_cells(self):
        target = round(min(1.0, self.units_alert / 1400.0) * NCELL)
        active = sum(1 for c in self.cells if c)
        for i in range(NCELL):
            if self.cells[i] and random.random() < 0.08:
                self.cells[i] = 0
        active = sum(1 for c in self.cells if c)
        while active < target:
            i = random.randint(0, NCELL - 1)
            if not self.cells[i]:
                self.cells[i] = _weighted([1, 2, 3], [4, 4, 3] if self.burst else [7, 3, 1.4])
                active += 1
        while active > target:
            i = random.randint(0, NCELL - 1)
            if self.cells[i]:
                self.cells[i] = 0
                active -= 1

    def _pct(self, p):
        if not self.lat_win:
            return 0.0
        a = sorted(self.lat_win)
        return a[min(len(a) - 1, int(p / 100 * len(a)))]

    # ---- snapshot -------------------------------------------------------
    def snapshot(self) -> dict:
        with self._lock:
            el = int(time.time() - self.t0)
            p50, p95, p99 = self._pct(50), self._pct(95), self._pct(99)
            mx = max(self.lat_win) if self.lat_win else 0
            buckets = [0, 0, 0, 0]
            for v in self.lat_win:
                buckets[0 if v <= 250 else 1 if v <= 1000 else 2 if v <= 5000 else 3] += 1
            top = sorted(self.site_hits.items(), key=lambda kv: -kv[1])[:4]
            now = time.time()
            return {
                "mode": self.mode,
                "engine": "Databricks · RTM" if self.mode == "rtm" else "Databricks · Micro-batch",
                "scenario_name": "Fleet hot zone · burst" if self.burst else "Normal operation",
                "burst": self.burst,
                "evps": self.evps,
                "clock": f"{el // 60:02d}:{el % 60:02d}",
                "business": {
                    "units_monitored": 25000,
                    "units_in_alert": round(self.units_alert),
                    "alerts": self.alerts,
                    "critical": self.critical,
                    "fresh_pct": (self.fresh_hits / self.fresh_total * 100) if self.fresh_total else 0.0,
                    "cells": [{"n": self.cell_nums[i], "s": self.cells[i]} for i in range(NCELL)],
                    "top_sites": top,
                    "by_type": [{"key": k, "label": lbl, "n": self.by_type[k]} for k, lbl, _ in TYPES],
                    "by_severity": dict(self.by_sev),
                    "feed": [{**f, "ago_s": max(0, round(now - f["ts"]))} for f in list(self.feed)[:30]],
                },
                "tech": {
                    "evps": self.evps, "alps": self.alps,
                    "p50": p50, "p95": p95, "p99": p99, "max": mx, "lag_ms": self.lag,
                    "vol_in": self.vol_in, "vol_out": self.vol_out,
                    "lat_series": list(self.lat_series), "ev_series": list(self.ev_series),
                    "al_series": list(self.al_series), "buckets": buckets,
                    "builtins": {
                        "proc50": round(p50 * 0.72), "proc99": round(p99 * 0.72),
                        "q50": round(p50 * 0.22), "q99": round(p99 * 0.28),
                        "e50": round(p50), "e99": round(p99),
                    },
                },
            }
