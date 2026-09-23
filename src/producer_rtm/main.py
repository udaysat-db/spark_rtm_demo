"""SignalNow producer — Real-Time Mode + ForeachWriter prototype (branch experiment).

An RTM variant of the producer, to A/B against the micro-batch one (src/producer/main.py).

Why this shape: RTM accepts the `rate` source and a `foreach` (ForeachWriter) sink, but
NOT `foreachBatch` and not the broadcast-nested-loop join the micro-batch producer uses to
attach the live scenario. So instead of shaping in the DataFrame, the ForeachWriter does it:

    rate source (RTM) → ForeachWriter
        open():    build a Kafka producer; read the current scenario from the control dir
        process(): refresh the scenario on a ~5s throttle, look up this row's freezer,
                   SHAPE the event for the current scenario, and produce it
        close():   flush

This keeps live scenario control with no foreachBatch and no disallowed join. The scenario
is re-read inside process() on a throttle (_CTL_REFRESH_MS) rather than relying on the
foreach open() cadence, so a console change takes effect within a few seconds no matter how
RTM schedules epochs. The event shaping is a pure-Python port of shared/scenarios.build_events
(same churn + intensity + severity-gradient model).

NOTE: the writer runs on executors and produces via kafka-python (a job library), so this
is at-least-once (fine for a synthetic generator). Uses a STABLE hash (crc32) — Python's
built-in hash() is per-process salted and would differ across executors.
"""
import argparse
import glob
import json
import random
import time
import uuid
import zlib

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.enrichment import load_fleet, materialize
from shared.scenarios import _STRESS, _BUCKET_MS, SCENARIOS

# How often each writer re-reads the control dir to pick up a live scenario change.
# Done inside process() (throttled) rather than relying on the foreach open() cadence,
# so live control has ~this granularity regardless of how RTM schedules epochs/open().
_CTL_REFRESH_MS = 5_000


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--kafka-bootstrap", required=True)
    p.add_argument("--secret-scope", default=None)
    p.add_argument("--topic", required=True)
    p.add_argument("--static-path", required=True)
    p.add_argument("--checkpoint", required=True)
    p.add_argument("--control-path", default=None, help="Control DIR; scenario re-read each epoch.")
    p.add_argument("--events-per-second", type=int, default=200)
    p.add_argument("--scenario", default="normal", help="Fallback when no control file is present.")
    p.add_argument("--num-partitions", type=int, default=4)
    p.add_argument("--rtm-trigger", default="1 minute",
                   help="RTM long-batch/checkpoint duration; also the scenario-refresh cadence (open()).")
    return p.parse_args()


def _stable01(s: str) -> float:
    """Stable [0,1) hash — crc32, not Python's per-process-salted hash()."""
    return (zlib.crc32(s.encode()) % 10000) / 10000.0


def read_scenario(control_dir, default):
    """Newest scenario from the append-only control dir (read via the /Volumes FUSE mount
    on the executor). Best-effort — any miss falls back to the default."""
    if not control_dir:
        return default
    try:
        best, best_ts = None, -1
        for fp in glob.glob(control_dir.rstrip("/") + "/*.json"):
            try:
                with open(fp) as f:
                    rec = json.load(f)
            except Exception:
                continue
            ts = rec.get("ts", 0) or 0
            if ts > best_ts and rec.get("scenario"):
                best, best_ts = rec["scenario"], ts
        return best or default
    except Exception:
        return default


def shape_event(a: dict, scenario: str, now_ms: int) -> dict:
    """Pure-Python port of shared/scenarios.build_events for one freezer's reading —
    same per-freezer churn (staggered bucket), intensity, and severity gradient."""
    rnd = random.random
    p = _STRESS.get(scenario, _STRESS["normal"])
    fid = a["freezer_id"]
    bucket = (now_ms + (zlib.crc32(fid.encode()) % _BUCKET_MS)) // _BUCKET_MS
    r = _stable01(f"{fid}-{bucket}")
    stressed = r < p
    frac = ((p - r) / p) if (stressed and p > 0) else 0.0
    intensity = frac * frac
    hard = stressed and intensity > 0.72

    upper = a["temperature_upper_limit"]
    nominal = upper - 3.0
    dlimit = a["door_open_limit_seconds"]
    warming = scenario in ("compressor_failure", "power_outage", "fleet_hot_zone")
    door_scn = scenario == "door_open"   # fleet_hot_zone is pure warming (→ TEMP_HIGH), no doors

    if stressed and warming:
        temp = upper + (intensity * 2.6 - 0.2)
    elif stressed and door_scn:
        temp = upper - 1.0 + intensity * 1.5
    else:
        temp = nominal + (rnd() - 0.5)

    door_open = bool(stressed and door_scn)
    door_secs = int(dlimit * (1.0 + intensity * 2.4)) if door_open else int(rnd() * 15)
    compressor_off = bool(hard and scenario == "compressor_failure")
    if compressor_off:
        comp_health = rnd() * 0.4
    elif stressed and scenario == "compressor_failure":
        comp_health = 0.55 + rnd() * 0.2
    else:
        comp_health = 0.85 + rnd() * 0.15
    on_power = bool(hard and scenario == "power_outage")
    power_state = (("outage" if rnd() < 0.5 else "battery") if on_power else "normal")
    battery = (rnd() * 20.0) if on_power else 100.0

    return {
        "event_id": str(uuid.uuid4()),
        "event_ts": now_ms,
        "device_id": fid,
        "freezer_id": fid,
        "site_id": a["site_id"],
        "temperature_c": round(temp, 2),
        "humidity_pct": round(35.0 + rnd() * 20.0, 1),
        "door_open": door_open,
        "door_open_seconds": door_secs,
        "compressor_on": not compressor_off,
        "compressor_health": round(comp_health, 2),
        "power_state": power_state,
        "battery_pct": round(battery, 1),
        "ambient_temp_c": round(20.0 + rnd() * 8.0, 1),
        "defrost_cycle_active": rnd() < 0.05,
        "scenario_name": scenario,
        "synthetic_severity": "elevated" if stressed else "none",
        "producer_ts": now_ms,
    }


class KafkaShapingWriter:
    """RTM ForeachWriter: shapes each rate row into a freezer event for the CURRENT
    scenario and produces it to Kafka. Scenario is read fresh in open() (per epoch)."""

    def __init__(self, bootstrap, topic, user, pw, mech, fleet, control_dir, default_scenario):
        self.bootstrap, self.topic = bootstrap, topic
        self.user, self.pw, self.mech = user, pw, mech
        self.fleet, self.n = fleet, len(fleet)
        self.control_dir, self.default = control_dir, default_scenario
        self.scenario = default_scenario
        self._next_ctl_check = 0        # force a control read on the first row

    def open(self, partition_id, epoch_id):
        from kafka import KafkaProducer
        kw = dict(
            bootstrap_servers=self.bootstrap.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            linger_ms=20, acks=1,
        )
        if self.user:
            kw.update(security_protocol="SASL_SSL", sasl_mechanism=self.mech,
                      sasl_plain_username=self.user, sasl_plain_password=self.pw)
        self.producer = KafkaProducer(**kw)
        self.scenario = read_scenario(self.control_dir, self.scenario)
        self._next_ctl_check = 0
        return True

    def process(self, row):
        now = int(time.time() * 1000)
        # Refresh the live scenario on a throttle (not every row, not tied to open()),
        # so a console scenario change takes effect within ~_CTL_REFRESH_MS.
        if self.control_dir and now >= self._next_ctl_check:
            self.scenario = read_scenario(self.control_dir, self.scenario)
            self._next_ctl_check = now + _CTL_REFRESH_MS
        a = self.fleet[int(row["value"]) % self.n]
        ev = shape_event(a, self.scenario, now)
        self.producer.send(self.topic, key=ev["freezer_id"], value=ev)

    def close(self, error):
        try:
            self.producer.flush()
            self.producer.close()
        except Exception:
            pass


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("signalnow-producer-rtm").getOrCreate()

    # Fleet, ordered so `value % n` maps deterministically; collected to the driver and
    # handed to the writer (small — ~500 rows).
    fleet = materialize(load_fleet(spark, args.static_path).orderBy("freezer_id"))
    fleet_rows = [r.asDict() for r in fleet.collect()]

    user = pw = None
    mech = "SCRAM-SHA-512"
    if args.secret_scope:
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            user = dbutils.secrets.get(scope=args.secret_scope, key="sasl_username")
            pw = dbutils.secrets.get(scope=args.secret_scope, key="sasl_password")
            mech = dbutils.secrets.get(scope=args.secret_scope, key="sasl_mechanism") or mech
        except Exception:
            pass

    rate = (spark.readStream.format("rate")
            .option("rowsPerSecond", args.events_per_second)
            .option("numPartitions", args.num_partitions)
            .load())

    writer = KafkaShapingWriter(args.kafka_bootstrap, args.topic, user, pw, mech,
                                fleet_rows, args.control_path, args.scenario)
    query = (rate.writeStream.foreach(writer)
             .option("checkpointLocation", args.checkpoint)
             .outputMode("update")
             .trigger(realTime=args.rtm_trigger)
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
