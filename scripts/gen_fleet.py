#!/usr/bin/env python3
"""Generate the synthetic fleet reference CSVs at demo scale into data/static/.

Deterministic (seeded), so re-running is reproducible and reviewable. The three CSVs
are the enrichment side of the broadcast join and the pool the producer draws from
(see docs/data-model.md).

    python scripts/gen_fleet.py                 # ~200 stores, ~500 freezers (default)
    python scripts/gen_fleet.py --sites 200 --freezers 500
"""
import argparse
import csv
import os
import random

# freezer_type, temperature_upper_limit (°C), door_open_limit_seconds — the 5 types.
TYPES = [
    ("walk_in_freezer", -15.0, 180),
    ("reach_in_freezer", -15.0, 90),
    ("display_freezer", -12.0, 60),
    ("walk_in_cooler", 4.0, 180),
    ("dairy_case", 4.0, 60),
]
TYPE_WEIGHTS = [3, 3, 2, 2, 2]
CITIES = [
    ("Austin", "TX"), ("Dallas", "TX"), ("Houston", "TX"), ("Phoenix", "AZ"),
    ("Tucson", "AZ"), ("Miami", "FL"), ("Tampa", "FL"), ("Orlando", "FL"),
    ("Atlanta", "GA"), ("Denver", "CO"), ("Sacramento", "CA"), ("Fresno", "CA"),
    ("Seattle", "WA"), ("Portland", "OR"), ("Chicago", "IL"), ("Detroit", "MI"),
    ("Columbus", "OH"), ("Nashville", "TN"), ("Charlotte", "NC"), ("Raleigh", "NC"),
    ("Boston", "MA"), ("Newark", "NJ"), ("Denver", "CO"), ("Omaha", "NE"),
    ("Boise", "ID"), ("Reno", "NV"), ("Kansas City", "MO"), ("Tulsa", "OK"),
]
PRIORITY, PRIORITY_W = ["low", "medium", "high"], [5, 3, 2]
VALUE, VALUE_W = ["low", "medium", "high"], [4, 3, 3]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sites", type=int, default=200)
    ap.add_argument("--freezers", type=int, default=500)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    if args.freezers < args.sites:
        ap.error("--freezers must be >= --sites (each store has at least one freezer)")

    random.seed(args.seed)
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    static = os.path.join(root, "data", "static")
    os.makedirs(static, exist_ok=True)

    # device_thresholds — one row per type
    with open(os.path.join(static, "device_thresholds.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["freezer_type", "temperature_upper_limit", "door_open_limit_seconds"])
        for t, lim, dsec in TYPES:
            w.writerow([t, lim, dsec])

    # sites — unique 4-digit store numbers
    nums = sorted(random.sample(range(100, 9999), args.sites))
    sites = []
    site_rows = [["site_id", "site_name", "site_region"]]
    for n in nums:
        city, state = random.choice(CITIES)
        sid = f"S{n:04d}"
        sites.append(sid)
        site_rows.append([sid, f"Store {n:04d} {city}", state])
    with open(os.path.join(static, "site_metadata.csv"), "w", newline="") as f:
        csv.writer(f).writerows(site_rows)
    # Ship a copy with the app so the console renders the full store grid at rest
    # (KafkaDataSource loads app/fleet_roster.csv by default). Same content, kept in
    # sync because this generator writes both.
    app_dir = os.path.join(root, "app")
    if os.path.isdir(app_dir):
        with open(os.path.join(app_dir, "fleet_roster.csv"), "w", newline="") as f:
            csv.writer(f).writerows(site_rows)

    # freezers — every store gets >=1, the rest distributed at random
    counts = [1] * args.sites
    for _ in range(args.freezers - args.sites):
        counts[random.randrange(args.sites)] += 1
    types = [t[0] for t in TYPES]
    total = 0
    with open(os.path.join(static, "freezer_metadata.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["freezer_id", "freezer_type", "site_id", "maintenance_priority", "inventory_value_band"])
        for sid, c in zip(sites, counts):
            num = sid[1:]
            for i in range(1, c + 1):
                w.writerow([f"FZ-{num}-{i:02d}", random.choices(types, TYPE_WEIGHTS)[0], sid,
                            random.choices(PRIORITY, PRIORITY_W)[0], random.choices(VALUE, VALUE_W)[0]])
                total += 1

    print(f"Wrote {args.sites} sites and {total} freezers to {static} (+ app/fleet_roster.csv)")


if __name__ == "__main__":
    main()
