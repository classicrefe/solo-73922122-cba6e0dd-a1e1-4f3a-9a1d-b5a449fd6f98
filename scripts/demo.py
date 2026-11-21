#!/usr/bin/env python3
"""End-to-end demo of the repartition planner against a running service.

Usage:
    python3 scripts/demo.py                       # http://localhost:8000
    python3 scripts/demo.py http://127.0.0.1:9000

The demo registers a source manifest for one day, plans a repartition,
confirms and publishes it, then shows what the rewrite job reads out of
``GET /days/{day}/publication``.  It also demonstrates two important
behaviours: an infeasible request publishes nothing, and registering a new
source version stops the old rewrite manifest from being served.
"""

from __future__ import annotations

import json
import sys

import httpx

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000").rstrip("/")
DAY = "2026-11-21"


def show(title, value):
    print(f"\n=== {title} " + "=" * max(3, 66 - len(title)))
    print(json.dumps(value, indent=2, ensure_ascii=False))


def main() -> int:
    client = httpx.Client(base_url=BASE, timeout=30)

    health = client.get("/health")
    print("health:", health.status_code, health.json())

    # Sixteen stations in four existing Parquet files, plus one brand-new station
    # that has no existing file yet.
    stations = []
    for index in range(16):
        station_id = 1000 + index
        start = 1000 + (index // 4) * 4
        stations.append(
            {
                "station_id": station_id,
                "estimated_bytes": (40 - index) * 1_000_000,
                "current_range": {"start_station": start, "end_station": start + 3},
            }
        )
    stations.append({"station_id": 1016, "estimated_bytes": 25_000_000})

    created = client.post(
        f"/days/{DAY}/source-manifests", json={"stations": stations}
    )
    print("register manifest:", created.status_code)
    show(
        "source manifest", created.json() if created.status_code < 300 else created.text
    )

    # Two partitions at 250 MB cannot hold 545 MB of data: nothing is published.
    infeasible = client.post(
        f"/days/{DAY}/plans", json={"partition_count": 2, "max_partition_bytes": 250_000_000}
    )
    show("infeasible plan (nothing published)", infeasible.json())

    # One partition per existing file + one for the new station: the four existing
    # files are reused untouched, only the new station has to be rewritten.
    plan = client.post(
        f"/days/{DAY}/plans",
        json={"partition_count": 5, "max_partition_bytes": 300_000_000},
    )
    show("plan", plan.json())
    plan_id = plan.json()["plan_id"]

    print("\nconfirm:", client.post(
        f"/days/{DAY}/plans/{plan_id}/confirm", json={"operator": "demo"}
    ).status_code)
    published = client.post(
        f"/days/{DAY}/plans/{plan_id}/publish", json={"operator": "demo"}
    )
    print("publish:", published.status_code, "created =", published.json()["created"])

    served = client.get(f"/days/{DAY}/publication")
    show("publication served to the rewrite job", served.json()["payload"])

    retry = client.post(
        f"/days/{DAY}/plans/{plan_id}/publish", json={"operator": "demo"}
    )
    print(
        "\nretry publish:",
        retry.status_code,
        "same receipt =",
        retry.json()["publication_id"] == published.json()["publication_id"],
    )

    updated = json.loads(json.dumps(stations))
    updated[0]["estimated_bytes"] += 1_000_000
    newer = client.post(
        f"/days/{DAY}/source-manifests", json={"version": 2, "stations": updated}
    )
    print("register v2:", newer.status_code)

    gone = client.get(f"/days/{DAY}/publication")
    show("old publication after a new source version", gone.json())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
