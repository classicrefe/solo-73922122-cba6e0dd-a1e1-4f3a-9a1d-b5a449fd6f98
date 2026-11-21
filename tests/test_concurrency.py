"""End-to-end concurrency checks against a real uvicorn server + SQLite file."""

from __future__ import annotations

import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.server_e2e import run_server  # noqa: E402

DAY = "2026-11-21"


def stations_spec():
    return [
        {"station_id": sid, "estimated_bytes": 40,
         "current_range": ({"start_station": 1, "end_station": 2} if sid <= 2 else None)}
        for sid in range(1, 7)
    ]


@pytest.fixture()
def server(tmp_path):
    with run_server(str(tmp_path / "e2e.db")) as base_url:
        yield base_url


def plan_and_confirm(base_url, plan_id):
    with httpx.Client(base_url=base_url, timeout=30) as client:
        client.post(f"/days/{DAY}/plans/{plan_id}/confirm", json={"operator": "load"})
        return client.post(
            f"/days/{DAY}/plans/{plan_id}/publish", json={"operator": "load"}
        )


def test_concurrent_manifest_registration_allocates_unique_versions(server):
    def register(_):
        with httpx.Client(base_url=server, timeout=30) as client:
            return client.post(
                f"/days/{DAY}/source-manifests", json={"stations": stations_spec()}
            )

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(register, range(12)))

    assert all(r.status_code == 201 for r in responses), [
        (r.status_code, r.text) for r in responses if r.status_code != 201
    ]
    versions = sorted(r.json()["manifest"]["version"] for r in responses)
    assert versions == list(range(1, 13)), versions

    with httpx.Client(base_url=server) as client:
        listing = client.get(f"/days/{DAY}/source-manifests").json()
        assert listing["current_version"] == 12
        assert len(listing["manifests"]) == 12


def test_concurrent_publication_yields_single_receipt(server):
    with httpx.Client(base_url=server, timeout=30) as client:
        client.post(f"/days/{DAY}/source-manifests", json={"stations": stations_spec()})
        plan = client.post(
            f"/days/{DAY}/plans",
            json={"partition_count": 2, "max_partition_bytes": 1000},
        ).json()
    plan_id = plan["plan_id"]
    with httpx.Client(base_url=server, timeout=30) as client:
        assert client.post(
            f"/days/{DAY}/plans/{plan_id}/confirm", json={"operator": "load"}
        ).status_code == 200

    barrier = threading.Barrier(8)

    def publish(_):
        with httpx.Client(base_url=server, timeout=30) as client:
            barrier.wait()
            response = client.post(
                f"/days/{DAY}/plans/{plan_id}/publish", json={"operator": "load"}
            )
            return response

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(publish, range(8)))

    ok = [r for r in responses if r.status_code == 201]
    replay = [r for r in responses if r.status_code == 200]
    assert len(ok) == 1, [(r.status_code, r.text) for r in responses]
    assert len(replay) == 7, [(r.status_code, r.text) for r in responses]
    assert {r.json()["publication_id"] for r in responses} == {ok[0].json()["publication_id"]}
    assert all(r.json()["created"] is False for r in replay)

    with httpx.Client(base_url=server) as client:
        retry = client.post(
            f"/days/{DAY}/plans/{plan_id}/publish", json={"operator": "load"}
        )
        assert retry.status_code == 200
        assert retry.json()["publication_id"] == ok[0].json()["publication_id"]
        assert retry.json()["created"] is False

        served = client.get(f"/days/{DAY}/publication")
        assert served.status_code == 200
        assert served.json()["publication_id"] == ok[0].json()["publication_id"]


def test_concurrent_plan_requests_return_same_plan(server):
    with httpx.Client(base_url=server, timeout=30) as client:
        client.post(f"/days/{DAY}/source-manifests", json={"stations": stations_spec()})

    barrier = threading.Barrier(10)

    def plan(_):
        with httpx.Client(base_url=server, timeout=30) as client:
            barrier.wait()
            return client.post(
                f"/days/{DAY}/plans",
                json={"partition_count": 3, "max_partition_bytes": 1000},
            )

    with ThreadPoolExecutor(max_workers=10) as pool:
        responses = list(pool.map(plan, range(10)))

    assert all(r.status_code == 200 for r in responses), [
        (r.status_code, r.text) for r in responses if r.status_code != 200
    ]
    plan_ids = {r.json()["plan_id"] for r in responses}
    assert len(plan_ids) == 1, plan_ids
