"""Targeted races and limits that are awkward to trigger over HTTP."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import planner, service as service_module  # noqa: E402
from app.config import Settings  # noqa: E402
from app.main import create_app  # noqa: E402

DAY = "2026-11-21"


def stations_spec():
    return [
        {"station_id": 1, "estimated_bytes": 10,
         "current_range": {"start_station": 1, "end_station": 2}},
        {"station_id": 2, "estimated_bytes": 10,
         "current_range": {"start_station": 1, "end_station": 2}},
        {"station_id": 3, "estimated_bytes": 10},
    ]


def test_manifest_superseded_while_planning(monkeypatch, tmp_path):
    """A plan computed against v1 must not be stored once v2 exists."""

    settings = Settings(database_path=str(tmp_path / "race.db"))
    app = create_app(settings=settings)
    real_plan = planner.plan_partitions
    original_holder = {}

    def slow_plan(stations, k, cap):
        client = original_holder["client"]
        if "registered" not in original_holder:
            original_holder["registered"] = True
            response = client.post(
                f"/days/{DAY}/source-manifests",
                json={"stations": stations_spec()},
            )
            assert response.status_code == 201
            assert response.json()["manifest"]["version"] == 2
        return real_plan(stations, k, cap)

    with TestClient(app) as client:
        original_holder["client"] = client
        assert client.post(
            f"/days/{DAY}/source-manifests", json={"stations": stations_spec()}
        ).status_code == 201

    # Monkeypatch after startup so the app's service uses it.
    with TestClient(app) as client:
        monkeypatch.setattr(service_module.planner, "plan_partitions", slow_plan)
        original_holder.pop("registered", None)
        original_holder["client"] = client
        response = client.post(
            f"/days/{DAY}/plans",
            json={"partition_count": 2, "max_partition_bytes": 100},
        )
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "stale_manifest"

    with TestClient(app) as client:
        listing = client.get(f"/days/{DAY}/plans").json()
        # Nothing was persisted for the superseded version.
        assert all(p["source_manifest_version"] != 1 for p in listing["plans"])


def test_manifest_size_limit(tmp_path):
    settings = Settings(
        database_path=str(tmp_path / "limit.db"), max_stations_per_manifest=2
    )
    app = create_app(settings=settings)
    with TestClient(app) as client:
        response = client.post(
            f"/days/{DAY}/source-manifests", json={"stations": stations_spec()}
        )
    assert response.status_code == 413
    assert response.json()["detail"]["code"] == "manifest_too_large"


def test_plan_dp_budget(tmp_path):
    settings = Settings(database_path=str(tmp_path / "budget.db"), max_dp_cells=4)
    app = create_app(settings=settings)
    with TestClient(app) as client:
        client.post(f"/days/{DAY}/source-manifests", json={"stations": stations_spec()})
        response = client.post(
            f"/days/{DAY}/plans",
            json={"partition_count": 3, "max_partition_bytes": 100},
        )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "plan_too_large"
