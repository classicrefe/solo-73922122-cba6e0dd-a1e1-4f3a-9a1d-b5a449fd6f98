from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import Settings  # noqa: E402
from app.main import create_app  # noqa: E402

DAY = "2026-11-21"


@pytest.fixture()
def client(tmp_path):
    settings = Settings(database_path=str(tmp_path / "test.db"))
    app = create_app(settings=settings)
    with TestClient(app) as test_client:
        yield test_client


def make_stations(spec):
    """spec: list of (station_id, bytes, (start,end)|None)."""

    return [
        {
            "station_id": sid,
            "estimated_bytes": size,
            "current_range": (
                {"start_station": rng[0], "end_station": rng[1]} if rng else None
            ),
        }
        for sid, size, rng in spec
    ]


BASE_SPEC = [
    (1001, 120, (1001, 1002)),
    (1002, 95, (1001, 1002)),
    (1003, 80, (1003, 1004)),
    (1004, 70, (1003, 1004)),
    (1005, 30, None),
]


def register(client, stations, version=None, day=DAY):
    body = {"stations": stations}
    if version is not None:
        body["version"] = version
    return client.post(f"/days/{day}/source-manifests", json=body)


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_openapi_docs_available(client):
    assert client.get("/openapi.json").status_code == 200
    assert client.get("/docs").status_code == 200


def test_manifest_versions_increase_and_duplicates_rejected(client):
    first = register(client, make_stations(BASE_SPEC))
    assert first.status_code == 201, first.text
    assert first.json()["manifest"]["version"] == 1

    second = register(client, make_stations(BASE_SPEC))
    assert second.status_code == 201
    assert second.json()["manifest"]["version"] == 2

    explicit = register(client, make_stations(BASE_SPEC), version=5)
    assert explicit.json()["manifest"]["version"] == 5

    backwards = register(client, make_stations(BASE_SPEC), version=4)
    assert backwards.status_code == 409
    assert backwards.json()["detail"]["code"] == "version_not_increasing"

    duplicate = client.post(
        f"/days/{DAY}/source-manifests",
        json={"stations": [
            {"station_id": 1001, "estimated_bytes": 1},
            {"station_id": 1001, "estimated_bytes": 2},
        ]},
    )
    assert duplicate.status_code == 422


def test_manifest_rejects_inconsistent_current_ranges(client):
    # 1002 declared in a range that skips 1003 but 1003 falls inside ids
    bad = [
        (1001, 10, (1001, 1003)),
        (1002, 10, (1001, 1003)),
        (1003, 10, None),
    ]
    response = register(client, make_stations(bad))
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "invalid_current_ranges"


def test_plan_prefers_exact_reuse(client):
    register(client, make_stations(BASE_SPEC))
    response = client.post(
        f"/days/{DAY}/plans",
        json={"partition_count": 3, "max_partition_bytes": 1000},
    )
    assert response.status_code == 200, response.text
    plan = response.json()
    # Only the trailing station 1005 falls outside a reusable range.
    assert plan["rewrite_bytes"] == 30
    assert plan["reused_bytes"] == 365
    actions = [p["action"] for p in plan["partitions"]]
    assert actions == ["reuse", "reuse", "rewrite"]
    assert plan["reused_ranges"] == [
        {"start_station": 1001, "end_station": 1002},
        {"start_station": 1003, "end_station": 1004},
    ]
    assert plan["rewritten_ranges"] == [{"start_station": 1005, "end_station": 1005}]


def test_two_partitions_have_to_rewrite_the_tail(client):
    register(client, make_stations(BASE_SPEC))
    plan = client.post(
        f"/days/{DAY}/plans",
        json={"partition_count": 2, "max_partition_bytes": 1000},
    ).json()
    # A 2-way split can keep at most one of the two existing ranges intact.
    assert plan["rewrite_bytes"] == 180
    assert plan["reused_ranges"] == [{"start_station": 1001, "end_station": 1002}]


def test_plan_minimises_rewrite_bytes(client):
    # Three candidate cuts; only one allows two exact ranges.
    spec = [
        (1, 50, (1, 2)),
        (2, 50, (1, 2)),
        (3, 50, (3, 4)),
        (4, 50, (3, 4)),
        (5, 50, (5, 5)),
    ]
    register(client, make_stations(spec))
    response = client.post(
        f"/days/{DAY}/plans",
        json={"partition_count": 3, "max_partition_bytes": 100},
    )
    plan = response.json()
    assert plan["rewrite_bytes"] == 0
    assert len(plan["reused_ranges"]) == 3


def test_plan_respects_consecutive_and_unsplittable(client):
    spec = [
        (10, 60, (10, 11)),
        (11, 60, (10, 11)),
        (12, 60, None),
    ]
    register(client, make_stations(spec))
    response = client.post(
        f"/days/{DAY}/plans",
        json={"partition_count": 2, "max_partition_bytes": 120},
    )
    plan = response.json()
    assert [p["station_ids"] for p in plan["partitions"]] == [[10, 11], [12]]
    assert plan["reused_ranges"] == [{"start_station": 10, "end_station": 11}]


def test_infeasible_plans_return_422(client):
    # station bigger than cap
    register(client, make_stations([(1, 500, None), (2, 10, None)]))
    response = client.post(
        f"/days/{DAY}/plans", json={"partition_count": 2, "max_partition_bytes": 100}
    )
    assert response.status_code == 422
    assert response.json()["detail"]["reason"] == "station_exceeds_cap"

    # too many partitions
    response = client.post(
        f"/days/{DAY}/plans", json={"partition_count": 5, "max_partition_bytes": 1000}
    )
    assert response.status_code == 422
    assert response.json()["detail"]["reason"] == "too_many_partitions"

    # capacity
    register(client, make_stations([(1, 100, None), (2, 100, None), (3, 100, None)]))
    response = client.post(
        f"/days/{DAY}/plans", json={"partition_count": 2, "max_partition_bytes": 120}
    )
    assert response.status_code == 422
    assert response.json()["detail"]["reason"] == "capacity_exceeded"


def test_plan_retry_is_idempotent(client):
    register(client, make_stations(BASE_SPEC))
    body = {"partition_count": 2, "max_partition_bytes": 1000}
    first = client.post(f"/days/{DAY}/plans", json=body)
    second = client.post(f"/days/{DAY}/plans", json=body)
    assert first.json()["plan_id"] == second.json()["plan_id"]


def test_publish_requires_confirmation(client):
    register(client, make_stations(BASE_SPEC))
    plan = client.post(
        f"/days/{DAY}/plans", json={"partition_count": 2, "max_partition_bytes": 1000}
    ).json()
    response = client.post(
        f"/days/{DAY}/plans/{plan['plan_id']}/publish", json={"operator": "ops"}
    )
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "plan_not_confirmed"


def test_confirm_then_publish_and_retry_receipt(client):
    register(client, make_stations(BASE_SPEC))
    plan = client.post(
        f"/days/{DAY}/plans", json={"partition_count": 2, "max_partition_bytes": 1000}
    ).json()
    confirm = client.post(
        f"/days/{DAY}/plans/{plan['plan_id']}/confirm", json={"operator": "ops"}
    )
    assert confirm.status_code == 200
    assert confirm.json()["confirmed"] is True

    first = client.post(
        f"/days/{DAY}/plans/{plan['plan_id']}/publish", json={"operator": "ops"}
    )
    assert first.status_code == 201
    assert first.json()["created"] is True
    pub_id = first.json()["publication_id"]

    retry = client.post(
        f"/days/{DAY}/plans/{plan['plan_id']}/publish", json={"operator": "ops"}
    )
    assert retry.status_code == 200
    assert retry.json()["created"] is False
    assert retry.json()["publication_id"] == pub_id

    served = client.get(f"/days/{DAY}/publication")
    assert served.status_code == 200
    payload = served.json()["payload"]
    assert payload["rewrite_bytes"] == 180
    assert payload["source_manifest"]["version"] == 1


def test_only_one_publication_per_source_version(client):
    register(client, make_stations(BASE_SPEC))
    p1 = client.post(
        f"/days/{DAY}/plans", json={"partition_count": 2, "max_partition_bytes": 1000}
    ).json()
    p2 = client.post(
        f"/days/{DAY}/plans", json={"partition_count": 3, "max_partition_bytes": 1000}
    ).json()
    client.post(f"/days/{DAY}/plans/{p1['plan_id']}/confirm", json={"operator": "a"})
    client.post(f"/days/{DAY}/plans/{p2['plan_id']}/confirm", json={"operator": "a"})
    assert client.post(
        f"/days/{DAY}/plans/{p1['plan_id']}/publish", json={"operator": "a"}
    ).status_code == 201
    clash = client.post(
        f"/days/{DAY}/plans/{p2['plan_id']}/publish", json={"operator": "b"}
    )
    assert clash.status_code == 409
    assert clash.json()["detail"]["code"] == "publication_exists"
    assert clash.json()["detail"]["published_plan_id"] == p1["plan_id"]


def test_new_source_version_supersedes_publication_and_blocks_old_plan(client):
    register(client, make_stations(BASE_SPEC))
    plan = client.post(
        f"/days/{DAY}/plans", json={"partition_count": 2, "max_partition_bytes": 1000}
    ).json()
    client.post(f"/days/{DAY}/plans/{plan['plan_id']}/confirm", json={"operator": "a"})
    client.post(f"/days/{DAY}/plans/{plan['plan_id']}/publish", json={"operator": "a"})
    assert client.get(f"/days/{DAY}/publication").status_code == 200

    changed = [(sid, size + 5, rng) for sid, size, rng in BASE_SPEC]
    register(client, make_stations(changed))

    served = client.get(f"/days/{DAY}/publication")
    assert served.status_code == 410
    assert served.json()["detail"]["code"] == "publication_superseded"

    confirm = client.post(
        f"/days/{DAY}/plans/{plan['plan_id']}/confirm", json={"operator": "a"}
    )
    assert confirm.status_code == 409
    assert confirm.json()["detail"]["code"] == "stale_plan"

    publish = client.post(
        f"/days/{DAY}/plans/{plan['plan_id']}/publish", json={"operator": "a"}
    )
    assert publish.status_code == 409
    assert publish.json()["detail"]["code"] == "stale_plan"

    # A fresh plan for the new version can be published.
    new_plan = client.post(
        f"/days/{DAY}/plans", json={"partition_count": 2, "max_partition_bytes": 1000}
    ).json()
    assert new_plan["source_manifest"]["version"] == 2
    client.post(f"/days/{DAY}/plans/{new_plan['plan_id']}/confirm", json={"operator": "a"})
    published = client.post(
        f"/days/{DAY}/plans/{new_plan['plan_id']}/publish", json={"operator": "a"}
    )
    assert published.status_code == 201


def test_unplanned_infeasible_request_does_not_block_publication(client):
    register(client, make_stations(BASE_SPEC))
    failed = client.post(
        f"/days/{DAY}/plans", json={"partition_count": 99, "max_partition_bytes": 10}
    )
    assert failed.status_code == 422
    plan = client.post(
        f"/days/{DAY}/plans", json={"partition_count": 2, "max_partition_bytes": 1000}
    ).json()
    client.post(f"/days/{DAY}/plans/{plan['plan_id']}/confirm", json={"operator": "a"})
    assert client.post(
        f"/days/{DAY}/plans/{plan['plan_id']}/publish", json={"operator": "a"}
    ).status_code == 201
