"""Property tests: the sliding-window DP must match exhaustive enumeration."""

from __future__ import annotations

import itertools
import random
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.errors import NoFeasiblePlan  # noqa: E402
from app.planner import Station, build_source_ranges, plan_partitions  # noqa: E402


def brute_force(stations, k, cap):
    """Enumerate every k-way consecutive split; return min rewrite bytes or None."""

    n = len(stations)
    if k < 1 or k > n:
        return None
    for st in stations:
        if st.estimated_bytes > cap:
            return None
    total = sum(st.estimated_bytes for st in stations)
    if total > k * cap:
        return None
    ranges = build_source_ranges(stations)
    reuse_by_end = {r.end_pos: (r.start_pos, r.station_ids) for r in ranges.values()}
    best = None
    for cuts in itertools.combinations(range(1, n), k - 1):
        bounds = (0, *cuts, n)
        if any(
            sum(st.estimated_bytes for st in stations[bounds[i] : bounds[i + 1]]) > cap
            for i in range(k)
        ):
            continue
        remainder = total
        for i in range(k):
            s, e = bounds[i], bounds[i + 1]
            r = reuse_by_end.get(e - 1)
            if r is not None and r[0] == s:
                remainder -= sum(station_bytes for station_bytes in _range_bytes(stations, s, e))
        if best is None or remainder < best:
            best = remainder
    return best


def _range_bytes(stations, s, e):
    return [st.estimated_bytes for st in stations[s:e]]


def random_station_list(rng, n):
    sizes = [rng.randint(0, 20) for _ in range(n)]
    specs = []
    pos = 0
    while pos < n:
        # Optionally group a contiguous run into an existing range.
        if rng.random() < 0.55:
            width = rng.randint(1, min(4, n - pos))
            start_id = 1000 + pos
            end_id = 1000 + pos + width - 1
            for offset in range(width):
                specs.append((1000 + pos + offset, sizes[pos + offset], (start_id, end_id)))
            pos += width
        else:
            specs.append((1000 + pos, sizes[pos], None))
            pos += 1
    return [
        Station(station_id=sid, estimated_bytes=size, current_start=r[0] if r else None,
                current_end=r[1] if r else None)
        for sid, size, r in specs
    ]


@pytest.mark.parametrize("seed", range(60))
def test_dp_matches_brute_force(seed):
    rng = random.Random(seed)
    n = rng.randint(1, 9)
    stations = random_station_list(rng, n)
    k = rng.randint(1, n)
    cap = rng.randint(0, 60)
    expected = brute_force(stations, k, cap)
    try:
        result = plan_partitions(stations, k, cap)
    except NoFeasiblePlan as exc:
        assert expected is None, (seed, exc.reason, stations, k, cap)
        return
    assert expected is not None, (seed, stations, k, cap)
    assert result["rewrite_bytes"] == expected, (seed, stations, k, cap, result)

    # Structural invariants.
    parts = result["partitions"]
    assert len(parts) == k
    assert parts[0]["station_ids"][0] == stations[0].station_id
    assert parts[-1]["station_ids"][-1] == stations[-1].station_id
    flat = [sid for p in parts for sid in p["station_ids"]]
    assert flat == [st.station_id for st in stations]
    assert all(p["bytes"] <= cap for p in parts)
    assert result["reused_bytes"] + result["rewrite_bytes"] == result["total_bytes"]
    ranges = build_source_ranges(stations)
    for p in parts:
        if p["action"] == "reuse":
            assert p["source_range"] is not None
            matched = [
                r for r in ranges.values()
                if r.start_station == p["start_station"] and r.end_station == p["end_station"]
            ]
            assert len(matched) == 1
        else:
            assert p["source_range"] is None


def test_lexicographic_tie_break():
    # No reusable range at all: every 2-way split rewrites the same total, so the
    # earliest boundary (lexicographically smallest) must be chosen.
    stations = [Station(1, 10), Station(2, 10), Station(3, 10)]
    result = plan_partitions(stations, 2, 100)
    assert [p["station_ids"] for p in result["partitions"]] == [[1], [2, 3]]
    assert result["rewrite_bytes"] == 30

    # An exact range does win over the earlier boundary, because reuse dominates
    # the tie-break.
    stations = [
        Station(1, 10, current_start=1, current_end=2),
        Station(2, 10, current_start=1, current_end=2),
        Station(3, 10),
    ]
    result = plan_partitions(stations, 2, 100)
    assert [p["station_ids"] for p in result["partitions"]] == [[1, 2], [3]]
    assert result["reused_ranges"] == [{"start_station": 1, "end_station": 2}]
