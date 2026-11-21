"""Pure partition-planning logic.

The planner splits the (station-id ordered) station list into ``k`` consecutive,
non-empty partitions.  A partition may cover an existing Parquet file *only* when
its station range is byte-for-byte identical to the range recorded in the source
manifest; everything else has to be rewritten.  The plan returned minimises the
estimated number of rewritten bytes, with a deterministic tie-break: of two plans
with the same rewrite volume, the one whose partition-end station ids are
lexicographically smallest wins.

The DP runs in O(k * n) time using sliding-window maxima; a naive O(k * n^2)
implementation would not cope with a few thousand stations and a high partition
count.
"""

from __future__ import annotations

from array import array
from collections import deque
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

from .errors import NoFeasiblePlan

REUSE = "reuse"
REWRITE = "rewrite"


@dataclass(frozen=True)
class Station:
    station_id: int
    estimated_bytes: int
    # Current (source) partition range this station belongs to; both None if the
    # station is not part of any reusable range yet.
    current_start: Optional[int] = None
    current_end: Optional[int] = None


@dataclass(frozen=True)
class SourceRange:
    start_station: int
    end_station: int
    start_pos: int
    end_pos: int
    bytes: int
    station_ids: tuple[int, ...]


def build_source_ranges(stations: Sequence[Station]) -> dict[int, SourceRange]:
    """Group stations by their declared current range.

    Returns a mapping keyed by the range's end position.  Stations without a
    declared range are simply not reusable.
    """

    grouped: dict[tuple[int, int], list[int]] = {}
    for pos, st in enumerate(stations):
        if st.current_start is None and st.current_end is None:
            continue
        if st.current_start is None or st.current_end is None:
            raise ValueError(
                f"station {st.station_id}: current_start and current_end must be set together"
            )
        if st.current_start > st.current_end:
            raise ValueError(
                f"station {st.station_id}: current_start {st.current_start} > "
                f"current_end {st.current_end}"
            )
        grouped.setdefault((st.current_start, st.current_end), []).append(pos)

    by_id = {st.station_id: pos for pos, st in enumerate(stations)}
    ranges: dict[int, SourceRange] = {}
    for (start_id, end_id), positions in grouped.items():
        if start_id not in by_id or end_id not in by_id:
            raise ValueError(
                f"current range {start_id}-{end_id} endpoints must be known stations"
            )
        positions.sort()
        start_pos = positions[0]
        end_pos = positions[-1]
        expected = list(range(start_pos, end_pos + 1))
        if positions != expected:
            raise ValueError(
                f"current range {start_id}-{end_id} is not contiguous in station order"
            )
        # Every station whose id falls inside the declared range must belong to
        # it; otherwise the declared Parquet file cannot cover the range exactly.
        in_bounds = [p for p, st in enumerate(stations) if start_id <= st.station_id <= end_id]
        if in_bounds != positions:
            raise ValueError(
                f"current range {start_id}-{end_id} does not exactly cover the "
                f"stations whose ids fall inside it"
            )
        if positions[0] != by_id[start_id] or positions[-1] != by_id[end_id]:
            raise ValueError(
                f"current range {start_id}-{end_id} endpoints are out of order"
            )
        total = sum(stations[p].estimated_bytes for p in positions)
        ranges[end_pos] = SourceRange(
            start_station=start_id,
            end_station=end_id,
            start_pos=start_pos,
            end_pos=end_pos,
            bytes=total,
            station_ids=tuple(stations[p].station_id for p in positions),
        )
    return ranges


def _prefix_sums(stations: Sequence[Station]) -> list[int]:
    prefix = [0] * (len(stations) + 1)
    for i, st in enumerate(stations):
        prefix[i + 1] = prefix[i] + st.estimated_bytes
    return prefix


def _best_reuse_dp(
    n: int,
    k: int,
    prefix: list[int],
    cap: int,
    reuse_end: dict[int, tuple[int, int]],
) -> list[array]:
    """f[s][i] = max reusable bytes for first i stations in s segments (-1 impossible).

    ``reuse_end`` maps an end position ``i-1`` to ``(start_pos, range_bytes)``.
    """

    neg = -1
    f: list[array] = [array("q", [neg]) * (n + 1) for _ in range(k + 1)]
    f[0][0] = 0
    for s in range(1, k + 1):
        prev = f[s - 1]
        cur = f[s]
        window: deque[int] = deque()
        lo = 0
        for i in range(1, n + 1):
            j = i - 1
            if prev[j] >= 0:
                while window and prev[window[-1]] <= prev[j]:
                    window.pop()
                window.append(j)
            while prefix[i] - prefix[lo] > cap:
                lo += 1
            while window and window[0] < lo:
                window.popleft()
            best = prev[window[0]] if window else neg
            exact = reuse_end.get(i - 1)
            if exact is not None:
                start_pos, range_bytes = exact
                if start_pos >= lo and prev[start_pos] >= 0 and range_bytes <= cap:
                    candidate = prev[start_pos] + range_bytes
                    if candidate > best:
                        best = candidate
            cur[i] = best
    return f


def plan_partitions(
    stations: Iterable[Station],
    partition_count: int,
    max_partition_bytes: int,
) -> dict:
    """Compute the minimum-rewrite plan or raise :class:`NoFeasiblePlan`."""

    stations = list(stations)
    n = len(stations)
    if n == 0:
        raise NoFeasiblePlan("empty_source", "source manifest contains no stations")
    if partition_count < 1:
        raise NoFeasiblePlan(
            "invalid_partition_count", "partition_count must be at least 1"
        )
    if max_partition_bytes < 1:
        raise NoFeasiblePlan(
            "invalid_byte_cap", "max_partition_bytes must be at least 1"
        )
    if partition_count > n:
        raise NoFeasiblePlan(
            "too_many_partitions",
            f"cannot split {n} stations into {partition_count} non-empty partitions",
            station_count=n,
            partition_count=partition_count,
        )
    oversized = [st.station_id for st in stations if st.estimated_bytes > max_partition_bytes]
    if oversized:
        raise NoFeasiblePlan(
            "station_exceeds_cap",
            "at least one station is larger than max_partition_bytes and stations "
            "cannot be split",
            station_ids=oversized,
        )

    prefix = _prefix_sums(stations)
    total_bytes = prefix[n]
    if total_bytes > partition_count * max_partition_bytes:
        raise NoFeasiblePlan(
            "capacity_exceeded",
            "partition_count * max_partition_bytes is smaller than total bytes",
            total_bytes=total_bytes,
            capacity=partition_count * max_partition_bytes,
        )

    source_ranges = build_source_ranges(stations)
    reuse_end = {
        r.end_pos: (r.start_pos, r.bytes) for r in source_ranges.values()
    }

    # Forward DP on the real order determines the optimum.
    forward = _best_reuse_dp(n, partition_count, prefix, max_partition_bytes, reuse_end)
    best_reuse = forward[partition_count][n]
    if best_reuse < 0:
        raise NoFeasiblePlan(
            "no_partitioning",
            "no partition layout satisfies both partition_count and "
            "max_partition_bytes",
            station_count=n,
            partition_count=partition_count,
            max_partition_bytes=max_partition_bytes,
        )

    # Suffix optimum = forward optimum on the reversed station list.
    rev_stations = list(reversed(stations))
    rev_prefix = _prefix_sums(rev_stations)
    rev_reuse_end: dict[int, tuple[int, int]] = {}
    for r in source_ranges.values():
        rev_reuse_end[n - 1 - r.start_pos] = (
            n - 1 - r.end_pos,
            r.bytes,
        )
    reverse = _best_reuse_dp(
        n, partition_count, rev_prefix, max_partition_bytes, rev_reuse_end
    )

    def suffix(k: int, start: int) -> int:
        if start == n:
            return 0 if k == 0 else -1
        return reverse[k][n - start]

    # Greedy reconstruction: always take the earliest end position that still
    # allows the remaining stations to reach the optimum.  This yields the
    # lexicographically smallest boundary list among optimal plans.
    partitions: list[dict] = []
    start = 0
    for remaining in range(partition_count, 0, -1):
        target = suffix(remaining, start)
        chosen = None
        for end in range(start, n):
            if prefix[end + 1] - prefix[start] > max_partition_bytes:
                break
            gain = 0
            exact = reuse_end.get(end)
            if exact is not None and exact[0] == start:
                gain = exact[1]
            rest = suffix(remaining - 1, end + 1)
            if rest >= 0 and gain + rest == target:
                chosen = end
                break
        if chosen is None:  # pragma: no cover - guarded by the DP above
            raise NoFeasiblePlan(
                "reconstruction_failed",
                "internal error while reconstructing the optimal partition",
            )
        partitions.append(
            _describe_partition(len(partitions) + 1, start, chosen, stations, source_ranges)
        )
        start = chosen + 1

    reused = [p for p in partitions if p["action"] == REUSE]
    rewritten = [p for p in partitions if p["action"] == REWRITE]
    reused_bytes = sum(p["bytes"] for p in reused)
    rewrite_bytes = sum(p["bytes"] for p in rewritten)
    return {
        "partitions": partitions,
        "partition_count": partition_count,
        "station_count": n,
        "max_partition_bytes": max_partition_bytes,
        "total_bytes": total_bytes,
        "reused_bytes": reused_bytes,
        "rewrite_bytes": rewrite_bytes,
        "reused_ranges": [
            {"start_station": p["start_station"], "end_station": p["end_station"]}
            for p in reused
        ],
        "rewritten_ranges": [
            {"start_station": p["start_station"], "end_station": p["end_station"]}
            for p in rewritten
        ],
    }


def _describe_partition(
    index: int,
    start: int,
    end: int,
    stations: Sequence[Station],
    source_ranges: dict[int, SourceRange],
) -> dict:
    reused_range = source_ranges.get(end)
    is_reuse = reused_range is not None and reused_range.start_pos == start
    partition_bytes = sum(st.estimated_bytes for st in stations[start : end + 1])
    return {
        "index": index,
        "start_station": stations[start].station_id,
        "end_station": stations[end].station_id,
        "station_ids": [st.station_id for st in stations[start : end + 1]],
        "bytes": partition_bytes,
        "action": REUSE if is_reuse else REWRITE,
        "source_range": (
            {
                "start_station": reused_range.start_station,
                "end_station": reused_range.end_station,
            }
            if is_reuse
            else None
        ),
    }
