"""Business logic: versioned source manifests, partition plans, publications.

Consistency rules
-----------------
* Source-manifest versions increase monotonically per day.  The highest version
  is the *current* one; only it may be planned against, confirmed or published.
* A plan is immutable and is reused when the same source version and the same
  planning parameters are requested again.
* A publication is created at most once per source version (DB unique
  constraint), and only from a confirmed plan of that version.  Retrying the
  same publication returns the original receipt.
* Once a newer source version exists, the publication of an older version is no
  longer served (HTTP 410) and can no longer be created.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Optional

from . import planner
from .db import Database, utc_now, write_tx
from .errors import ApiError
from .models import (
    ManifestRef,
    PartitionOut,
    PlanOut,
    PlanRequest,
    PublicationOut,
    RangeOut,
    SourceManifestIn,
    SourceManifestOut,
)
from .planner import Station


class RepartitionService:
    def __init__(
        self,
        db: Database,
        max_stations_per_manifest: int = 20000,
        max_dp_cells: int = 4_000_000,
    ) -> None:
        self.db = db
        self.max_stations_per_manifest = max_stations_per_manifest
        self.max_dp_cells = max_dp_cells

    # ------------------------------------------------------------------ util
    def _conn(self) -> sqlite3.Connection:
        return self.db.connect()

    @staticmethod
    def _current_manifest(conn: sqlite3.Connection, day: str) -> Optional[sqlite3.Row]:
        return conn.execute(
            "SELECT * FROM source_manifests WHERE day = ? ORDER BY version DESC LIMIT 1",
            (day,),
        ).fetchone()

    # --------------------------------------------------------------- manifests
    def register_manifest(self, day: str, body: SourceManifestIn) -> dict:
        if len(body.stations) > self.max_stations_per_manifest:
            raise ApiError(
                413,
                "manifest_too_large",
                f"manifest has {len(body.stations)} stations, limit is "
                f"{self.max_stations_per_manifest}",
            )
        station_in = sorted(body.stations, key=lambda st: st.station_id)
        conn = self._conn()
        try:
            with write_tx(conn):
                current = self._current_manifest(conn, day)
                current_version = current["version"] if current else 0
                if body.version is not None:
                    if body.version <= current_version:
                        raise ApiError(
                            409,
                            "version_not_increasing",
                            f"version {body.version} must be greater than the current "
                            f"version {current_version} for day {day}",
                            current_version=current_version,
                        )
                    version = body.version
                else:
                    version = current_version + 1
                existing = conn.execute(
                    "SELECT * FROM source_manifests WHERE day = ? AND version = ?",
                    (day, version),
                ).fetchone()
                if existing is not None:  # pragma: no cover - guarded above
                    raise ApiError(
                        409,
                        "version_exists",
                        f"version {version} already exists for day {day}",
                    )

                # Validate the declared current ranges as a whole before writing.
                stations = [
                    Station(
                        station_id=item.station_id,
                        estimated_bytes=item.estimated_bytes,
                        current_start=(
                            item.current_range.start_station
                            if item.current_range
                            else None
                        ),
                        current_end=(
                            item.current_range.end_station
                            if item.current_range
                            else None
                        ),
                    )
                    for item in station_in
                ]
                try:
                    source_ranges = planner.build_source_ranges(stations)
                except ValueError as exc:
                    raise ApiError(422, "invalid_current_ranges", str(exc)) from exc

                total_bytes = sum(st.estimated_bytes for st in stations)
                ranges_json = json.dumps(
                    [
                        {
                            "start_station": r.start_station,
                            "end_station": r.end_station,
                            "bytes": r.bytes,
                            "station_ids": list(r.station_ids),
                        }
                        for r in sorted(source_ranges.values(), key=lambda r: r.start_pos)
                    ],
                    ensure_ascii=False,
                )
                superseded_publication = None
                if current is not None:
                    superseded_publication = conn.execute(
                        "SELECT id FROM publications WHERE source_manifest_id = ?",
                        (current["id"],),
                    ).fetchone()
                created_at = utc_now()
                cursor = conn.execute(
                    "INSERT INTO source_manifests "
                    "(day, version, created_at, station_count, total_bytes, ranges_json) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        day,
                        version,
                        created_at,
                        len(stations),
                        total_bytes,
                        ranges_json,
                    ),
                )
                manifest_id = int(cursor.lastrowid)
                conn.executemany(
                    "INSERT INTO stations "
                    "(manifest_id, position, station_id, estimated_bytes, "
                    " current_start_station, current_end_station) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    [
                        (
                            manifest_id,
                            pos,
                            st.station_id,
                            st.estimated_bytes,
                            st.current_start,
                            st.current_end,
                        )
                        for pos, st in enumerate(stations)
                    ],
                )
        finally:
            conn.close()

        return {
            "manifest": {
                "id": manifest_id,
                "day": day,
                "version": version,
                "created_at": created_at,
                "station_count": len(stations),
                "total_bytes": total_bytes,
            },
            "created": True,
            "previous_version": current_version or None,
            "superseded_publication_id": (
                superseded_publication["id"] if superseded_publication else None
            ),
        }

    def get_current_manifest(self, day: str) -> SourceManifestOut:
        conn = self._conn()
        try:
            row = self._current_manifest(conn, day)
            if row is None:
                raise ApiError(404, "manifest_not_found", f"no source manifest for {day}")
            return SourceManifestOut(
                id=row["id"],
                day=row["day"],
                version=row["version"],
                created_at=row["created_at"],
                station_count=row["station_count"],
                total_bytes=row["total_bytes"],
                is_current=True,
            )
        finally:
            conn.close()

    def list_manifests(self, day: str) -> dict:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM source_manifests WHERE day = ? ORDER BY version",
                (day,),
            ).fetchall()
            if not rows:
                raise ApiError(404, "manifest_not_found", f"no source manifest for {day}")
            newest = rows[-1]["version"]
            return {
                "day": day,
                "current_version": newest,
                "manifests": [
                    SourceManifestOut(
                        id=r["id"],
                        day=r["day"],
                        version=r["version"],
                        created_at=r["created_at"],
                        station_count=r["station_count"],
                        total_bytes=r["total_bytes"],
                        is_current=r["version"] == newest,
                        why_not_current=(
                            None
                            if r["version"] == newest
                            else "superseded by a newer source version"
                        ),
                    )
                    for r in rows
                ],
            }
        finally:
            conn.close()

    # ------------------------------------------------------------------- plans
    def create_plan(self, day: str, body: PlanRequest) -> tuple[PlanOut, bool]:
        conn = self._conn()
        try:
            # Read phase: no write lock, so a long-running DP never blocks
            # manifest registration.
            manifest = self._current_manifest(conn, day)
            if manifest is None:
                raise ApiError(404, "manifest_not_found", f"no source manifest for {day}")
            existing = conn.execute(
                "SELECT * FROM plans WHERE source_manifest_id = ? "
                "AND partition_count = ? AND max_partition_bytes = ?",
                (manifest["id"], body.partition_count, body.max_partition_bytes),
            ).fetchone()
            if existing is not None:
                return self._plan_out(existing), False

            stations = self._load_stations(conn, manifest["id"])
            result = self._plan_or_422(
                stations, body.partition_count, body.max_partition_bytes
            )
            payload = {
                "source_manifest": {
                    "id": manifest["id"],
                    "day": manifest["day"],
                    "version": manifest["version"],
                },
                **result,
            }

            # Write phase: short transaction; re-check that the manifest is still
            # current and that nobody stored the same plan in the meantime.
            with write_tx(conn):
                current = self._current_manifest(conn, day)
                if current is None or current["id"] != manifest["id"]:
                    raise ApiError(
                        409,
                        "stale_manifest",
                        "a newer source version was registered while the plan was "
                        "being computed; request the plan again",
                        current_version=current["version"] if current else None,
                    )
                stored = conn.execute(
                    "SELECT * FROM plans WHERE source_manifest_id = ? "
                    "AND partition_count = ? AND max_partition_bytes = ?",
                    (manifest["id"], body.partition_count, body.max_partition_bytes),
                ).fetchone()
                if stored is not None:
                    return self._plan_out(stored), False

                created_at = utc_now()
                cursor = conn.execute(
                    "INSERT INTO plans (source_manifest_id, partition_count, "
                    "max_partition_bytes, rewrite_bytes, reused_bytes, plan_json, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        manifest["id"],
                        body.partition_count,
                        body.max_partition_bytes,
                        result["rewrite_bytes"],
                        result["reused_bytes"],
                        json.dumps(payload, ensure_ascii=False),
                        created_at,
                    ),
                )
                plan_id = int(cursor.lastrowid)
                return self._plan_out_from_payload(plan_id, payload, created_at), True
        finally:
            conn.close()

    def _plan_or_422(
        self, stations: list[Station], partition_count: int, max_partition_bytes: int
    ) -> dict:
        n = len(stations)
        # Guard the DP's memory use (O(k*n) int64 cells).
        if n and partition_count * n > self.max_dp_cells:
            raise ApiError(
                422,
                "plan_too_large",
                f"{n} stations x {partition_count} partitions exceeds the planner "
                f"limit of {self.max_dp_cells} cells",
                station_count=n,
                partition_count=partition_count,
            )
        return planner.plan_partitions(stations, partition_count, max_partition_bytes)

    def _load_stations(self, conn: sqlite3.Connection, manifest_id: int) -> list[Station]:
        rows = conn.execute(
            "SELECT station_id, estimated_bytes, current_start_station, "
            "current_end_station FROM stations WHERE manifest_id = ? ORDER BY position",
            (manifest_id,),
        ).fetchall()
        return [
            Station(
                station_id=r["station_id"],
                estimated_bytes=r["estimated_bytes"],
                current_start=r["current_start_station"],
                current_end=r["current_end_station"],
            )
            for r in rows
        ]

    def _plan_out(self, row: sqlite3.Row) -> PlanOut:
        return self._plan_out_from_payload(
            row["id"],
            json.loads(row["plan_json"]),
            row["created_at"],
            confirmed_at=row["confirmed_at"],
            confirmed_by=row["confirmed_by"],
        )

    def _plan_out_from_payload(
        self,
        plan_id: int,
        payload: dict,
        created_at: str,
        confirmed_at: Optional[str] = None,
        confirmed_by: Optional[str] = None,
    ) -> PlanOut:
        ref = payload["source_manifest"]
        return PlanOut(
            plan_id=plan_id,
            source_manifest=ManifestRef(**ref),
            partition_count=payload["partition_count"],
            max_partition_bytes=payload["max_partition_bytes"],
            station_count=payload["station_count"],
            total_bytes=payload["total_bytes"],
            reused_bytes=payload["reused_bytes"],
            rewrite_bytes=payload["rewrite_bytes"],
            partitions=[PartitionOut(**p) for p in payload["partitions"]],
            reused_ranges=[RangeOut(**r) for r in payload["reused_ranges"]],
            rewritten_ranges=[RangeOut(**r) for r in payload["rewritten_ranges"]],
            created_at=created_at,
            confirmed=confirmed_at is not None,
            confirmed_at=confirmed_at,
            confirmed_by=confirmed_by,
        )

    def get_plan(self, day: str, plan_id: int) -> PlanOut:
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT p.*, m.day AS m_day, m.version AS m_version FROM plans p "
                "JOIN source_manifests m ON m.id = p.source_manifest_id "
                "WHERE p.id = ? AND m.day = ?",
                (plan_id, day),
            ).fetchone()
            if row is None:
                raise ApiError(404, "plan_not_found", f"plan {plan_id} not found for {day}")
            payload = json.loads(row["plan_json"])
            return self._plan_out_from_payload(
                plan_id,
                payload,
                row["created_at"],
                confirmed_at=row["confirmed_at"],
                confirmed_by=row["confirmed_by"],
            )
        finally:
            conn.close()

    def list_plans(self, day: str) -> dict:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT p.id, p.source_manifest_id, p.partition_count, "
                "p.max_partition_bytes, p.rewrite_bytes, p.reused_bytes, "
                "p.confirmed_at, m.version "
                "FROM plans p JOIN source_manifests m ON m.id = p.source_manifest_id "
                "WHERE m.day = ? ORDER BY p.id",
                (day,),
            ).fetchall()
            if not rows:
                return {"day": day, "plans": []}
            current = self._current_manifest(conn, day)
            return {
                "day": day,
                "current_version": current["version"] if current else None,
                "plans": [
                    {
                        "plan_id": r["id"],
                        "source_manifest_version": r["version"],
                        "partition_count": r["partition_count"],
                        "max_partition_bytes": r["max_partition_bytes"],
                        "rewrite_bytes": r["rewrite_bytes"],
                        "reused_bytes": r["reused_bytes"],
                        "confirmed": r["confirmed_at"] is not None,
                        "confirmed_at": r["confirmed_at"],
                        "is_current": current is not None
                        and r["source_manifest_id"] == current["id"],
                    }
                    for r in rows
                ],
            }
        finally:
            conn.close()

    def confirm_plan(self, day: str, plan_id: int, operator: str) -> PlanOut:
        conn = self._conn()
        try:
            with write_tx(conn):
                row = conn.execute(
                    "SELECT p.*, m.day AS m_day, m.id AS manifest_id "
                    "FROM plans p JOIN source_manifests m ON m.id = p.source_manifest_id "
                    "WHERE p.id = ? AND m.day = ?",
                    (plan_id, day),
                ).fetchone()
                if row is None:
                    raise ApiError(
                        404, "plan_not_found", f"plan {plan_id} not found for {day}"
                    )
                current = self._current_manifest(conn, day)
                if current is None or row["manifest_id"] != current["id"]:
                    raise ApiError(
                        409,
                        "stale_plan",
                        "the source manifest has been superseded; old plans cannot be "
                        "confirmed or published",
                        plan_source_version=self._version_of(conn, row["manifest_id"]),
                        current_version=current["version"] if current else None,
                    )
                if row["confirmed_at"] is None:
                    confirmed_at = utc_now()
                    conn.execute(
                        "UPDATE plans SET confirmed_at = ?, confirmed_by = ? WHERE id = ?",
                        (confirmed_at, operator, plan_id),
                    )
                else:
                    confirmed_at = row["confirmed_at"]
                payload = json.loads(row["plan_json"])
                return self._plan_out_from_payload(
                    plan_id,
                    payload,
                    row["created_at"],
                    confirmed_at=confirmed_at,
                    confirmed_by=row["confirmed_by"] or operator,
                )
        finally:
            conn.close()

    @staticmethod
    def _version_of(conn: sqlite3.Connection, manifest_id: int) -> Optional[int]:
        row = conn.execute(
            "SELECT version FROM source_manifests WHERE id = ?", (manifest_id,)
        ).fetchone()
        return row["version"] if row else None

    # ------------------------------------------------------------- publications
    @staticmethod
    def _publication_out(row: sqlite3.Row, day: str, created: bool) -> PublicationOut:
        payload = json.loads(row["payload_json"])
        return PublicationOut(
            publication_id=row["id"],
            day=day,
            source_manifest=ManifestRef(**payload["source_manifest"]),
            plan_id=row["plan_id"],
            operator=row["operator"],
            created_at=row["created_at"],
            created=created,
            payload=payload,
        )

    def publish(self, day: str, plan_id: int, operator: str) -> tuple[PublicationOut, bool]:
        conn = self._conn()
        try:
            with write_tx(conn):
                current = self._current_manifest(conn, day)
                if current is None:
                    raise ApiError(
                        404, "manifest_not_found", f"no source manifest for {day}"
                    )
                plan = conn.execute(
                    "SELECT * FROM plans WHERE id = ? AND source_manifest_id = ?",
                    (plan_id, current["id"]),
                ).fetchone()
                if plan is None:
                    old = conn.execute(
                        "SELECT p.id FROM plans p JOIN source_manifests m "
                        "ON m.id = p.source_manifest_id "
                        "WHERE p.id = ? AND m.day = ?",
                        (plan_id, day),
                    ).fetchone()
                    if old is None:
                        raise ApiError(
                            404, "plan_not_found", f"plan {plan_id} not found for {day}"
                        )
                    raise ApiError(
                        409,
                        "stale_plan",
                        "the source manifest has been superseded; old plans cannot be "
                        "published",
                        current_version=current["version"],
                    )

                existing = conn.execute(
                    "SELECT * FROM publications WHERE source_manifest_id = ?",
                    (current["id"],),
                ).fetchone()
                if existing is not None:
                    if existing["plan_id"] != plan_id:
                        raise ApiError(
                            409,
                            "publication_exists",
                            "a different plan is already published for this source version",
                            published_plan_id=existing["plan_id"],
                            publication_id=existing["id"],
                        )
                    return self._publication_out(existing, day, created=False), False

                if plan["confirmed_at"] is None:
                    raise ApiError(
                        409,
                        "plan_not_confirmed",
                        "the plan must be confirmed before it can be published",
                        plan_id=plan_id,
                    )

                payload = json.loads(plan["plan_json"])
                publication_payload = {
                    "day": day,
                    "source_manifest": {
                        "id": current["id"],
                        "day": day,
                        "version": current["version"],
                    },
                    "plan_id": plan_id,
                    "partition_count": payload["partition_count"],
                    "max_partition_bytes": payload["max_partition_bytes"],
                    "total_bytes": payload["total_bytes"],
                    "rewrite_bytes": payload["rewrite_bytes"],
                    "reused_bytes": payload["reused_bytes"],
                    "partitions": payload["partitions"],
                    "reused_ranges": payload["reused_ranges"],
                    "rewritten_ranges": payload["rewritten_ranges"],
                }
                created_at = utc_now()
                cursor = conn.execute(
                    "INSERT INTO publications "
                    "(source_manifest_id, plan_id, operator, created_at, payload_json) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        current["id"],
                        plan_id,
                        operator,
                        created_at,
                        json.dumps(publication_payload, ensure_ascii=False),
                    ),
                )
                row = {
                    "id": int(cursor.lastrowid),
                    "plan_id": plan_id,
                    "operator": operator,
                    "created_at": created_at,
                    "payload_json": json.dumps(publication_payload, ensure_ascii=False),
                }
                return self._publication_out(row, day, created=True), True
        except sqlite3.IntegrityError as exc:
            # Lost a race against a concurrent publication for the same version.
            raise ApiError(
                409,
                "publication_exists",
                "a publication already exists for this source version; retry to "
                "receive its receipt",
            ) from exc
        finally:
            conn.close()

    def get_publication(self, day: str) -> PublicationOut:
        conn = self._conn()
        try:
            current = self._current_manifest(conn, day)
            if current is None:
                raise ApiError(
                    404, "manifest_not_found", f"no source manifest for {day}"
                )
            pub = conn.execute(
                "SELECT p.*, m.version, m.id AS manifest_id FROM publications p "
                "JOIN source_manifests m ON m.id = p.source_manifest_id "
                "WHERE m.day = ? ORDER BY p.id DESC LIMIT 1",
                (day,),
            ).fetchone()
            if pub is None:
                raise ApiError(
                    404, "publication_not_found", f"nothing published for {day}"
                )
            if pub["manifest_id"] != current["id"]:
                raise ApiError(
                    410,
                    "publication_superseded",
                    "the published rewrite manifest belongs to a superseded source "
                    "version and is no longer served; publish a plan for the current "
                    "version",
                    published_version=pub["version"],
                    current_version=current["version"],
                )
            return self._publication_out(pub, day, created=False)
        finally:
            conn.close()
