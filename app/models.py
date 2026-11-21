"""Request/response schemas (Pydantic v2) with Swagger examples."""

from __future__ import annotations

from datetime import date
from typing import Annotated, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Day = Annotated[str, Field(pattern=r"^\d{4}-\d{2}-\d{2}$", examples=["2026-11-21"])]


def _validate_day(value: str) -> str:
    try:
        date.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - message stability
        raise ValueError("day must be a valid ISO date (YYYY-MM-DD)") from exc
    return value


class RangeIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start_station: int = Field(..., ge=1, examples=[1001])
    end_station: int = Field(..., ge=1, examples=[1020])

    @model_validator(mode="after")
    def check_order(self) -> "RangeIn":
        if self.start_station > self.end_station:
            raise ValueError("start_station must be <= end_station")
        return self


class StationIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    station_id: int = Field(..., ge=1, examples=[1001])
    estimated_bytes: int = Field(
        ..., ge=0, examples=[42_000_000], description="Estimated Parquet bytes for this station"
    )
    current_range: Optional[RangeIn] = Field(
        default=None,
        description=(
            "Current partition range this station belongs to. Stations sharing a range "
            "must form one contiguous run in station-id order, and the range must cover "
            "exactly the stations whose ids fall inside it."
        ),
    )


class SourceManifestIn(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "version": 3,
                    "stations": [
                        {"station_id": 1001, "estimated_bytes": 120000000,
                         "current_range": {"start_station": 1001, "end_station": 1020}},
                        {"station_id": 1002, "estimated_bytes": 95000000,
                         "current_range": {"start_station": 1001, "end_station": 1020}},
                        {"station_id": 1021, "estimated_bytes": 80000000,
                         "current_range": {"start_station": 1021, "end_station": 1040}},
                        {"station_id": 1041, "estimated_bytes": 30000000},
                    ],
                }
            ]
        },
    )

    version: Optional[int] = Field(
        default=None,
        ge=1,
        description="Optional explicit version. Must be greater than the current version "
        "for the day. Omit to let the service allocate the next version.",
    )
    stations: list[StationIn] = Field(..., min_length=1)

    @field_validator("stations")
    @classmethod
    def unique_station_ids(cls, stations: list[StationIn]) -> list[StationIn]:
        ids = [st.station_id for st in stations]
        if len(ids) != len(set(ids)):
            duplicates = sorted({i for i in ids if ids.count(i) > 1})
            raise ValueError(f"duplicate station_id values: {duplicates}")
        return stations


class PlanRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [{"partition_count": 4, "max_partition_bytes": 300000000}]
        },
    )

    partition_count: int = Field(..., ge=1, le=100_000, examples=[4])
    max_partition_bytes: int = Field(..., ge=1, examples=[300_000_000])


class PublishRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={"examples": [{"operator": "ops-zhang"}]},
    )

    operator: str = Field(..., min_length=1, max_length=128, examples=["ops-zhang"])


class RangeOut(BaseModel):
    start_station: int
    end_station: int


class PartitionOut(BaseModel):
    index: int
    start_station: int
    end_station: int
    station_ids: list[int]
    bytes: int
    action: str = Field(..., pattern="^(reuse|rewrite)$")
    source_range: Optional[RangeOut] = None


class ManifestRef(BaseModel):
    id: int
    day: str
    version: int


class SourceManifestOut(ManifestRef):
    created_at: str
    station_count: int
    total_bytes: int
    is_current: bool
    why_not_current: Optional[str] = None


class PlanOut(BaseModel):
    plan_id: int
    source_manifest: ManifestRef
    partition_count: int
    max_partition_bytes: int
    station_count: int
    total_bytes: int
    reused_bytes: int
    rewrite_bytes: int
    partitions: list[PartitionOut]
    reused_ranges: list[RangeOut]
    rewritten_ranges: list[RangeOut]
    created_at: str
    created: bool = Field(
        default=True, description="False when an identical request returned the stored plan"
    )
    confirmed: bool = False
    confirmed_at: Optional[str] = None
    confirmed_by: Optional[str] = None


class PublicationOut(BaseModel):
    publication_id: int
    day: str
    source_manifest: ManifestRef
    plan_id: int
    operator: str
    created_at: str
    created: bool = Field(
        default=True, description="False when a retry returned the stored publication"
    )
    payload: dict


class HealthOut(BaseModel):
    status: str
    version: str
