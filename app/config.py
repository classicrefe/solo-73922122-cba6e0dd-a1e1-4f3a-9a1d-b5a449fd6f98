"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_path: str = "/data/repartition.db"
    max_stations_per_manifest: int = 20000
    max_dp_cells: int = 4_000_000

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_path=os.environ.get("DATABASE_PATH", cls.database_path),
            max_stations_per_manifest=int(
                os.environ.get("MAX_STATIONS_PER_MANIFEST", cls.max_stations_per_manifest)
            ),
            max_dp_cells=int(os.environ.get("MAX_DP_CELLS", cls.max_dp_cells)),
        )
