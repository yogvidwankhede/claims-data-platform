"""Platform-wide settings, read from the environment so every entry point (CLI,
Airflow tasks, tests, Databricks jobs) resolves the same locations.

    CLAIMS_DATA_DIR     root for landing / lakehouse / exchange (default ./data)
    CLAIMS_WAREHOUSE    'duckdb' (local) or 'snowflake'
    CLAIMS_DUCKDB_PATH  local warehouse file (default <data>/warehouse.duckdb)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

SOURCES = ("eligibility", "providers", "medical_claims", "pharmacy_claims")
LINES_OF_BUSINESS = ("COMMERCIAL", "MEDICARE", "MEDICAID")


@dataclass(frozen=True)
class Paths:
    root: Path

    @property
    def landing(self) -> Path:
        return self.root / "landing"

    @property
    def lakehouse(self) -> Path:
        return self.root / "lakehouse"

    @property
    def exchange(self) -> Path:
        """Parquet hand-off from the lakehouse to the warehouse (a stage, in Snowflake terms)."""
        return self.root / "exchange"

    @property
    def reports(self) -> Path:
        return self.root / "reports"

    def delivery(self, source: str, day: str) -> Path:
        return self.landing / source / f"dt={day}"

    def table(self, layer: str, name: str) -> Path:
        return self.lakehouse / layer / name


def paths() -> Paths:
    return Paths(Path(os.environ.get("CLAIMS_DATA_DIR", "data")).resolve())


def warehouse_kind() -> str:
    kind = os.environ.get("CLAIMS_WAREHOUSE", "duckdb")
    if kind not in ("duckdb", "snowflake"):
        raise ValueError(f"CLAIMS_WAREHOUSE must be duckdb or snowflake, not {kind!r}")
    return kind


def duckdb_path() -> Path:
    return Path(os.environ.get("CLAIMS_DUCKDB_PATH", str(paths().root / "warehouse.duckdb"))).resolve()
