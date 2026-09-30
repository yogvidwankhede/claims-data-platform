"""Versioned, forward-only Snowflake migrations (the schemachange / Flyway model).

    snowflake/migrations/V001__warehouses_and_cost_controls.sql
    snowflake/migrations/V002__roles_and_grants.sql
    ...

Rules the runner enforces:

  * scripts apply in version order, each exactly once, recorded in a change-history
    table with a sha256 checksum;
  * editing a script that has already been applied is an error (checksum drift):
    a change to production goes in a new version, never by rewriting history;
  * a gap or duplicate in version numbers is an error;
  * a failing script stops the run and is not recorded, so the next run retries it
    (scripts are written with IF NOT EXISTS so a partial apply is safe to repeat).

`claims-platform migrate --dry-run` prints the plan without touching anything; CI
runs it against the change history of each environment before a deploy.
"""

from __future__ import annotations

import hashlib
import io
import re
from dataclasses import dataclass
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "snowflake" / "migrations"
HISTORY_TABLE = "PLATFORM_OPS.MIGRATIONS.CHANGE_HISTORY"
_NAME = re.compile(r"^V(\d{3,})__([a-z0-9_]+)\.sql$")


class MigrationError(Exception):
    pass


@dataclass(frozen=True)
class Migration:
    version: int
    description: str
    path: Path

    @property
    def sql(self) -> str:
        return self.path.read_text()

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode()).hexdigest()

    def statements(self) -> list[str]:
        from snowflake.connector.util_text import split_statements

        return [s for s, _ in split_statements(io.StringIO(self.sql), remove_comments=True) if s.strip()]


def discover(directory: Path = MIGRATIONS_DIR) -> list[Migration]:
    found = []
    for p in sorted(directory.glob("*.sql")):
        m = _NAME.match(p.name)
        if not m:
            raise MigrationError(f"{p.name}: expected V<NNN>__<snake_case>.sql")
        found.append(Migration(int(m.group(1)), m.group(2), p))
    found.sort(key=lambda m: m.version)
    versions = [m.version for m in found]
    if versions != list(range(1, len(found) + 1)):
        raise MigrationError(f"versions must be 1..N with no gaps or duplicates, got {versions}")
    return found


def plan(migrations: list[Migration], applied: dict[int, str]) -> list[Migration]:
    """Pending migrations, after checking that history matches the scripts on disk."""
    by_version = {m.version: m for m in migrations}
    for version, checksum in applied.items():
        if version not in by_version:
            raise MigrationError(f"V{version:03d} is recorded as applied but its script is missing")
        if by_version[version].checksum != checksum:
            raise MigrationError(
                f"V{version:03d} ({by_version[version].path.name}) was edited after it was applied; "
                "add a new migration instead of changing history"
            )
    return [m for m in migrations if m.version not in applied]


def _ensure_history(cur) -> None:
    cur.execute("CREATE DATABASE IF NOT EXISTS PLATFORM_OPS")
    cur.execute("CREATE SCHEMA IF NOT EXISTS PLATFORM_OPS.MIGRATIONS")
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (VERSION INTEGER NOT NULL, DESCRIPTION VARCHAR NOT NULL, "
        "SCRIPT VARCHAR NOT NULL, CHECKSUM VARCHAR NOT NULL, APPLIED_BY VARCHAR, "
        "APPLIED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())"
    )


def applied_versions(cur, create: bool = True) -> dict[int, str]:
    if create:
        _ensure_history(cur)
    else:  # dry run: read-only; a missing history table means nothing is applied yet
        try:
            cur.execute(f"SELECT 1 FROM {HISTORY_TABLE} LIMIT 1")
        except Exception:
            return {}
    return {int(v): c for v, c in cur.execute(f"SELECT VERSION, CHECKSUM FROM {HISTORY_TABLE}").fetchall()}


def migrate(con, directory: Path = MIGRATIONS_DIR, dry_run: bool = False, deploy_role: str | None = None) -> dict:
    cur = con.cursor()
    pending = plan(discover(directory), applied_versions(cur, create=not dry_run))
    names = [m.path.name for m in pending]
    if dry_run:
        return {"dry_run": True, "pending": names}
    for m in pending:
        for statement in m.statements():
            try:
                cur.execute(statement)
            except Exception as exc:
                raise MigrationError(f"{m.path.name} failed at: {statement[:120]!r}: {exc}") from exc
        if deploy_role:  # scripts switch roles with USE ROLE; record history as the deploying role
            cur.execute(f"USE ROLE {deploy_role}")
        cur.execute(
            f"INSERT INTO {HISTORY_TABLE} (VERSION, DESCRIPTION, SCRIPT, CHECKSUM, APPLIED_BY) "
            "VALUES (%s, %s, %s, %s, CURRENT_USER())",
            (m.version, m.description, m.path.name, m.checksum),
        )
    return {"applied": names}
