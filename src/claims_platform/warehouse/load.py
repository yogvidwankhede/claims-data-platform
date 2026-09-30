"""Load the lakehouse's published Parquet into the warehouse RAW layer.

    duckdb     local development and CI: one transaction per load
    snowflake  production: PUT to an internal stage, then DELETE + COPY INTO inside
               one transaction

Snowflake note: the familiar "load into a new table, then ALTER TABLE ... SWAP"
pattern is avoided on purpose. Masking and row-access policies are attached to
the *table's columns*; swapping in a fresh table would silently drop them and
expose PII. Reloading in place keeps the governance attached.

Every load appends to raw._load_audit, which dbt's source freshness checks read.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from ..config import duckdb_path, paths, warehouse_kind

TABLES = ("members", "providers", "medical_claims_current", "pharmacy_claims")


def load(day: str, tables: tuple[str, ...] = TABLES) -> dict:
    return _load_duckdb(day, tables) if warehouse_kind() == "duckdb" else _load_snowflake(day, tables)


def _parquet_glob(table: str) -> str:
    folder = paths().exchange / table
    if not any(folder.glob("*.parquet")):
        raise FileNotFoundError(f"nothing exported for {table} at {folder}; run the export step first")
    return str(folder / "*.parquet")


def _load_duckdb(day: str, tables: tuple[str, ...]) -> dict:
    import duckdb

    path = duckdb_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    counts = {}
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute(
            "CREATE TABLE IF NOT EXISTS raw._load_audit (table_name VARCHAR, batch_date DATE, row_count BIGINT, "
            "loaded_at TIMESTAMP)"
        )
        con.execute("BEGIN TRANSACTION")
        for t in tables:
            con.execute(f"CREATE OR REPLACE TABLE raw.{t} AS SELECT * FROM read_parquet(?)", [_parquet_glob(t)])
            n = con.execute(f"SELECT count(*) FROM raw.{t}").fetchone()[0]
            con.execute(
                "INSERT INTO raw._load_audit VALUES (?, ?, ?, ?)",
                [t, day, n, datetime.now(timezone.utc).replace(tzinfo=None)],
            )
            counts[t] = n
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    return counts


def snowflake_connection():
    """Key-pair auth from the environment (no passwords in code or Airflow variables)."""
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        role=os.environ.get("SNOWFLAKE_ROLE", "LOADER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "LOAD_WH"),
        database=os.environ.get("SNOWFLAKE_RAW_DATABASE", "RAW"),
        schema="CLAIMS",
    )


def snowflake_statements(table: str, files: list[Path]) -> list[str]:
    """The exact statements run for one table; kept separate so tests can assert on them."""
    stage = f"@RAW.CLAIMS.EXCHANGE/{table}/"
    puts = [f"PUT 'file://{f.as_posix()}' {stage} OVERWRITE = TRUE AUTO_COMPRESS = FALSE" for f in files]
    return puts + [
        "BEGIN",
        f"DELETE FROM RAW.CLAIMS.{table.upper()}",
        (
            f"COPY INTO RAW.CLAIMS.{table.upper()} FROM {stage} "
            "FILE_FORMAT = (FORMAT_NAME = RAW.CLAIMS.PARQUET_FF) "
            "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE ON_ERROR = ABORT_STATEMENT"
        ),
        "COMMIT",
    ]


def _load_snowflake(day: str, tables: tuple[str, ...]) -> dict:
    con = snowflake_connection()
    counts = {}
    try:
        cur = con.cursor()
        for t in tables:
            files = sorted((paths().exchange / t).glob("*.parquet"))
            try:
                for sql in snowflake_statements(t, files):
                    cur.execute(sql)
            except Exception:
                cur.execute("ROLLBACK")
                raise
            n = cur.execute(f"SELECT COUNT(*) FROM RAW.CLAIMS.{t.upper()}").fetchone()[0]
            cur.execute(
                "INSERT INTO RAW.CLAIMS._LOAD_AUDIT (TABLE_NAME, BATCH_DATE, ROW_COUNT, LOADED_AT) "
                "VALUES (%s, %s, %s, CURRENT_TIMESTAMP())",
                (t, day, n),
            )
            counts[t] = n
    finally:
        con.close()
    return counts
