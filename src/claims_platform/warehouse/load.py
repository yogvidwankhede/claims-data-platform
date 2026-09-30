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


def snowflake_connection(role: str | None = None, database: str | None = None):
    """Key-pair auth from the environment (no passwords in code or Airflow variables)."""
    import snowflake.connector

    kwargs = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        **_private_key(),
        "role": role or os.environ.get("SNOWFLAKE_ROLE", "LOADER"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "LOAD_WH"),
    }
    database = os.environ.get("SNOWFLAKE_RAW_DATABASE", "RAW") if database is None else database
    if database:  # "" = no default database (migrations create them)
        kwargs["database"] = database
    return snowflake.connector.connect(**kwargs)


def _private_key() -> dict:
    """Key-pair credentials: a key file on disk (Airflow workers, laptops) or the PEM
    itself in SNOWFLAKE_PRIVATE_KEY (a Databricks secret exposed as an env var)."""
    pem = os.environ.get("SNOWFLAKE_PRIVATE_KEY")
    if not pem:
        return {"private_key_file": os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]}
    from cryptography.hazmat.primitives import serialization

    key = serialization.load_pem_private_key(pem.encode(), password=None)
    der = key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    return {"private_key": der}


def snowflake_statements(table: str, files: list[Path], day: str) -> list[tuple[str, tuple]]:
    """The exact statements run for one table, as (sql, params); kept separate so
    tests can assert on them.

    REMOVE first: a stage folder still holding yesterday's extra file would
    otherwise be loaded alongside today's. The reload and its audit row commit
    together, so the audit table never claims a load that was rolled back.
    """
    stage = f"@RAW.CLAIMS.EXCHANGE/{table}/"
    target = f"RAW.CLAIMS.{table.upper()}"
    puts = [(f"PUT 'file://{f.as_posix()}' {stage} OVERWRITE = TRUE AUTO_COMPRESS = FALSE", ()) for f in files]
    return (
        [(f"REMOVE {stage}", ())]
        + puts
        + [
            ("BEGIN", ()),
            (f"DELETE FROM {target}", ()),
            (
                f"COPY INTO {target} FROM {stage} "
                "FILE_FORMAT = (FORMAT_NAME = RAW.CLAIMS.PARQUET_FF) "
                "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE ON_ERROR = ABORT_STATEMENT",
                (),
            ),
            (
                "INSERT INTO RAW.CLAIMS._LOAD_AUDIT (TABLE_NAME, BATCH_DATE, ROW_COUNT, LOADED_AT) "
                # SYSDATE() is UTC; CURRENT_TIMESTAMP() into an NTZ column would store the
                # session's local wall-clock time and skew dbt's freshness checks
                f"SELECT %s, %s, COUNT(*), SYSDATE() FROM {target}",
                (table, day),
            ),
            ("COMMIT", ()),
        ]
    )


def _load_snowflake(day: str, tables: tuple[str, ...], con=None) -> dict:
    con = con or snowflake_connection()
    counts = {}
    try:
        cur = con.cursor()
        for t in tables:
            files = sorted((paths().exchange / t).glob("*.parquet"))
            if not files:
                raise FileNotFoundError(f"nothing exported for {t}; run the export step first")
            try:
                for sql, params in snowflake_statements(t, files, day):
                    cur.execute(sql, params or None)
            except Exception:
                cur.execute("ROLLBACK")
                raise
            counts[t] = cur.execute(f"SELECT COUNT(*) FROM RAW.CLAIMS.{t.upper()}").fetchone()[0]
    finally:
        con.close()
    return counts
