"""Snowflake platform-as-code: migration runner, DDL vs data contracts, PII governance
and the loader's statements. Runs offline: fakesnow emulates Snowflake on DuckDB for
the objects it supports (databases, schemas, tables, stages); account-level objects
(warehouses, roles, policies) are checked statically and by sqlfluff in CI."""

import re
from pathlib import Path

import pytest

fakesnow = pytest.importorskip("fakesnow")
import snowflake.connector  # noqa: E402

from claims_platform.contracts import load_contract  # noqa: E402
from claims_platform.warehouse import load  # noqa: E402
from claims_platform.warehouse.migrate import (  # noqa: E402
    HISTORY_TABLE,
    MigrationError,
    discover,
    migrate,
    plan,
)

# duplicated on purpose: this test is the tripwire between the lakehouse export and the DDL
PUBLISHED = {
    "members": "eligibility",
    "providers": "providers",
    "medical_claims_current": "medical_claims",
    "pharmacy_claims": "pharmacy_claims",
}
SNOWFLAKE_TYPE = {"string": "TEXT", "int": "NUMBER", "decimal": "NUMBER", "date": "DATE", "bool": "BOOLEAN"}


@pytest.fixture
def sf():
    with fakesnow.patch():
        con = snowflake.connector.connect()
        yield con
        con.close()


def write(directory: Path, name: str, sql: str) -> None:
    (directory / name).write_text(sql)


def test_repository_migrations_are_well_formed():
    ms = discover()
    assert [m.version for m in ms] == list(range(1, len(ms) + 1)) and len(ms) >= 4
    for m in ms:
        assert m.statements(), m.path.name


def test_gaps_and_bad_names_are_rejected(tmp_path):
    write(tmp_path, "V001__a.sql", "SELECT 1;")
    write(tmp_path, "V003__c.sql", "SELECT 1;")
    with pytest.raises(MigrationError, match="no gaps"):
        discover(tmp_path)
    (tmp_path / "V003__c.sql").unlink()
    write(tmp_path, "v2-bad.sql", "SELECT 1;")
    with pytest.raises(MigrationError, match="expected V"):
        discover(tmp_path)


def test_editing_an_applied_migration_is_refused(tmp_path):
    write(tmp_path, "V001__a.sql", "SELECT 1;")
    ms = discover(tmp_path)
    assert plan(ms, {}) == ms
    with pytest.raises(MigrationError, match="edited after it was applied"):
        plan(ms, {1: "not-the-checksum"})
    with pytest.raises(MigrationError, match="script is missing"):
        plan(ms, {1: ms[0].checksum, 2: "x"})


def test_migrate_applies_once_in_order_and_records_history(sf, tmp_path):
    write(tmp_path, "V001__schema.sql", "CREATE DATABASE IF NOT EXISTS D;\nCREATE SCHEMA IF NOT EXISTS D.S;")
    write(
        tmp_path,
        "V002__table.sql",
        "-- comment\nCREATE TABLE IF NOT EXISTS D.S.T (ID INTEGER);\nINSERT INTO D.S.T VALUES (1);",
    )
    assert migrate(sf, tmp_path, dry_run=True) == {"dry_run": True, "pending": ["V001__schema.sql", "V002__table.sql"]}
    assert migrate(sf, tmp_path) == {"applied": ["V001__schema.sql", "V002__table.sql"]}
    assert migrate(sf, tmp_path) == {"applied": []}  # re-running a deploy is a no-op
    cur = sf.cursor()
    assert cur.execute("SELECT COUNT(*) FROM D.S.T").fetchone()[0] == 1  # V002 ran exactly once
    assert cur.execute(f"SELECT VERSION FROM {HISTORY_TABLE} ORDER BY VERSION").fetchall() == [(1,), (2,)]


def test_a_failing_migration_is_not_recorded_and_is_retried(sf, tmp_path):
    write(tmp_path, "V001__ok.sql", "CREATE DATABASE IF NOT EXISTS D;")
    write(tmp_path, "V002__broken.sql", "CREATE TABLE D.NOPE.T (ID INTEGER);")
    with pytest.raises(MigrationError, match="V002__broken.sql failed"):
        migrate(sf, tmp_path)
    assert sf.cursor().execute(f"SELECT VERSION FROM {HISTORY_TABLE}").fetchall() == [(1,)]
    write(tmp_path, "V002__broken.sql", "CREATE SCHEMA IF NOT EXISTS D.NOPE;\nCREATE TABLE D.NOPE.T (ID INTEGER);")
    assert migrate(sf, tmp_path) == {"applied": ["V002__broken.sql"]}


def _apply_raw_ddl(sf, tmp_path):
    """V003 on fakesnow. USE ROLE is account-level (not emulated); the schema comes from V001."""
    v003 = next(m for m in discover() if m.version == 3)
    cur = sf.cursor()
    cur.execute("CREATE DATABASE RAW")
    cur.execute("CREATE SCHEMA RAW.CLAIMS")
    for s in v003.statements():
        if not s.upper().startswith("USE ROLE"):
            cur.execute(s)
    return cur


def test_raw_tables_match_the_data_contracts(sf, tmp_path):
    cur = _apply_raw_ddl(sf, tmp_path)
    for table, source in PUBLISHED.items():
        rows = cur.execute(
            "SELECT column_name, data_type FROM RAW.information_schema.columns "
            "WHERE table_schema = 'CLAIMS' AND table_name = %s",
            (table.upper(),),
        ).fetchall()
        actual = {name: typ for name, typ in rows}
        expected = {c["name"].upper(): SNOWFLAKE_TYPE[c["type"]] for c in load_contract(source)["columns"]}
        if source != "providers":
            expected["_BATCH_DATE"] = "DATE"
        assert actual == expected, table


def test_every_pii_column_in_the_contracts_is_tagged_for_masking():
    governance = next(m for m in discover() if m.version == 4).sql
    tagged = set(
        re.findall(r"ALTER TABLE RAW\.CLAIMS\.(\w+) MODIFY COLUMN (\w+) SET TAG GOVERNANCE\.POLICIES\.PII", governance)
    )
    pii = {
        (table.upper(), c["name"].upper())
        for table, source in PUBLISHED.items()
        for c in load_contract(source)["columns"]
        if c.get("pii")
    }
    assert pii and tagged == pii


def test_no_migration_uses_swap_which_would_drop_policies():
    for m in discover():
        assert "SWAP WITH" not in m.sql.upper(), m.path.name


def test_loader_clears_the_stage_and_audits_inside_the_transaction(tmp_path):
    files = [tmp_path / "part-0.parquet"]
    stmts = [s for s, _ in load.snowflake_statements("members", files, "2026-01-05")]
    assert stmts[0] == "REMOVE @RAW.CLAIMS.EXCHANGE/members/"
    assert stmts[1].startswith("PUT 'file://")
    begin, commit = stmts.index("BEGIN"), stmts.index("COMMIT")
    body = stmts[begin + 1 : commit]
    assert body[0] == "DELETE FROM RAW.CLAIMS.MEMBERS"
    assert body[1].startswith("COPY INTO RAW.CLAIMS.MEMBERS") and "ON_ERROR = ABORT_STATEMENT" in body[1]
    assert body[2].startswith("INSERT INTO RAW.CLAIMS._LOAD_AUDIT")


class _Cursor:
    def __init__(self, fail_on: str):
        self.fail_on, self.executed = fail_on, []

    def execute(self, sql, params=None):
        self.executed.append(sql)
        if sql.startswith(self.fail_on):
            raise RuntimeError("simulated COPY failure")
        return self

    def fetchone(self):
        return (0,)


class _Con:
    def __init__(self, cur):
        self.cur, self.closed = cur, False

    def cursor(self):
        return self.cur

    def close(self):
        self.closed = True


def test_a_failed_copy_rolls_back_and_closes_the_connection(tmp_path, monkeypatch):
    monkeypatch.setenv("CLAIMS_DATA_DIR", str(tmp_path))
    (tmp_path / "exchange" / "members").mkdir(parents=True)
    (tmp_path / "exchange" / "members" / "part-0.parquet").write_bytes(b"x")
    cur = _Cursor(fail_on="COPY INTO")
    con = _Con(cur)
    with pytest.raises(RuntimeError, match="simulated"):
        load._load_snowflake("2026-01-05", ("members",), con=con)
    assert cur.executed[-1] == "ROLLBACK" and not any(s.startswith("COMMIT") for s in cur.executed)
    assert not any(s.startswith("INSERT INTO RAW.CLAIMS._LOAD_AUDIT") for s in cur.executed)
    assert con.closed
