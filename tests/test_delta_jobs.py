"""Integration tests for the Delta I/O layer: idempotency, version ordering,
schema evolution and rollback. Need the Delta jars (Maven, or DELTA_JARS_DIR)."""

import json
import os
from datetime import date

import pytest

pytest.importorskip("delta")

from claims_platform.generator import (  # noqa: E402
    ELIGIBILITY_COLUMNS,
    MEDICAL_COLUMNS,
    PHARMACY_COLUMNS,
    PROVIDER_COLUMNS,
    _write_delivery,
)
from claims_platform.lakehouse import jobs  # noqa: E402
from claims_platform.lakehouse.spark import delta_table, get_spark, read_table  # noqa: E402

pytestmark = [pytest.mark.spark, pytest.mark.delta]

NPI = "1234567893"


@pytest.fixture(scope="module")
def spark():
    return get_spark("delta-tests")


@pytest.fixture
def root(tmp_path, monkeypatch):
    monkeypatch.setenv("CLAIMS_DATA_DIR", str(tmp_path))
    return tmp_path


def line(claim_id, version, line_number=1, status="PAID", paid="80.00", **over):
    r = {
        "claim_id": claim_id, "claim_version": version, "claim_status": status, "claim_type": "PROFESSIONAL",
        "member_id": "M1", "billing_npi": NPI, "service_from": "2025-12-01", "service_to": "2025-12-01",
        "admit_date": "", "discharge_date": "", "place_of_service": "11", "primary_dx": "I10", "secondary_dx": "",
        "line_number": line_number, "procedure_code": "99213", "units": 1, "billed_amount": "200.00",
        "allowed_amount": "90.00", "paid_amount": paid, "paid_date": "2026-01-01",
    }  # fmt: skip
    r.update(over)
    return r


def deliver(root, day: str, claims: list[dict], drift: bool = False):
    d = date.fromisoformat(day)
    member = {
        "member_id": "M1", "first_name": "A", "last_name": "B", "birth_date": "1970-01-01", "gender": "F",
        "zip3": "631", "line_of_business": "COMMERCIAL", "coverage_start": "2020-01-01", "coverage_end": "",
    }  # fmt: skip
    provider = {
        "npi": NPI,
        "provider_name": "Dr. X",
        "specialty": "Family Medicine",
        "state": "MO",
        "is_facility": "false",
    }
    _write_delivery(root, "eligibility", d, ELIGIBILITY_COLUMNS, [member], 1)
    _write_delivery(root, "providers", d, PROVIDER_COLUMNS, [provider], 1)
    cols = MEDICAL_COLUMNS + (["rendering_npi"] if drift else [])
    _write_delivery(root, "medical_claims", d, cols, claims, 2 if drift else 1)
    _write_delivery(root, "pharmacy_claims", d, PHARMACY_COLUMNS, [], 1)


def run_day(spark, day):
    for s in ("eligibility", "providers", "medical_claims", "pharmacy_claims"):
        jobs.ingest_bronze(spark, s, day)
    for step in (jobs.build_members, jobs.build_providers, jobs.build_medical, jobs.build_pharmacy):
        step(spark, day)


def current(spark):
    return sorted(
        (r.claim_id, r.claim_version, r.line_number, r.claim_status)
        for r in read_table(spark, "silver", "medical_claims_current").collect()
    )


def test_replaying_a_day_is_a_no_op(spark, root):
    deliver(root, "2026-01-01", [line("C1", 1), line("C1", 1, 2), line("C2", 1)])
    run_day(spark, "2026-01-01")
    before = (current(spark), read_table(spark, "bronze", "medical_claims").count())
    run_day(spark, "2026-01-01")  # an Airflow retry or a backfill
    after = (current(spark), read_table(spark, "bronze", "medical_claims").count())
    assert before == after and before[1] == 3


def test_reversal_arriving_before_the_original_still_wins(spark, root):
    deliver(root, "2026-01-01", [line("C1", 2, status="REVERSED")])
    run_day(spark, "2026-01-01")
    deliver(root, "2026-01-02", [line("C1", 1)])  # the late original
    run_day(spark, "2026-01-02")
    assert current(spark) == [("C1", 2, 1, "REVERSED")]
    history = read_table(spark, "silver", "medical_claim_lines").count()
    assert history == 2  # both versions are kept in the history table


def test_new_version_with_fewer_lines_leaves_no_stale_lines(spark, root):
    deliver(root, "2026-01-01", [line("C1", 1, 1), line("C1", 1, 2), line("C1", 1, 3)])
    run_day(spark, "2026-01-01")
    deliver(root, "2026-01-02", [line("C1", 2, 1, paid="50.00")])
    run_day(spark, "2026-01-02")
    assert current(spark) == [("C1", 2, 1, "PAID")]


def test_additive_drift_evolves_bronze_and_keeps_old_rows(spark, root):
    deliver(root, "2026-01-01", [line("C1", 1)])
    run_day(spark, "2026-01-01")
    deliver(root, "2026-01-02", [line("C2", 1, rendering_npi=NPI)], drift=True)
    run_day(spark, "2026-01-02")
    bronze = {r.claim_id: r.rendering_npi for r in read_table(spark, "bronze", "medical_claims").collect()}
    assert bronze == {"C1": None, "C2": NPI}
    silver = {r.claim_id: r.rendering_npi for r in read_table(spark, "silver", "medical_claims_current").collect()}
    assert silver == {"C1": None, "C2": NPI}


def test_bad_rows_are_quarantined_with_reasons_and_metrics_recorded(spark, root):
    deliver(root, "2026-01-01", [line("C1", 1), line("C2", 1, primary_dx="XYZ"), line("C3", 1, member_id="M9")])
    run_day(spark, "2026-01-01")
    q = {
        r.claim_id: sorted(r._failed_rules) for r in read_table(spark, "silver", "quarantine_medical_claims").collect()
    }
    assert q == {"C2": ["primary_dx_is_icd10"], "C3": ["member_known"]}
    metrics = json.loads((root / "reports" / "runs" / "2026-01-01" / "silver_medical_claims.json").read_text())
    assert metrics["quarantined"] == 2 and metrics["merged"] == 1


def test_restore_rolls_back_a_bad_load(spark, root):
    deliver(root, "2026-01-01", [line("C1", 1)])
    run_day(spark, "2026-01-01")
    good = delta_table(spark, "silver", "medical_claims_current").history(1).collect()[0].version
    deliver(root, "2026-01-02", [line("C1", 2, paid="99999.00")])  # a vendor sends garbage
    run_day(spark, "2026-01-02")
    jobs.restore(spark, "silver", "medical_claims_current", good)
    assert current(spark) == [("C1", 1, 1, "PAID")]


@pytest.mark.skipif(os.environ.get("CLAIMS_CATALOG") is not None, reason="path-based tables only")
def test_optimize_runs(spark, root):
    deliver(root, "2026-01-01", [line("C1", 1)])
    run_day(spark, "2026-01-01")
    assert jobs.optimize(spark) == {"optimized": True}
