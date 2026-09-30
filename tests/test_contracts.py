"""Delivery-level contract checks and the synthetic feed that exercises them."""

import json
from datetime import date

import pytest

from claims_platform.contracts import validate_delivery
from claims_platform.generator import MEDICAL_COLUMNS, _write_delivery, generate, is_valid_npi


@pytest.fixture(scope="module")
def feed(tmp_path_factory):
    root = tmp_path_factory.mktemp("feed")
    counts = generate(root, date(2026, 1, 1), days=3, members=300, seed=11, drift_on_day=2)
    return root, counts


def folder(root, source, day="2026-01-01"):
    return root / "landing" / source / f"dt={day}"


@pytest.mark.parametrize("source", ["eligibility", "providers", "medical_claims", "pharmacy_claims"])
def test_generated_deliveries_are_accepted(feed, source):
    root, _ = feed
    report = validate_delivery(folder(root, source), source)
    assert report.status == "ACCEPTED", report.violations
    assert report.rows > 0


def test_additive_drift_is_accepted_and_recorded(feed):
    root, _ = feed
    report = validate_delivery(folder(root, "medical_claims", "2026-01-03"), "medical_claims")
    assert report.status == "ACCEPTED"
    assert any("rendering_npi" in d for d in report.drift)


def test_generated_providers_have_valid_npis(feed):
    root, _ = feed
    lines = (folder(root, "providers") / "providers_20260101.csv").read_text().splitlines()[1:]
    assert lines and all(is_valid_npi(line.split(",")[0]) for line in lines)


def _medical(tmp_path, rows, columns=MEDICAL_COLUMNS):
    _write_delivery(tmp_path, "medical_claims", date(2026, 1, 1), columns, rows, 1)
    return folder(tmp_path, "medical_claims")


ROW = {
    "claim_id": "C1", "claim_version": 1, "claim_status": "PAID", "claim_type": "PROFESSIONAL",
    "member_id": "M1", "billing_npi": "1234567893", "service_from": "2026-01-01", "service_to": "2026-01-01",
    "admit_date": "", "discharge_date": "", "place_of_service": "11", "primary_dx": "I10", "secondary_dx": "",
    "line_number": 1, "procedure_code": "99213", "units": 1, "billed_amount": 1, "allowed_amount": 1,
    "paid_amount": 1, "paid_date": "2026-01-02",
}  # fmt: skip


def test_missing_manifest_means_incomplete(tmp_path):
    d = _medical(tmp_path, [ROW])
    (d / "_manifest.json").unlink()
    assert "manifest missing" in validate_delivery(d, "medical_claims").violations[0]


def test_file_changed_after_delivery_is_rejected(tmp_path):
    d = _medical(tmp_path, [ROW])
    csv = d / "medical_claims_20260101.csv"
    csv.write_text(csv.read_text().replace("99213", "99214"))
    assert "sha256 mismatch" in validate_delivery(d, "medical_claims").violations[0]


def test_truncated_delivery_is_rejected(tmp_path):
    d = _medical(tmp_path, [ROW, {**ROW, "line_number": 2}])
    m = json.loads((d / "_manifest.json").read_text())
    m["rows"] = 3
    (d / "_manifest.json").write_text(json.dumps(m))
    assert any("row count" in v for v in validate_delivery(d, "medical_claims").violations)


def test_dropped_required_column_is_rejected(tmp_path):
    d = _medical(tmp_path, [ROW], [c for c in MEDICAL_COLUMNS if c != "paid_amount"])
    report = validate_delivery(d, "medical_claims")
    assert report.status == "REJECTED" and "paid_amount" in report.violations[0]


def test_retyped_column_is_rejected_but_a_few_bad_values_are_not(tmp_path):
    good = [{**ROW, "line_number": i} for i in range(1, 101)]
    one_bad = good[:99] + [{**good[99], "units": "two"}]
    assert validate_delivery(_medical(tmp_path / "a", one_bad), "medical_claims").status == "ACCEPTED"
    all_bad = [{**r, "units": "two"} for r in good]
    report = validate_delivery(_medical(tmp_path / "b", all_bad), "medical_claims")
    assert report.status == "REJECTED" and "retyped" in report.violations[0]
