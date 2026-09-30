"""Unit tests for the pure lakehouse transforms: plain Spark, no Delta, no storage."""

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from claims_platform.contracts import load_contract  # noqa: E402
from claims_platform.generator import MEDICAL_COLUMNS, is_valid_npi  # noqa: E402
from claims_platform.lakehouse import transforms as T  # noqa: E402

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s


def claim_row(**over):
    base = {
        "claim_id": "C1", "claim_version": "1", "claim_status": "PAID", "claim_type": "PROFESSIONAL",
        "member_id": "M1", "billing_npi": "1234567893", "service_from": "2026-01-02", "service_to": "2026-01-02",
        "admit_date": "", "discharge_date": "", "place_of_service": "11", "primary_dx": "I10",
        "secondary_dx": "", "line_number": "1", "procedure_code": "99213", "units": "1",
        "billed_amount": "200.00", "allowed_amount": "90.00", "paid_amount": "80.00", "paid_date": "2026-01-20",
    }  # fmt: skip
    base.update(over)
    return base


def frame(spark, rows, columns=MEDICAL_COLUMNS):
    schema = ", ".join(f"{c} string" for c in columns)  # a DDL string; a list would be taken as column names
    df = spark.createDataFrame([[r.get(c, "") for c in columns] for r in rows], schema=schema)
    return T.with_ingest_metadata(df, "2026-01-21", "f.csv")


def test_exact_duplicates_collapse_but_versions_do_not(spark):
    rows = [claim_row(), claim_row(), claim_row(claim_version="2", paid_amount="70.00")]
    assert T.dedupe_exact(frame(spark, rows)).count() == 2


def test_cast_to_contract_types_and_nulls_unparseable(spark):
    contract = load_contract("medical_claims")
    df = T.cast_to_contract(frame(spark, [claim_row(units="two", paid_amount="80.5")]), contract)
    r = df.collect()[0]
    assert r.units is None  # unparseable -> null -> fails units_present
    assert float(r.paid_amount) == 80.5 and str(r.service_from) == "2026-01-02"
    assert "rendering_npi" in df.columns  # older deliveries gain the newer optional column as null


def test_expectations_quarantine_with_every_reason(spark):
    contract = load_contract("medical_claims")
    rows = [
        claim_row(claim_id="GOOD"),
        claim_row(claim_id="BADDX", primary_dx="XYZ"),
        claim_row(claim_id="NEG", paid_amount="-5.00"),
        claim_row(claim_id="DATES", service_to="2000-01-01"),
        claim_row(claim_id="", primary_dx="XYZ"),  # two defects at once
        claim_row(claim_id="DENIEDZERO", claim_status="DENIED", paid_amount="0"),
    ]
    typed = T.cast_to_contract(frame(spark, rows), contract)
    valid, bad = T.apply_expectations(typed, T.medical_expectations(contract))
    assert sorted(r.claim_id for r in valid.collect()) == ["DENIEDZERO", "GOOD"]
    reasons = {r.claim_id: sorted(r._failed_rules) for r in bad.collect()}
    assert reasons["BADDX"] == ["primary_dx_is_icd10"]
    assert reasons["NEG"] == ["paid_claim_not_negative"]
    assert reasons["DATES"] == ["service_dates_ordered"]
    assert reasons[None] == ["claim_id_present", "primary_dx_is_icd10"]


@pytest.mark.parametrize("code,ok", [("I10", True), ("S72.001A", True), ("J44.9", True), ("XYZ", False),
                                     ("U07.1", False), ("I1", False), ("E11.9999X", False)])  # fmt: skip
def test_icd10_pattern(spark, code, ok):
    got = spark.createDataFrame([(code,)], "c string").select(F.col("c").rlike(T.ICD10_PATTERN)).first()[0]
    assert got is ok


def test_unknown_members_are_quarantined(spark):
    contract = load_contract("medical_claims")
    claims = T.cast_to_contract(frame(spark, [claim_row(member_id="M1"), claim_row(claim_id="C2", member_id="M9")]),
                                contract)  # fmt: skip
    members = spark.createDataFrame([("M1",)], "member_id string")
    valid, unknown = T.flag_unknown_members(claims, members)
    assert [r.claim_id for r in valid.collect()] == ["C1"]
    assert [(r.claim_id, r._failed_rules) for r in unknown.collect()] == [("C2", ["member_known"])]


def test_latest_version_wins_regardless_of_arrival_order(spark):
    contract = load_contract("medical_claims")
    rows = [
        claim_row(claim_version="2", claim_status="REVERSED"),  # the reversal arrived first
        claim_row(claim_version="1"),
        claim_row(claim_id="C2", claim_version="1", line_number="1"),
        claim_row(claim_id="C2", claim_version="1", line_number="2", procedure_code="80053"),
    ]
    latest = T.latest_versions(T.cast_to_contract(frame(spark, rows), contract)).collect()
    by_claim = {}
    for r in latest:
        by_claim.setdefault(r.claim_id, []).append((r.claim_version, r.claim_status))
    assert by_claim == {"C1": [(2, "REVERSED")], "C2": [(1, "PAID"), (1, "PAID")]}


def test_npi_check_digit():
    assert is_valid_npi("1234567893")  # the CMS documentation example
    assert not is_valid_npi("1234567890")
    assert not is_valid_npi("12345")


def test_spark_npi_expression_matches_python_reference(spark):
    import random

    from claims_platform.generator import npi_check_digit

    rng = random.Random(3)
    samples = []
    for _ in range(500):
        body = "".join(str(rng.randint(0, 9)) for _ in range(9))
        samples.append(body + str(npi_check_digit(body)))  # valid
        samples.append(body + str((npi_check_digit(body) + rng.randint(1, 9)) % 10))  # wrong check digit
    samples += ["12345", "", None, "12345678X3", "12345678931"]
    df = spark.createDataFrame([(s,) for s in samples], "npi string")
    got = {r.npi: r.ok for r in df.select("npi", T.npi_is_valid(F.col("npi")).alias("ok")).collect()}
    for s in samples:
        assert got[s] == (s is not None and is_valid_npi(s)), s
