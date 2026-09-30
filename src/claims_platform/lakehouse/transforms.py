"""Pure DataFrame transformations for the medallion layers.

Nothing here reads or writes tables: every function takes and returns
DataFrames, so the logic is unit-tested with plain Spark (no Delta, no
storage) and reused unchanged on Databricks.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

SPARK_TYPES = {"string": "string", "int": "int", "decimal": "decimal(12,2)", "date": "date", "bool": "boolean"}
META = ["_batch_date", "_source_file", "_ingested_at", "_row_hash"]

# ICD-10-CM: letter (U reserved), digit, alphanumeric, optional dot + 1-4 alphanumerics
ICD10_PATTERN = r"^[A-TV-Z][0-9][0-9A-Z](\.[0-9A-Z]{1,4})?$"


def with_ingest_metadata(df: DataFrame, batch_date: str, source_file: str) -> DataFrame:
    """Bronze lineage columns. _row_hash covers every delivered column, so exact
    duplicate rows collide while any real difference (e.g. a new version) doesn't."""
    business = [c for c in df.columns if not c.startswith("_")]
    return (
        df.withColumn("_batch_date", F.lit(batch_date).cast("date"))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_row_hash", F.sha2(F.concat_ws("␟", *[F.coalesce(F.col(c), F.lit("")) for c in business]), 256))
    )


def cast_to_contract(df: DataFrame, contract: dict) -> DataFrame:
    """String columns -> contracted types. Unparseable values become null (try_cast);
    the expectations below turn a null in a required column into a quarantine reason.
    Columns that are in the contract but absent from older deliveries are added as nulls."""
    out = df
    for col in contract["columns"]:
        name, typ = col["name"], SPARK_TYPES[col["type"]]
        if name not in df.columns:
            out = out.withColumn(name, F.lit(None).cast(typ))
        elif typ == "boolean":
            out = out.withColumn(name, F.lower(F.trim(F.col(name))) == F.lit("true"))
        else:
            src = F.nullif(F.trim(F.col(name)), F.lit(""))
            out = out.withColumn(name, src.try_cast(typ))
    keep = [c["name"] for c in contract["columns"]] + [c for c in META if c in df.columns]
    return out.select(*keep)


def dedupe_exact(df: DataFrame) -> DataFrame:
    """Drop exact duplicate rows within a delivery (same _row_hash)."""
    return df.dropDuplicates(["_row_hash"])


@dataclass(frozen=True)
class Expectation:
    name: str
    condition: Column  # True means the row passes


def required_not_null(contract: dict) -> list[Expectation]:
    return [
        Expectation(f"{c['name']}_present", F.col(c["name"]).isNotNull())
        for c in contract["columns"]
        if c.get("required")
    ]


def allowed_values(contract: dict) -> list[Expectation]:
    return [
        Expectation(f"{c['name']}_allowed", F.col(c["name"]).isNull() | F.col(c["name"]).isin(c["allowed"]))
        for c in contract["columns"]
        if c.get("allowed")
    ]


def medical_expectations(contract: dict) -> list[Expectation]:
    return (
        required_not_null(contract)
        + allowed_values(contract)
        + [
            Expectation("primary_dx_is_icd10", F.col("primary_dx").rlike(ICD10_PATTERN)),
            Expectation("paid_claim_not_negative", ~((F.col("claim_status") == "PAID") & (F.col("paid_amount") < 0))),
            Expectation("service_dates_ordered", F.col("service_to") >= F.col("service_from")),
            Expectation("positive_units", F.col("units") > 0),
        ]
    )


def pharmacy_expectations(contract: dict) -> list[Expectation]:
    return required_not_null(contract) + [
        Expectation("ndc_is_11_digits", F.col("ndc").rlike(r"^[0-9]{11}$")),
        Expectation("paid_not_negative", F.col("paid_amount") >= 0),
        Expectation("days_supply_plausible", F.col("days_supply").between(1, 365)),
    ]


def eligibility_expectations(contract: dict) -> list[Expectation]:
    return (
        required_not_null(contract)
        + allowed_values(contract)
        + [
            Expectation(
                "coverage_dates_ordered",
                F.col("coverage_end").isNull() | (F.col("coverage_end") >= F.col("coverage_start")),
            )
        ]
    )


def npi_is_valid(col: Column) -> Column:
    """NPI check digit (Luhn over "80840" + first 9 digits) as a native Spark
    expression. A Python UDF would ship every row to a Python worker; this stays
    in the JVM and is optimised like any other column expression. The "80840"
    prefix always contributes 24 to the Luhn sum; in the 9 body digits the
    even positions (0-based) are doubled."""
    s = F.coalesce(col, F.lit(""))
    total = F.lit(24)
    for p in range(9):
        d = F.substring(s, p + 1, 1).cast("int")
        if p % 2 == 0:
            d = F.when(d * 2 > 9, d * 2 - 9).otherwise(d * 2)
        total = total + d
    check = (F.lit(10) - total % 10) % 10
    return F.coalesce(s.rlike(r"^[0-9]{10}$") & (check == F.substring(s, 10, 1).cast("int")), F.lit(False))


def provider_expectations(contract: dict) -> list[Expectation]:
    return required_not_null(contract) + [Expectation("npi_check_digit", npi_is_valid(F.col("npi")))]


def apply_expectations(df: DataFrame, expectations: list[Expectation]) -> tuple[DataFrame, DataFrame]:
    """Split into (valid, quarantined). Quarantined rows carry `_failed_rules`, the
    names of every rule they broke, so the vendor gets a complete defect list."""
    failed = F.filter(
        F.array(*[F.when(~F.coalesce(e.condition, F.lit(False)), F.lit(e.name)) for e in expectations]),
        lambda x: x.isNotNull(),
    )
    tagged = df.withColumn("_failed_rules", failed)
    valid = tagged.filter(F.size("_failed_rules") == 0).drop("_failed_rules")
    quarantined = tagged.filter(F.size("_failed_rules") > 0)
    return valid, quarantined


def flag_unknown_members(claims: DataFrame, members: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Referential check against the member roster: claims for members we don't
    know go to quarantine (they may resolve when a late roster arrives)."""
    known = members.select("member_id").distinct().withColumn("_known", F.lit(True))
    joined = claims.join(known, "member_id", "left")
    valid = joined.filter(F.col("_known")).drop("_known")
    unknown = (
        joined.filter(F.col("_known").isNull())
        .drop("_known")
        .withColumn("_failed_rules", F.array(F.lit("member_known")))
    )
    return valid.select(claims.columns), unknown.select(*claims.columns, "_failed_rules")


def latest_versions(lines: DataFrame) -> DataFrame:
    """Keep only the lines of each claim's highest version (adjustments and
    reversals supersede earlier versions, whatever order they arrived in)."""
    w = Window.partitionBy("claim_id")
    return (
        lines.withColumn("_max_version", F.max("claim_version").over(w))
        .filter(F.col("claim_version") == F.col("_max_version"))
        .drop("_max_version")
    )
