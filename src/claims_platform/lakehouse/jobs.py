"""Lakehouse jobs: bronze ingest, silver builds, publish. The Delta I/O lives here;
the logic lives in transforms.py. Every job is idempotent per batch date, so an
Airflow retry or a backfill of the same day converges to the same tables.

Tables (layer/name):
  bronze/<source>                       raw rows + lineage, partitioned by _batch_date
  silver/members, silver/providers      current reference state (full snapshots)
  silver/medical_claim_lines            every version of every claim line (history)
  silver/medical_claims_current         latest version per claim, order-independent
  silver/pharmacy_claims                one row per fill
  silver/quarantine_<source>            rejected rows with _failed_rules, per batch
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import paths
from ..contracts import load_contract
from . import transforms as T
from .spark import delta_table, read_table, table_exists, writer

METRICS: dict = {}


def _record(step: str, day: str, **counts) -> dict:
    out = {"step": step, "batch_date": day, "at": datetime.now(timezone.utc).isoformat(), **counts}
    folder = paths().reports / "runs" / day
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{step}.json").write_text(json.dumps(out, indent=2))
    return out


# ---------------- bronze ----------------


def ingest_bronze(spark: SparkSession, source: str, day: str) -> dict:
    folder = paths().delivery(source, day)
    manifest = json.loads((folder / "_manifest.json").read_text())
    raw = spark.read.option("header", True).option("inferSchema", False).csv(str(folder / manifest["file"]))
    df = T.with_ingest_metadata(raw, day, manifest["file"])
    if table_exists(spark, "bronze", source):
        # replaceWhere: re-running a day swaps that day's partition instead of appending twice.
        # mergeSchema: additive drift (new vendor columns) evolves the table instead of failing.
        writer(df, "bronze", source, "overwrite", replaceWhere=f"_batch_date = '{day}'", mergeSchema="true")()
    else:
        writer(df, "bronze", source, "overwrite", partition_by=["_batch_date"])()
    return _record(f"bronze_{source}", day, rows=manifest["rows"], columns=len(raw.columns))


def _bronze_batch(spark: SparkSession, source: str, day: str) -> DataFrame:
    return read_table(spark, "bronze", source).filter(F.col("_batch_date") == F.lit(day).cast("date"))


def _write_quarantine(spark: SparkSession, source: str, day: str, bad: DataFrame) -> int:
    n = bad.count()
    df = bad.withColumn("_batch_date", F.lit(day).cast("date"))
    name = f"quarantine_{source}"
    if table_exists(spark, "silver", name):
        writer(df, "silver", name, "overwrite", replaceWhere=f"_batch_date = '{day}'", mergeSchema="true")()
    else:
        writer(df, "silver", name, "overwrite")()
    return n


def _overwrite(spark: SparkSession, df: DataFrame, layer: str, name: str) -> None:
    writer(df, layer, name, "overwrite", overwriteSchema="true")()


# ---------------- silver: reference data ----------------


def build_members(spark: SparkSession, day: str) -> dict:
    contract = load_contract("eligibility")
    batch = T.dedupe_exact(_bronze_batch(spark, "eligibility", day))
    typed = T.cast_to_contract(batch, contract)
    valid, bad = T.apply_expectations(typed, T.eligibility_expectations(contract))
    _overwrite(spark, valid.drop("_row_hash"), "silver", "members")  # full snapshot: current roster
    return _record(
        "silver_members", day, rows=valid.count(), quarantined=_write_quarantine(spark, "eligibility", day, bad)
    )


def build_providers(spark: SparkSession, day: str) -> dict:
    contract = load_contract("providers")
    typed = T.cast_to_contract(T.dedupe_exact(_bronze_batch(spark, "providers", day)), contract)
    valid, bad = T.apply_expectations(typed, T.provider_expectations(contract))
    _overwrite(spark, valid.drop("_row_hash"), "silver", "providers")
    return _record(
        "silver_providers", day, rows=valid.count(), quarantined=_write_quarantine(spark, "providers", day, bad)
    )


# ---------------- silver: claims ----------------


def build_medical(spark: SparkSession, day: str) -> dict:
    contract = load_contract("medical_claims")
    batch = _bronze_batch(spark, "medical_claims", day)
    delivered = batch.count()
    deduped = T.dedupe_exact(batch)
    typed = T.cast_to_contract(deduped, contract)
    valid, bad = T.apply_expectations(typed, T.medical_expectations(contract))
    valid, unknown = T.flag_unknown_members(valid, read_table(spark, "silver", "members"))
    quarantined = _write_quarantine(spark, "medical_claims", day, bad.unionByName(unknown))
    valid = valid.cache()

    # 1) history: every version of every line, keyed so a replayed day is a no-op
    key = "t.claim_id = s.claim_id AND t.claim_version = s.claim_version AND t.line_number = s.line_number"
    if table_exists(spark, "silver", "medical_claim_lines"):
        delta_table(spark, "silver", "medical_claim_lines").alias("t").merge(
            valid.alias("s"), key
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        _overwrite(spark, valid, "silver", "medical_claim_lines")

    # 2) current: latest version per claim, correct whatever order versions arrive in
    latest = T.latest_versions(valid)
    if table_exists(spark, "silver", "medical_claims_current"):
        current = read_table(spark, "silver", "medical_claims_current")
        have = current.groupBy("claim_id").agg(F.max("claim_version").alias("_have"))
        # an older version arriving late must not overwrite a newer one already applied
        winners = (
            latest.join(have, "claim_id", "left")
            .filter(F.col("_have").isNull() | (F.col("claim_version") >= F.col("_have")))
            .drop("_have")
        )
        winner_versions = winners.groupBy("claim_id").agg(F.max("claim_version").alias("v"))
        dt = delta_table(spark, "silver", "medical_claims_current")
        # drop lines of superseded versions (a new version may have fewer lines)
        dt.alias("t").merge(
            winner_versions.alias("s"), "t.claim_id = s.claim_id AND t.claim_version < s.v"
        ).whenMatchedDelete().execute()
        dt.alias("t").merge(
            winners.alias("s"), "t.claim_id = s.claim_id AND t.line_number = s.line_number"
        ).whenMatchedUpdateAll(condition="s.claim_version >= t.claim_version").whenNotMatchedInsertAll().execute()
    else:
        _overwrite(spark, latest, "silver", "medical_claims_current")

    out = _record(
        "silver_medical_claims",
        day,
        delivered=delivered,
        duplicates_dropped=delivered - deduped.count(),
        quarantined=quarantined,
        merged=valid.count(),
    )
    valid.unpersist()
    return out


def build_pharmacy(spark: SparkSession, day: str) -> dict:
    contract = load_contract("pharmacy_claims")
    batch = _bronze_batch(spark, "pharmacy_claims", day)
    delivered = batch.count()
    typed = T.cast_to_contract(T.dedupe_exact(batch), contract)
    valid, bad = T.apply_expectations(typed, T.pharmacy_expectations(contract))
    valid, unknown = T.flag_unknown_members(valid, read_table(spark, "silver", "members"))
    quarantined = _write_quarantine(spark, "pharmacy_claims", day, bad.unionByName(unknown))
    if table_exists(spark, "silver", "pharmacy_claims"):
        delta_table(spark, "silver", "pharmacy_claims").alias("t").merge(
            valid.alias("s"), "t.rx_claim_id = s.rx_claim_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        _overwrite(spark, valid, "silver", "pharmacy_claims")
    return _record("silver_pharmacy_claims", day, delivered=delivered, quarantined=quarantined, merged=valid.count())


# ---------------- publish ----------------

PUBLISHED = {
    "members": [
        "member_id",
        "first_name",
        "last_name",
        "birth_date",
        "gender",
        "zip3",
        "line_of_business",
        "coverage_start",
        "coverage_end",
        "_batch_date",  # roster date: dbt's member snapshot uses it as the SCD2 effective date
    ],  # fmt: skip
    "providers": ["npi", "provider_name", "specialty", "state", "is_facility"],
    "medical_claims_current": [
        "claim_id",
        "claim_version",
        "claim_status",
        "claim_type",
        "member_id",
        "billing_npi",
        "rendering_npi",
        "service_from",
        "service_to",
        "admit_date",
        "discharge_date",
        "place_of_service",
        "primary_dx",
        "secondary_dx",
        "line_number",
        "procedure_code",
        "units",
        "billed_amount",
        "allowed_amount",
        "paid_amount",
        "paid_date",
        "_batch_date",
    ],  # fmt: skip
    "pharmacy_claims": [
        "rx_claim_id",
        "member_id",
        "ndc",
        "drug_name",
        "fill_date",
        "days_supply",
        "quantity",
        "paid_amount",
        "prescriber_npi",
        "_batch_date",
    ],  # fmt: skip
}


def export_for_warehouse(spark: SparkSession, day: str) -> dict:
    """Write the conformed silver tables as Parquet to the exchange area (a Snowflake
    external stage in production). The warehouse loader picks them up from there."""
    counts = {}
    for name, cols in PUBLISHED.items():
        df = read_table(spark, "silver", name).select(*cols)
        out = paths().exchange / name
        df.coalesce(1).write.mode("overwrite").parquet(str(out))
        counts[name] = df.count()
    return _record("export", day, **counts)


# ---------------- maintenance ----------------


def optimize(spark: SparkSession) -> dict:
    """Compact small files and co-locate each member's claims (most queries filter by member)."""
    for name, zcol in (("medical_claim_lines", "member_id"), ("medical_claims_current", "member_id"),
                       ("pharmacy_claims", "member_id")):  # fmt: skip
        if table_exists(spark, "silver", name):
            delta_table(spark, "silver", name).optimize().executeZOrderBy(zcol)
    return {"optimized": True}


def restore(spark: SparkSession, layer: str, name: str, version: int) -> dict:
    """Roll a table back to an earlier Delta version (bad load recovery)."""
    delta_table(spark, layer, name).restoreToVersion(version)
    return {"restored": f"{layer}/{name}", "version": version}
