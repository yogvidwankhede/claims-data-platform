"""Spark session and table addressing, identical locally and on Databricks.

Locally, tables are path-based Delta tables under <data>/lakehouse/<layer>/<name>.
On Databricks, set CLAIMS_CATALOG (a Unity Catalog catalog) and the same code
addresses <catalog>.<layer>.<name> instead. Nothing else changes.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from ..config import paths


def get_spark(app: str = "claims-platform", delta: bool = True) -> SparkSession:
    active = SparkSession.getActiveSession()
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        # Databricks owns the session and its cluster config (Delta is built in):
        # attach to it, never set a master or replace it
        return active or SparkSession.builder.getOrCreate()
    if active is not None:
        has_delta = "DeltaSparkSessionExtension" in active.conf.get("spark.sql.extensions", "")
        if has_delta or not delta:
            return active
        active.stop()  # a plain session can't do Delta; replace it
    builder = (
        SparkSession.builder.appName(app)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")  # try_cast semantics for dirty vendor data
    )
    if delta:
        from delta import configure_spark_with_delta_pip

        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # Delta rebuilds table state from its log with 50 tasks by default, sized for
            # clusters; on a laptop or CI runner that overhead dominates small tables.
            .config("spark.databricks.delta.snapshotPartitions", os.environ.get("DELTA_SNAPSHOT_PARTITIONS", "2"))
        )
        jars_dir = os.environ.get("DELTA_JARS_DIR")
        if jars_dir:  # offline: pre-downloaded, checksum-verified jars (no Maven access)
            jars = ",".join(str(p) for p in sorted(Path(jars_dir).glob("*.jar")))
            builder = builder.config("spark.jars", jars)
        else:
            builder = configure_spark_with_delta_pip(builder)  # resolves the Delta jars from Maven
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def catalog() -> str | None:
    return os.environ.get("CLAIMS_CATALOG") or None


def table_id(layer: str, name: str) -> str:
    """Unity Catalog name on Databricks, a Delta path locally."""
    if catalog():
        return f"{catalog()}.{layer}.{name}"
    return str(paths().table(layer, name))


def read_table(spark: SparkSession, layer: str, name: str) -> DataFrame:
    tid = table_id(layer, name)
    return spark.read.table(tid) if catalog() else spark.read.format("delta").load(tid)


def table_exists(spark: SparkSession, layer: str, name: str) -> bool:
    from delta.tables import DeltaTable

    tid = table_id(layer, name)
    return spark.catalog.tableExists(tid) if catalog() else DeltaTable.isDeltaTable(spark, tid)


def delta_table(spark: SparkSession, layer: str, name: str):
    from delta.tables import DeltaTable

    tid = table_id(layer, name)
    return DeltaTable.forName(spark, tid) if catalog() else DeltaTable.forPath(spark, tid)


def writer(df: DataFrame, layer: str, name: str, mode: str, partition_by: list[str] | None = None, **options):
    """Returns a function that performs the write, so callers set options first."""
    w = df.write.format("delta").mode(mode)
    if partition_by:
        w = w.partitionBy(*partition_by)
    for k, v in options.items():
        w = w.option(k, v)
    tid = table_id(layer, name)
    if not catalog():
        return lambda: w.save(tid)

    def save_managed() -> None:
        df.sparkSession.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog()}.{layer}")
        w.saveAsTable(tid)

    return save_managed
