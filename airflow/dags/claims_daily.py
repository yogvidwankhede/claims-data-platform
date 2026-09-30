"""claims_daily: one run per delivery day, from vendor files to analytics marts.

    ingest[source] = wait_for_delivery -> validate -> bronze   (mapped over the 4 feeds)
        -> silver -> publish (RAW) -> dbt source freshness -> dbt build -> dq_report

Design notes (see docs/adr for the reasoning):
  * Airflow orchestrates; it does not transform. Every task runs the same
    `claims-platform` CLI a developer or CI runs, so behaviour never depends on
    where a step is executed, and Airflow workers need none of the heavy deps.
  * Every step is idempotent for its batch date (Delta replaceWhere / MERGE,
    transactional warehouse reload, dbt delete+insert), so retries, clears and
    backfills are safe.
  * A rejected delivery is not transient: validation has no retries and stops
    that day's run; infrastructure steps retry with exponential backoff.
  * Runs are serialised (max_active_runs=1): the member roster is a full daily
    snapshot and the SCD2 history must be built in date order.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, DeadlineAlert, DeadlineReference, SyncCallback, dag, task, task_group
from claims_alerts import task_failed

SOURCES = ["eligibility", "providers", "medical_claims", "pharmacy_claims"]
DATA_DIR = os.environ.get("CLAIMS_DATA_DIR", "/opt/claims/data")
PLATFORM = os.environ.get("CLAIMS_PLATFORM_BIN", "claims-platform")
DBT = os.environ.get("CLAIMS_DBT_BIN", "dbt")
DBT_DIR = os.environ.get("CLAIMS_DBT_DIR", "/opt/claims/dbt")
DBT_TARGET = os.environ.get("CLAIMS_DBT_TARGET", "dev")

RAW_TABLES = Asset(
    "claims://warehouse/raw", extra={"tables": "members,providers,medical_claims_current,pharmacy_claims"}
)
MARTS = Asset("claims://warehouse/marts", extra={"models": "mart_pmpm,mart_utilization,mart_readmission_rates"})

ENV = {"CLAIMS_DATA_DIR": DATA_DIR}

default_args = {
    "owner": "claims-data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": task_failed,
}


def cli(task_id: str, args: str, **kwargs) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"{PLATFORM} {args}",
        env=ENV,
        append_env=True,
        **kwargs,
    )


@dag(
    dag_id="claims_daily",
    schedule="0 6 * * *",  # vendors deliver overnight; run at 06:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=True,  # a missed day is backfilled, in order
    max_active_runs=1,
    default_args=default_args,
    tags=["claims", "lakehouse", "warehouse", "dbt"],
    doc_md=__doc__,
    # a run must finish within 6h of starting (06:00 -> 12:00 UTC, when the business
    # reads the marts). Measured from queue time, not logical date, so catch-up runs
    # after an outage don't all fire "missed" the moment they are created.
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(hours=6),
        callback=SyncCallback("claims_alerts.deadline_missed", kwargs={"dag_id": "claims_daily"}),
    ),
)
def claims_daily():
    @task_group
    def ingest(source: str):
        """Per source: wait for the delivery, check it against its contract, land it in bronze.
        Mapped over sources, so one late vendor does not hold up the others' ingestion."""

        @task.sensor(poke_interval=300, timeout=6 * 3600, mode="reschedule", retries=0)
        def wait_for_delivery(source: str, ds=None) -> bool:
            """The vendor writes _manifest.json last, so its presence means the delivery is complete."""
            return (Path(DATA_DIR) / "landing" / source / f"dt={ds}" / "_manifest.json").exists()

        @task.bash(env=ENV, append_env=True, retries=0)  # a rejected file won't fix itself on retry
        def validate(source: str) -> str:
            return f"{PLATFORM} validate --source {source} --date {{{{ ds }}}}"

        @task.bash(env=ENV, append_env=True)
        def bronze(source: str) -> str:
            return f"{PLATFORM} bronze --source {source} --date {{{{ ds }}}}"

        wait_for_delivery(source) >> validate(source) >> bronze(source)

    landed = ingest.expand(source=SOURCES)

    silver = cli("silver", "silver --date {{ ds }}")
    publish = cli("publish", "publish --date {{ ds }}", outlets=[RAW_TABLES])

    dbt_env = {**ENV, "DBT_TARGET": DBT_TARGET}
    freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_DIR} && {DBT} source freshness --profiles-dir . --target {DBT_TARGET}",
        env=dbt_env,
        append_env=True,
        retries=1,
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && {DBT} build --profiles-dir . --target {DBT_TARGET}",
        env=dbt_env,
        append_env=True,
        outlets=[MARTS],
    )
    # exit 3 = degraded feed: fail loudly (alert) but only after the marts are built
    dq = cli("dq_report", "dq-report --date {{ ds }}", retries=0)

    landed >> silver >> publish >> freshness >> dbt_build >> dq


claims_daily()
