"""claims_marts_consumers: data-aware scheduling. Runs whenever claims_daily
updates the marts asset, not on a clock, so downstream extracts never read a
half-built day. Here it refreshes dbt docs (the data catalogue analysts browse)."""

from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, dag
from claims_alerts import task_failed

MARTS = Asset("claims://warehouse/marts")


@dag(
    dag_id="claims_marts_consumers",
    schedule=[MARTS],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "on_failure_callback": task_failed},
    tags=["claims", "dbt", "catalogue"],
    doc_md=__doc__,
)
def claims_marts_consumers():
    dbt_dir = os.environ.get("CLAIMS_DBT_DIR", "/opt/claims/dbt")
    BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {dbt_dir} && {os.environ.get('CLAIMS_DBT_BIN', 'dbt')} docs generate --profiles-dir . "
        f"--target {os.environ.get('CLAIMS_DBT_TARGET', 'dev')}",
        env={"CLAIMS_DATA_DIR": os.environ.get("CLAIMS_DATA_DIR", "/opt/claims/data")},
        append_env=True,
    )


claims_marts_consumers()
