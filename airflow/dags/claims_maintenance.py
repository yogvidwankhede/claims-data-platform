"""claims_maintenance: weekly Delta table maintenance (compaction + Z-order by member).

Scheduled on Sunday, when no daily load runs against the same tables, because
OPTIMIZE rewrites files that a concurrent MERGE may be reading.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from claims_alerts import task_failed


@dag(
    dag_id="claims_maintenance",
    schedule="0 2 * * 0",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10), "on_failure_callback": task_failed},
    tags=["claims", "lakehouse", "maintenance"],
    doc_md=__doc__,
)
def claims_maintenance():
    BashOperator(
        task_id="optimize_zorder",
        bash_command=f"{os.environ.get('CLAIMS_PLATFORM_BIN', 'claims-platform')} optimize",
        env={"CLAIMS_DATA_DIR": os.environ.get("CLAIMS_DATA_DIR", "/opt/claims/data")},
        append_env=True,
        execution_timeout=timedelta(hours=2),
    )


claims_maintenance()
