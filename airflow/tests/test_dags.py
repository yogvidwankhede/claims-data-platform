"""DAG integrity tests: every DAG imports, and the pipeline's shape and safety
settings are what the design says. Run in the Airflow venv:

    AIRFLOW_HOME=$(mktemp -d) .venv-airflow/bin/python -m pytest airflow/tests
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

DAGS = Path(__file__).resolve().parents[1] / "dags"
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(DAGS)
sys.path.insert(0, str(DAGS))  # Airflow puts the dags folder on sys.path; so do we


@pytest.fixture(scope="session")
def dagbag():
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(dag_folder=str(DAGS))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == {"claims_daily", "claims_maintenance", "claims_marts_consumers"}


def test_daily_pipeline_order(dagbag):
    dag = dagbag.dags.get("claims_daily")
    order = ["ingest.bronze", "silver", "publish", "dbt_source_freshness", "dbt_build", "dq_report"]
    for upstream, downstream in zip(order, order[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids, (upstream, downstream)
    assert dag.get_task("ingest.validate").downstream_task_ids == {"ingest.bronze"}
    assert dag.get_task("ingest.wait_for_delivery").downstream_task_ids == {"ingest.validate"}


def test_daily_runs_are_serialised_and_backfilled(dagbag):
    dag = dagbag.dags.get("claims_daily")
    assert dag.max_active_runs == 1  # SCD2 roster history must be built in date order
    assert dag.catchup is True


def test_validation_is_not_retried_but_infrastructure_is(dagbag):
    dag = dagbag.dags.get("claims_daily")
    assert dag.get_task("ingest.validate").retries == 0
    assert dag.get_task("dq_report").retries == 0
    for task_id in ("ingest.bronze", "silver", "publish", "dbt_build"):
        task = dag.get_task(task_id)
        assert task.retries >= 3 and task.retry_exponential_backoff, task_id


def test_every_task_alerts_on_failure(dagbag):
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            assert task.on_failure_callback, f"{dag.dag_id}.{task.task_id}"


def test_daily_has_a_deadline(dagbag):
    assert dagbag.dags.get("claims_daily").deadline


def test_marts_asset_links_producer_and_consumer(dagbag):
    producer = dagbag.dags.get("claims_daily").get_task("dbt_build")
    assert [a.uri for a in producer.outlets] == ["claims://warehouse/marts"]
    consumer = dagbag.dags.get("claims_marts_consumers")
    assert "claims://warehouse/marts" in str(consumer.timetable.asset_condition)


def test_commands_call_the_platform_cli(dagbag):
    dag = dagbag.dags.get("claims_daily")
    assert dag.get_task("silver").bash_command.endswith("silver --date {{ ds }}")
    assert dag.get_task("publish").bash_command.endswith("publish --date {{ ds }}")
