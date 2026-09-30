"""Structural checks on the Databricks Asset Bundle: every job task calls a real
CLI subcommand with arguments the CLI accepts. (Deploying needs a workspace; this
catches the mistakes that would otherwise surface only at deploy time.)"""

from pathlib import Path

import pytest
import yaml

from claims_platform import cli

ROOT = Path(__file__).resolve().parents[1]


def _tasks():
    jobs = yaml.safe_load((ROOT / "databricks" / "jobs.yml").read_text())["resources"]["jobs"]
    for job_name, job in jobs.items():
        for t in job["tasks"]:
            yield job_name, t.get("for_each_task", {}).get("task", t), t.get("for_each_task")


def test_bundle_includes_the_jobs_file():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert "databricks/*.yml" in bundle["include"]
    assert set(bundle["targets"]) == {"dev", "prod"}


@pytest.mark.parametrize("job_name,task,for_each", list(_tasks()))
def test_every_task_is_a_valid_cli_invocation(job_name, task, for_each, monkeypatch):
    wheel = task["python_wheel_task"]
    assert wheel["entry_point"] == "claims-platform"
    args = [a.replace("{{input}}", "medical_claims").replace("{{job.parameters.batch_date}}", "2026-01-05")
            for a in wheel["parameters"]]  # fmt: skip
    called = {}
    monkeypatch.setattr(cli, f"cmd_{args[0].replace('-', '_')}", lambda a: called.setdefault("args", a) and 0)
    cli.main(args)  # argparse exits non-zero on an unknown subcommand or bad flag
    assert called, f"{job_name}.{task['task_key']} did not dispatch"


def test_wheel_paths_resolve_from_the_jobs_file():
    # bundle paths are relative to the YAML file that declares them
    text = (ROOT / "databricks" / "jobs.yml").read_text()
    assert "./dist/" not in text.replace("../dist/", "")


def test_permission_levels_are_valid_for_bundles():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    for target in bundle["targets"].values():
        for p in target.get("permissions", []):
            assert p["level"] in {"CAN_VIEW", "CAN_RUN", "CAN_MANAGE"}


def test_publish_is_configured_for_snowflake():
    jobs = yaml.safe_load((ROOT / "databricks" / "jobs.yml").read_text())["resources"]["jobs"]
    daily = jobs["claims_lakehouse_daily"]
    env = daily["job_clusters"][0]["new_cluster"]["spark_env_vars"]
    assert env["CLAIMS_WAREHOUSE"] == "snowflake" and env["SNOWFLAKE_PRIVATE_KEY"].startswith("{{secrets/")
    publish = next(t for t in daily["tasks"] if t["task_key"] == "publish")
    assert any("snowflake-connector-python" in lib.get("pypi", {}).get("package", "") for lib in publish["libraries"])


def test_jobs_run_one_day_at_a_time():
    jobs = yaml.safe_load((ROOT / "databricks" / "jobs.yml").read_text())["resources"]["jobs"]
    assert jobs["claims_lakehouse_daily"]["max_concurrent_runs"] == 1
