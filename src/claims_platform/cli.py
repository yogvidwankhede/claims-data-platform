"""One CLI for every pipeline step. Airflow tasks, CI, the Databricks job and a
developer at a terminal all run the same commands:

    claims-platform generate --start 2026-01-01 --days 30
    claims-platform validate --source medical_claims --date 2026-01-05
    claims-platform bronze   --source medical_claims --date 2026-01-05
    claims-platform silver   --date 2026-01-05
    claims-platform publish  --date 2026-01-05
    claims-platform run-day  --date 2026-01-05          # all of the above, in order
    claims-platform restore  --table silver/medical_claims_current --version 12
    claims-platform dq-report --date 2026-01-05         # scorecard; exit 3 if degraded
    claims-platform migrate  --dry-run                  # Snowflake platform-as-code

Exit code is non-zero on any failure, including a rejected delivery, so the
orchestrator stops downstream work for that day.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date

from .config import SOURCES, paths


def _print(obj) -> None:
    print(json.dumps(obj, indent=2, default=str))


def cmd_generate(a) -> int:
    from .generator import generate

    _print(generate(paths().root, date.fromisoformat(a.start), a.days, a.members, a.seed, a.drift_on_day))
    return 0


def cmd_validate(a) -> int:
    from .contracts import validate_delivery

    report = validate_delivery(paths().delivery(a.source, a.date), a.source)
    out = paths().reports / "validation" / a.source
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{a.date}.json").write_text(report.to_json())
    print(report.to_json())
    return 0 if report.status == "ACCEPTED" else 2


def _spark():
    from .lakehouse.spark import get_spark

    return get_spark()


def cmd_bronze(a) -> int:
    from .lakehouse import jobs

    _print(jobs.ingest_bronze(_spark(), a.source, a.date))
    return 0


def cmd_silver(a) -> int:
    from .lakehouse import jobs

    spark = _spark()
    # members first: claims are checked against the roster
    for step in (jobs.build_members, jobs.build_providers, jobs.build_medical, jobs.build_pharmacy):
        _print(step(spark, a.date))
    return 0


def cmd_publish(a) -> int:
    from .lakehouse import jobs
    from .warehouse import load

    _print(jobs.export_for_warehouse(_spark(), a.date))
    _print(load.load(a.date))
    return 0


def cmd_run_day(a) -> int:
    for source in SOURCES:
        if cmd_validate(argparse.Namespace(source=source, date=a.date)) != 0:
            print(f"delivery {source}/{a.date} rejected; stopping", file=sys.stderr)
            return 2
    for source in SOURCES:
        cmd_bronze(argparse.Namespace(source=source, date=a.date))
    cmd_silver(a)
    cmd_publish(a)
    return 0


def cmd_optimize(a) -> int:
    from .lakehouse import jobs

    _print(jobs.optimize(_spark()))
    return 0


def cmd_restore(a) -> int:
    from .lakehouse import jobs

    layer, name = a.table.split("/", 1)
    _print(jobs.restore(_spark(), layer, name, a.version))
    return 0


def cmd_dq_report(a) -> int:
    from dataclasses import asdict

    from .quality import build

    card = build(a.date)
    _print(asdict(card))
    return 0 if card.status == "PASS" else 3


def cmd_migrate(a) -> int:
    from .warehouse.load import snowflake_connection
    from .warehouse.migrate import migrate

    con = snowflake_connection(role=a.deploy_role, database="")
    try:
        _print(migrate(con, dry_run=a.dry_run, deploy_role=a.deploy_role))
    finally:
        con.close()
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="claims-platform", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate")
    g.add_argument("--start", required=True)
    g.add_argument("--days", type=int, default=30)
    g.add_argument("--members", type=int, default=2000)
    g.add_argument("--seed", type=int, default=7)
    g.add_argument("--drift-on-day", type=int, default=20)
    for name in ("validate", "bronze"):
        p = sub.add_parser(name)
        p.add_argument("--source", required=True, choices=SOURCES)
        p.add_argument("--date", required=True)
    for name in ("silver", "publish", "run-day", "dq-report"):
        sub.add_parser(name).add_argument("--date", required=True)
    sub.add_parser("optimize")
    r = sub.add_parser("restore")
    r.add_argument("--table", required=True, help="layer/name, e.g. silver/medical_claims_current")
    r.add_argument("--version", type=int, required=True)
    m = sub.add_parser("migrate", help="apply pending snowflake/migrations in order")
    m.add_argument("--dry-run", action="store_true")
    m.add_argument("--deploy-role", default="ACCOUNTADMIN", help="role the deploy user connects as")
    a = ap.parse_args(argv)
    handler = {
        "generate": cmd_generate, "validate": cmd_validate, "bronze": cmd_bronze, "silver": cmd_silver,
        "publish": cmd_publish, "run-day": cmd_run_day, "optimize": cmd_optimize, "restore": cmd_restore,
        "migrate": cmd_migrate, "dq-report": cmd_dq_report,
    }[a.cmd]  # fmt: skip
    return handler(a)


def run() -> None:
    """Console-script entry point. Raises SystemExit so the exit code reaches every
    runner: a shell, Airflow's BashOperator, and Databricks' python_wheel_task,
    which ignores an entry point's return value."""
    raise SystemExit(main())


if __name__ == "__main__":
    run()
