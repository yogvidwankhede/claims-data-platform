# ADR 0001: One CLI; Airflow, Databricks and CI all call it

**Status:** accepted

## Context
The same pipeline steps run in four places: a developer's terminal, CI, Airflow,
and a Databricks job. If each place carries its own copy of the logic (Python
callables inside DAG files, notebooks, CI scripts), they drift apart. Failures then
show up in only one of them, and they can't be reproduced anywhere else.

## Decision
Every step is a subcommand of one package CLI (`claims-platform validate | bronze |
silver | publish | dq-report | optimize | restore | migrate`). Airflow tasks are
`BashOperator`s that invoke it. Databricks tasks are `python_wheel_task`s with
the same entry point. CI and `scripts/e2e.sh` call it directly. Every subcommand
is idempotent for its batch date and signals failure through its exit code.

Airflow runs in its own virtualenv and never imports the platform package.

## Consequences
- Anything that fails in Airflow can be reproduced with one shell command.
- The Airflow image stays small, and its dependency constraints never clash with
  Spark, dbt or the Snowflake connector (Airflow pins hundreds of packages).
- Cross-task data does not go through XCom. Each step reads the previous step's
  tables, which are the durable interface anyway.
- In production, `DatabricksRunNowOperator` or `KubernetesPodOperator` would replace
  `BashOperator` for the Spark steps. The command line stays the same.
