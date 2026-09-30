# ADR 0005: SCD2 member history is dated by the roster, not by the clock

**Status:** accepted

## Context
PMPM and utilization have to attribute each month to the plan the member was in
*at that time*. dbt snapshots stamp versions with the time the snapshot ran by
default. Under that default, a backfill or a late run would date a plan change to
when the pipeline happened to run, not to when the plan actually changed.

## Decision
The lakehouse publishes the roster's delivery date (`_batch_date`) with members.
The snapshot uses the `check` strategy on (line_of_business, zip3, coverage_end)
with `updated_at` = the roster date, so rerunning a day reproduces the same history.
Terminations arrive as `coverage_end`, a checked column, so hard deletes are ignored:
invalidating a member who drops off the roster would stamp `dbt_valid_to` with the
wall clock and break the rule.
`int_member_history` stretches each member's first version back to the start of
time, because history before the first snapshot is best described by the earliest
version we have. Member-months count any month with at least one covered day and
attribute it to the plan in force at the start of the month, so every paid claim
lands in some member-month. The `pmpm_reconciles_to_claims` test checks this to 0.5%.

The daily DAG runs one date at a time (`max_active_runs=1`, `catchup=True`), because
the roster is a full snapshot and history must be built in date order.

## Consequences
Re-running the latest day is a no-op (CI checks this). A snapshot only moves forward,
so re-running an *older* day after later ones would write a version dated before the
current one. Correcting history for an old day therefore means rebuilding the member
history: drop the snapshot, then replay every day from the first in order. The
runbook has the procedure.
