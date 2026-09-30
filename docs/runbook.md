# Runbook: claims data platform

On-call reference for the `claims_daily` pipeline. Every command below is the same
command the orchestrator runs; set `CLAIMS_DATA_DIR` (and `CLAIMS_WAREHOUSE=snowflake`
plus the `SNOWFLAKE_*` variables in production) first.

## Where to look

| Question | Look at |
|---|---|
| Did a delivery pass its contract? | `reports/validation/<source>/<date>.json` |
| What did each step do? | `reports/runs/<date>/<step>.json` (row counts, quarantined, merged) |
| Is a feed degrading? | `reports/dq/<date>.json` (`status`, `breaches`) |
| Which rows were rejected and why? | silver `quarantine_<source>` table, `_failed_rules` column |
| When did RAW last load? | `raw._load_audit` (Snowflake: `RAW.CLAIMS._LOAD_AUDIT`) |
| What migrations are applied? | `PLATFORM_OPS.MIGRATIONS.CHANGE_HISTORY` |

## Alerts

### `validate` failed (delivery rejected)
The file broke its contract, and nothing from that source was ingested for the day.
Validation is not retried on purpose.

1. Read the reason: `claims-platform validate --source <s> --date <d>`.
2. `manifest missing`: the vendor is still uploading. The sensor waits up to 6h.
   If the upload finished without a manifest, ask the vendor to re-send.
   `sha256 mismatch` / `row count`: the file is corrupt or truncated, so ask for a re-send.
   `required columns missing` / `retyped`: the vendor changed the format. Don't
   loosen the contract to get the run green; agree the change first (ADR 0002).
3. Once the corrected file is in place, clear the failed `ingest.validate` task
   instance (its map index is the source) with **Downstream** selected. Every step
   is idempotent, so nothing is duplicated.

### `dq_report` failed (exit 3, feed degraded)
Every step succeeded and the marts are built, but the quarantine rate for a source
went above 2%.

1. `reports/dq/<date>.json` names the source and the rate.
2. Group the quarantine table by `_failed_rules` to see which rule is firing.
   A spike in `member_known` usually means the roster was missing members: the
   claims arrived before their enrolment. They are recovered only when a roster
   containing those members is ingested and their claims are re-delivered, so ask
   the enrolment and claims vendors for corrected files.
3. Send the vendor the grouped defect list.

### Deadline missed (marts not ready by 12:00 UTC)
Find the running or failed task in the `claims_daily` grid. Queued, never-started
sensors mean no delivery yet. Tell the finance and care-management consumers
(exposures in `dbt/models/marts/_marts.yml`) which marts are late.

### `dbt_build` failed
- **Contract error**: a model's columns or types no longer match its declared
  contract. This is a breaking change for BI. Either fix the model or version the
  contract deliberately.
- **`on_schema_change='fail'` on `fct_claims`**: the incremental model's columns
  changed. After review, run `dbt build -s fct_claims+ --full-refresh`.
- **`pmpm_reconciles_to_claims`**: paid claims fell outside every member-month,
  which points to an enrolment or attribution bug. Don't publish the numbers.

## Procedures

### Re-run a day
Clear the day's run in Airflow, or run `claims-platform run-day --date <d>` and
then `dbt build`. Re-running the **latest** day changes nothing (CI checks this).

### Backfill or correct an older day
Claims, pharmacy and quarantine tables are safe to re-run for any day: MERGEs
resolve by claim version, not arrival order. The member history is different
(ADR 0005). The roster is a full daily snapshot and the SCD2 snapshot only moves
forward, so re-running an old day after later ones would date a version before the
current one. To correct member history:

1. Pause `claims_daily`.
2. Drop the snapshot: `drop table ANALYTICS.SNAPSHOTS.SNP_MEMBERS`.
3. Replay every day from the first delivery in order: clear all runs from the
   first day with **Future** selected (`max_active_runs=1` keeps them in order).
4. Unpause.

A missed day that never ran (the scheduler was down) needs none of this:
`catchup=True` runs it before the later days.

### Roll back a bad lakehouse load
Every Delta write is versioned. Restore `medical_claims_current` **and**
`medical_claim_lines` to versions from the same run, or the history and current
tables disagree. Each table's `history()` shows the timestamp of each version.

```bash
# find the last good version
python -c "from claims_platform.lakehouse.spark import get_spark, delta_table; \
  delta_table(get_spark(), 'silver', 'medical_claims_current').history(10).show()"
claims-platform restore --table silver/medical_claims_current --version <n>
claims-platform restore --table silver/medical_claim_lines --version <m>
claims-platform publish --date <d>          # push the restored state to RAW
(cd dbt && dbt build)
```

### Apply Snowflake changes
Add a new `snowflake/migrations/V<next>__<what>.sql`. Never edit an applied one;
the runner refuses. Always dry-run first:

```bash
claims-platform migrate --dry-run
claims-platform migrate
```

### Grant an analyst a line of business
```sql
INSERT INTO GOVERNANCE.POLICIES.LOB_ENTITLEMENTS VALUES ('<ROLE>', 'MEDICAID');
```
The row access policy on the marts picks this up on the analyst's next query.
`PHI_READER` (care management) sees unmasked dates of birth across **all** lines of
business. Grant it per person and audit it.

### Weekly maintenance
`claims_maintenance` runs `claims-platform optimize` (compaction plus Z-order by
`member_id`) at 02:00 UTC on Sundays with a 2h timeout, ahead of the 06:00 daily
run. If the two overlap, Delta's optimistic concurrency fails one of the conflicting
commits, and that task's retry re-runs it. Pause maintenance during backfills.
