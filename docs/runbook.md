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
3. Once the corrected file is in place, clear the failed `ingest` task in Airflow.
   Every step is idempotent, so nothing is duplicated.

### `dq_report` failed (exit 3, feed degraded)
Every step succeeded and the marts are built, but the quarantine rate for a source
went above 2%.

1. `reports/dq/<date>.json` names the source and the rate.
2. Group the quarantine table by `_failed_rules` to see which rule is firing.
   A spike in `member_known` usually means a late eligibility file: the claims
   arrived before their members. Re-running the day after the roster lands
   recovers them.
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

### Backfill an older day
The member roster is a full daily snapshot, and its SCD2 history must be built in
date order (ADR 0005). Clear the day **with "Downstream" and "Future"** selected so
the later days re-run after it, in order (`max_active_runs=1`).

### Roll back a bad lakehouse load
Every Delta write is versioned.

```bash
# find the last good version
python -c "from claims_platform.lakehouse.spark import get_spark, delta_table; \
  delta_table(get_spark(), 'silver', 'medical_claims_current').history(10).show()"
claims-platform restore --table silver/medical_claims_current --version <n>
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
PHI (unmasked date of birth) needs the separate, audited `PHI_READER` role.

### Weekly maintenance
`claims_maintenance` runs `claims-platform optimize` (compaction plus Z-order by
`member_id`) on Sundays, when no daily load writes to the same tables.
