# ADR 0004: Reload RAW in place; govern PII with tags

**Status:** accepted

## Context
The usual zero-downtime pattern is to load into a new table and then `ALTER TABLE ...
SWAP WITH`. That swaps the table object, and in Snowflake, masking and row access
policies are attached to the table's columns. A swap would silently publish
unmasked PHI.

## Decision
- **Load:** `REMOVE` the stage prefix, `PUT` the files, then `BEGIN; DELETE; COPY INTO
  ... ON_ERROR = ABORT_STATEMENT; INSERT audit row; COMMIT`. Readers see either the
  old table or the new one. The policies stay attached, and the audit row commits
  or rolls back with the data. A test enforces that no migration uses `SWAP`.
- **Governance:** PII columns carry the `GOVERNANCE.POLICIES.PII` tag, and the tag
  carries the masking policies (strings to `***MASKED***`, dates of birth generalised
  to the year). Any newly tagged column is masked with no per-column work. The dbt
  post-hook `apply_governance()` re-applies tags (snapshot and marts) and attaches
  the line-of-business row access policy to every mart that lacks one, including
  the claim-level facts, which carry the plan in force at the time of service.
  Analysts are granted the `MARTS` schema only, never the snapshot or intermediate
  layers. A test fails if a contract column marked `pii: true` is not tagged in the
  migrations. These features require Snowflake Enterprise edition.
- **Minimum necessary:** names never leave RAW. Staging drops them.
- **Platform as code:** versioned, forward-only migrations (`snowflake/migrations`)
  with checksums in a change-history table. Editing an applied migration is an error.
  A failed script is retried from the top, so every statement is re-runnable
  (`IF NOT EXISTS`, `SET TAG` overwrites, `ALTER TAG ... SET MASKING POLICY ... FORCE`).
  The migrations cover warehouses split by workload, each with a resource monitor, and
  functional roles layered over access roles.

## Consequences
A reload rewrites the table (fine at this scale; it would become a MERGE at larger
volume). Account-level objects cannot be emulated offline. They are checked by
sqlfluff and by static tests, and the runner itself is tested on fakesnow.
