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
  post-hook `apply_governance()` re-applies tags and the line-of-business row access
  policy to the marts after each rebuild. A test fails if a contract column marked
  `pii: true` is not tagged in the migrations.
- **Minimum necessary:** names never leave RAW. Staging drops them.
- **Platform as code:** versioned, forward-only migrations (`snowflake/migrations`)
  with checksums in a change-history table. Editing an applied migration is an error.
  The migrations cover warehouses split by workload, each with a resource monitor, and
  functional roles layered over access roles.

## Consequences
A reload rewrites the table (fine at this scale; it would become a MERGE at larger
volume). Account-level objects cannot be emulated offline. They are checked by
sqlfluff and by static tests, and the runner itself is tested on fakesnow.
