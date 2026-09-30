# ADR 0003: Claim versions are resolved by version number, not arrival order

**Status:** accepted

## Context
Payers adjust and reverse claims weeks after they're paid, so a claim can arrive as
version 1, then version 2 (an adjustment) or a reversal. Files arrive late and get
replayed, which means version 2 can land *before* version 1. A new version can also
have fewer lines than the old one.

"Latest file wins" is wrong in both cases. It resurrects superseded versions, and it
leaves stale lines behind when a claim shrinks.

## Decision
Silver keeps two tables:
- `medical_claim_lines`: full history, MERGE on (claim_id, claim_version, line_number).
  A replayed day matches its own rows and rewrites them unchanged, so it adds nothing.
- `medical_claims_current`: only each claim's highest version. For each incoming
  claim whose version is at least the stored one, lines of older versions are
  deleted (MERGE delete) and the new lines are upserted. The upsert is guarded by
  `s.claim_version >= t.claim_version`, so a late original never overwrites a newer
  adjustment.

Downstream (dbt `fct_claims`), the incremental strategy is delete+insert on
`claim_id`, which replaces whole claims. Reversed and denied claims carry
`net_paid_amount = 0`.

## Consequences
The Delta integration tests cover each case: replay is a no-op, a reversal that
arrives before the original still wins, and a shrinking claim leaves no stale lines.
The cost is a second MERGE per batch, which is small next to correctness in financial totals.
