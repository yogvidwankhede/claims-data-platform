# ADR 0002: Contracts reject deliveries; expectations quarantine rows

**Status:** accepted

## Context
Vendor feeds fail in two different ways, and each needs a different response.

1. **The file is wrong**: truncated, modified after delivery, a required column
   missing, or a column retyped. Loading part of it corrupts downstream totals
   silently.
2. **Some rows are wrong**: an invalid ICD-10 code, a negative payment, an unknown
   member. The rest of the file is fine and the business needs it today.

## Decision
- **Delivery contracts** (`contracts/*.yaml`, checked by `claims-platform validate`)
  decide whether a file may enter at all. The checks are manifest present, sha256,
  row count, required columns, and type parse rate (more than 2% unparseable means
  the column was retyped, not that a few values are bad). A violation rejects the
  whole delivery, and the day stops before bronze. Validation is **not retried**,
  because a bad file does not fix itself.
- **Additive schema drift** (a new optional column) is accepted and recorded.
  Bronze evolves with `mergeSchema`. Rejecting a harmless vendor addition would stop
  the business for nothing.
- **Row expectations** (silver) split each batch into valid rows and quarantined rows.
  Each quarantined row keeps `_failed_rules`, the full list of rules it broke, so
  the vendor gets a complete defect report in one round.
- The **DQ scorecard** (`dq-report`) turns quarantine rates into a gate. Above 2% of
  a day's rows, the feed counts as degraded and the run alerts, even though every
  step "succeeded".

## Consequences
Rejection is loud and total. Row defects are quiet, local and tracked. The 2% thresholds
are policy choices, set in code next to the reasoning.
