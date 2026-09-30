"""Daily data-quality scorecard: one JSON per batch date, built from the reports the
pipeline steps already wrote (validation verdicts, quarantine counts, load counts).

Thresholds turn it into a gate. A quarantine rate above the limit means a vendor
feed has degraded; the run is flagged and the orchestrator alerts, even though every
individual step "succeeded".
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field

from .config import SOURCES, paths

MAX_QUARANTINE_RATE = 0.02  # above 2% of a day's rows, a feed is degraded, not just noisy

_SILVER_STEP = {
    "eligibility": "silver_members",
    "providers": "silver_providers",
    "medical_claims": "silver_medical_claims",
    "pharmacy_claims": "silver_pharmacy_claims",
}


@dataclass
class Scorecard:
    batch_date: str
    status: str = "PASS"  # PASS | DEGRADED
    sources: dict = field(default_factory=dict)
    breaches: list[str] = field(default_factory=list)


def _read(path):
    return json.loads(path.read_text()) if path.exists() else None


def build(day: str, max_quarantine_rate: float = MAX_QUARANTINE_RATE) -> Scorecard:
    root = paths().reports
    card = Scorecard(batch_date=day)
    for source in SOURCES:
        validation = _read(root / "validation" / source / f"{day}.json") or {}
        silver = _read(root / "runs" / day / f"{_SILVER_STEP[source]}.json") or {}
        delivered = validation.get("rows", 0)
        quarantined = silver.get("quarantined", 0)
        rate = quarantined / delivered if delivered else 0.0
        card.sources[source] = {
            "validation": validation.get("status", "MISSING"),
            "delivered_rows": delivered,
            "quarantined_rows": quarantined,
            "quarantine_rate": round(rate, 4),
            "schema_drift": validation.get("drift", []),
        }
        if validation.get("status") != "ACCEPTED":
            card.breaches.append(f"{source}: delivery {validation.get('status', 'MISSING')}")
        if rate > max_quarantine_rate:
            card.breaches.append(f"{source}: quarantine rate {rate:.2%} > {max_quarantine_rate:.0%}")
    if card.breaches:
        card.status = "DEGRADED"
    out = root / "dq"
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{day}.json").write_text(json.dumps(asdict(card), indent=2))
    return card
