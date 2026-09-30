"""Delivery-level data contracts: decide whether a file may enter the lakehouse at all.

Two layers of defence, deliberately separate:

  * contract (here, per delivery): is this the file we agreed on? Manifest
    present, checksum and row count match, required columns present, values in
    each column parse as the contracted type. A violation rejects the whole
    delivery: nothing is ingested for that source and day, and the pipeline
    stops before touching downstream tables.
  * expectations (lakehouse silver, per row): is each record usable? Bad rows
    are quarantined with a reason; good rows flow on.

Additive schema drift (a new optional column) is accepted and recorded, because
rejecting a vendor's harmless addition would stop the business for nothing.
"""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path

import yaml

CONTRACT_DIR = Path(__file__).resolve().parents[2] / "contracts"
TYPE_SAMPLE_ROWS = 5000  # parse-check this many rows per delivery
MAX_UNPARSEABLE_FRACTION = 0.02  # above this, the column is treated as retyped, not as a few bad rows


class ContractViolation(Exception):
    pass


@dataclass
class ValidationReport:
    source: str
    delivery_date: str
    status: str = "ACCEPTED"  # ACCEPTED | REJECTED
    rows: int = 0
    violations: list[str] = field(default_factory=list)
    drift: list[str] = field(default_factory=list)
    unparseable: dict[str, int] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)


def load_contract(source: str, directory: Path = CONTRACT_DIR) -> dict:
    path = directory / f"{source}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"no contract for source {source!r} at {path}")
    return yaml.safe_load(path.read_text())


def _parses(value: str, typ: str) -> bool:
    if value == "":
        return True  # emptiness is judged by `required`, not by type
    try:
        if typ == "int":
            int(value)
        elif typ == "decimal":
            float(value)
        elif typ == "date":
            date.fromisoformat(value)
        elif typ == "bool" and value.lower() not in ("true", "false"):
            return False
        return True
    except ValueError:
        return False


def validate_delivery(folder: Path, source: str, contract: dict | None = None) -> ValidationReport:
    contract = contract or load_contract(source)
    report = ValidationReport(source=source, delivery_date=folder.name.removeprefix("dt="))

    def reject(msg: str) -> ValidationReport:
        report.status = "REJECTED"
        report.violations.append(msg)
        return report

    manifest_path = folder / "_manifest.json"
    if not manifest_path.exists():
        return reject("manifest missing: delivery incomplete")
    manifest = json.loads(manifest_path.read_text())
    data_path = folder / manifest["file"]
    if not data_path.exists():
        return reject(f"data file {manifest['file']} named in manifest is missing")
    raw = data_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != manifest["sha256"]:
        return reject("sha256 mismatch: file corrupted or modified after delivery")

    reader = csv.DictReader(raw.decode().splitlines())
    header = reader.fieldnames or []
    contracted = {c["name"]: c for c in contract["columns"]}
    missing = [c for c, spec in contracted.items() if spec.get("required") and c not in header]
    if missing:
        reject(f"required columns missing: {missing}")
    extra = [c for c in header if c not in contracted]
    if extra:
        if contract.get("schema_evolution") == "additive":
            report.drift.append(f"new columns (additive, accepted): {extra}")
        else:
            reject(f"unexpected columns and schema_evolution is not additive: {extra}")
    optional_new = [c for c in header if c in contracted and contracted[c].get("since_schema_version")]
    if optional_new:
        report.drift.append(f"versioned optional columns present: {optional_new}")

    rows = 0
    bad = {c: 0 for c in contracted if c in header}
    for row in reader:
        rows += 1
        if rows <= TYPE_SAMPLE_ROWS:
            for c in bad:
                if not _parses(row[c], contracted[c]["type"]):
                    bad[c] += 1
    report.rows = rows
    if rows != manifest["rows"]:
        reject(f"row count {rows} != manifest {manifest['rows']}: truncated or padded delivery")
    sampled = min(rows, TYPE_SAMPLE_ROWS)
    report.unparseable = {c: n for c, n in bad.items() if n}
    for c, n in report.unparseable.items():
        if sampled and n / sampled > MAX_UNPARSEABLE_FRACTION:
            reject(f"column {c}: {n}/{sampled} values do not parse as {contracted[c]['type']} (retyped column?)")
    return report
