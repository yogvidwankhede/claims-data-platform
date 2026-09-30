"""Synthetic healthcare-claims feeds with the messiness of real payer data.

Produces daily vendor-style deliveries under <data>/landing/<source>/dt=YYYY-MM-DD/:

    eligibility      full member roster each day (834-like): enrollments, terms,
                     line-of-business and address changes over time (-> SCD2)
    providers        full provider reference (NPI with a valid check digit)
    medical_claims   incremental claim lines (837-like, flattened): office,
                     emergency and inpatient encounters, with readmissions
    pharmacy_claims  incremental pharmacy fills (NCPDP-like)

and deliberately injects what a production pipeline must survive:

    * claim adjustments (new versions) and reversals, arriving weeks later
    * late-arriving claims (30-90 days after the date of service)
    * exact duplicate rows inside a delivery
    * additive schema drift (medical claims gain `rendering_npi` mid-period)
    * bad rows: malformed ICD-10, negative paid on a paid claim, unknown member,
      service_to before service_from, missing claim_id

Every delivery ships a manifest (row count + sha256), the way vendors let the
receiver prove a file arrived complete. Output is deterministic for a seed.
All people are fictional; no real PHI is involved.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import random
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

LOB_MIX = {"COMMERCIAL": 0.55, "MEDICARE": 0.25, "MEDICAID": 0.20}
LOB_PRICE = {"COMMERCIAL": 1.20, "MEDICARE": 0.85, "MEDICAID": 0.70}

FIRST = ["Aarav", "Maya", "Noah", "Sofia", "Liam", "Priya", "Ethan", "Zoe", "Omar", "Lena", "Mateo", "Ava", "Kai",
         "Nora", "Ravi", "Iris", "Jonah", "Mei", "Luca", "Hana"]  # fmt: skip
LAST = ["Patel", "Nguyen", "Garcia", "Smith", "Kim", "Okafor", "Rossi", "Cohen", "Silva", "Novak", "Haddad",
        "Brown", "Singh", "Lopez", "Muller", "Tanaka", "Ali", "Moreau", "Walker", "Ivanova"]  # fmt: skip
ZIP3 = ["631", "630", "606", "100", "941", "750", "331", "852", "981", "021"]

CHRONIC = {"I50.9": "heart failure", "J44.9": "COPD", "E11.9": "type 2 diabetes", "I10": "hypertension"}
ACUTE_ER = ["R07.9", "J18.9", "S52.501A", "N39.0", "R10.9", "J06.9"]
OFFICE_DX = ["Z00.00", "M54.50", "J06.9", "E11.9", "I10", "F41.1", "K21.9"]
INPATIENT_DX = ["I50.9", "J44.1", "J18.9", "S72.001A", "K35.80", "A41.9"]

CPT_PRICE = {  # base allowed amount, USD
    "99213": 95, "99214": 140, "80053": 18, "85025": 12, "71046": 45,  # office, labs, chest x-ray
    "99284": 420, "99285": 690, "70450": 310,  # ER visit levels, head CT
    "99223": 290, "99232": 110, "99238": 105,  # inpatient initial / subsequent / discharge
}  # fmt: skip
DRUGS = [  # ndc, name, base paid per 30-day fill, chronic condition it treats (or None)
    ("00093721401", "metformin 500mg", 9, "E11.9"),
    ("00172375810", "lisinopril 10mg", 7, "I10"),
    ("00378395177", "atorvastatin 20mg", 11, "I10"),
    ("00173068220", "albuterol inhaler", 58, "J44.9"),
    ("00054429731", "furosemide 40mg", 6, "I50.9"),
    ("00088222033", "insulin glargine", 310, "E11.9"),
    ("00074433902", "adalimumab (specialty)", 6900, None),
]

MEDICAL_COLUMNS = [
    "claim_id", "claim_version", "claim_status", "claim_type", "member_id", "billing_npi",
    "service_from", "service_to", "admit_date", "discharge_date", "place_of_service",
    "primary_dx", "secondary_dx", "line_number", "procedure_code", "units",
    "billed_amount", "allowed_amount", "paid_amount", "paid_date",
]  # fmt: skip
DRIFT_COLUMN = "rendering_npi"
PHARMACY_COLUMNS = ["rx_claim_id", "member_id", "ndc", "drug_name", "fill_date", "days_supply", "quantity",
                    "paid_amount", "prescriber_npi"]  # fmt: skip
ELIGIBILITY_COLUMNS = ["member_id", "first_name", "last_name", "birth_date", "gender", "zip3", "line_of_business",
                       "coverage_start", "coverage_end"]  # fmt: skip
PROVIDER_COLUMNS = ["npi", "provider_name", "specialty", "state", "is_facility"]


def npi_check_digit(first9: str) -> int:
    """NPI check digit: Luhn over the 9 digits prefixed with the 80840 health-industry code."""
    digits = [int(c) for c in "80840" + first9]
    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 0:
            d *= 2
            d = d - 9 if d > 9 else d
        total += d
    return (10 - total % 10) % 10


def is_valid_npi(npi: str) -> bool:
    return len(npi) == 10 and npi.isdigit() and npi_check_digit(npi[:9]) == int(npi[9])


@dataclass
class Member:
    member_id: str
    first_name: str
    last_name: str
    birth_date: date
    gender: str
    zip3: str
    lob: str
    coverage_start: date
    coverage_end: date | None
    chronic: list[str] = field(default_factory=list)
    change_on: date | None = None  # line-of-business / address change (SCD2)
    new_lob: str | None = None
    new_zip3: str | None = None

    def as_of(self, day: date) -> dict:
        changed = self.change_on is not None and day >= self.change_on
        return {
            "member_id": self.member_id,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "birth_date": self.birth_date.isoformat(),
            "gender": self.gender,
            "zip3": (self.new_zip3 or self.zip3) if changed else self.zip3,
            "line_of_business": (self.new_lob or self.lob) if changed else self.lob,
            "coverage_start": self.coverage_start.isoformat(),
            "coverage_end": self.coverage_end.isoformat() if self.coverage_end else "",
        }

    def covered(self, day: date) -> bool:
        return self.coverage_start <= day and (self.coverage_end is None or day <= self.coverage_end)

    def lob_on(self, day: date) -> str:
        return (self.new_lob or self.lob) if self.change_on and day >= self.change_on else self.lob


@dataclass
class Row:
    file_date: date
    values: dict


class Generator:
    def __init__(self, start: date, days: int, members: int, seed: int, history_days: int = 120):
        self.start, self.days, self.n_members = start, days, members
        self.end = start + timedelta(days=days - 1)
        self.history_start = start - timedelta(days=history_days)
        self.rng = random.Random(seed)
        self.claim_seq = 0
        self.rx_seq = 0

    # ---------------- reference data ----------------

    def make_providers(self) -> list[dict]:
        specialties = [
            ("Family Medicine", False),
            ("Internal Medicine", False),
            ("Cardiology", False),
            ("Pulmonology", False),
            ("Emergency Medicine", True),
            ("General Acute Care Hospital", True),
        ]
        out = []
        for i in range(60):
            first9 = f"1{self.rng.randrange(10**7, 10**8):08d}"
            spec, facility = specialties[i % len(specialties)]
            out.append({
                "npi": first9 + str(npi_check_digit(first9)),
                "provider_name": (f"{self.rng.choice(LAST)} Health {i}" if facility
                                  else f"Dr. {self.rng.choice(FIRST)} {self.rng.choice(LAST)}"),
                "specialty": spec,
                "state": "MO" if i % 3 else "IL",
                "is_facility": str(facility).lower(),
            })  # fmt: skip
        return out

    def make_members(self) -> list[Member]:
        out = []
        for i in range(self.n_members):
            lob = self.rng.choices(list(LOB_MIX), weights=list(LOB_MIX.values()))[0]
            age = self.rng.randint(66, 92) if lob == "MEDICARE" else self.rng.randint(1, 64)
            birth = date(self.start.year - age, self.rng.randint(1, 12), self.rng.randint(1, 28))
            # most are enrolled before history starts; some join or leave inside the window
            start = self.history_start - timedelta(days=self.rng.randint(0, 900))
            if self.rng.random() < 0.08:
                start = self.history_start + timedelta(days=self.rng.randint(0, (self.end - self.history_start).days))
            end = None
            if self.rng.random() < 0.05:
                end = start + timedelta(days=self.rng.randint(60, 400))
            chronic_p = 0.45 if lob == "MEDICARE" else 0.12
            chronic = [c for c in CHRONIC if self.rng.random() < chronic_p / 2]
            m = Member(f"M{10_000_000 + i}", self.rng.choice(FIRST), self.rng.choice(LAST), birth,
                       self.rng.choice("FM"), self.rng.choice(ZIP3), lob, start, end, chronic)  # fmt: skip
            if self.rng.random() < 0.03:  # mid-period change -> SCD2 history
                m.change_on = self.start + timedelta(days=self.rng.randint(1, max(1, self.days - 1)))
                m.new_lob = self.rng.choice([x for x in LOB_MIX if x != lob]) if self.rng.random() < 0.5 else None
                m.new_zip3 = self.rng.choice(ZIP3) if m.new_lob is None else None
            out.append(m)
        return out

    # ---------------- claims ----------------

    def _next_claim_id(self) -> str:
        self.claim_seq += 1
        return f"C{self.claim_seq:09d}"

    def _price(self, cpt: str, lob: str, units: int = 1) -> tuple[float, float, float]:
        allowed = CPT_PRICE[cpt] * units * LOB_PRICE[lob] * self.rng.uniform(0.9, 1.1)
        billed = allowed * self.rng.uniform(1.6, 3.2)
        paid = allowed * self.rng.uniform(0.75, 1.0)
        return round(billed, 2), round(allowed, 2), round(paid, 2)

    def _claim_rows(self, m: Member, providers: list[dict], kind: str, svc: date) -> list[tuple[date, list[dict]]]:
        """Returns [(file_date, [line dicts])...]: the original claim plus any later versions."""
        lob = m.lob_on(svc)
        pros = [p for p in providers if p["is_facility"] == "false"]
        facs = [p for p in providers if p["is_facility"] == "true"]
        admit = discharge = None
        if kind == "office":
            ctype, pos, prov = "PROFESSIONAL", "11", self.rng.choice(pros)
            dx = self.rng.choice(m.chronic) if m.chronic and self.rng.random() < 0.6 else self.rng.choice(OFFICE_DX)
            lines = [(self.rng.choice(["99213", "99214"]), 1)]
            if self.rng.random() < 0.4:
                lines += [("80053", 1), ("85025", 1)]
            svc_to = svc
        elif kind == "er":
            ctype, pos, prov = "INSTITUTIONAL", "23", self.rng.choice(facs)
            dx = self.rng.choice(ACUTE_ER)
            lines = [(self.rng.choice(["99284", "99285"]), 1)] + ([("70450", 1)] if self.rng.random() < 0.3 else [])
            svc_to = svc
        else:  # inpatient
            ctype, pos, prov = "INSTITUTIONAL", "21", self.rng.choice(facs)
            dx = self.rng.choice(m.chronic) if m.chronic and self.rng.random() < 0.7 else self.rng.choice(INPATIENT_DX)
            los = self.rng.randint(2, 7)
            svc_to = svc + timedelta(days=los)
            admit, discharge = svc, svc_to
            lines = [("99223", 1), ("99232", los - 1), ("99238", 1)]

        secondary = self.rng.choice(m.chronic) if m.chronic and dx not in m.chronic else ""
        denied = self.rng.random() < 0.03
        paid_date = svc_to + timedelta(days=self.rng.randint(7, 30))
        file_date = paid_date + timedelta(days=self.rng.randint(0, 2))
        if self.rng.random() < 0.05:  # late-arriving claim
            file_date = svc + timedelta(days=self.rng.randint(30, 90))
        claim_id = self._next_claim_id()
        rendering = self.rng.choice(pros)["npi"]

        def version(v: int, status: str, factor: float, when: date) -> list[dict]:
            rows = []
            for n, (cpt, units) in enumerate(lines, start=1):
                billed, allowed, paid = self._price(cpt, lob, units)
                paid = 0.0 if status == "DENIED" else round(paid * factor, 2)
                rows.append({
                    "claim_id": claim_id, "claim_version": v, "claim_status": status, "claim_type": ctype,
                    "member_id": m.member_id, "billing_npi": prov["npi"],
                    "service_from": svc.isoformat(), "service_to": svc_to.isoformat(),
                    "admit_date": admit.isoformat() if admit else "",
                    "discharge_date": discharge.isoformat() if discharge else "",
                    "place_of_service": pos, "primary_dx": dx, "secondary_dx": secondary,
                    "line_number": n, "procedure_code": cpt, "units": units,
                    "billed_amount": billed, "allowed_amount": allowed, "paid_amount": paid,
                    "paid_date": (when - timedelta(days=1)).isoformat(),
                    DRIFT_COLUMN: rendering,
                })  # fmt: skip
            return rows

        out = [(file_date, version(1, "DENIED" if denied else "PAID", 1.0, file_date))]
        if not denied and self.rng.random() < 0.06:  # adjustment or reversal, weeks later
            later = file_date + timedelta(days=self.rng.randint(10, 40))
            if self.rng.random() < 0.35:
                out.append((later, version(2, "REVERSED", 1.0, later)))
            else:
                out.append((later, version(2, "PAID", self.rng.uniform(0.7, 1.3), later)))
        return out

    def make_medical(self, members: list[Member], providers: list[dict]) -> list[Row]:
        rows: list[Row] = []
        day = self.history_start
        while day <= self.end:
            for m in members:
                if not m.covered(day):
                    continue
                lob = m.lob_on(day)
                sick = 1.0 + 1.5 * len(m.chronic)
                if self.rng.random() < 0.010 * sick:
                    kind = "office"
                elif self.rng.random() < (0.0012 if lob != "MEDICAID" else 0.0022) * sick:
                    kind = "er"
                elif self.rng.random() < 0.00025 * sick:
                    kind = "inpatient"
                else:
                    continue
                for file_date, lines in self._claim_rows(m, providers, kind, day):
                    rows += [Row(file_date, r) for r in lines]
                # readmissions: heart failure / COPD patients bounce back
                if kind == "inpatient" and {"I50.9", "J44.9"} & set(m.chronic) and self.rng.random() < 0.25:
                    back = day + timedelta(days=self.rng.randint(8, 28))
                    if m.covered(back):
                        for file_date, lines in self._claim_rows(m, providers, "inpatient", back):
                            rows += [Row(file_date, r) for r in lines]
            day += timedelta(days=1)
        return rows

    def make_pharmacy(self, members: list[Member], providers: list[dict]) -> list[Row]:
        rows: list[Row] = []
        pros = [p for p in providers if p["is_facility"] == "false"]
        for m in members:
            drugs = [d for d in DRUGS if d[3] in m.chronic]
            if self.rng.random() < 0.004:
                drugs.append(DRUGS[-1])  # rare specialty drug: drives high-cost members
            for ndc, name, base, _ in drugs:
                fill = self.history_start + timedelta(days=self.rng.randint(0, 29))
                while fill <= self.end:
                    if m.covered(fill):
                        self.rx_seq += 1
                        rows.append(Row(fill + timedelta(days=self.rng.randint(0, 2)), {
                            "rx_claim_id": f"R{self.rx_seq:09d}", "member_id": m.member_id, "ndc": ndc,
                            "drug_name": name, "fill_date": fill.isoformat(), "days_supply": 30,
                            "quantity": 30, "paid_amount": round(base * LOB_PRICE[m.lob_on(fill)]
                                                                 * self.rng.uniform(0.9, 1.1), 2),
                            "prescriber_npi": self.rng.choice(pros)["npi"],
                        }))  # fmt: skip
                    fill += timedelta(days=30 + self.rng.randint(-2, 5))
        return rows

    # ---------------- messiness ----------------

    def corrupt(self, rows: list[dict]) -> list[dict]:
        """Inject ~0.5% bad rows and ~1% exact duplicates into one delivery."""
        out = []
        for r in rows:
            r = dict(r)
            x = self.rng.random()
            if x < 0.001:
                r["primary_dx"] = "XYZ"
            elif x < 0.002:
                r["paid_amount"] = -abs(float(r["paid_amount"]) or 10.0)
                r["claim_status"] = "PAID"
            elif x < 0.003:
                r["member_id"] = "M99999999"
            elif x < 0.004:
                r["service_to"] = "2000-01-01"
            elif x < 0.005:
                r["claim_id"] = ""
            out.append(r)
            if self.rng.random() < 0.01:
                out.append(dict(r))
        return out


def _write_delivery(root: Path, source: str, day: date, columns: list[str], rows: list[dict], schema_version: int):
    folder = root / "landing" / source / f"dt={day.isoformat()}"
    folder.mkdir(parents=True, exist_ok=True)
    fname = f"{source}_{day:%Y%m%d}.csv"
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    data = buf.getvalue().encode()
    (folder / fname).write_bytes(data)
    manifest = {
        "source": source,
        "delivery_date": day.isoformat(),
        "file": fname,
        "rows": len(rows),
        "sha256": hashlib.sha256(data).hexdigest(),
        "schema_version": schema_version,
    }
    # the manifest is written last: its presence is the "delivery complete" signal
    (folder / "_manifest.json").write_text(json.dumps(manifest, indent=2))


def generate(root: Path, start: date, days: int, members: int = 2000, seed: int = 7,
             drift_on_day: int = 20) -> dict:  # fmt: skip
    """Write `days` daily deliveries starting at `start`. Returns row counts per source."""
    g = Generator(start, days, members, seed)
    providers = g.make_providers()
    roster = g.make_members()
    medical = g.make_medical(roster, providers)
    pharmacy = g.make_pharmacy(roster, providers)
    counts = {s: 0 for s in ("eligibility", "providers", "medical_claims", "pharmacy_claims")}

    def bucket(rows: list[Row]) -> dict[date, list[dict]]:
        by: dict[date, list[dict]] = {}
        for r in rows:
            # everything that "arrived" before the first delivery ships in it: the initial history load
            d = max(r.file_date, start)
            if d <= g.end:
                by.setdefault(d, []).append(r.values)
        return by

    med_by, rx_by = bucket(medical), bucket(pharmacy)
    for i in range(days):
        day = start + timedelta(days=i)
        elig = [m.as_of(day) for m in roster if m.coverage_start <= day]
        _write_delivery(root, "eligibility", day, ELIGIBILITY_COLUMNS, elig, 1)
        _write_delivery(root, "providers", day, PROVIDER_COLUMNS, providers, 1)
        drifted = i >= drift_on_day
        cols = MEDICAL_COLUMNS + ([DRIFT_COLUMN] if drifted else [])
        med = g.corrupt(sorted(med_by.get(day, []), key=lambda r: (r["claim_id"], r["line_number"])))
        _write_delivery(root, "medical_claims", day, cols, med, 2 if drifted else 1)
        _write_delivery(root, "pharmacy_claims", day, PHARMACY_COLUMNS, rx_by.get(day, []), 1)
        counts["eligibility"] += len(elig)
        counts["providers"] += len(providers)
        counts["medical_claims"] += len(med)
        counts["pharmacy_claims"] += len(rx_by.get(day, []))
    return counts
