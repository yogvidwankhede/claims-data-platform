import json

from claims_platform import quality


def _reports(root, quarantined_medical: int):
    for source, step in quality._SILVER_STEP.items():
        v = root / "reports" / "validation" / source
        v.mkdir(parents=True, exist_ok=True)
        (v / "2026-01-05.json").write_text(json.dumps({"status": "ACCEPTED", "rows": 1000, "drift": []}))
        r = root / "reports" / "runs" / "2026-01-05"
        r.mkdir(parents=True, exist_ok=True)
        q = quarantined_medical if source == "medical_claims" else 0
        (r / f"{step}.json").write_text(json.dumps({"quarantined": q}))


def test_healthy_day_passes_and_is_written(tmp_path, monkeypatch):
    monkeypatch.setenv("CLAIMS_DATA_DIR", str(tmp_path))
    _reports(tmp_path, quarantined_medical=15)
    card = quality.build("2026-01-05")
    assert card.status == "PASS" and card.sources["medical_claims"]["quarantine_rate"] == 0.015
    assert json.loads((tmp_path / "reports" / "dq" / "2026-01-05.json").read_text())["status"] == "PASS"


def test_degraded_feed_is_flagged(tmp_path, monkeypatch):
    monkeypatch.setenv("CLAIMS_DATA_DIR", str(tmp_path))
    _reports(tmp_path, quarantined_medical=50)
    card = quality.build("2026-01-05")
    assert card.status == "DEGRADED" and "medical_claims: quarantine rate 5.00% > 2%" in card.breaches


def test_missing_validation_is_a_breach(tmp_path, monkeypatch):
    monkeypatch.setenv("CLAIMS_DATA_DIR", str(tmp_path))
    card = quality.build("2026-01-05")
    assert card.status == "DEGRADED" and len(card.breaches) == 4
