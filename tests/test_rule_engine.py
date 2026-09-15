import sys

sys.path.insert(0, ".")

from processor.rule_engine import evaluate_rules


def test_low_risk_small_amount():
    r = evaluate_rules(amount=500, tx_count_last_minute=1)
    assert r["rule_score"] < 30 and r["level"] == "LOW"


def test_high_risk_all_signals():
    r = evaluate_rules(amount=180_000, tx_count_last_minute=7,
                       is_new_device=True, is_new_country=True)
    assert r["rule_score"] == 30 + 40 + 20 + 30
    assert r["level"] == "HIGH"
    assert len(r["reasons"]) == 4


def test_medium_boundary():
    r = evaluate_rules(amount=150_000)  # 30 pts
    assert r["level"] == "MEDIUM"
