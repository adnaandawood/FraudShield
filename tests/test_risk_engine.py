import sys

sys.path.insert(0, ".")

from processor.risk_engine import aggregate


def test_approve_review_block():
    assert aggregate(10, None)["decision"] == "APPROVE"
    assert aggregate(40, 0.4)["decision"] == "REVIEW"
    assert aggregate(70, 0.83)["decision"] == "BLOCK"


def test_rules_only_high():
    assert aggregate(90, None)["decision"] == "BLOCK"
