"""Risk aggregator: rules + ML -> APPROVE / REVIEW / BLOCK (Stage 5)."""
from __future__ import annotations

import os

RULE_WEIGHT = float(os.getenv("RULE_WEIGHT", "0.5"))
ML_WEIGHT = float(os.getenv("ML_WEIGHT", "0.5"))

APPROVE_BELOW = 0.30
BLOCK_ABOVE = 0.70


def aggregate(rule_score_0_100: int, ml_probability: float | None = None,
              rule_weight: float = RULE_WEIGHT,
              ml_weight: float = ML_WEIGHT) -> dict:
    rule_norm = max(0, min(100, rule_score_0_100)) / 100.0
    if ml_probability is None:  # rules-only mode (Stage 2/3)
        final = rule_norm
    else:
        ml_p = max(0.0, min(1.0, ml_probability))
        total = rule_weight + ml_weight
        final = (rule_norm * rule_weight + ml_p * ml_weight) / total

    if final < APPROVE_BELOW:
        decision = "APPROVE"
    elif final > BLOCK_ABOVE:
        decision = "BLOCK"
    else:
        decision = "REVIEW"
    return {"final_score": round(final, 4), "decision": decision}


if __name__ == "__main__":
    print(aggregate(10, None))        # APPROVE
    print(aggregate(70, 0.83))        # BLOCK-ish
    print(aggregate(40, 0.4))         # REVIEW
