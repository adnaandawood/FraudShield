"""Deterministic rule engine (Stage 2). Pure functions — no Kafka/Redis needed.

Scoring (matches the design doc):
    Large amount (>100k INR)      +30
    New device                    +20
    New country                   +30
    Velocity (>5 txns in 1 min)   +40

    risk < 30   -> LOW
    30-60       -> MEDIUM
    > 60        -> HIGH
"""
from __future__ import annotations

LARGE_AMOUNT_THRESHOLD = 100_000
VELOCITY_COUNT_THRESHOLD = 5  # txns in the last minute

SCORE_LARGE_AMOUNT = 30
SCORE_NEW_DEVICE = 20
SCORE_NEW_COUNTRY = 30
SCORE_VELOCITY = 40


def evaluate_rules(
    amount: float,
    tx_count_last_minute: int = 0,
    is_new_device: bool = False,
    is_new_country: bool = False,
    large_amount_threshold: float = LARGE_AMOUNT_THRESHOLD,
) -> dict:
    reasons: list[str] = []
    score = 0

    if amount > large_amount_threshold:
        score += SCORE_LARGE_AMOUNT
        reasons.append(f"Large amount: Rs.{amount:,.0f} > Rs.{large_amount_threshold:,.0f}")

    if tx_count_last_minute > VELOCITY_COUNT_THRESHOLD:
        score += SCORE_VELOCITY
        reasons.append(
            f"Velocity: {tx_count_last_minute} txns in last minute "
            f"(> {VELOCITY_COUNT_THRESHOLD})"
        )

    if is_new_device:
        score += SCORE_NEW_DEVICE
        reasons.append("New device for this user")

    if is_new_country:
        score += SCORE_NEW_COUNTRY
        reasons.append("New country for this user")

    if score < 30:
        level = "LOW"
    elif score <= 60:
        level = "MEDIUM"
    else:
        level = "HIGH"

    return {"rule_score": score, "level": level, "reasons": reasons}


if __name__ == "__main__":  # tiny manual demo
    examples = [
        {"amount": 500, "tx_count_last_minute": 1},
        {"amount": 180_000, "tx_count_last_minute": 7,
         "is_new_device": True, "is_new_country": True},
    ]
    for e in examples:
        print(e, "->", evaluate_rules(**e))
