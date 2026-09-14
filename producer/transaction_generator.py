"""Fake transaction generator. No Kafka/Redis needed to run this."""
from __future__ import annotations

import random
import time
import uuid
from datetime import datetime, timezone

MERCHANTS = [
    ("Amazon", 500, 8000),
    ("Swiggy", 150, 900),
    ("Flipkart", 400, 20000),
    ("IRCTC", 300, 5000),
    ("Electronics Store", 5000, 200000),
    ("Jewellery Shop", 20000, 500000),
]
LOCATIONS = [
    ("Bengaluru", "IN"),
    ("Mumbai", "IN"),
    ("Delhi", "IN"),
    ("Hyderabad", "IN"),
    ("Moscow", "RU"),
    ("Lagos", "NG"),
    ("Singapore", "SG"),
]
DEVICES = [f"device_{i}" for i in range(1, 8)]
USERS = [f"user_{i}" for i in range(1, 50)]


def generate_transaction(
    user_id: str | None = None,
    force_fraud: bool = False,
) -> dict:
    """Return one transaction dict matching config.Transaction schema."""
    merchant, lo, hi = random.choice(MERCHANTS)
    location, country = random.choice(LOCATIONS)
    amount = round(random.uniform(lo, hi), 2)

    if force_fraud:
        # Make it obviously suspicious: huge amount + foreign country + rare device
        amount = round(random.uniform(100_000, 450_000), 2)
        location, country = random.choice([("Moscow", "RU"), ("Lagos", "NG")])
        merchant = random.choice(["Electronics Store", "Jewellery Shop"])

    return {
        "transactionId": f"txn_{uuid.uuid4().hex[:8]}",
        "userId": user_id or random.choice(USERS),
        "amount": amount,
        "currency": "INR",
        "merchant": merchant,
        "location": location,
        "country": country,
        "deviceId": random.choice(DEVICES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def generate_batch(n: int, fraud_ratio: float = 0.05) -> list[dict]:
    return [
        generate_transaction(force_fraud=(random.random() < fraud_ratio))
        for _ in range(n)
    ]


if __name__ == "__main__":  # quick demo: python -m producer.transaction_generator
    import argparse
    import json

    p = argparse.ArgumentParser(description="Print fake transactions")
    p.add_argument("--count", type=int, default=5)
    p.add_argument("--fraud-ratio", type=float, default=0.2)
    args = p.parse_args()
    for tx in generate_batch(args.count, args.fraud_ratio):
        print(json.dumps(tx))
        time.sleep(0.1)
