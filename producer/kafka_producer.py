"""Publish transactions to Kafka. Falls back to dry-run printing if Kafka is down.

Usage:
    python -m producer.kafka_producer --count 100 --rate 50
    python -m producer.kafka_producer --count 1000 --rate 500 --fraud-ratio 0.05
"""
from __future__ import annotations

import argparse
import json
import sys
import time

sys.path.insert(0, ".")

from producer.transaction_generator import generate_transaction

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC


def get_producer():
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k else None,
        retries=3,
        linger_ms=10,
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Transaction Kafka producer")
    p.add_argument("--count", type=int, default=100, help="total transactions to send")
    p.add_argument("--rate", type=float, default=50, help="transactions per second")
    p.add_argument("--fraud-ratio", type=float, default=0.05)
    p.add_argument("--dry-run", action="store_true", help="print instead of sending")
    args = p.parse_args()

    producer = None
    if not args.dry_run:
        try:
            producer = get_producer()
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}, topic={KAFKA_TOPIC}")
        except Exception as e: 
            print(f"Kafka unavailable ({e}). Falling back to dry-run printing.")
            producer = None

    delay = 1.0 / args.rate if args.rate > 0 else 0
    sent = 0
    t0 = time.time()
    try:
        for _ in range(args.count):
            import random

            tx = generate_transaction(
                force_fraud=(random.random() < args.fraud_ratio)
            )
            if producer is None:
                print(json.dumps(tx))
            else:
                producer.send(KAFKA_TOPIC, key=tx["userId"], value=tx)
            sent += 1
            if delay:
                time.sleep(delay)
    except KeyboardInterrupt:
        pass
    finally:
        if producer is not None:
            producer.flush(10)
            producer.close()
    elapsed = time.time() - t0
    print(f"Sent {sent} txns in {elapsed:.1f}s ({sent / elapsed:.1f} txn/s)")


if __name__ == "__main__":
    main()
