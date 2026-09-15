"""Fraud stream processor: Kafka -> Redis features -> rules (+ML) -> Postgres.

Usage:
    python -m processor.consumer            # full pipeline
    python -m processor.consumer --no-kafka # demo mode: score synthetic stream

Metrics exposed on :8001 (/metrics) for Prometheus.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, ".")

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP, KAFKA_TOPIC
from processor import feature_engine, risk_engine, rule_engine

try:
    from ml.predict import score_ml
except Exception:
    def score_ml(_features: dict):
        return None

try:
    from prometheus_client import Counter, Histogram, start_http_server
    TXN_COUNTER = Counter("fraud_transactions_total", "Transactions processed", ["decision"])
    FRAUD_COUNTER = Counter("fraud_blocked_total", "Blocked transactions")
    LATENCY = Histogram("fraud_processing_latency_seconds", "End-to-end scoring latency")
    METRICS_OK = True
except ImportError:
    METRICS_OK = False


def parse_time(ts: str) -> float:
    try:
        return datetime.fromisoformat(ts).timestamp()
    except Exception:
        return time.time()


# ---------------------------------------------------------------- persistence (optional)
def persist(scored: dict) -> None:
    """Best-effort Postgres write; silently skips if DB is down."""
    try:
        from sqlalchemy import create_engine, text  # lazy import

        from config import DATABASE_URL

        eng = create_engine(DATABASE_URL, pool_pre_ping=True)
        tx = scored["transaction"]
        with eng.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO transactions "
                    "(transaction_id, user_id, amount, currency, merchant, country,"
                    " location, device_id, ts, risk_score, decision, reasons) "
                    "VALUES (:tid, :uid, :amt, :cur, :mer, :cou, :loc, :dev, :ts, :rs, :dec, CAST(:rea AS JSONB)) "
                    "ON CONFLICT (transaction_id) DO NOTHING"
                ),
                {
                    "tid": tx["transactionId"], "uid": tx["userId"],
                    "amt": tx["amount"], "cur": tx.get("currency", "INR"),
                    "mer": tx.get("merchant", ""), "cou": tx.get("country", "IN"),
                    "loc": tx.get("location", ""), "dev": tx.get("deviceId", ""),
                    "ts": datetime.fromtimestamp(parse_time(tx.get("timestamp", "")), tz=timezone.utc),
                    "rs": scored["final_score"], "dec": scored["decision"],
                    "rea": json.dumps(scored.get("reasons", [])),
                },
            )
            if scored["decision"] in ("REVIEW", "BLOCK"):
                conn.execute(
                    text(
                        "INSERT INTO fraud_alerts (transaction_id, risk_score, reason) "
                        "VALUES (:tid, :rs, :rea)"
                    ),
                    {"tid": tx["transactionId"], "rs": scored["final_score"],
                     "rea": "; ".join(scored.get("reasons", []))[:500]},
                )
    except Exception as e:
        print(f"[persist skipped] {e}")


# ---------------------------------------------------------------- core scoring
def score_transaction(tx: dict) -> dict:
    t0 = time.time()
    epoch = parse_time(tx.get("timestamp", ""))
    user = tx.get("userId", "unknown")
    device = tx.get("deviceId", "unknown")
    country = tx.get("country", "IN")

    feats = feature_engine.get_features(user, device, country, epoch)
    rule = rule_engine.evaluate_rules(
        amount=float(tx.get("amount", 0)),
        tx_count_last_minute=feats["tx_count_1m"],
        is_new_device=bool(feats["new_device"]),
        is_new_country=bool(feats["new_country"]),
    )
    ml_p = score_ml(feature_engine.build_feature_vector(float(tx.get("amount", 0)), feats))
    agg = risk_engine.aggregate(rule["rule_score"], ml_p)

    scored = {
        "transaction": tx,
        "features": feats,
        "rule_score": rule["rule_score"],
        "rule_level": rule["level"],
        "ml_probability": ml_p,
        "final_score": agg["final_score"],
        "decision": agg["decision"],
        "reasons": rule["reasons"] + ([f"ML fraud probability {ml_p:.2f}"] if ml_p is not None and ml_p > 0.5 else []),
        "latency_ms": round((time.time() - t0) * 1000, 2),
    }
    # Record AFTER scoring so current txn doesn't inflate its own velocity.
    feature_engine.record_transaction(user, float(tx.get("amount", 0)), device, country, epoch)
    return scored


def run_kafka_loop() -> None:
    import traceback

    from kafka import KafkaConsumer

    backoff = 2
    while True:  # reconnect loop: survives rebalances / broker restarts
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id=KAFKA_GROUP,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            print(f"Listening on Kafka {KAFKA_BOOTSTRAP_SERVERS} topic={KAFKA_TOPIC} ...")
            backoff = 2
            for msg in consumer:
                t0 = time.time()
                scored = score_transaction(msg.value)
                persist(scored)
                tx = scored["transaction"]
                print(
                    f"[{scored['decision']:7s} score={scored['final_score']:.2f}] "
                    f"{tx.get('transactionId')} {tx.get('userId')} "
                    f"Rs.{float(tx.get('amount', 0)):,.0f} reasons={scored['reasons'] or ['clean']}",
                    flush=True,
                )
                if METRICS_OK:
                    TXN_COUNTER.labels(decision=scored["decision"]).inc()
                    if scored["decision"] == "BLOCK":
                        FRAUD_COUNTER.inc()
                    LATENCY.observe(time.time() - t0)
        except KeyboardInterrupt:
            break
        except Exception:
            traceback.print_exc()
            print(f"Consumer crashed, retrying in {backoff}s ... (new group? set KAFKA_CONSUMER_GROUP)")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


def run_demo_loop(n: int = 20) -> None:
    from producer.transaction_generator import generate_batch

    print("Kafka not requested/available — demo mode (synthetic stream, no Kafka).")
    for tx in generate_batch(n, fraud_ratio=0.3):
        scored = score_transaction(tx)
        print(
            f"[{scored['decision']:7s} score={scored['final_score']:.2f}] "
            f"{tx['transactionId']} {tx['userId']} Rs.{tx['amount']:,.0f} "
            f"reasons={scored['reasons'] or ['clean']}"
        )
        time.sleep(0.05)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--no-kafka", action="store_true", help="score synthetic data w/o Kafka")
    p.add_argument("--metrics-port", type=int, default=8001)
    p.add_argument("--no-persist", action="store_true", help="skip Postgres writes")
    args = p.parse_args()

    global persist
    if args.no_persist:
        persist = lambda scored: None

    if METRICS_OK and not args.no_kafka:
        try:
            start_http_server(args.metrics_port)
            print(f"Metrics on :{args.metrics_port}/metrics")
        except Exception as e:
            print(f"Metrics server skipped: {e}")

    if args.no_kafka:
        run_demo_loop()
        return
    run_kafka_loop()  # never returns; reconnects internally (Ctrl+C to stop)


if __name__ == "__main__":
    main()
