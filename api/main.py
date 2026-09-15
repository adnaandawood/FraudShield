"""FastAPI ingress + review queue (Stages 1 & 6).

POST /transactions  -> validate, publish to Kafka (or score inline if Kafka down)
GET  /health        -> dependency status
GET  /alerts        -> recent REVIEW/BLOCK decisions (in-memory ring + Postgres attempt)
POST /alerts/{id}/review -> mark alert reviewed (Postgres when available)
"""
from __future__ import annotations

import json
import sys
import time
from collections import deque
from datetime import datetime, timezone

sys.path.insert(0, ".")

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, Response

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, Transaction

try:
    from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

    INGEST_COUNTER = Counter("api_ingest_total", "Ingested transactions", ["outcome"])
    METRICS_OK = True
except ImportError:  # pragma: no cover
    METRICS_OK = False

app = FastAPI(title="Fraud Detection API", version="0.1.0")
# Allow the static dashboard (opened via file:// or any host) to fetch the API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)
RECENT: deque = deque(maxlen=200)  # in-memory fallback review queue


def _kafka_send(tx: dict) -> bool:
    try:
        from kafka import KafkaProducer

        prod = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            value_serializer=lambda v: json.dumps(v).encode(),
            key_serializer=lambda k: str(k).encode() if k else None,
            retries=2, request_timeout_ms=3000,
        )
        prod.send(KAFKA_TOPIC, key=tx["userId"], value=tx).get(timeout=5)
        prod.close()
        return True
    except Exception:
        return False


@app.get("/health")
def health():
    status = {"api": "ok", "time": datetime.now(timezone.utc).isoformat()}
    # Dependency probes (non-fatal)
    try:
        from kafka import KafkaProducer

        p = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                          request_timeout_ms=2000)
        p.close()
        status["kafka"] = "up"
    except Exception:
        status["kafka"] = "down"
    try:
        import redis

        from config import REDIS_HOST, REDIS_PORT

        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, socket_connect_timeout=1)
        r.ping()
        status["redis"] = "up"
    except Exception:
        status["redis"] = "down"
    return status


class IngestRequest(Transaction):
    pass


@app.post("/transactions", status_code=202)
def ingest(tx: IngestRequest):
    data = tx.to_kafka_dict()
    if _kafka_send(data):
        if METRICS_OK:
            INGEST_COUNTER.labels(outcome="queued").inc()
        return {"status": "queued", "transactionId": data["transactionId"]}
    # Kafka down -> score inline so the API is still demoable
    from processor.consumer import score_transaction

    scored = score_transaction(data)
    RECENT.appendleft({
        "id": len(RECENT) + 1,
        "transaction": data,
        "decision": scored["decision"],
        "risk_score": scored["final_score"],
        "reasons": scored["reasons"],
        "status": "OPEN",
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    if METRICS_OK:
        INGEST_COUNTER.labels(outcome="scored_inline").inc()
    return {"status": "scored-inline", "decision": scored["decision"],
            "risk_score": scored["final_score"], "reasons": scored["reasons"]}


@app.get("/alerts")
def alerts(limit: int = 20):
    """Recent REVIEW/BLOCK items. Tries Postgres, falls back to memory."""
    try:
        from sqlalchemy import create_engine, text

        from config import DATABASE_URL

        eng = create_engine(DATABASE_URL)
        with eng.connect() as c:
            rows = c.execute(text(
                "SELECT alert_id, transaction_id, risk_score, reason, status, created_at "
                "FROM fraud_alerts ORDER BY alert_id DESC LIMIT :lim"), {"lim": limit}).mappings().all()
            return {"source": "postgres", "alerts": [dict(r) for r in rows]}
    except Exception:
        return {"source": "memory", "alerts": list(RECENT)[:limit]}


@app.get("/metrics")
def metrics():
    """Prometheus exposition endpoint (what Prometheus scrapes)."""
    if not METRICS_OK:
        return PlainTextResponse("prometheus_client not installed\n", status_code=503)
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/metrics-text")
def metrics_text():
    if not RECENT:
        return PlainTextResponse("no decisions yet")
    decisions = [a["decision"] for a in RECENT]
    lines = [f"recent_{d.lower()} {decisions.count(d)}" for d in set(decisions)]
    return PlainTextResponse("\n".join(lines) + "\n")


if __name__ == "__main__":  # python -m api.main
    import os

    import uvicorn

    # NOTE: reload left off by default — the Windows reloader tends to wedge.
    # Set API_RELOAD=1 explicitly if you want autoreload during development.
    _reload = os.getenv("API_RELOAD", "0") == "1"
    # Pass the app OBJECT (not "api.main:app") when not reloading: otherwise this
    # process imports the module twice (as __main__ and as api.main) and the
    # module-level Prometheus Counter registers twice -> DuplicateTimeseries.
    uvicorn.run("api.main:app" if _reload else app, host="0.0.0.0",
                port=int(os.getenv("API_PORT", "8000")), reload=_reload)
