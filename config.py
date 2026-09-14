"""Shared settings + transaction schema. No infra imports here."""
from __future__ import annotations

import os
from datetime import datetime, timezone

from pydantic import BaseModel, Field

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions")
KAFKA_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "fraud-processor-v1")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://fraud:fraudpass@localhost:5433/frauddb",
)

MODEL_PATH = os.getenv("MODEL_PATH", "ml/model.pkl")
RULE_WEIGHT = float(os.getenv("RULE_WEIGHT", "0.5"))
ML_WEIGHT = float(os.getenv("ML_WEIGHT", "0.5"))


class Transaction(BaseModel):
    transactionId: str = Field(alias="transactionId")
    userId: str = Field(alias="userId")
    amount: float
    currency: str = "INR"
    merchant: str = "Unknown"
    # Support both styles: `country` (IN/RU/...) and `location` (Bengaluru/...)
    country: str = "IN"
    location: str = ""
    deviceId: str = Field(default="unknown", alias="deviceId")
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    model_config = {"populate_by_name": True}

    def to_kafka_dict(self) -> dict:
        return self.model_dump(by_alias=True)


class ScoredTransaction(BaseModel):
    transaction: Transaction
    risk_score: float  # 0..1
    level: str         # LOW | MEDIUM | HIGH
    decision: str      # APPROVE | REVIEW | BLOCK
    reasons: list[str] = []
    ml_probability: float | None = None
    rule_score: int = 0
