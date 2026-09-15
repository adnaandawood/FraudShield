"""Feature store + feature engineering (Stage 3).

Redis layout per user:
    user:{id}:tx_times    -> sorted set of epoch seconds (last 24h)
    user:{id}:amounts     -> sorted set score=epoch, member={ts}:{amount} (last 24h)
    user:{id}:last_device -> string
    user:{id}:last_country-> string

Falls back to an in-memory store when Redis is unavailable so that
tests and local demos run without Docker.
"""
from __future__ import annotations

import time
from collections import defaultdict, deque

try:
    import redis  # type: ignore
except ImportError:  # pragma: no cover
    redis = None  # type: ignore

from config import REDIS_DB, REDIS_HOST, REDIS_PORT


# ---------------------------------------------------------------- in-memory fallback
class _MemoryStore:
    def __init__(self) -> None:
        self.tx_times: dict[str, deque] = defaultdict(deque)  # user -> [epochs]
        self.amounts: dict[str, deque] = defaultdict(deque)   # user -> [(epoch, amt)]
        self.last_device: dict[str, str] = {}
        self.last_country: dict[str, str] = {}

    def record(self, user_id: str, epoch: float, amount: float,
               device: str, country: str) -> None:
        self.tx_times[user_id].append(epoch)
        self.amounts[user_id].append((epoch, amount))
        self.last_device[user_id] = device
        self.last_country[user_id] = country

    def features(self, user_id: str, epoch: float, device: str,
                 country: str) -> dict:
        times = [t for t in self.tx_times[user_id] if epoch - t <= 24 * 3600]
        self.tx_times[user_id] = deque(times)
        amts = [(t, a) for t, a in self.amounts[user_id] if epoch - t <= 24 * 3600]
        self.amounts[user_id] = deque(amts)
        tx_1m = sum(1 for t in times if epoch - t <= 60)
        tx_5m = sum(1 for t in times if epoch - t <= 300)
        tx_24h = len(times)
        amt_24h = sum(a for _, a in amts)
        return {
            "tx_count_1m": tx_1m,
            "tx_count_5m": tx_5m,
            "tx_count_24h": tx_24h,
            "amount_24h": round(amt_24h, 2),
            "new_device": int(bool(self.last_device.get(user_id)) and self.last_device[user_id] != device),
            "new_country": int(bool(self.last_country.get(user_id)) and self.last_country[user_id] != country),
        }


_MEM = _MemoryStore()


def get_redis():
    if redis is None:
        return None
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
                        socket_connect_timeout=2, decode_responses=True)
        r.ping()
        return r
    except Exception:
        return None


def record_transaction(user_id: str, amount: float, device: str,
                       country: str, epoch: float | None = None) -> None:
    """Persist one event; updates last_device/last_country AFTER feature calc."""
    epoch = epoch or time.time()
    r = get_redis()
    if r is None:
        _MEM.record(user_id, epoch, amount, device, country)
        return
    pipe = r.pipeline()
    pipe.zadd(f"user:{user_id}:tx_times", {str(epoch): epoch})
    pipe.zadd(f"user:{user_id}:amounts", {f"{epoch}:{amount}": epoch})
    pipe.zremrangebyscore(f"user:{user_id}:tx_times", 0, epoch - 24 * 3600)
    pipe.zremrangebyscore(f"user:{user_id}:amounts", 0, epoch - 24 * 3600)
    pipe.expire(f"user:{user_id}:tx_times", 26 * 3600)
    pipe.expire(f"user:{user_id}:amounts", 26 * 3600)
    pipe.set(f"user:{user_id}:last_device", device, ex=30 * 24 * 3600)
    pipe.set(f"user:{user_id}:last_country", country, ex=30 * 24 * 3600)
    pipe.execute()


def get_features(user_id: str, device: str, country: str,
                 epoch: float | None = None) -> dict:
    """Features BEFORE recording the current txn (avoids self-counting)."""
    epoch = epoch or time.time()
    r = get_redis()
    if r is None:
        return _MEM.features(user_id, epoch, device, country)
    tx_times = r.zrangebyscore(f"user:{user_id}:tx_times", epoch - 24 * 3600, epoch, withscores=True)
    epochs = [s for _, s in tx_times]
    raw_amounts = r.zrangebyscore(f"user:{user_id}:amounts", epoch - 24 * 3600, epoch)
    amt_24h = 0.0
    for m in raw_amounts:
        try:
            amt_24h += float(m.rsplit(":", 1)[1])
        except (ValueError, IndexError):
            continue
    last_device = r.get(f"user:{user_id}:last_device")
    last_country = r.get(f"user:{user_id}:last_country")
    return {
        "tx_count_1m": sum(1 for t in epochs if epoch - t <= 60),
        "tx_count_5m": sum(1 for t in epochs if epoch - t <= 300),
        "tx_count_24h": len(epochs),
        "amount_24h": round(amt_24h, 2),
        "new_device": int(last_device is not None and last_device != device),
        "new_country": int(last_country is not None and last_country != country),
    }


def build_feature_vector(amount: float, features: dict) -> dict:
    """Flat dict the ML model consumes."""
    return {
        "amount": amount,
        "tx_count_5m": features.get("tx_count_5m", 0),
        "tx_count_24h": features.get("tx_count_24h", 0),
        "amount_24h": features.get("amount_24h", 0.0),
        "new_device": features.get("new_device", 0),
        "new_country": features.get("new_country", 0),
    }
