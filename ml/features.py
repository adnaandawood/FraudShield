"""Shared ML feature contract. Keep COLUMN ORDER stable — model depends on it."""
from __future__ import annotations

FEATURE_COLUMNS = [
    "amount",
    "tx_count_5m",
    "tx_count_24h",
    "amount_24h",
    "new_device",
    "new_country",
]


def vectorize(fv: dict) -> list[float]:
    return [float(fv.get(c, 0)) for c in FEATURE_COLUMNS]
