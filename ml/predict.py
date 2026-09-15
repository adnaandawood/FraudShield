"""Heuristic ML scorer used before a real model is trained.

If ml/model.pkl exists (see ml/train.py) it is used; otherwise a
transparent amount+velocity heuristic returns a probability so the
pipeline works end-to-end from day one.
"""
from __future__ import annotations

import os

from ml.features import FEATURE_COLUMNS, vectorize

_MODEL = None
_MODEL_PATH = os.getenv("MODEL_PATH", "ml/model.pkl")


def _load():
    global _MODEL
    if _MODEL is not None:
        return _MODEL
    if not os.path.exists(_MODEL_PATH):
        return None
    try:
        import joblib

        _MODEL = joblib.load(_MODEL_PATH)
        print(f"Loaded ML model from {_MODEL_PATH}")
    except Exception as e:
        print(f"Could not load model ({e}); using heuristic.")
        _MODEL = None
    return _MODEL


def score_ml(feature_vector: dict) -> float | None:
    model = _load()
    if model is not None:
        try:
            import pandas as pd  # keep feature names -> no sklearn warnings

            X = pd.DataFrame([vectorize(feature_vector)], columns=FEATURE_COLUMNS)
            proba = model.predict_proba(X)[0]
            # assume binary classes [legit, fraud]; take fraud class
            return float(proba[1] if len(proba) > 1 else proba[0])
        except Exception:
            pass
    # Heuristic fallback: high amount + velocity + anomalies -> high proba
    amt = feature_vector.get("amount", 0)
    v5 = feature_vector.get("tx_count_5m", 0)
    nd = feature_vector.get("new_device", 0)
    nc = feature_vector.get("new_country", 0)
    s = 0.02
    if amt > 100_000:
        s += 0.45
    elif amt > 30_000:
        s += 0.2
    s += min(v5, 10) * 0.04
    s += 0.1 * nd + 0.15 * nc
    return round(min(max(s, 0.01), 0.99), 4)
