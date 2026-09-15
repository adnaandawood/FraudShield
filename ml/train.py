"""Train a baseline fraud model (Stage 4).

Two modes:
  1. Real data:  python -m ml.train --csv data/creditcard.csv
     (expects Kaggle credit-card-fraud columns incl. `Class`)
  2. Synthetic:  python -m ml.train --synthetic 20000   (default, no download needed)

Models: LogisticRegression -> RandomForest (best F1 kept). Saves:
    ml/model.pkl     (sklearn pipeline: scaler + classifier)
    ml/metrics.json  (precision/recall/F1/PR-AUC/ROC-AUC)
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, ".")


def synthetic_data(n: int = 20000):
    import numpy as np
    import pandas as pd

    rng = np.random.default_rng(42)
    amount = rng.lognormal(mean=7.5, sigma=1.0, size=n)  # ~Rs.1.8k median
    tx5 = rng.poisson(0.6, n)
    tx24 = tx5 + rng.poisson(2.0, n)
    amt24 = amount * (1 + rng.poisson(2, n) * 0.6)
    nd = rng.binomial(1, 0.08, n)
    nc = rng.binomial(1, 0.05, n)
    # Latent fraud logit: big amount + velocity + anomalies
    logit = (
        -6.0
        + 1.6 * (amount > 100_000)
        + 0.9 * (amount > 30_000)
        + 0.35 * np.minimum(tx5, 8)
        + 0.9 * nd
        + 1.2 * nc
        + rng.normal(0, 0.6, n)
    )
    prob = 1 / (1 + np.exp(-logit))
    y = (rng.random(n) < prob).astype(int)
    X = pd.DataFrame({
        "amount": amount, "tx_count_5m": tx5, "tx_count_24h": tx24,
        "amount_24h": amt24, "new_device": nd, "new_country": nc,
    })
    return X, y


def evaluate(y_true, y_prob, threshold: float = 0.5) -> dict:
    from sklearn.metrics import (average_precision_score, f1_score,
                                 precision_score, recall_score,
                                 roc_auc_score)

    y_pred = (y_prob >= threshold).astype(int)
    return {
        "threshold": threshold,
        "precision": round(float(precision_score(y_true, y_pred, zero_division=0)), 4),
        "recall": round(float(recall_score(y_true, y_pred, zero_division=0)), 4),
        "f1": round(float(f1_score(y_true, y_pred, zero_division=0)), 4),
        "pr_auc": round(float(average_precision_score(y_true, y_prob)), 4),
        "roc_auc": round(float(roc_auc_score(y_true, y_prob)), 4),
        "fraud_rate": round(float(y_true.mean()), 5),
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--csv", default=None, help="path to Kaggle creditcard.csv")
    p.add_argument("--synthetic", type=int, default=20000)
    p.add_argument("--out", default="ml/model.pkl")
    args = p.parse_args()

    import pandas as pd
    from sklearn.linear_model import LogisticRegression
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler
    import joblib

    if args.csv and os.path.exists(args.csv):
        df = pd.read_csv(args.csv)
        print(f"Loaded {args.csv}: {df.shape}, fraud_rate={df['Class'].mean():.5f}")
        # Map Kaggle V1..V28+Amount onto our 6-feature contract where possible;
        # extra columns are ignored by vectorize() at inference, so keep it simple:
        raise SystemExit(
            "Kaggle CSV has PCA features (V1..V28), not our streaming features. "
            "Recommended: use --synthetic for the streaming-model baseline, and use "
            "the Kaggle set as a separate notebook experiment for PR-AUC comparison."
        )
    else:
        X, y = synthetic_data(args.synthetic)
        print(f"Synthetic data: {X.shape}, fraud_rate={y.mean():.4f}")

    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)

    candidates = {
        "logreg": make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000, class_weight="balanced")),
        "random_forest": RandomForestClassifier(n_estimators=200, min_samples_leaf=5,
                                                class_weight="balanced_subsample", n_jobs=-1, random_state=42),
    }
    best = None
    for name, model in candidates.items():
        model.fit(Xtr, ytr)
        prob = model.predict_proba(Xte)[:, 1]
        m = evaluate(yte, prob)
        print(f"{name:14s} P={m['precision']} R={m['recall']} F1={m['f1']} PR-AUC={m['pr_auc']} ROC-AUC={m['roc_auc']}")
        m["model"] = name
        if best is None or m["f1"] > best["f1"]:
            best = m
            best_model = model

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    joblib.dump(best_model, args.out)
    with open("ml/metrics.json", "w") as f:
        json.dump(best, f, indent=2)
    print(f"Saved {args.out} + ml/metrics.json :: {best}")


if __name__ == "__main__":
    main()
