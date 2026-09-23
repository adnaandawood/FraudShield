# Real-Time Fraud Detection (Kafka + Redis + Postgres + ML)

Streaming fraud scoring in stages — start with infrastructure, add ML later.

```
Producer → Kafka (transactions) → Processor → Redis features + Rule engine (+ML)
        → Risk aggregator → APPROVE / REVIEW / BLOCK → Postgres → Dashboard
```

## Project layout

```
fraud-detection/
├── producer/            # transaction_generator.py, kafka_producer.py
├── processor/           # consumer.py, feature_engine.py, rule_engine.py, risk_engine.py
├── ml/                  # train.py, features.py, predict.py
├── api/                 # FastAPI ingress + review queue (main.py)
├── dashboard/           # static review-queue page (index.html)
├── db/schema.sql        # Postgres tables
├── tests/               # pytest, no infra needed
├── docker-compose.yml   # Kafka + Redis + Postgres + Prometheus + Grafana
└── README.md
```

## Prerequisites

- Python 3.11+
- Docker + Docker Compose

## 0. Setup

```powershell
cd <Path-To-Repo>
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
docker compose up -d
docker compose ps
```

Kafka topic `transactions` is auto-created by `kafka-init`. Redis `:6379`, Postgres `:5433` (host — mapped from container 5432 because a local Postgres often already owns 5432), Prometheus `:9090`, Grafana `:3000` (admin/admin).

## Stage 1 — Producer → Kafka → Consumer (print)

Terminal 1 — consumer (use `-u` so live logs aren't buffered):
```powershell
python -u -m processor.consumer
```

Terminal 2 — producer (100 txns @ 50/s):
```powershell
python -m producer.kafka_producer --count 100 --rate 50
```

Expect: `Received`-style scored lines like `[APPROVE  score=0.02] txn_… user_3 Rs.500 reasons=['clean']`.

No-Kafka demo (works without Docker):
```powershell
python -m producer.transaction_generator --count 5
python -m processor.consumer --no-kafka --no-persist
python -m producer.kafka_producer --count 10 --dry-run
```

## Stage 2 — Rule engine

```powershell
python -m processor.rule_engine
pytest tests/test_rule_engine.py -v
```

Rules: amount>₹100k +30, velocity>5/min +40, new device +20, new country +30. `<30 LOW`, `30–60 MEDIUM`, `>60 HIGH`.

## Stage 3 — Redis features

```powershell
pytest tests/test_feature_engine.py -v
```

Per-user sorted sets track `tx_count_1m/5m/24h`, `amount_24h`, `new_device`, `new_country`. Falls back to in-memory when Redis is down.

## Stage 4 — ML baseline (no download needed)

```powershell
python -m ml.train --synthetic 20000
cat ml/metrics.json
```

Then the processor auto-loads `ml/model.pkl` via `ml/predict.py`. Before training, a transparent heuristic is used so the pipeline works end-to-end.

> Imbalanced-data note: never use accuracy. Report precision/recall/F1/PR-AUC/ROC-AUC (see `ml/metrics.json`).

## Stage 5 — Rules + ML

`processor/risk_engine.py`: `final = (rule_norm*RULE_W + ml*ML_W)/(sum)`, `<0.30 APPROVE`, `0.30–0.70 REVIEW`, `>0.70 BLOCK`. Tune via `RULE_WEIGHT` / `ML_WEIGHT` in `.env`.

## Stage 6 — API + Postgres + dashboard

```powershell
python -m api.main   # :8000
```

- `POST /transactions` → publishes to Kafka (or scores inline if Kafka down)
- `GET /alerts` → review queue (Postgres, else in-memory)
- Open `dashboard/index.html` in a browser for the analyst view.

```powershell
curl -X POST localhost:8000/transactions -H "Content-Type: application/json" `
  -d '{"transactionId":"txn_1001","userId":"user_42","amount":185000,"currency":"INR","merchant":"Electronics Store","location":"Bengaluru","country":"IN","deviceId":"device_91"}'
```

## Stage 7 — Monitoring

- Processor metrics: `localhost:8001/metrics` (transactions/sec, latency, blocks)
- Prometheus: `localhost:9090`, Grafana: `localhost:3000`

## Stage 8 — Load test

```powershell
python -m producer.kafka_producer --count 1000 --rate 500
python -m producer.kafka_producer --count 5000 --rate 1000
```

Watch consumer latency, Kafka lag (`docker exec fraud-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group fraud-processor-v1`), Redis latency, and ML inference time. Record throughput/latency numbers in your README results table.

## Run all tests

```powershell
pytest -v
```
