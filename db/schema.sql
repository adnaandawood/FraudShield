-- Fraud detection durable schema
CREATE TABLE IF NOT EXISTS transactions (
  transaction_id TEXT PRIMARY KEY,
  user_id        TEXT NOT NULL,
  amount         NUMERIC NOT NULL,
  currency       TEXT NOT NULL DEFAULT 'INR',
  merchant       TEXT NOT NULL DEFAULT '',
  country        TEXT NOT NULL DEFAULT 'IN',
  location       TEXT NOT NULL DEFAULT '',
  device_id      TEXT NOT NULL DEFAULT '',
  ts             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  risk_score     DOUBLE PRECISION NOT NULL DEFAULT 0,
  decision       TEXT NOT NULL DEFAULT 'APPROVE',
  reasons        JSONB NOT NULL DEFAULT '[]',
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_tx_user_ts ON transactions (user_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_tx_decision ON transactions (decision);

CREATE TABLE IF NOT EXISTS fraud_alerts (
  alert_id       BIGSERIAL PRIMARY KEY,
  transaction_id TEXT NOT NULL REFERENCES transactions(transaction_id),
  risk_score     DOUBLE PRECISION NOT NULL,
  reason         TEXT NOT NULL DEFAULT '',
  status         TEXT NOT NULL DEFAULT 'OPEN',  -- OPEN | REVIEWED | CONFIRMED_FRAUD | FALSE_POSITIVE
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_alerts_status ON fraud_alerts (status);
