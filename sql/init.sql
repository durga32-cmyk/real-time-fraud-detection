-- Runs automatically the first time the Postgres container starts.

-- One row per transaction that the model flagged. PK makes replays harmless.
CREATE TABLE IF NOT EXISTS alerts (
    txn_id        BIGINT PRIMARY KEY,
    event_time    TIMESTAMPTZ      NOT NULL,
    txn_type      TEXT,
    amount        DOUBLE PRECISION,
    name_orig     TEXT,
    name_dest     TEXT,
    fraud_prob    DOUBLE PRECISION,
    is_true_fraud BOOLEAN,
    created_at    TIMESTAMPTZ DEFAULT now()
);

-- Metrics per (1-minute window, micro-batch). Keyed by batch_id so that a
-- replayed micro-batch overwrites its own rows instead of double counting.
CREATE TABLE IF NOT EXISTS window_metrics_batch (
    window_start     TIMESTAMPTZ NOT NULL,
    batch_id         BIGINT      NOT NULL,
    n_txn            BIGINT,
    n_alerts         BIGINT,
    n_fraud          BIGINT,
    n_caught         BIGINT,
    fraud_amt_caught DOUBLE PRECISION,
    fraud_amt_missed DOUBLE PRECISION,
    total_amount     DOUBLE PRECISION,
    PRIMARY KEY (window_start, batch_id)
);

-- Final 1-minute view: sums the per-batch rows.
CREATE OR REPLACE VIEW window_metrics_1m AS
SELECT
    window_start,
    window_start + INTERVAL '1 minute'                         AS window_end,
    SUM(n_txn)                                                 AS n_txn,
    SUM(n_alerts)                                              AS n_alerts,
    SUM(n_fraud)                                               AS n_fraud,
    SUM(n_caught)                                              AS n_caught,
    SUM(fraud_amt_caught)                                      AS fraud_amt_caught,
    SUM(fraud_amt_missed)                                      AS fraud_amt_missed,
    SUM(total_amount)                                          AS total_amount,
    SUM(n_caught)::DOUBLE PRECISION / NULLIF(SUM(n_alerts), 0) AS precision_,
    SUM(n_caught)::DOUBLE PRECISION / NULLIF(SUM(n_fraud), 0)  AS recall_
FROM window_metrics_batch
GROUP BY window_start;
