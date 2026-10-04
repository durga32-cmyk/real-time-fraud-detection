"""Structured Streaming job: Kafka -> features -> GBT scoring -> PostgreSQL.

    docker compose run --rm app python src/stream_job.py
"""
import json

import psycopg2
from psycopg2.extras import execute_values
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from config import (CHECKPOINT_DIR, KAFKA_BOOTSTRAP, KAFKA_TOPIC, PG,
                    PIPELINE_PATH, THRESHOLD_PATH)
from features import STREAM_SCHEMA, add_features
from spark_session import get_spark

ALERT_UPSERT = """
INSERT INTO alerts (txn_id, event_time, txn_type, amount, name_orig, name_dest, fraud_prob, is_true_fraud)
VALUES %s
ON CONFLICT (txn_id) DO UPDATE SET
    event_time = EXCLUDED.event_time, txn_type = EXCLUDED.txn_type, amount = EXCLUDED.amount,
    name_orig = EXCLUDED.name_orig, name_dest = EXCLUDED.name_dest,
    fraud_prob = EXCLUDED.fraud_prob, is_true_fraud = EXCLUDED.is_true_fraud
"""

METRIC_UPSERT = """
INSERT INTO window_metrics_batch
    (window_start, batch_id, n_txn, n_alerts, n_fraud, n_caught, fraud_amt_caught, fraud_amt_missed, total_amount)
VALUES %s
ON CONFLICT (window_start, batch_id) DO UPDATE SET
    n_txn = EXCLUDED.n_txn, n_alerts = EXCLUDED.n_alerts, n_fraud = EXCLUDED.n_fraud,
    n_caught = EXCLUDED.n_caught, fraud_amt_caught = EXCLUDED.fraud_amt_caught,
    fraud_amt_missed = EXCLUDED.fraud_amt_missed, total_amount = EXCLUDED.total_amount
"""


def process_batch(batch_df, batch_id):
    """Called once per micro-batch. Upserts make a replayed batch harmless."""
    if batch_df.isEmpty():
        return
    batch_df.persist()

    fraud = F.col("isFraud") == 1
    alert = F.col("is_alert")
    metrics = (
        batch_df.groupBy(F.window("kafka_ts", "1 minute").alias("w"))
        .agg(
            F.count("*").alias("n_txn"),
            F.sum(F.when(alert, 1).otherwise(0)).alias("n_alerts"),
            F.sum(F.when(fraud, 1).otherwise(0)).alias("n_fraud"),
            F.sum(F.when(alert & fraud, 1).otherwise(0)).alias("n_caught"),
            F.sum(F.when(alert & fraud, F.col("amount")).otherwise(0.0)).alias("fraud_amt_caught"),
            F.sum(F.when(~alert & fraud, F.col("amount")).otherwise(0.0)).alias("fraud_amt_missed"),
            F.sum("amount").alias("total_amount"),
        )
        .select(F.col("w.start").alias("window_start"), "n_txn", "n_alerts", "n_fraud", "n_caught",
                "fraud_amt_caught", "fraud_amt_missed", "total_amount")
        .collect()
    )
    alerts = (
        batch_df.filter(alert)
        .select("txn_id", "kafka_ts", "type", "amount", "nameOrig", "nameDest", "fraud_prob", "isFraud")
        .collect()
    )

    metric_rows = [(m.window_start, int(batch_id), m.n_txn, m.n_alerts, m.n_fraud, m.n_caught,
                    m.fraud_amt_caught, m.fraud_amt_missed, m.total_amount) for m in metrics]
    alert_rows = [(a.txn_id, a.kafka_ts, a.type, a.amount, a.nameOrig, a.nameDest,
                   float(a.fraud_prob), bool(a.isFraud)) for a in alerts]

    conn = psycopg2.connect(**PG)
    try:
        with conn, conn.cursor() as cur:          # one transaction: all or nothing
            if metric_rows:
                execute_values(cur, METRIC_UPSERT, metric_rows)
            if alert_rows:
                execute_values(cur, ALERT_UPSERT, alert_rows)
    finally:
        conn.close()

    print(f"batch {batch_id}: {sum(m.n_txn for m in metrics):,} txns, {len(alert_rows):,} alerts")
    batch_df.unpersist()


def main():
    spark = get_spark("fraud-stream", with_kafka=True)
    model = PipelineModel.load(PIPELINE_PATH)
    threshold = json.loads(THRESHOLD_PATH.read_text())["threshold"]
    print(f"Loaded model, alert threshold = {threshold}")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 20000)     # cap micro-batch size
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), STREAM_SCHEMA).alias("t"),
                   F.col("timestamp").alias("kafka_ts"))
        .select("t.*", "kafka_ts")
    )
    scored = (
        model.transform(add_features(parsed))
        .withColumn("fraud_prob", vector_to_array("probability")[1])
        .withColumn("is_alert", F.col("fraud_prob") >= F.lit(threshold))
    )

    query = (
        scored.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)   # offsets survive restarts
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
