"""Train the fraud model, pick the alert threshold by net business saving,
and compare against a static-rule baseline. Run:

    docker compose run --rm --no-deps app python src/train_model.py
"""
import json

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from config import (BASELINE_AMOUNT, METRICS_PATH, MODEL_DIR, NEG_KEEP_FRACTION,
                    PIPELINE_PATH, REVIEW_COST, THRESHOLD_PATH, TRAIN_MAX_STEP,
                    VAL_MAX_STEP, find_csv)
from features import CSV_SCHEMA, NUMERIC_FEATURES, add_features
from spark_session import get_spark


def summarise(df, alert_cond, name):
    """Business metrics for any 'alert' rule on a labelled DataFrame."""
    fraud = F.col("isFraud") == 1
    r = df.agg(
        F.count("*").alias("n"),
        F.sum(F.when(alert_cond, 1).otherwise(0)).alias("alerts"),
        F.sum(F.when(alert_cond & fraud, 1).otherwise(0)).alias("caught"),
        F.sum(F.when(fraud, 1).otherwise(0)).alias("fraud"),
        F.sum(F.when(alert_cond & fraud, F.col("amount")).otherwise(0.0)).alias("saved"),
        F.sum(F.when(fraud, F.col("amount")).otherwise(0.0)).alias("fraud_value"),
    ).first()
    alerts, caught, fraud_n = int(r["alerts"]), int(r["caught"]), int(r["fraud"])
    saved, fraud_value = float(r["saved"]), float(r["fraud_value"])
    return {
        "name": name,
        "rows": int(r["n"]),
        "alerts": alerts,
        "fraud_total": fraud_n,
        "fraud_caught": caught,
        "precision": caught / alerts if alerts else 0.0,
        "recall": caught / fraud_n if fraud_n else 0.0,
        "fraud_value_total": fraud_value,
        "fraud_value_stopped": saved,
        "review_cost": REVIEW_COST * alerts,
        "net_saving": saved - REVIEW_COST * alerts,
    }


def main():
    spark = get_spark("fraud-train")
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    df = spark.read.csv(find_csv(), header=True, schema=CSV_SCHEMA)
    df = add_features(df).drop("isFlaggedFraud")

    train = df.filter(F.col("step") <= TRAIN_MAX_STEP)
    val = df.filter((F.col("step") > TRAIN_MAX_STEP) & (F.col("step") <= VAL_MAX_STEP))
    test = df.filter(F.col("step") > VAL_MAX_STEP)

    # Keep every fraud row, but only a fraction of normal rows (fraud is ~0.13% of data).
    train_bal = train.filter((F.col("isFraud") == 1) | (F.rand(42) < NEG_KEEP_FRACTION))

    pipeline = Pipeline(stages=[
        StringIndexer(inputCol="type", outputCol="typeIdx", handleInvalid="keep"),
        OneHotEncoder(inputCols=["typeIdx"], outputCols=["typeVec"], handleInvalid="keep"),
        VectorAssembler(inputCols=NUMERIC_FEATURES + ["typeVec"], outputCol="features"),
        GBTClassifier(labelCol="isFraud", featuresCol="features", maxIter=40, maxDepth=5, seed=42),
    ])
    print("Training gradient-boosted trees ...")
    model = pipeline.fit(train_bal)

    def score(d):
        return (model.transform(d)
                .withColumn("prob", vector_to_array("probability")[1])
                .select("step", "type", "amount", "isFraud", "prob", "rawPrediction")
                .persist())

    val_s, test_s = score(val), score(test)

    # ---- choose the threshold on VALIDATION by net saving (one pass over the data) ----
    thresholds = [round(float(t), 2) for t in np.arange(0.05, 1.0, 0.05)]
    exprs = []
    for i, t in enumerate(thresholds):
        alert = F.col("prob") >= t
        exprs.append(F.sum(F.when(alert, 1).otherwise(0)).alias(f"alerts_{i}"))
        exprs.append(F.sum(F.when(alert & (F.col("isFraud") == 1), F.col("amount")).otherwise(0.0)).alias(f"saved_{i}"))
    row = val_s.agg(*exprs).first()
    sweep = []
    for i, t in enumerate(thresholds):
        alerts, saved = int(row[f"alerts_{i}"]), float(row[f"saved_{i}"])
        sweep.append({"threshold": t, "alerts": alerts, "saved": saved,
                      "net_saving": saved - REVIEW_COST * alerts})
    best = max(sweep, key=lambda s: s["net_saving"])
    threshold = best["threshold"]
    print(f"Best threshold on validation: {threshold} (net saving {best['net_saving']:,.0f})")

    # ---- final numbers on the untouched TEST period ----
    model_res = summarise(test_s, F.col("prob") >= threshold, f"GBT model @ {threshold}")
    baseline_res = summarise(
        test_s, (F.col("type") == "TRANSFER") & (F.col("amount") > BASELINE_AMOUNT),
        f"Static rule (TRANSFER & amount > {BASELINE_AMOUNT:,})",
    )
    auc_pr = BinaryClassificationEvaluator(
        labelCol="isFraud", rawPredictionCol="rawPrediction", metricName="areaUnderPR"
    ).evaluate(test_s)

    print("\n=== TEST RESULTS ===")
    for r in (model_res, baseline_res):
        print(f"{r['name']}\n  alerts={r['alerts']:,}  caught={r['fraud_caught']:,}/{r['fraud_total']:,}"
              f"  precision={r['precision']:.3f}  recall={r['recall']:.3f}"
              f"\n  stopped={r['fraud_value_stopped']:,.0f}  review cost={r['review_cost']:,.0f}"
              f"  NET SAVING={r['net_saving']:,.0f}")
    print(f"AUC-PR (test): {auc_pr:.4f}")

    model.write().overwrite().save(PIPELINE_PATH)
    THRESHOLD_PATH.write_text(json.dumps({"threshold": threshold, "review_cost": REVIEW_COST}))
    METRICS_PATH.write_text(json.dumps(
        {"auc_pr_test": auc_pr, "model": model_res, "baseline": baseline_res, "threshold_sweep": sweep},
        indent=2))
    print(f"\nSaved model -> {PIPELINE_PATH}\nSaved metrics -> {METRICS_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
