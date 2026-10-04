"""Schemas and feature engineering shared by training AND streaming.

Using the exact same function in both places avoids train/serve skew.
All transformations are row-wise (stateless), so they work on streaming DataFrames.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

_BASE = [
    ("step", IntegerType()),
    ("type", StringType()),
    ("amount", DoubleType()),
    ("nameOrig", StringType()),
    ("oldbalanceOrg", DoubleType()),
    ("newbalanceOrig", DoubleType()),
    ("nameDest", StringType()),
    ("oldbalanceDest", DoubleType()),
    ("newbalanceDest", DoubleType()),
    ("isFraud", IntegerType()),
]

# Column order of the PaySim CSV
CSV_SCHEMA = StructType(
    [StructField(n, t, True) for n, t in _BASE] + [StructField("isFlaggedFraud", IntegerType(), True)]
)

# JSON messages on the Kafka topic
STREAM_SCHEMA = StructType(
    [StructField("txn_id", LongType(), True)] + [StructField(n, t, True) for n, t in _BASE]
)

NUMERIC_FEATURES = [
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
    "errorBalanceOrig",
    "errorBalanceDest",
    "origEmptied",
    "destZero",
    "hourOfDay",
]


def add_features(df: DataFrame) -> DataFrame:
    return (
        df
        # If the books balance this is 0; fraud often breaks the arithmetic.
        .withColumn("errorBalanceOrig", F.col("newbalanceOrig") + F.col("amount") - F.col("oldbalanceOrg"))
        .withColumn("errorBalanceDest", F.col("oldbalanceDest") + F.col("amount") - F.col("newbalanceDest"))
        # "Sender account emptied" clue
        .withColumn(
            "origEmptied",
            F.when((F.col("oldbalanceOrg") > 0) & (F.col("newbalanceOrig") == 0), 1.0).otherwise(0.0),
        )
        # Destination balance did not move at all
        .withColumn(
            "destZero",
            F.when((F.col("oldbalanceDest") == 0) & (F.col("newbalanceDest") == 0), 1.0).otherwise(0.0),
        )
        .withColumn("hourOfDay", (F.col("step") % 24).cast("double"))
    )
