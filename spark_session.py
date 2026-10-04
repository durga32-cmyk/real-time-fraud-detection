import os

from pyspark.sql import SparkSession

from config import SPARK_KAFKA_PACKAGE


def get_spark(app_name: str, with_kafka: bool = False) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
    )
    if with_kafka:
        builder = builder.config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
