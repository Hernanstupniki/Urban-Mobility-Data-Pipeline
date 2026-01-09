import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import

JOB_NAME = "raw_to_bronze"
RAW_BASE_PATH = "data/raw/trips"

spark = SparkSession.builder \
    .appName(JOB_NAME) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Spark performance
spark.conf.set("spark.sql.shuffle.partitions", "4")
spark.conf.set("spark.default.parallelism", "4")
spark.conf.set("spark.sql.files.maxPartitionBytes", "64MB")

ENV = os.getenv("ENV", "dev")
BASE_PATH = f"data/{ENV}"

df_silver = spark.read.parquet(f"{RAW_BASE_PATH }")