import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta.tables import DeltaTable

ENV = os.getenv("ENV", "dev")
CONTROL_PATH = f"data/{ENV}/_control/etl_control"

spark = SparkSession.builder.appName("bootstrap_control_tables").getOrCreate()

schema = StructType([
    StructField("job_name", StringType(), False),
    StructField("last_loaded_ts", TimestampType(), True),
    StructField("last_success_ts", TimestampType(), True),
    StructField("last_status", StringType(), True),
])

if not DeltaTable.isDeltaTable(spark, CONTROL_PATH):
    spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(CONTROL_PATH)
    print("Created Delta control table:", CONTROL_PATH)

spark.stop()
