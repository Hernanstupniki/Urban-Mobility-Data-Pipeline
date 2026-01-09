import os
from datetime import datetime

import uuid
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, max as spark_max

# Config
JOB_NAME = "trips_oltp_to_bronze"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "mobility_oltp")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:5432/{DB_NAME}"

ENV = os.getenv("ENV", "dev")
RAW_BASE_PATH = "data/{ENV}/bronze/trips"


def upsert_etl_control(job_name, last_loaded_ts, status):
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()

    sql = """
    INSERT INTO mobility.etl_control (
        job_name,
        last_loaded_ts,
        last_success_ts,
        last_status
    )
    VALUES (%s, %s, NOW(), %s)
    ON CONFLICT (job_name)
    DO UPDATE SET
        last_loaded_ts  = EXCLUDED.last_loaded_ts,
        last_success_ts = EXCLUDED.last_success_ts,
        last_status     = EXCLUDED.last_status;
    """

    cur.execute(sql, (job_name, last_loaded_ts, status))
    conn.commit()
    cur.close()
    conn.close()


# Main ETL
def main():
    spark = (
        SparkSession.builder
        .appName(JOB_NAME)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Spark performance
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "64MB")

    # Read etl_control (watermark)
    etl_control_df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "mobility.etl_control")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
        .filter(col("job_name") == lit(JOB_NAME))
    )

    if etl_control_df.rdd.isEmpty():
        last_loaded_ts = datetime(1970, 1, 1)
    else:
        last_loaded_ts = etl_control_df.select("last_loaded_ts").collect()[0][0]
        if last_loaded_ts is None:
            last_loaded_ts = datetime(1970, 1, 1)

    print(f"Last loaded timestamp: {last_loaded_ts}")

    # Read trips incrementally
    trips_df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "mobility.trips")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
        .filter(col("requested_at") > lit(last_loaded_ts))
    )

    if trips_df.rdd.isEmpty():
        print("No new trips to load")
        spark.stop()
        return
    
    # Generate batch id
    batch_id = str(uuid.uuid4())

    # Add RAW metadata
    trips_df = (
        trips_df
        .withColumn("source_system", lit("mobiity_oltp"))
        .withColumn("raw_loaded_at",current_timestamp())
        .withColumn("batch_id", lit(batch_id))
    )

    # Write to RAW (partitioned by load_date)
    load_date = datetime.now().strftime("%Y-%m-%d")
    output_path = f"{RAW_BASE_PATH}/load_date={load_date}"

    (
        trips_df
        .write
        .mode("append")
        .parquet(output_path)
    )

    row_count = trips_df.count()
    max_ts = trips_df.select(spark_max("requested_at")).collect()[0][0]

    print(f"Wrote {row_count} trips to Bronze")

    spark.stop()

    upsert_etl_control(
    job_name=JOB_NAME,
    last_loaded_ts=max_ts,
    status="SUCCESS"
)


if __name__ == "__main__":
    main()
