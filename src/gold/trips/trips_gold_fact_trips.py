import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, date_format, unix_timestamp, when,
    sha2, concat_ws, coalesce, max as spark_max, concat, substring, to_date
)
from delta.tables import DeltaTable

JOB_NAME = "gold_trips_fact_trips"

ENV = os.getenv("ENV", "dev")

SILVER_TRIPS_PATH = f"data/{ENV}/silver/trips"
GOLD_BASE = f"data/{ENV}/gold/trips"
FACT_TRIPS_PATH = f"{GOLD_BASE}/fact_trips"
DIM_DATE_PATH = f"{GOLD_BASE}/dim_date"

CONTROL_BASE_PATH = f"data/{ENV}/_control"
ETL_CONTROL_PATH = f"{CONTROL_BASE_PATH}/etl_control"


def ensure_etl_control_table(spark: SparkSession):
    if DeltaTable.isDeltaTable(spark, ETL_CONTROL_PATH):
        return
    (spark.createDataFrame([], "job_name string, last_loaded_ts timestamp, last_success_ts timestamp, last_status string")
        .write.format("delta").mode("overwrite").save(ETL_CONTROL_PATH))


def read_last_loaded_ts(spark: SparkSession) -> datetime:
    if not DeltaTable.isDeltaTable(spark, ETL_CONTROL_PATH):
        return datetime(1970, 1, 1)

    df = spark.read.format("delta").load(ETL_CONTROL_PATH).filter(col("job_name") == lit(JOB_NAME))
    if df.rdd.isEmpty():
        return datetime(1970, 1, 1)

    ts = df.select("last_loaded_ts").first()[0]
    return ts or datetime(1970, 1, 1)


def upsert_etl_control(spark: SparkSession, job_name: str, last_loaded_ts, status: str):
    ensure_etl_control_table(spark)
    target = DeltaTable.forPath(spark, ETL_CONTROL_PATH)

    updates = (
        spark.createDataFrame([(job_name, last_loaded_ts, status)],
                              "job_name string, last_loaded_ts timestamp, last_status string")
        .withColumn("last_success_ts", current_timestamp())
    )

    (target.alias("t")
        .merge(updates.alias("s"), "t.job_name = s.job_name")
        .whenMatchedUpdate(set={
            "last_loaded_ts": "coalesce(s.last_loaded_ts, t.last_loaded_ts)",
            "last_success_ts": "s.last_success_ts",
            "last_status": "s.last_status",
        })
        .whenNotMatchedInsert(values={
            "job_name": "s.job_name",
            "last_loaded_ts": "s.last_loaded_ts",
            "last_success_ts": "s.last_success_ts",
            "last_status": "s.last_status",
        })
        .execute())


def key(prefix: str, id_col):
    return sha2(concat_ws("||", lit(prefix), coalesce(id_col.cast("string"), lit(""))), 256)


def upsert_dim_date(spark: SparkSession, fact_df):
    # agarramos TODAS las date_keys del fact y las unimos en una sola columna "date_key"
    date_keys = (
        fact_df.select(col("requested_date_key").alias("date_key"))
        .union(fact_df.select(col("accepted_date_key").alias("date_key")))
        .union(fact_df.select(col("started_date_key").alias("date_key")))
        .union(fact_df.select(col("ended_date_key").alias("date_key")))
        .union(fact_df.select(col("canceled_date_key").alias("date_key")))
        .where(col("date_key").isNotNull())
        .distinct()
    )

    dates = (
        date_keys
        .withColumn("date", to_date_from_key(col("date_key")))
        .withColumn("year",  (col("date_key") / 10000).cast("int"))
        .withColumn("month", ((col("date_key") / 100).cast("int") % 100).cast("int"))
        .withColumn("day",   (col("date_key") % 100).cast("int"))
        .withColumn("gold_loaded_at", current_timestamp())
        .select("date_key", "date", "year", "month", "day", "gold_loaded_at")
    )

    if not DeltaTable.isDeltaTable(spark, DIM_DATE_PATH):
        dates.write.format("delta").mode("overwrite").save(DIM_DATE_PATH)
        return

    dim = DeltaTable.forPath(spark, DIM_DATE_PATH)
    (
        dim.alias("t")
        .merge(dates.alias("s"), "t.date_key = s.date_key")
        .whenNotMatchedInsertAll()
        .execute()
    )

def to_date_from_key(date_key_col):
    # date_key = yyyyMMdd -> string yyyy-MM-dd -> date
    s = date_key_col.cast("string")
    return to_date(concat(substring(s, 1, 4), lit("-"), substring(s, 5, 2), lit("-"), substring(s, 7, 2)))


def main():
    spark = (
        SparkSession.builder
        .appName(JOB_NAME)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "4")

    try:
        last_ts = read_last_loaded_ts(spark)
        print(f"[{JOB_NAME}] last_loaded_ts(valid_from): {last_ts}")

        silver = spark.read.format("delta").load(SILVER_TRIPS_PATH)

        silver_incr = silver.filter(col("valid_from") > lit(last_ts))
        incr = silver_incr.filter(col("is_current") == lit(True))

        if incr.rdd.isEmpty():
            upsert_etl_control(spark, JOB_NAME, None, "NOOP")
            print("No new current trips")
            return

        fact_df = (
            incr
            .withColumn("requested_date_key", date_format(col("requested_at"), "yyyyMMdd").cast("int"))
            .withColumn("accepted_date_key",  date_format(col("accepted_at"),  "yyyyMMdd").cast("int"))
            .withColumn("started_date_key",   date_format(col("started_at"),   "yyyyMMdd").cast("int"))
            .withColumn("ended_date_key",     date_format(col("ended_at"),     "yyyyMMdd").cast("int"))
            .withColumn("canceled_date_key",  date_format(col("canceled_at"),  "yyyyMMdd").cast("int"))

            # surrogate-like keys (aunque todavía no tengas dim_driver/passenger)
            .withColumn("pickup_zone_key",  key("zone", col("pickup_zone_id")))
            .withColumn("dropoff_zone_key", key("zone", col("dropoff_zone_id")))
            .withColumn("driver_key",       key("driver", col("driver_id")))
            .withColumn("passenger_key",    key("passenger", col("passenger_id")))
            .withColumn("vehicle_key",      key("vehicle", col("vehicle_id")))

            .withColumn(
                "wait_time_sec",
                when(col("accepted_at").isNotNull() & col("requested_at").isNotNull(),
                     unix_timestamp(col("accepted_at")) - unix_timestamp(col("requested_at")))
                .otherwise(lit(None))
            )
            .withColumn(
                "trip_duration_sec",
                when(col("ended_at").isNotNull() & col("started_at").isNotNull(),
                     unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at")))
                .otherwise(lit(None))
            )
            .withColumn("is_canceled",  col("status") == lit("canceled"))
            .withColumn("is_completed", col("status") == lit("completed"))
            .withColumn("gold_loaded_at", current_timestamp())
        )
        
        fact_df = fact_df.select(
            "trip_id",
            "requested_date_key", "accepted_date_key", "started_date_key", "ended_date_key", "canceled_date_key",
            "pickup_zone_key", "dropoff_zone_key",
            "driver_key", "passenger_key", "vehicle_key",
            "status",
            "fare_amount", "estimated_distance_km", "actual_distance_km",
            "wait_time_sec", "trip_duration_sec",
            "is_canceled", "is_completed",
            "valid_from",  # para watermark/audit
            "gold_loaded_at"
        )


        # dim_date (mínimo) para BI
        upsert_dim_date(spark, fact_df)

        # upsert fact_trips
        if not DeltaTable.isDeltaTable(spark, FACT_TRIPS_PATH):
            fact_df.write.format("delta").mode("overwrite").save(FACT_TRIPS_PATH)
        else:
            tgt = DeltaTable.forPath(spark, FACT_TRIPS_PATH)
            (tgt.alias("t")
                .merge(fact_df.alias("s"), "t.trip_id = s.trip_id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())

        max_ts = fact_df.select(spark_max("valid_from")).first()[0]
        upsert_etl_control(spark, JOB_NAME, max_ts, "SUCCESS")
        print("fact_trips upsert OK")

    except Exception as e:
        try:
            upsert_etl_control(spark, JOB_NAME, None, f"FAIL: {type(e).__name__}")
        except Exception:
            pass
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
