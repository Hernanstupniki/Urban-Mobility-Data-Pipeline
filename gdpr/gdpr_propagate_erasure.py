import os
import uuid
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, max as spark_max,
    coalesce as sp_coalesce, lower,
    broadcast, sha2, concat_ws, array, expr
)
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ============================================================
# Config
# ============================================================
ENV = os.getenv("ENV", "dev")
JOB_NAME = os.getenv("JOB_NAME", "gdpr_propagate_erasure")

# OLTP connection
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "mobility_oltp")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")  # required at runtime
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:5432/{DB_NAME}"

# Lake base paths (no new tables, same Delta structure)
BRONZE_BASE = os.getenv("BRONZE_BASE", f"data/{ENV}/bronze")
SILVER_BASE = os.getenv("SILVER_BASE", f"data/{ENV}/silver")

# Default table paths (override if you want)
BRONZE_PASSENGERS_PATH = os.getenv("BRONZE_PASSENGERS_PATH", f"{BRONZE_BASE}/passengers")
BRONZE_DRIVERS_PATH    = os.getenv("BRONZE_DRIVERS_PATH",    f"{BRONZE_BASE}/drivers")
BRONZE_VEHICLES_PATH   = os.getenv("BRONZE_VEHICLES_PATH",   f"{BRONZE_BASE}/vehicles")
BRONZE_RATINGS_PATH    = os.getenv("BRONZE_RATINGS_PATH",    f"{BRONZE_BASE}/ratings")
BRONZE_TRIPS_PATH      = os.getenv("BRONZE_TRIPS_PATH",      f"{BRONZE_BASE}/trips")
BRONZE_PAYMENTS_PATH   = os.getenv("BRONZE_PAYMENTS_PATH",   f"{BRONZE_BASE}/payments")

SILVER_PASSENGERS_PATH = os.getenv("SILVER_PASSENGERS_PATH", f"{SILVER_BASE}/passengers")
SILVER_DRIVERS_PATH    = os.getenv("SILVER_DRIVERS_PATH",    f"{SILVER_BASE}/drivers")
SILVER_VEHICLES_PATH   = os.getenv("SILVER_VEHICLES_PATH",   f"{SILVER_BASE}/vehicles")
SILVER_RATINGS_PATH    = os.getenv("SILVER_RATINGS_PATH",    f"{SILVER_BASE}/ratings")
SILVER_TRIPS_PATH      = os.getenv("SILVER_TRIPS_PATH",      f"{SILVER_BASE}/trips")
SILVER_PAYMENTS_PATH   = os.getenv("SILVER_PAYMENTS_PATH",   f"{SILVER_BASE}/payments")

# Control table for watermark
CONTROL_BASE_PATH = os.getenv("CONTROL_BASE_PATH", f"data/{ENV}/_control")
GDPR_CONTROL_PATH = os.getenv("GDPR_CONTROL_PATH", f"{CONTROL_BASE_PATH}/gdpr_control")

# AUDIT (Delta) — en _control también
AUDIT_ENABLED = os.getenv("AUDIT_ENABLED", "true").lower() == "true"
GDPR_AUDIT_PATH = os.getenv("GDPR_AUDIT_PATH", f"{CONTROL_BASE_PATH}/gdpr_audit")

# sal para que la huella no sea trivial (portfolio)
GDPR_AUDIT_SALT = os.getenv("GDPR_AUDIT_SALT", f"{ENV}-salt")
RUN_ID = os.getenv("RUN_ID", str(uuid.uuid4()))

# Anon values
ANON_NAME = os.getenv("ANON_NAME", "ANONYMIZED")
ANON_PLATE_PREFIX = os.getenv("ANON_PLATE_PREFIX", "ANON-PLATE-")  # deterministic placeholder


# ============================================================
# Spark
# ============================================================
def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # DEV tuning (same vibe as your ETLs)
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "64MB")
    return spark


# ============================================================
# GDPR control table helpers
# ============================================================
def ensure_gdpr_control_table(spark: SparkSession):
    if DeltaTable.isDeltaTable(spark, GDPR_CONTROL_PATH):
        return

    (
        spark.createDataFrame(
            [],
            "job_name string, last_processed_at timestamp, last_success_ts timestamp, last_status string"
        )
        .write.format("delta")
        .mode("overwrite")
        .save(GDPR_CONTROL_PATH)
    )


def read_last_processed_at(spark: SparkSession) -> datetime:
    if not DeltaTable.isDeltaTable(spark, GDPR_CONTROL_PATH):
        return datetime(1970, 1, 1)

    df = (
        spark.read.format("delta").load(GDPR_CONTROL_PATH)
        .filter(col("job_name") == lit(JOB_NAME))
    )

    if df.rdd.isEmpty():
        return datetime(1970, 1, 1)

    ts = df.select("last_processed_at").first()[0]
    return ts or datetime(1970, 1, 1)


def upsert_gdpr_control(spark: SparkSession, last_processed_at, status: str):
    """
    Upsert in Delta:
    - If last_processed_at is None (FAIL), DO NOT step on the previous watermark.
    """
    ensure_gdpr_control_table(spark)
    target = DeltaTable.forPath(spark, GDPR_CONTROL_PATH)

    updates = (
        spark.createDataFrame(
            [(JOB_NAME, last_processed_at, status)],
            "job_name string, last_processed_at timestamp, last_status string"
        )
        .withColumn("last_success_ts", current_timestamp())
    )

    (
        target.alias("t")
        .merge(updates.alias("s"), "t.job_name = s.job_name")
        .whenMatchedUpdate(set={
            "last_processed_at": "coalesce(s.last_processed_at, t.last_processed_at)",
            "last_success_ts": "s.last_success_ts",
            "last_status": "s.last_status",
        })
        .whenNotMatchedInsert(values={
            "job_name": "s.job_name",
            "last_processed_at": "s.last_processed_at",
            "last_success_ts": "s.last_success_ts",
            "last_status": "s.last_status",
        })
        .execute()
    )


# ============================================================
# AUDIT helpers (Delta)
# ============================================================
def ensure_gdpr_audit_table(spark: SparkSession):
    if not AUDIT_ENABLED:
        return
    if DeltaTable.isDeltaTable(spark, GDPR_AUDIT_PATH):
        return

    schema = """
      env string,
      run_id string,
      job_name string,
      request_id string,
      processed_at timestamp,
      applied_at timestamp,
      subject_type string,
      subject_id_hash string,
      layer string,
      table_name string,
      action string,
      columns_scrubbed array<string>,
      status string
    """

    (
        spark.createDataFrame([], schema)
        .write.format("delta")
        .mode("overwrite")
        .save(GDPR_AUDIT_PATH)
    )


def subject_fingerprint(subject_type_col, subject_id_col):
    # “huella digital”: hash irreversible del sujeto (no guardás el id real)
    return sha2(
        concat_ws("||",
                  lit(GDPR_AUDIT_SALT),
                  lower(subject_type_col.cast("string")),
                  subject_id_col.cast("string")),
        256
    )


def append_gdpr_audit(spark: SparkSession, df):
    if not AUDIT_ENABLED:
        return
    ensure_gdpr_audit_table(spark)

    # IMPORTANT: escribimos siempre el MISMO schema que la tabla
    out = df.select(
        col("env").cast("string"),
        col("run_id").cast("string"),
        col("job_name").cast("string"),
        col("request_id").cast("string"),
        col("processed_at").cast("timestamp"),
        col("applied_at").cast("timestamp"),
        col("subject_type").cast("string"),
        col("subject_id_hash").cast("string"),
        col("layer").cast("string"),
        col("table_name").cast("string"),
        col("action").cast("string"),
        col("columns_scrubbed"),
        col("status").cast("string"),
    )

    (
        out.write
        .format("delta")
        .mode("append")
        .save(GDPR_AUDIT_PATH)
    )


def audit_log_action(
    spark: SparkSession,
    subjects_df,          # must contain: request_id, processed_at, subject_type, subject_id
    layer: str,
    table_name: str,
    action: str,
    columns_scrubbed_list,
    status: str
):
    if not AUDIT_ENABLED:
        return

    if subjects_df is None or subjects_df.rdd.isEmpty():
        return

    cols_arr = expr("array()") if not columns_scrubbed_list else array(*[lit(x) for x in columns_scrubbed_list])

    out = (
        subjects_df
        .select(
            lit(ENV).alias("env"),
            lit(RUN_ID).alias("run_id"),
            lit(JOB_NAME).alias("job_name"),
            col("request_id").cast("string").alias("request_id"),
            col("processed_at").cast("timestamp").alias("processed_at"),
            current_timestamp().alias("applied_at"),
            lower(col("subject_type")).cast("string").alias("subject_type"),
            subject_fingerprint(col("subject_type"), col("subject_id")).alias("subject_id_hash"),
            lit(layer).alias("layer"),
            lit(table_name).alias("table_name"),
            lit(action).alias("action"),
            cols_arr.alias("columns_scrubbed"),
            lit(status).alias("status"),
        )
    )

    # Si el audit falla, NO volteamos todo el GDPR (portfolio-friendly)
    try:
        append_gdpr_audit(spark, out)
    except Exception as e:
        logging.warning(f"AUDIT write failed (ignored): {type(e).__name__}: {e}")


# ============================================================
# Read OLTP GDPR requests (incremental)
# ============================================================
def read_processed_erasure_requests(spark: SparkSession, last_processed_at: datetime):
    """
    Reads processed erasure requests incrementally using processed_at watermark.
    Source of truth: mobility.gdpr_requests (OLTP).
    Supports:
      - NEW: subject_type + subject_id
      - LEGACY: passenger_id
    """
    if DB_PASSWORD is None:
        raise ValueError("DB_PASSWORD env var is required")

    last_ts_str = last_processed_at.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
      (SELECT
          request_id,
          passenger_id,
          subject_type,
          subject_id,
          request_type,
          status,
          processed_at
       FROM mobility.gdpr_requests
       WHERE request_type = 'erasure'
         AND status = 'processed'
         AND processed_at IS NOT NULL
         AND processed_at > TIMESTAMP '{last_ts_str}'
      ) AS t
    """

    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", query)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def normalize_subjects(req_df):
    """
    Canonical (subject_type, subject_id) from new + legacy columns.
    - if subject_type is NULL and passenger_id exists => passenger
    - if subject_id is NULL and passenger_id exists => passenger_id
    """
    return (
        req_df
        .withColumn("subject_type_norm", lower(col("subject_type")))
        .withColumn("subject_type_norm", sp_coalesce(col("subject_type_norm"), lit("passenger")))
        .withColumn("subject_id_norm", sp_coalesce(col("subject_id"), col("passenger_id")))
        .select(
            col("request_id").cast("string").alias("request_id"),   # FIX: siempre string
            col("processed_at").cast("timestamp").alias("processed_at"),
            col("subject_type_norm").alias("subject_type"),
            col("subject_id_norm").cast("long").alias("subject_id")
        )
        .filter(col("subject_id").isNotNull())
    )


# ============================================================
# Delta update helpers (no subqueries)
# ============================================================
def _is_delta(spark: SparkSession, path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


def merge_update_by_ids(
    spark: SparkSession,
    table_path: str,
    ids_df,
    key_col: str,
    set_map: dict,
    match_condition: str = None,
):
    """
    Delta UPDATE with ids using MERGE (Delta doesn't support subqueries in UPDATE conditions).
    - Updates rows where t.key_col matches ids_df.key_col
    - Optional extra match_condition (e.g., "t.comment IS NOT NULL")
    """
    if not _is_delta(spark, table_path):
        logging.warning(f"SKIP (no Delta table): {table_path}")
        return

    ids = (
        ids_df.select(col(key_col).cast("long").alias(key_col))
        .where(col(key_col).isNotNull())
        .distinct()
    )

    if ids.rdd.isEmpty():
        logging.info(f"{table_path} | No ids to update ({key_col})")
        return

    target = DeltaTable.forPath(spark, table_path)
    m = target.alias("t").merge(ids.alias("s"), f"t.{key_col} = s.{key_col}")

    if match_condition:
        m = m.whenMatchedUpdate(condition=match_condition, set=set_map)
    else:
        m = m.whenMatchedUpdate(set=set_map)

    m.execute()


# ============================================================
# GDPR actions (return status + columns for audit)
# ============================================================
def anonymize_passengers_delta(spark: SparkSession, table_path: str, passenger_ids_df):
    if not _is_delta(spark, table_path):
        logging.warning(f"SKIP (no Delta table): {table_path}")
        return "SKIP_NO_TABLE", []

    cols = set(spark.read.format("delta").load(table_path).columns)
    if "passenger_id" not in cols:
        logging.warning(f"SKIP (no passenger_id column): {table_path}")
        return "SKIP_NO_KEY", []

    set_map = {}
    if "full_name" in cols: set_map["full_name"] = f"'{ANON_NAME}'"
    if "email" in cols:     set_map["email"] = "NULL"
    if "phone" in cols:     set_map["phone"] = "NULL"
    if "city" in cols:      set_map["city"] = "NULL"

    if "is_deleted" in cols: set_map["is_deleted"] = "true"
    if "deleted_at" in cols: set_map["deleted_at"] = "current_timestamp()"
    if "updated_at" in cols: set_map["updated_at"] = "current_timestamp()"

    if not set_map:
        logging.warning(f"SKIP (no columns to anonymize): {table_path}")
        return "SKIP_NO_COLUMNS", []

    logging.info(f"{table_path} | GDPR passenger anonymize | set={list(set_map.keys())}")
    merge_update_by_ids(spark, table_path, passenger_ids_df, "passenger_id", set_map)
    return "APPLIED", list(set_map.keys())


def anonymize_drivers_delta(spark: SparkSession, table_path: str, driver_ids_df):
    if not _is_delta(spark, table_path):
        logging.warning(f"SKIP (no Delta table): {table_path}")
        return "SKIP_NO_TABLE", []

    cols = set(spark.read.format("delta").load(table_path).columns)
    if "driver_id" not in cols:
        logging.warning(f"SKIP (no driver_id column): {table_path}")
        return "SKIP_NO_KEY", []

    set_map = {}
    if "full_name" in cols:       set_map["full_name"] = f"'{ANON_NAME}'"
    if "license_number" in cols:  set_map["license_number"] = "NULL"
    if "status" in cols:          set_map["status"] = "'inactive'"

    if "is_deleted" in cols: set_map["is_deleted"] = "true"
    if "deleted_at" in cols: set_map["deleted_at"] = "current_timestamp()"
    if "updated_at" in cols: set_map["updated_at"] = "current_timestamp()"

    if not set_map:
        logging.warning(f"SKIP (no columns to anonymize): {table_path}")
        return "SKIP_NO_COLUMNS", []

    logging.info(f"{table_path} | GDPR driver anonymize | set={list(set_map.keys())}")
    merge_update_by_ids(spark, table_path, driver_ids_df, "driver_id", set_map)
    return "APPLIED", list(set_map.keys())


def anonymize_vehicles_delta(spark: SparkSession, table_path: str, vehicle_ids_df):
    if not _is_delta(spark, table_path):
        logging.warning(f"SKIP (no Delta table): {table_path}")
        return "SKIP_NO_TABLE", []

    cols = set(spark.read.format("delta").load(table_path).columns)
    if "vehicle_id" not in cols:
        logging.warning(f"SKIP (no vehicle_id column): {table_path}")
        return "SKIP_NO_KEY", []

    set_map = {}
    if "plate_number" in cols:
        # deterministic placeholder: ANON-PLATE-{vehicle_id} (use source alias s)
        set_map["plate_number"] = f"concat('{ANON_PLATE_PREFIX}', cast(s.vehicle_id as string))"

    if "is_deleted" in cols: set_map["is_deleted"] = "true"
    if "deleted_at" in cols: set_map["deleted_at"] = "current_timestamp()"
    if "updated_at" in cols: set_map["updated_at"] = "current_timestamp()"

    if not set_map:
        logging.warning(f"SKIP (no columns to anonymize): {table_path}")
        return "SKIP_NO_COLUMNS", []

    logging.info(f"{table_path} | GDPR vehicle anonymize | set={list(set_map.keys())}")
    merge_update_by_ids(spark, table_path, vehicle_ids_df, "vehicle_id", set_map)
    return "APPLIED", list(set_map.keys())


def scrub_ratings_delta(spark: SparkSession, table_path: str, passenger_ids_df, driver_ids_df):
    """
    Scrub ratings.comment for passenger_id and driver_id (if present).
    Returns dict statuses for audit.
    """
    statuses = {"passenger": "SKIP", "driver": "SKIP"}
    cols_used = ["comment"]

    if not _is_delta(spark, table_path):
        logging.warning(f"SKIP (no Delta table): {table_path}")
        statuses["passenger"] = "SKIP_NO_TABLE"
        statuses["driver"] = "SKIP_NO_TABLE"
        return statuses, cols_used

    cols = set(spark.read.format("delta").load(table_path).columns)
    if "comment" not in cols:
        logging.warning(f"SKIP (no comment column): {table_path}")
        statuses["passenger"] = "SKIP_NO_COLUMN"
        statuses["driver"] = "SKIP_NO_COLUMN"
        return statuses, cols_used

    set_map = {"comment": "NULL"}
    if "updated_at" in cols:
        set_map["updated_at"] = "current_timestamp()"
        cols_used.append("updated_at")

    # passenger_id pass
    if "passenger_id" in cols and not passenger_ids_df.rdd.isEmpty():
        logging.info(f"{table_path} | GDPR scrub ratings.comment (passenger_id)")
        merge_update_by_ids(
            spark, table_path, passenger_ids_df, "passenger_id", set_map,
            match_condition="t.comment IS NOT NULL"
        )
        statuses["passenger"] = "APPLIED"
    else:
        statuses["passenger"] = "SKIP_NO_IDS_OR_COL"

    # driver_id pass
    if "driver_id" in cols and not driver_ids_df.rdd.isEmpty():
        logging.info(f"{table_path} | GDPR scrub ratings.comment (driver_id)")
        merge_update_by_ids(
            spark, table_path, driver_ids_df, "driver_id", set_map,
            match_condition="t.comment IS NOT NULL"
        )
        statuses["driver"] = "APPLIED"
    else:
        statuses["driver"] = "SKIP_NO_IDS_OR_COL"

    return statuses, cols_used


def scrub_trips_delta(spark: SparkSession, table_path: str, passenger_ids_df, driver_ids_df, vehicle_ids_df):
    """
    Scrub trips.cancel_note for passenger_id / driver_id / vehicle_id.
    Returns dict statuses for audit.
    """
    statuses = {"passenger": "SKIP", "driver": "SKIP", "vehicle": "SKIP"}
    cols_used = ["cancel_note"]

    if not _is_delta(spark, table_path):
        logging.warning(f"SKIP (no Delta table): {table_path}")
        statuses["passenger"] = "SKIP_NO_TABLE"
        statuses["driver"] = "SKIP_NO_TABLE"
        statuses["vehicle"] = "SKIP_NO_TABLE"
        return statuses, cols_used

    cols = set(spark.read.format("delta").load(table_path).columns)
    if "cancel_note" not in cols:
        logging.warning(f"SKIP (no cancel_note column): {table_path}")
        statuses["passenger"] = "SKIP_NO_COLUMN"
        statuses["driver"] = "SKIP_NO_COLUMN"
        statuses["vehicle"] = "SKIP_NO_COLUMN"
        return statuses, cols_used

    set_map = {"cancel_note": "NULL"}
    if "updated_at" in cols:
        set_map["updated_at"] = "current_timestamp()"
        cols_used.append("updated_at")

    if "passenger_id" in cols and not passenger_ids_df.rdd.isEmpty():
        logging.info(f"{table_path} | GDPR scrub trips.cancel_note (passenger_id)")
        merge_update_by_ids(
            spark, table_path, passenger_ids_df, "passenger_id", set_map,
            match_condition="t.cancel_note IS NOT NULL"
        )
        statuses["passenger"] = "APPLIED"
    else:
        statuses["passenger"] = "SKIP_NO_IDS_OR_COL"

    if "driver_id" in cols and not driver_ids_df.rdd.isEmpty():
        logging.info(f"{table_path} | GDPR scrub trips.cancel_note (driver_id)")
        merge_update_by_ids(
            spark, table_path, driver_ids_df, "driver_id", set_map,
            match_condition="t.cancel_note IS NOT NULL"
        )
        statuses["driver"] = "APPLIED"
    else:
        statuses["driver"] = "SKIP_NO_IDS_OR_COL"

    if "vehicle_id" in cols and not vehicle_ids_df.rdd.isEmpty():
        logging.info(f"{table_path} | GDPR scrub trips.cancel_note (vehicle_id)")
        merge_update_by_ids(
            spark, table_path, vehicle_ids_df, "vehicle_id", set_map,
            match_condition="t.cancel_note IS NOT NULL"
        )
        statuses["vehicle"] = "APPLIED"
    else:
        statuses["vehicle"] = "SKIP_NO_IDS_OR_COL"

    return statuses, cols_used


def derive_trip_ids_from_trips(spark: SparkSession, trips_path: str, passenger_ids_df, driver_ids_df, vehicle_ids_df):
    """
    Compute affected trip_id set (UNION DISTINCT of matches per FK).
    No SQL subqueries, no collect().
    """
    if not _is_delta(spark, trips_path):
        logging.warning(f"SKIP (no Delta table): {trips_path}")
        return None

    trips_df = spark.read.format("delta").load(trips_path)
    cols = set(trips_df.columns)
    if "trip_id" not in cols:
        logging.warning(f"SKIP (trips missing trip_id): {trips_path}")
        return None

    pieces = []

    if "passenger_id" in cols and not passenger_ids_df.rdd.isEmpty():
        p = broadcast(passenger_ids_df.select(col("passenger_id").cast("long").alias("passenger_id")).distinct())
        pieces.append(
            trips_df.select(col("trip_id").cast("long").alias("trip_id"), col("passenger_id").cast("long").alias("passenger_id"))
                   .join(p, "passenger_id", "inner")
                   .select("trip_id")
        )

    if "driver_id" in cols and not driver_ids_df.rdd.isEmpty():
        d = broadcast(driver_ids_df.select(col("driver_id").cast("long").alias("driver_id")).distinct())
        pieces.append(
            trips_df.select(col("trip_id").cast("long").alias("trip_id"), col("driver_id").cast("long").alias("driver_id"))
                   .join(d, "driver_id", "inner")
                   .select("trip_id")
        )

    if "vehicle_id" in cols and not vehicle_ids_df.rdd.isEmpty():
        v = broadcast(vehicle_ids_df.select(col("vehicle_id").cast("long").alias("vehicle_id")).distinct())
        pieces.append(
            trips_df.select(col("trip_id").cast("long").alias("trip_id"), col("vehicle_id").cast("long").alias("vehicle_id"))
                   .join(v, "vehicle_id", "inner")
                   .select("trip_id")
        )

    if not pieces:
        return None

    out = pieces[0]
    for df in pieces[1:]:
        out = out.unionByName(df)

    return out.distinct()


def scrub_payments_delta(
    spark: SparkSession,
    payments_path: str,
    trips_path: str,
    passenger_ids_df,
    driver_ids_df,
    vehicle_ids_df
):
    """
    payments.provider_ref can identify someone via payment gateway.
    We scrub provider_ref for payments whose trip_id belongs to affected trips.
    Returns status + columns for audit.
    """
    if not _is_delta(spark, payments_path):
        logging.warning(f"SKIP (no Delta table): {payments_path}")
        return "SKIP_NO_TABLE", ["provider_ref"]

    pay_cols = set(spark.read.format("delta").load(payments_path).columns)
    if "provider_ref" not in pay_cols:
        logging.warning(f"SKIP (no provider_ref column): {payments_path}")
        return "SKIP_NO_COLUMN", ["provider_ref"]
    if "trip_id" not in pay_cols:
        logging.warning(f"SKIP (no trip_id column): {payments_path}")
        return "SKIP_NO_KEY", ["provider_ref"]

    trip_ids_df = derive_trip_ids_from_trips(spark, trips_path, passenger_ids_df, driver_ids_df, vehicle_ids_df)
    if trip_ids_df is None or trip_ids_df.rdd.isEmpty():
        logging.info(f"{payments_path} | no matched trips => no payments to scrub")
        return "NO_MATCHED_TRIPS", ["provider_ref"]

    set_map = {"provider_ref": "NULL"}
    cols_used = ["provider_ref"]
    if "updated_at" in pay_cols:
        set_map["updated_at"] = "current_timestamp()"
        cols_used.append("updated_at")

    logging.info(f"{payments_path} | GDPR scrub payments.provider_ref (via trips.trip_id)")
    merge_update_by_ids(
        spark, payments_path, trip_ids_df, "trip_id", set_map,
        match_condition="t.provider_ref IS NOT NULL"
    )
    return "APPLIED", cols_used


# ============================================================
# Main
# ============================================================
def main():
    global spark
    spark = build_spark(f"{JOB_NAME}_{ENV}")

    try:
        last_processed_at = read_last_processed_at(spark)

        logging.info("========================================")
        logging.info("GDPR propagate erasure -> Lake (Bronze/Silver) [MERGE update] + AUDIT")
        logging.info(f"ENV={ENV}")
        logging.info(f"JOB_NAME={JOB_NAME}")
        logging.info(f"RUN_ID={RUN_ID}")
        logging.info(f"JDBC_URL={JDBC_URL}")
        logging.info(f"GDPR_CONTROL_PATH={GDPR_CONTROL_PATH}")
        logging.info(f"GDPR_AUDIT_PATH={GDPR_AUDIT_PATH} (enabled={AUDIT_ENABLED})")
        logging.info(f"last_processed_at={last_processed_at}")
        logging.info("========================================")

        req_df = read_processed_erasure_requests(spark, last_processed_at)
        if req_df.rdd.isEmpty():
            logging.info("No new processed erasure requests to propagate")
            upsert_gdpr_control(spark, last_processed_at, "SUCCESS (no-op)")
            spark.stop()
            return

        subj_df = normalize_subjects(req_df).cache()

        passenger_subjects_df = subj_df.filter(col("subject_type") == lit("passenger")).cache()
        driver_subjects_df    = subj_df.filter(col("subject_type") == lit("driver")).cache()
        vehicle_subjects_df   = subj_df.filter(col("subject_type") == lit("vehicle")).cache()

        passenger_ids_df = passenger_subjects_df.select(col("subject_id").alias("passenger_id")).distinct()
        driver_ids_df    = driver_subjects_df.select(col("subject_id").alias("driver_id")).distinct()
        vehicle_ids_df   = vehicle_subjects_df.select(col("subject_id").alias("vehicle_id")).distinct()

        n_p = passenger_ids_df.count()
        n_d = driver_ids_df.count()
        n_v = vehicle_ids_df.count()
        logging.info(f"Subjects to anonymize: passengers={n_p}, drivers={n_d}, vehicles={n_v}")

        # PASSENGERS
        if n_p > 0:
            st, cols_used = anonymize_passengers_delta(spark, BRONZE_PASSENGERS_PATH, passenger_ids_df)
            audit_log_action(spark, passenger_subjects_df, "bronze", "passengers", "anonymize", cols_used, st)

            st, cols_used = anonymize_passengers_delta(spark, SILVER_PASSENGERS_PATH, passenger_ids_df)
            audit_log_action(spark, passenger_subjects_df, "silver", "passengers", "anonymize", cols_used, st)

        # DRIVERS
        if n_d > 0:
            st, cols_used = anonymize_drivers_delta(spark, BRONZE_DRIVERS_PATH, driver_ids_df)
            audit_log_action(spark, driver_subjects_df, "bronze", "drivers", "anonymize", cols_used, st)

            st, cols_used = anonymize_drivers_delta(spark, SILVER_DRIVERS_PATH, driver_ids_df)
            audit_log_action(spark, driver_subjects_df, "silver", "drivers", "anonymize", cols_used, st)

        # VEHICLES
        if n_v > 0:
            st, cols_used = anonymize_vehicles_delta(spark, BRONZE_VEHICLES_PATH, vehicle_ids_df)
            audit_log_action(spark, vehicle_subjects_df, "bronze", "vehicles", "anonymize", cols_used, st)

            st, cols_used = anonymize_vehicles_delta(spark, SILVER_VEHICLES_PATH, vehicle_ids_df)
            audit_log_action(spark, vehicle_subjects_df, "silver", "vehicles", "anonymize", cols_used, st)

        # RATINGS.comment (passenger + driver)
        if (n_p + n_d) > 0:
            statuses, cols_used = scrub_ratings_delta(spark, BRONZE_RATINGS_PATH, passenger_ids_df, driver_ids_df)
            if n_p > 0:
                audit_log_action(spark, passenger_subjects_df, "bronze", "ratings", "scrub_comment", cols_used, statuses["passenger"])
            if n_d > 0:
                audit_log_action(spark, driver_subjects_df, "bronze", "ratings", "scrub_comment", cols_used, statuses["driver"])

            statuses, cols_used = scrub_ratings_delta(spark, SILVER_RATINGS_PATH, passenger_ids_df, driver_ids_df)
            if n_p > 0:
                audit_log_action(spark, passenger_subjects_df, "silver", "ratings", "scrub_comment", cols_used, statuses["passenger"])
            if n_d > 0:
                audit_log_action(spark, driver_subjects_df, "silver", "ratings", "scrub_comment", cols_used, statuses["driver"])

        # TRIPS.cancel_note (passenger + driver + vehicle)
        if (n_p + n_d + n_v) > 0:
            statuses, cols_used = scrub_trips_delta(spark, BRONZE_TRIPS_PATH, passenger_ids_df, driver_ids_df, vehicle_ids_df)
            if n_p > 0:
                audit_log_action(spark, passenger_subjects_df, "bronze", "trips", "scrub_cancel_note", cols_used, statuses["passenger"])
            if n_d > 0:
                audit_log_action(spark, driver_subjects_df, "bronze", "trips", "scrub_cancel_note", cols_used, statuses["driver"])
            if n_v > 0:
                audit_log_action(spark, vehicle_subjects_df, "bronze", "trips", "scrub_cancel_note", cols_used, statuses["vehicle"])

            statuses, cols_used = scrub_trips_delta(spark, SILVER_TRIPS_PATH, passenger_ids_df, driver_ids_df, vehicle_ids_df)
            if n_p > 0:
                audit_log_action(spark, passenger_subjects_df, "silver", "trips", "scrub_cancel_note", cols_used, statuses["passenger"])
            if n_d > 0:
                audit_log_action(spark, driver_subjects_df, "silver", "trips", "scrub_cancel_note", cols_used, statuses["driver"])
            if n_v > 0:
                audit_log_action(spark, vehicle_subjects_df, "silver", "trips", "scrub_cancel_note", cols_used, statuses["vehicle"])

        # PAYMENTS.provider_ref (via trips.trip_id)
        if (n_p + n_d + n_v) > 0:
            st, cols_used = scrub_payments_delta(spark, BRONZE_PAYMENTS_PATH, BRONZE_TRIPS_PATH, passenger_ids_df, driver_ids_df, vehicle_ids_df)
            if n_p > 0:
                audit_log_action(spark, passenger_subjects_df, "bronze", "payments", "scrub_provider_ref", cols_used, st)
            if n_d > 0:
                audit_log_action(spark, driver_subjects_df, "bronze", "payments", "scrub_provider_ref", cols_used, st)
            if n_v > 0:
                audit_log_action(spark, vehicle_subjects_df, "bronze", "payments", "scrub_provider_ref", cols_used, st)

            st, cols_used = scrub_payments_delta(spark, SILVER_PAYMENTS_PATH, SILVER_TRIPS_PATH, passenger_ids_df, driver_ids_df, vehicle_ids_df)
            if n_p > 0:
                audit_log_action(spark, passenger_subjects_df, "silver", "payments", "scrub_provider_ref", cols_used, st)
            if n_d > 0:
                audit_log_action(spark, driver_subjects_df, "silver", "payments", "scrub_provider_ref", cols_used, st)
            if n_v > 0:
                audit_log_action(spark, vehicle_subjects_df, "silver", "payments", "scrub_provider_ref", cols_used, st)

        # Advance watermark
        new_watermark = subj_df.select(spark_max("processed_at")).first()[0]
        logging.info(f"New last_processed_at watermark: {new_watermark}")

        upsert_gdpr_control(spark, new_watermark, f"SUCCESS (p={n_p}, d={n_d}, v={n_v})")
        logging.info("GDPR propagation finished OK")
        spark.stop()

    except Exception as e:
        logging.error(f"GDPR propagation failed: {type(e).__name__}: {e}")
        try:
            upsert_gdpr_control(spark, None, f"FAIL: {type(e).__name__}")
        except Exception:
            pass
        spark.stop()
        raise


if __name__ == "__main__":
    main()
