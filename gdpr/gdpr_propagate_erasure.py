"""Propagate processed GDPR erasures through Bronze, Silver and Gold.

The timestamp watermark is deliberately inclusive. A successful per-request
audit marker supplies the second half of the compound watermark, so requests
sharing the same ``processed_at`` value cannot be skipped.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, current_timestamp, lit, lower

from src.common.config import Settings, env_bool
from src.common.logging import log_event
from src.common.spark import build_spark


JOB_NAME = "gdpr_propagate_erasure"
EPOCH = datetime(1970, 1, 1)
CONTROL_SCHEMA = "job_name string, last_processed_at timestamp, last_success_ts timestamp, last_status string"
AUDIT_SCHEMA = """
env string, run_id string, job_name string, request_id string,
processed_at timestamp, applied_at timestamp, subject_type string,
subject_id_hash string, layer string, table_name string, action string,
columns_scrubbed array<string>, status string
"""


def _delta_exists(spark, path: str) -> bool:
    # Do not mask storage/catalog exceptions: only a real false means absent.
    return DeltaTable.isDeltaTable(spark, path)


def _ensure_delta(spark, path: str, schema: str) -> None:
    if not _delta_exists(spark, path):
        spark.createDataFrame([], schema).write.format("delta").mode("errorifexists").save(path)


def _watermark(spark, path: str) -> datetime:
    if not _delta_exists(spark, path):
        return EPOCH
    rows = spark.read.format("delta").load(path).filter(col("job_name") == JOB_NAME).select("last_processed_at").take(1)
    return rows[0][0] if rows and rows[0][0] else EPOCH


def _record_control(spark, path: str, status: str, processed_at=None) -> None:
    _ensure_delta(spark, path, CONTROL_SCHEMA)
    success = status == "SUCCESS"
    source = spark.createDataFrame(
        [(JOB_NAME, processed_at, status)],
        "job_name string, last_processed_at timestamp, last_status string",
    ).withColumn("attempted_at", current_timestamp())
    DeltaTable.forPath(spark, path).alias("t").merge(source.alias("s"), "t.job_name = s.job_name").whenMatchedUpdate(set={
        "last_processed_at": "coalesce(s.last_processed_at, t.last_processed_at)",
        "last_success_ts": "s.attempted_at" if success else "t.last_success_ts",
        "last_status": "s.last_status",
    }).whenNotMatchedInsert(values={
        "job_name": "s.job_name", "last_processed_at": "s.last_processed_at",
        "last_success_ts": "s.attempted_at" if success else "CAST(NULL AS TIMESTAMP)",
        "last_status": "s.last_status",
    }).execute()


def _completed_request_ids(spark, audit_path: str) -> DataFrame:
    _ensure_delta(spark, audit_path, AUDIT_SCHEMA)
    return spark.read.format("delta").load(audit_path).filter(
        (col("job_name") == JOB_NAME) & (col("action") == "complete") & (col("status") == "SUCCESS")
    ).select(col("request_id").cast("long").alias("completed_request_id")).dropDuplicates()


def _audit_rows(spark, audit_path: str, rows: list[tuple]) -> None:
    if not rows:
        return
    _ensure_delta(spark, audit_path, AUDIT_SCHEMA)
    source = spark.createDataFrame(rows, AUDIT_SCHEMA)
    target = DeltaTable.forPath(spark, audit_path)
    target.alias("t").merge(source.alias("s"), """
        t.request_id = s.request_id AND t.layer = s.layer
        AND t.table_name = s.table_name AND t.action = s.action
    """).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def _token_expression(secret_digest: str, key: str) -> str:
    return f"concat('ERASED_', substring(sha2(concat('{secret_digest}', cast(t.{key} as string)), 256), 1, 24))"


def _ids(spark, values: set[int], column: str) -> DataFrame:
    return spark.createDataFrame([(value,) for value in sorted(values)], f"{column} long")


def _collect_ids(frame: DataFrame, column: str) -> set[int]:
    if column not in frame.columns:
        return set()
    return {int(row[0]) for row in frame.select(column).where(col(column).isNotNull()).distinct().collect()}


def _matching_ids(spark, path: str, filter_column: str, values: set[int], result_column: str) -> set[int]:
    if not values or not _delta_exists(spark, path):
        return set()
    frame = spark.read.format("delta").load(path)
    if filter_column not in frame.columns or result_column not in frame.columns:
        return set()
    return _collect_ids(frame.filter(col(filter_column).isin(sorted(values))), result_column)


def _update(spark, path: str, key: str, values: set[int], assignments: dict[str, str], *, required=False) -> list[str]:
    if not values:
        return []
    if not _delta_exists(spark, path):
        if required:
            raise RuntimeError(f"Required GDPR target is missing: {path}")
        return []
    columns = set(spark.read.format("delta").load(path).columns)
    if key not in columns:
        if required:
            raise ValueError(f"Required GDPR key {key} is missing from {path}")
        return []
    selected = {name: expression for name, expression in assignments.items() if name in columns}
    if not selected:
        return []
    source = _ids(spark, values, key)
    DeltaTable.forPath(spark, path).alias("t").merge(source.alias("s"), f"t.{key} = s.{key}").whenMatchedUpdate(set=selected).execute()
    return sorted(selected)


def _dimension_assignments(kind: str, secret: str, key: str) -> dict[str, str]:
    token = _token_expression(secret, key)
    if kind == "passenger":
        values = {"full_name": token, "email": "CAST(NULL AS STRING)", "phone": "CAST(NULL AS STRING)", "city": "CAST(NULL AS STRING)"}
    elif kind == "driver":
        values = {"full_name": token, "license_number": token}
    else:
        values = {"plate_number": token}
    values.update({f"prev_{name}": expression for name, expression in list(values.items())})
    values.update({"is_deleted": "true", "deleted_at": "coalesce(t.deleted_at, current_timestamp())"})
    flag_values = {
        "missing_email": "true", "missing_phone": "true", "invalid_email_format": "false",
        "invalid_phone_format": "false", "potential_duplicate_passenger": "false",
        "canonical_passenger_id": f"t.{key}",
        "missing_full_name": "false", "missing_license_number": "false", "missing_plate_number": "false",
    }
    values.update(flag_values)
    return values


def _subject_paths(settings: Settings, kind: str) -> list[tuple[str, str, bool]]:
    plural = {"passenger": "passengers", "driver": "drivers", "vehicle": "vehicles"}[kind]
    return [
        ("bronze", settings.path("bronze", plural), True),
        ("silver", settings.path("silver", plural), True),
        ("gold_hist", settings.path("gold", "_conformed", "hist", f"dim_{kind}_hist"), False),
        ("gold_hist_legacy", settings.path("gold", "_conformed", "hist", f"dim_{kind}"), False),
        ("gold_snapshot", settings.path("gold", "_conformed", "snapshot", f"dim_{kind}"), False),
    ]


def _process_request(spark, settings: Settings, row, secret: str, run_id: str, audit_path: str) -> tuple[set[str], tuple]:
    kind = row.subject_type
    subject_id = int(row.subject_id)
    subject_ids = {subject_id}
    touched: set[str] = set()
    audit: list[tuple] = []
    key = f"{kind}_id"
    now = datetime.utcnow()
    fingerprint = hashlib.sha256(f"{secret}:{kind}:{subject_id}".encode()).hexdigest()

    vehicle_ids = subject_ids if kind == "vehicle" else set()
    if kind == "driver":
        for layer in ("bronze", "silver"):
            vehicle_ids |= _matching_ids(spark, settings.path(layer, "vehicles"), "driver_id", subject_ids, "vehicle_id")

    for layer, path, required in _subject_paths(settings, kind):
        columns = _update(spark, path, key, subject_ids, _dimension_assignments(kind, secret, key), required=required)
        if columns:
            touched.add(path)
            audit.append((settings.env, run_id, JOB_NAME, str(row.request_id), row.processed_at, now, kind, fingerprint, layer, path.rsplit("/", 1)[-1], "anonymize", columns, "SUCCESS"))

    # A driver erasure also owns vehicle plates. Apply to every layer and history.
    if vehicle_ids and kind == "driver":
        for layer, path, required in _subject_paths(settings, "vehicle"):
            columns = _update(spark, path, "vehicle_id", vehicle_ids, _dimension_assignments("vehicle", secret, "vehicle_id"), required=required)
            if columns:
                touched.add(path)
                audit.append((settings.env, run_id, JOB_NAME, str(row.request_id), row.processed_at, now, kind, fingerprint, layer, path.rsplit("/", 1)[-1], "anonymize_derived_vehicle", columns, "SUCCESS"))

    passenger_ids = subject_ids if kind == "passenger" else set()
    driver_ids = subject_ids if kind == "driver" else set()
    trip_ids: set[int] = set()
    for layer in ("bronze", "silver"):
        trips_path = settings.path(layer, "trips")
        filters = [("passenger_id", passenger_ids), ("driver_id", driver_ids), ("vehicle_id", vehicle_ids)]
        for filter_column, values in filters:
            trip_ids |= _matching_ids(spark, trips_path, filter_column, values, "trip_id")
            columns = _update(spark, trips_path, filter_column, values, {"cancel_note": "CAST(NULL AS STRING)"})
            if columns:
                touched.add(trips_path)
        ratings_path = settings.path(layer, "ratings")
        for filter_column, values in (("passenger_id", passenger_ids), ("driver_id", driver_ids)):
            columns = _update(spark, ratings_path, filter_column, values, {"comment": "CAST(NULL AS STRING)"})
            if columns:
                touched.add(ratings_path)
        payments_path = settings.path(layer, "payments")
        columns = _update(spark, payments_path, "trip_id", trip_ids, {"provider_ref": "CAST(NULL AS STRING)"})
        if columns:
            touched.add(payments_path)

    fact_trips = settings.path("gold", "_marts", "facts", "fact_trips")
    for filter_column, values in (("passenger_id", passenger_ids), ("driver_id", driver_ids), ("vehicle_id", vehicle_ids)):
        trip_ids |= _matching_ids(spark, fact_trips, filter_column, values, "trip_id")
        columns = _update(spark, fact_trips, filter_column, values, {"cancel_note": "CAST(NULL AS STRING)"})
        if columns:
            touched.add(fact_trips)
    fact_payments = settings.path("gold", "_marts", "facts", "fact_payments")
    columns = _update(spark, fact_payments, "trip_id", trip_ids, {"provider_ref": "CAST(NULL AS STRING)"})
    if columns:
        touched.add(fact_payments)

    _audit_rows(spark, audit_path, audit)
    completion = (settings.env, run_id, JOB_NAME, str(row.request_id), row.processed_at, now, kind, fingerprint, "control", "gdpr_request", "complete", [], "SUCCESS")
    return touched, completion


def main() -> None:
    settings = Settings.from_env(require_database=True)
    secret = os.getenv("GDPR_HASH_KEY", "")
    if len(secret) < 16:
        raise ValueError("GDPR_HASH_KEY must be set to at least 16 characters; predictable defaults are forbidden")
    secret_digest = hashlib.sha256(secret.encode()).hexdigest()
    control_path = settings.path("_control", "gdpr_control")
    audit_path = settings.path("_control", "gdpr_audit")
    audit_enabled = env_bool("AUDIT_ENABLED", True)
    if not audit_enabled:
        raise ValueError("AUDIT_ENABLED=false is not allowed because completion markers protect the watermark")
    run_id = str(uuid.uuid4())
    vacuum_hours = int(os.getenv("GDPR_VACUUM_HOURS", "168"))
    unsafe = env_bool("GDPR_UNSAFE_VACUUM", False)
    if vacuum_hours < 168 and not (settings.env == "dev" and unsafe):
        raise ValueError("GDPR_VACUUM_HOURS below 168 requires ENV=dev and GDPR_UNSAFE_VACUUM=1")
    if unsafe:
        os.environ.setdefault("DELTA_RETENTION_DURATION_CHECK_ENABLED", "false")
    spark = build_spark(JOB_NAME)
    touched: set[str] = set()
    completion_rows: list[tuple] = []
    try:
        watermark = _watermark(spark, control_path)
        completed = _completed_request_ids(spark, audit_path)
        requests = (
            spark.read.format("jdbc").option("url", settings.jdbc_url)
            .option("dbtable", "mobility.gdpr_requests").option("user", settings.db_user)
            .option("password", settings.db_password).option("driver", "org.postgresql.Driver").load()
            .withColumn("subject_type", lower(coalesce(col("subject_type").cast("string"), lit("passenger"))))
            .withColumn("subject_id", coalesce(col("subject_id"), col("passenger_id")).cast("long"))
            .filter((col("status") == "processed") & (col("request_type") == "erasure") & col("processed_at").isNotNull())
            .filter(col("processed_at") >= lit(watermark))
            .join(completed, col("request_id") == col("completed_request_id"), "left_anti")
            .select("request_id", "processed_at", "subject_type", "subject_id")
            .orderBy("processed_at", "request_id")
        )
        rows = requests.collect()
        for row in rows:
            if row.subject_type not in {"passenger", "driver", "vehicle"} or row.subject_id is None:
                raise ValueError(f"Unsupported or malformed GDPR request {row.request_id}")
            request_touched, completion = _process_request(spark, settings, row, secret_digest, run_id, audit_path)
            touched |= request_touched
            completion_rows.append(completion)

        for path in sorted(touched):
            DeltaTable.forPath(spark, path).vacuum(vacuum_hours)
        _audit_rows(spark, audit_path, completion_rows)
        if rows:
            _record_control(spark, control_path, "SUCCESS", max(row.processed_at for row in rows))
        else:
            _record_control(spark, control_path, "SUCCESS", watermark)
        log_event(JOB_NAME, "propagate", "SUCCESS", row_count=len(rows), touched_tables=len(touched), vacuum_hours=vacuum_hours)
    except Exception as exc:
        try:
            _record_control(spark, control_path, f"FAIL:{type(exc).__name__}")
        except Exception as control_exc:
            log_event(JOB_NAME, "control", "FAIL", error=repr(control_exc))
        log_event(JOB_NAME, "propagate", "FAIL", error=repr(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
