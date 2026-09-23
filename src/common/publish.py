"""Publish Gold marts to the separate analytics PostgreSQL serving layer.

Strategy per table: full refresh via staging + atomic swap.

1. Spark JDBC writes the whole Delta table to staging.<table> (overwrite).
2. ONE PostgreSQL transaction swaps staging into reporting, validates the
   row count against the Delta source, upserts control.publish_state and
   appends control.publish_log. On any error it rolls back: readers keep the
   previous published version and the control state does not advance.

Why full refresh for facts too: Gold facts are deterministic full rebuilds
(including GDPR erasure), so publishing incrementally would need CDC
semantics (tombstones, deletes propagation) for no real gain at this scale.
Full refresh keeps idempotency by construction: any retry converges to the
same published content, never duplicates.

Control state is owned here and lives in the analytics DB, fully separate
from Bronze/Silver/Gold watermarks in the lake _control tables.
"""

from __future__ import annotations

import os
import uuid

import psycopg2
from delta.tables import DeltaTable
from psycopg2 import sql

from src.common.config import AnalyticsSettings, Settings
from src.common.logging import log_event
from src.common.spark import build_spark

# Serving-layer specs: Delta source, primary key and secondary indexes.
# Column lists were validated against the Gold contracts (see data_contracts.md).
SPECS = {
    "dim_date": {
        "source": ("gold", "_conformed", "static", "dim_date"),
        "pk": ["date_key"],
        "indexes": [],
    },
    "dim_payment_method": {
        "source": ("gold", "_conformed", "static", "dim_payment_method"),
        "pk": ["payment_method_key"],
        "indexes": [],
    },
    "dim_zone": {
        "source": ("gold", "_conformed", "static", "dim_zone"),
        "pk": ["zone_id"],
        "indexes": [],
    },
    "dim_passenger": {
        "source": ("gold", "_conformed", "snapshot", "dim_passenger"),
        "pk": ["passenger_id"],
        "indexes": [],
    },
    "dim_driver": {
        "source": ("gold", "_conformed", "snapshot", "dim_driver"),
        "pk": ["driver_id"],
        "indexes": [],
    },
    "dim_vehicle": {
        "source": ("gold", "_conformed", "snapshot", "dim_vehicle"),
        "pk": ["vehicle_id"],
        "indexes": [["driver_id"]],
    },
    "fact_trips": {
        "source": ("gold", "_marts", "facts", "fact_trips"),
        "pk": ["trip_id"],
        "indexes": [["request_date_key"], ["driver_key"], ["passenger_key"], ["pickup_zone_key"], ["dropoff_zone_key"], ["driver_skey"], ["passenger_skey"], ["vehicle_skey"]],
    },
    "fact_payments": {
        "source": ("gold", "_marts", "facts", "fact_payments"),
        "pk": ["payment_id"],
        "indexes": [["trip_id"], ["payment_date_key"], ["payment_method_key"]],
    },
    "fact_ratings": {
        "source": ("gold", "_marts", "facts", "fact_ratings"),
        "pk": ["rating_id"],
        "indexes": [["trip_key"], ["driver_key"], ["passenger_key"], ["rating_date_key"], ["driver_skey"], ["passenger_skey"]],
    },
    "agg_trips_daily": {
        "source": ("gold", "_marts", "aggregates", "agg_trips_daily"),
        "pk": ["date_key"],
        "indexes": [],
    },
    "agg_driver_daily": {
        "source": ("gold", "_marts", "aggregates", "agg_driver_daily"),
        "pk": ["trip_date_key", "driver_id"],
        "indexes": [["driver_id"]],
    },
}

_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS reporting;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS control.publish_state (
    table_name text PRIMARY KEY,
    last_batch_id text,
    last_row_count bigint,
    source_row_count bigint,
    last_successful_publish timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS control.publish_log (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id text NOT NULL,
    table_name text NOT NULL,
    status text NOT NULL,
    source_row_count bigint,
    published_row_count bigint,
    message text,
    created_at timestamptz NOT NULL DEFAULT now()
);
"""


def _connect(analytics: AnalyticsSettings):
    return psycopg2.connect(
        host=analytics.host,
        port=analytics.port,
        dbname=analytics.db,
        user=analytics.user,
        password=analytics.password,
        connect_timeout=10,
    )


def _tables_to_publish() -> list[str]:
    raw = os.getenv("PUBLISH_TABLES", ",".join(SPECS))
    tables = [t.strip() for t in raw.split(",") if t.strip()]
    unknown = [t for t in tables if t not in SPECS]
    if unknown:
        raise ValueError(f"Unknown table(s) in PUBLISH_TABLES: {unknown}")
    return tables


def _log_failure(analytics: AnalyticsSettings, batch_id: str, table: str, message: str) -> None:
    # Autocommit best-effort audit: a publish_state update would be rolled
    # back with the failed swap, but the log must survive it.
    conn = _connect(analytics)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("INSERT INTO control.publish_log (batch_id, table_name, status, message) VALUES (%s, %s, %s, %s)"),
                (batch_id, table, "FAIL", message[:500]),
            )
    except Exception:
        pass
    finally:
        conn.close()


def _publish_table(spark, settings: Settings, analytics: AnalyticsSettings, table: str, batch_id: str) -> int:
    spec = SPECS[table]
    source_path = settings.path(*spec["source"])
    if not DeltaTable.isDeltaTable(spark, source_path):
        raise RuntimeError(f"Required Gold table not found: {source_path}")
    frame = spark.read.format("delta").load(source_path)

    columns = set(frame.columns)
    missing_pk = [c for c in spec["pk"] if c not in columns]
    if missing_pk:
        raise RuntimeError(f"{source_path} is missing primary key columns: {missing_pk}")

    source_count = frame.count()

    # 1) Full JDBC write to staging (drops+recreates staging.<table>).
    frame.write.format("jdbc").options(
        url=analytics.jdbc_url,
        dbtable=f"staging.{table}",
        user=analytics.user,
        password=analytics.password,
        driver="org.postgresql.Driver",
        batchsize="5000",
    ).mode("overwrite").save()

    # 2) Atomic swap + control update in ONE transaction.
    conn = _connect(analytics)
    try:
        with conn:  # commit on success, rollback on exception
            with conn.cursor() as cur:
                published_name = sql.Identifier("reporting", table)
                staged_name = sql.Identifier("staging", table)
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(published_name))
                cur.execute(sql.SQL("ALTER TABLE {} SET SCHEMA reporting").format(staged_name))
                pk = sql.SQL(", ").join(map(sql.Identifier, spec["pk"]))
                cur.execute(
                    sql.SQL("ALTER TABLE {} ADD PRIMARY KEY ({})").format(published_name, pk)
                )
                for i, idx_cols in enumerate(spec["indexes"], start=1):
                    cols = sql.SQL(", ").join(map(sql.Identifier, idx_cols))
                    cur.execute(
                        sql.SQL("CREATE INDEX {} ON {} ({})").format(
                            sql.Identifier(f"{table}_ix{i}"), published_name, cols
                        )
                    )
                cur.execute(sql.SQL("SELECT count(*) FROM {}").format(published_name))
                published = cur.fetchone()[0]
                if published != source_count:
                    raise RuntimeError(
                        f"Row count mismatch for {table}: source={source_count} published={published}"
                    )
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO control.publish_state
                            (table_name, last_batch_id, last_row_count, source_row_count, last_successful_publish, updated_at)
                        VALUES (%s, %s, %s, %s, now(), now())
                        ON CONFLICT (table_name) DO UPDATE SET
                            last_batch_id = EXCLUDED.last_batch_id,
                            last_row_count = EXCLUDED.last_row_count,
                            source_row_count = EXCLUDED.source_row_count,
                            last_successful_publish = EXCLUDED.last_successful_publish,
                            updated_at = EXCLUDED.updated_at
                        """
                    ),
                    (table, batch_id, published, source_count),
                )
                cur.execute(
                    sql.SQL(
                        "INSERT INTO control.publish_log (batch_id, table_name, status, source_row_count, published_row_count) VALUES (%s, %s, %s, %s, %s)"
                    ),
                    (batch_id, table, "SUCCESS", source_count, published),
                )
        return published
    except Exception:
        _log_failure(analytics, batch_id, table, f"swap failed for {table}")
        raise
    finally:
        conn.close()


def publish_reporting() -> None:
    settings = Settings.from_env()
    analytics = AnalyticsSettings.from_env()
    tables = _tables_to_publish()
    batch_id = str(uuid.uuid4())
    job_name = "publish_reporting"

    spark = build_spark(job_name)
    try:
        conn = _connect(analytics)
        with conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS control")
                cur.execute(_SCHEMA_SQL)
        conn.close()
        log_event(job_name, "begin", "START", batch_id=batch_id, tables=len(tables))
        for table in tables:
            published = _publish_table(spark, settings, analytics, table, batch_id)
            log_event(job_name, table, "SUCCESS", row_count=published)
        log_event(job_name, "end", "SUCCESS", batch_id=batch_id, tables=len(tables))
    finally:
        spark.stop()
