from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql.functions import col

from src.common.gold_dimensions import build_hist_frame
from src.common.gold_marts import _temporal_skeys
from src.common.spark import build_spark


SCHEMA = """
driver_id long, full_name string, license_number string, status string,
is_deleted boolean, deleted_at timestamp, created_at timestamp, updated_at timestamp,
missing_full_name boolean, missing_license_number boolean, invalid_status boolean,
source_system string, batch_id string, raw_loaded_at timestamp,
scd_hash string, valid_from timestamp, valid_to timestamp, is_current boolean
"""


def _driver_row(driver_id, status, version_hash, valid_from, valid_to, is_current):
    ts = valid_from
    return (driver_id, "D", "L123", status, False, None, ts, ts,
            False, False, False, "test", str(version_hash), ts,
            str(version_hash), valid_from, valid_to, is_current)


@pytest.mark.integration
def test_gold_temporal_lookup_points_to_event_time_version(tmp_path, monkeypatch):
    monkeypatch.setenv("SPARK_MASTER", "local[1]")
    spark = build_spark("test_scd2_temporal")
    try:
        v1_from = datetime(2026, 1, 1)
        v2_from = datetime(2026, 2, 1)
        rows = [
            _driver_row(7, "active", "h1", v1_from, v2_from, False),
            _driver_row(7, "suspended", "h2", v2_from, None, True),
            _driver_row(8, "active", "h3", v1_from, None, True),
        ]
        silver = spark.createDataFrame(rows, SCHEMA)
        hist = build_hist_frame(silver, "driver")

        # 1. distinct surrogate key per version; business key preserved
        versions = hist.filter(col("driver_id") == 7).orderBy("valid_from").collect()
        assert len(versions) == 2
        assert versions[0].surrogate_key != versions[1].surrogate_key
        old_skey, new_skey = versions[0].surrogate_key, versions[1].surrogate_key

        # 2. exactly one current version per business key
        currents = hist.filter(col("is_current")).groupBy("driver_id").count().collect()
        assert {row["driver_id"]: row["count"] for row in currents} == {7: 1, 8: 1}

        # 3. determinism: rebuilding the projection yields identical surrogates
        again = build_hist_frame(silver, "driver").select("driver_id", "valid_from", "surrogate_key").orderBy("driver_id", "valid_from")
        first = hist.select("driver_id", "valid_from", "surrogate_key").orderBy("driver_id", "valid_from")
        assert [tuple(r) for r in first.collect()] == [tuple(r) for r in again.collect()]

        # 4. temporal lookup: facts resolve to the version valid at event time
        hist_path = str(tmp_path / "dim_driver_hist")
        hist.write.format("delta").mode("overwrite").save(hist_path)
        facts = spark.createDataFrame(
            [
                (1, 7, datetime(2026, 1, 15)),   # old trip -> old version
                (2, 7, datetime(2026, 2, 15)),   # new trip -> new version
                (3, 7, datetime(2025, 12, 25)),  # before known history -> unknown (no retroactive attribution)
                (4, 99, datetime(2026, 2, 15)),  # unknown driver -> 0
                (5, 7, None),                    # null event -> 0
                (6, 7, datetime(2027, 6, 1)),    # after current -> open current version
            ],
            "trip_id long, driver_id long, requested_at timestamp",
        )
        keyed = _temporal_skeys(spark, facts, hist_path, "driver_id", "driver_skey", ["requested_at"])
        resolved = {row.trip_id: row.driver_skey for row in keyed.collect()}
        assert resolved[1] == old_skey
        assert resolved[2] == new_skey
        assert resolved[3] == 0
        assert resolved[4] == 0
        assert resolved[5] == 0
        assert resolved[6] == new_skey

        # 5. row count preserved (lookup never duplicates fact grain)
        assert keyed.count() == 6
    finally:
        spark.stop()
