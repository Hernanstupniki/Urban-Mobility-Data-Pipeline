from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")

from src.common.gold_dimensions import build_scd3_frame
from src.common.spark import build_spark


@pytest.mark.integration
def test_passenger_scd3_uses_immediate_predecessor(monkeypatch):
    monkeypatch.setenv("SPARK_MASTER", "local[1]")
    spark = build_spark("test_scd3")
    try:
        schema = """
        passenger_id long, full_name string, email string, phone string, city string,
        is_deleted boolean, deleted_at timestamp, created_at timestamp, updated_at timestamp,
        missing_full_name boolean, missing_email boolean, missing_phone boolean,
        invalid_email_format boolean, invalid_phone_format boolean, canonical_passenger_id long,
        potential_duplicate_passenger boolean, source_system string, batch_id string, raw_loaded_at timestamp,
        valid_from timestamp, valid_to timestamp, is_current boolean
        """
        rows = []
        for index, email in enumerate(["first@example.com", "second@example.com", "third@example.com"], 1):
            timestamp = datetime(2026, 1, index)
            rows.append((7, "P", email, "123", "City", False, None, timestamp, timestamp,
                         False, False, False, False, False, 7, False, "test", str(index), timestamp,
                         timestamp, None, index == 3))
        source = spark.createDataFrame(rows, schema)
        result = build_scd3_frame(source, "passenger").first()
        assert result.email == "third@example.com"
        assert result.prev_email == "second@example.com"
    finally:
        spark.stop()
