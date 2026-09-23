"""Check source timestamp recovery and name casing with the shared Spark factory."""

import unittest
from datetime import datetime

from pyspark.sql.functions import col

from src.common.spark import build_spark
from src.common.spark_normalization import normalized_person_name, normalize_requested_at_source


class SourceNormalizationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = build_spark("source_normalization_test")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_names_and_dates(self):
        typed = datetime(2026, 9, 2, 11, 30, 15)
        rows = [
            ("  michael   powell ", typed, "2026/09/02 08:30:15"),
            ("RACHEL SMITH", typed, "02/09/2026 08:30:15"),
            ("ANONYMIZED", typed, "not-a-date"),
            ("lower case", typed, "2026-09-03T08:30:15"),
            ("Clean Name", typed, None),
        ]
        frame = self.spark.createDataFrame(rows, ["full_name", "requested_at", "requested_at_source"])
        result = (
            normalize_requested_at_source(frame)
            .withColumn("full_name", normalized_person_name(col("full_name")))
            .collect()
        )
        assert [(r.full_name, r.requested_at_was_normalized, r.requested_at_source_invalid)
                for r in result] == [
            ("Michael Powell", True, False),
            ("Rachel Smith", True, False),
            ("ANONYMIZED", False, True),
            ("Lower Case", False, True),
            ("Clean Name", False, False),
        ]
        assert all(row.requested_at == typed for row in result)


    def test_existing_delta_table_accepts_new_source_columns(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trips"
            self.spark.createDataFrame([(1,)], "trip_id long").write.format("delta").save(str(path))
            self.spark.sql(
                f"ALTER TABLE delta.`{path}` ADD COLUMNS (requested_at_source STRING)"
            )
            updated = self.spark.createDataFrame(
                [(2, "02/09/2026 08:30:15")],
                "trip_id long, requested_at_source string",
            )
            updated.write.format("delta").mode("append").option(
                "mergeSchema", "true"
            ).save(str(path))
            rows = self.spark.read.format("delta").load(str(path)).orderBy("trip_id").collect()
            assert [(row.trip_id, row.requested_at_source) for row in rows] == [
                (1, None), (2, "02/09/2026 08:30:15")
            ]


if __name__ == "__main__":
    unittest.main()
