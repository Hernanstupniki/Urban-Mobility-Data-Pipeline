---
name: pyspark-processing
description: Use for PySpark DataFrame, Spark SQL, Delta or Parquet transformations, partitioning, window functions, and distributed performance in the Urban Mobility pipeline.
---

# PySpark processing

Apply to changes under `src/bronze`, `src/silver`, `src/gold`, or Spark logic in `src/common` and pipeline tests. Inspect the input grain and contracts before changing a transform. Preserve idempotence and the Bronze/Silver/Gold boundaries.

Use the shared `build_spark()` factory in `src/common/spark.py`; do not create a second SparkSession. Review joins, shuffles, partition counts, null behavior, and Delta merge keys. Avoid collecting large datasets to the driver. Validate representative rows and counts, then run focused tests.

Reference: https://github.com/palantir/pyspark-style-guide (style guide, not the source of this skill).
