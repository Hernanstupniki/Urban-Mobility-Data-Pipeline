"""Create the shared ETL control table without overwriting existing state."""

from src.common.config import Settings
from src.common.delta_control import ensure_etl_control_table
from src.common.logging import log_event
from src.common.spark import build_spark


JOB_NAME = "migration_000_create_control_tables"


def main() -> None:
    settings = Settings.from_env()
    spark = build_spark(JOB_NAME)
    try:
        path = settings.path("_control", "etl_control")
        ensure_etl_control_table(spark, path)
        log_event(JOB_NAME, "etl_control", "SUCCESS", target=path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
