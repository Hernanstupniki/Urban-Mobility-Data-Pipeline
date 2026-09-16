"""Small runtime check for the configured local or standalone Spark master."""

from src.common.spark import build_spark


def main() -> None:
    spark = build_spark("urban_mobility_spark_smoke")
    try:
        conf = spark.sparkContext.getConf()
        print(f"SMOKE_MASTER={spark.sparkContext.master}")
        print(f"SMOKE_DRIVER_MEMORY={conf.get('spark.driver.memory')}")
        print(f"SMOKE_EXECUTOR_MEMORY={conf.get('spark.executor.memory')}")
        jdbc_driver = spark._jvm.java.lang.Class.forName(  # noqa: SLF001
            "org.postgresql.Driver"
        ).getName()
        print(f"SMOKE_JDBC_DRIVER={jdbc_driver}")
        print(f"SMOKE_COUNT={spark.range(100).repartition(2).count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
