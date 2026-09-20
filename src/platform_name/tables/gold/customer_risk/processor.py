"""Gold processor: per-security risk score, computed with PySpark's
DataFrame API.

Demonstrates ``execution.mode: pyspark`` end to end: a real
:class:`~platform_name.tables.pyspark_base.BasePySparkProcessor` subclass,
reading/writing Parquet via
:class:`~platform_name.storage.spark_parquet.SparkParquetStorage`. Runs
locally against a `local[*]` SparkSession (see dev setup in
docs/PYSPARK_AND_SQL_GUIDE.md) and in test/prod against whatever Spark
session `entrypoints/run_single_table.py` attaches to (local or a real
Databricks cluster) -- identical processor code either way.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from platform_name.storage.spark_parquet import SparkParquetStorage
from platform_name.tables.pyspark_base import BasePySparkProcessor


class CustomerRiskProcessor(BasePySparkProcessor):
    """Risk score per security: mean absolute daily return."""

    def __init__(self, spark, returns_path: str, output_path: str) -> None:
        super().__init__(spark)
        self.returns_path = returns_path
        self.output_path = output_path
        self.storage = SparkParquetStorage(spark)

    def read(self) -> DataFrame:
        return self.storage.read_parquet(self.returns_path)

    def transform(self, data: DataFrame) -> DataFrame:
        return (
            data.filter(F.col("daily_return").isNotNull())
            .groupBy("security_id")
            .agg(F.avg(F.abs(F.col("daily_return"))).alias("risk_score"))
        )

    def write(self, data: DataFrame) -> None:
        self.storage.write_parquet(data, self.output_path)
