"""Spark-backed Parquet storage adapter.

The PySpark equivalent of ``storage/local.py``'s
:class:`~platform_name.storage.local.LocalFileSystemStorage`: a small
object owning physical read/write concerns for PySpark DataFrame-API
processors, so processor business logic never calls Spark I/O directly.

Deliberately Parquet, not Delta -- this keeps a first PySpark table
runnable with nothing more than ``pip install pyspark`` (no Delta Lake JAR
configuration needed), and reads the exact same files a Pandas-mode
upstream table already produces. Reach for
:class:`~platform_name.storage.delta.DeltaStorageAdapter` instead once a
table needs Delta's transactional/merge features against real ADLS/Unity
Catalog storage.
"""
from __future__ import annotations

from typing import Any


class SparkParquetStorage:
    def __init__(self, spark: Any) -> None:
        if spark is None:
            raise ValueError("SparkParquetStorage requires a live `spark` SparkSession.")
        self.spark = spark

    def read_parquet(self, path: str):
        return self.spark.read.parquet(str(path))

    def write_parquet(self, df: Any, path: str) -> None:
        df.write.mode("overwrite").parquet(str(path))
