"""Spark-backed :class:`SqlEngine`.

Runs SQL via ``spark.sql(...)`` against Parquet-backed views -- the same
physical format the rest of this repository's example domain already uses
(see ``storage/local.py``), so a Spark-mode table can read a Pandas-mode
upstream table's output directly with no format conversion. Swapping to
Delta later (once real ADLS/Unity Catalog storage is in place -- see
``storage/delta.py``) only means changing the two ``format("parquet")``
calls below to ``format("delta")``; nothing about how processors or
metadata declare paths changes.

Works identically whether ``spark`` is a local ``local[*]`` session (dev,
if a team prefers real Spark over DuckDB there) or a Databricks cluster
session obtained automatically in production -- see
``entrypoints/run_single_table.py``.
"""
from __future__ import annotations

from typing import Any


class SparkSqlEngine:
    def __init__(self, spark: Any) -> None:
        if spark is None:
            raise ValueError(
                "SparkSqlEngine requires a live `spark` SparkSession. See "
                "entrypoints/run_single_table.py for how one is obtained in "
                "production, or docs/PYSPARK_AND_SQL_GUIDE.md for local dev."
            )
        self.spark = spark

    def register_view(self, name: str, path: str) -> None:
        self.spark.read.format("parquet").load(path).createOrReplaceTempView(name)

    def run_query(self, sql_text: str) -> Any:
        return self.spark.sql(sql_text)

    def write_result(self, result: Any, output_path: str) -> None:
        result.write.format("parquet").mode("overwrite").save(output_path)
