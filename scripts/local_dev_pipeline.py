"""Runs the full example pipeline locally against dev.yaml -- Pandas
tables via TableRunner as usual, plus the PySpark table
(gold.customer_risk) and the SQL table (gold.top_movers, running on DuckDB
since dev.yaml sets sql_backend: duckdb).

Intended to run inside the Docker dev container (see docker-compose.yml),
or any local environment with:

    pip install -e ".[dev,spark,duckdb]"

and a JVM available for PySpark. See docs/PYSPARK_AND_SQL_GUIDE.md for the
full explanation of what's happening here.

Usage:
    docker compose run --rm dev python scripts/local_dev_pipeline.py
    # or, outside Docker, with pyspark/duckdb installed locally:
    python scripts/local_dev_pipeline.py
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from platform_name.common.config import Config
from platform_name.engine.models import ExecutionContext
from platform_name.engine.runner import TableRunner
from platform_name.sql.factory import build_sql_engine


def main() -> None:
    # Real, distributed Spark -- just running across this machine's cores
    # via local[*] rather than a cluster. Needed for gold.customer_risk
    # (execution.mode: pyspark). gold.top_movers (execution.mode: sql)
    # doesn't use this session at all locally, since dev.yaml's
    # sql_backend: duckdb selects DuckDBSqlEngine instead -- included here
    # only because build_sql_engine's signature accepts an optional spark
    # session for environments where sql_backend: spark is selected.
    spark = SparkSession.builder.master("local[*]").appName("local-dev-pipeline").getOrCreate()

    config = Config("configs/environments/dev.yaml")
    sql_engine = build_sql_engine(config, spark=spark)
    context = ExecutionContext(config=config, spark=spark, sql_engine=sql_engine)
    runner = TableRunner()

    # Each of these shares the same context, so gold.returns -- the common
    # upstream dependency of both the PySpark and the SQL table -- only
    # executes once, exactly like it would in a single Databricks job run.
    targets = ["gold.performance_summary", "gold.customer_risk", "gold.top_movers"]
    for table in targets:
        result = runner.run(table_name=table, context=context)
        print(f"{table}: {result.status} (rows={result.row_count})")

    print("\nFull execution history for this run:")
    for name, result in context.extra["execution_results"].items():
        duration = result.duration_seconds or 0.0
        print(f"  {name}: {result.status} ({duration:.3f}s, rows={result.row_count})")

    spark.stop()


if __name__ == "__main__":
    main()
