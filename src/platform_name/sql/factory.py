"""Selects a :class:`SqlEngine` implementation based on environment
configuration -- the one place that decides "DuckDB locally, Spark in
test/prod", so no processor or metadata file ever needs to know or care
which backend it's actually running against.
"""
from __future__ import annotations

from typing import Any

from platform_name.common.config import Config
from platform_name.common.exceptions import ConfigurationError
from platform_name.sql.base import SqlEngine

_VALID_BACKENDS = {"duckdb", "spark"}
_DEFAULT_BACKEND = "spark"


def build_sql_engine(config: Config, spark: Any = None) -> SqlEngine:
    """Builds the SQL engine this environment is configured to use.

    Reads ``sql_backend:`` from the environment YAML (defaulting to
    ``"spark"`` if omitted, so existing environments that don't set it keep
    working). ``"duckdb"`` needs nothing else. ``"spark"`` requires a live
    ``spark`` session to be passed in -- the caller (see
    ``entrypoints/run_single_table.py``) is responsible for sourcing that,
    whether it's a local ``local[*]`` session or an already-running
    Databricks cluster session.
    """
    backend = config.get("sql_backend", _DEFAULT_BACKEND)
    if backend not in _VALID_BACKENDS:
        raise ConfigurationError(
            "Invalid sql_backend in environment configuration.",
            context={
                "environment": config.environment,
                "sql_backend": backend,
                "valid_values": sorted(_VALID_BACKENDS),
            },
        )

    if backend == "duckdb":
        from platform_name.sql.duckdb_engine import DuckDBSqlEngine

        return DuckDBSqlEngine()

    from platform_name.sql.spark_engine import SparkSqlEngine

    return SparkSqlEngine(spark)
