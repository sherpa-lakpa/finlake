"""SQL execution engine abstraction.

A :class:`SqlEngine` is what :class:`~platform_name.tables.sql_base.BaseSqlProcessor`
runs its ``.sql`` files against. Exactly two things vary between backends:
how an input path becomes a queryable view, and how a query result gets
persisted -- the processor's own code (and its ``.sql`` file) never
changes between backends.

Two implementations ship with this framework:

* :class:`~platform_name.sql.duckdb_engine.DuckDBSqlEngine` -- an embedded,
  in-process SQL engine with zero JVM/cluster dependency, ideal for local
  development: `dev.yaml` can point at this so a laptop needs nothing more
  than ``pip install duckdb`` to run every SQL-mode table.
* :class:`~platform_name.sql.spark_engine.SparkSqlEngine` -- runs the exact
  same query via ``spark.sql(...)``, against either a local ``SparkSession``
  or a real Databricks cluster session. `test.yaml`/`prod.yaml` should
  select this one.

Which backend a given environment uses is a **configuration** decision
(``sql_backend:`` in that environment's YAML -- see
:mod:`platform_name.sql.factory`), never a per-table one: a table's
``metadata.yaml`` only ever says ``execution.mode: sql``, and the same
``.sql`` file runs unmodified against whichever engine the environment
selects.
"""
from __future__ import annotations

from typing import Any, Protocol


class SqlEngine(Protocol):
    """Minimal contract a SQL execution backend must satisfy."""

    def register_view(self, name: str, path: str) -> None:
        """Makes the data at ``path`` queryable in SQL as ``name``."""
        ...

    def run_query(self, sql_text: str) -> Any:
        """Runs ``sql_text`` and returns an engine-native, lazy result
        (a Spark DataFrame or a DuckDB relation) -- not yet materialized,
        so :meth:`write_result` can persist it without an extra copy."""
        ...

    def write_result(self, result: Any, output_path: str) -> None:
        """Persists a result previously returned by :meth:`run_query`."""
        ...
