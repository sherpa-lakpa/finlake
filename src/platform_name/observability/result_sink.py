"""Pluggable sinks for structured :class:`ExecutionResult` records.

``StdoutResultSink`` is what runs by default inside a Databricks task --
the Databricks Jobs UI captures task stdout automatically, so this alone
gives every task's outcome a queryable-by-eye log line with zero extra
infrastructure required to get started.

For cross-run trend analysis (row-count drift, duration drift over weeks of
runs) you eventually want these records queryable *as a table*, not just as
log lines. ``DeltaResultSink`` is the documented extension point for that --
swap it in once a ``SparkSession`` is available (inside an actual
Databricks job), pointed at a control table. It is intentionally not
implemented here: this repository has no Spark dependency by design (see
README, "Databricks deployment concept").
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Protocol

from platform_name.engine.models import ExecutionResult


class ResultSink(Protocol):
    """Anything that can durably record an :class:`ExecutionResult`."""

    def record(self, result: ExecutionResult) -> None: ...


class StdoutResultSink:
    """Prints one JSON line per result.

    Captured automatically by Databricks task logs -- no extra
    infrastructure required. This is the default sink used by
    :mod:`platform_name.entrypoints.run_single_table`.
    """

    def record(self, result: ExecutionResult) -> None:
        print(json.dumps(result.to_dict()))


class JsonlFileResultSink:
    """Appends one JSON line per result to a local file.

    Useful for local development and CI smoke tests. Not intended for
    production use across a distributed cluster (concurrent writers to a
    single local file are not coordinated).
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, result: ExecutionResult) -> None:
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(result.to_dict()) + "\n")


class DeltaResultSink:
    """Extension point: write ``ExecutionResult`` rows to a Delta control
    table from inside a Databricks job, where a ``SparkSession`` is
    available.

    Not implemented in this repository. To wire this up once running inside
    Databricks::

        spark.createDataFrame([result.to_dict()]).write.format("delta") \\
            .mode("append").save(control_table_path)

    or, for a Unity Catalog table::

        spark.createDataFrame([result.to_dict()]).write.format("delta") \\
            .mode("append").saveAsTable("catalog.schema.execution_results")
    """

    def __init__(self, spark: object, control_table_path: str) -> None:
        self.spark = spark
        self.control_table_path = control_table_path

    def record(self, result: ExecutionResult) -> None:
        raise NotImplementedError(
            "DeltaResultSink requires a Spark environment. See the class "
            "docstring for the implementation to add once running inside "
            "Databricks."
        )
