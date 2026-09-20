"""SQL-driven processor base class, backend-agnostic.

For transformation logic that's most naturally expressed as SQL rather
than DataFrame method chains -- useful when SQL-fluent analysts, not just
Python engineers, need to read and edit the transformation. A subclass
points at a ``.sql`` file living next to its own ``processor.py``; this
base class registers every resolved input path as a queryable view (via
whichever :class:`~platform_name.sql.base.SqlEngine` was injected) and
then runs that file's query.

Deliberately **not** tied to Spark directly: the injected ``sql_engine``
might be :class:`~platform_name.sql.duckdb_engine.DuckDBSqlEngine` (local
dev, zero cluster dependency) or
:class:`~platform_name.sql.spark_engine.SparkSqlEngine` (test/prod, a real
Spark/Databricks session) -- selected entirely by environment
configuration (see :mod:`platform_name.sql.factory`), never by this class
or by the table's own metadata. The same ``.sql`` file and the same
processor code run against either backend unmodified.

Set ``execution.mode: sql`` in metadata for these processors, exactly like
``execution.mode: pyspark`` for a plain DataFrame-API processor selects
:class:`~platform_name.tables.pyspark_base.BasePySparkProcessor` -- both
receive their respective injected dependency the same generic way, keyed
only on ``execution_mode`` (see ``engine/processor_factory.py``).
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from platform_name.tables.base import BaseProcessor


class BaseSqlProcessor(BaseProcessor):
    """Runs a ``.sql`` file against views built from resolved input paths,
    via whichever :class:`~platform_name.sql.base.SqlEngine` was injected.

    Subclasses set the class attribute ``sql_file`` to a filename resolved
    relative to their own module's directory, and accept every declared
    input path as a keyword argument -- each becomes a view named after
    that keyword. For example, metadata declaring::

        paths:
          some_input:
            file: some_input.parquet
            root: silver
            parameter: some_input

    makes a view named ``some_input`` available to the SQL file.
    """

    sql_file: str

    def __init__(self, sql_engine: Any, output_path: str, **input_paths: str) -> None:
        if sql_engine is None:
            raise ValueError(
                "BaseSqlProcessor requires an injected `sql_engine`. "
                "Locally, this typically means no ExecutionContext.sql_engine "
                "was built for this run -- see docs/PYSPARK_AND_SQL_GUIDE.md."
            )
        self.sql_engine = sql_engine
        self.output_path = output_path
        self.input_paths = input_paths

    def _sql_file_path(self) -> Path:
        module = sys.modules[type(self).__module__]
        module_file = getattr(module, "__file__", None)
        if module_file is None:
            raise RuntimeError(
                f"Cannot resolve `sql_file` for {type(self).__name__}: its "
                "module has no `__file__` (e.g. defined interactively rather "
                "than in a real module file)."
            )
        return Path(module_file).parent / self.sql_file

    def read(self) -> None:
        for view_name, path in self.input_paths.items():
            self.sql_engine.register_view(view_name, path)
        return None

    def transform(self, _: None) -> Any:
        query = self._sql_file_path().read_text(encoding="utf-8")
        return self.sql_engine.run_query(query)

    def write(self, data: Any) -> None:
        self.sql_engine.write_result(data, self.output_path)
