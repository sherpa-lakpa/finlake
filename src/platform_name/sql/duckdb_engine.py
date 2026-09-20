"""DuckDB-backed :class:`SqlEngine`.

An embedded, in-process SQL engine -- no JVM, no cluster, `pip install
duckdb` and it runs. Reads and writes the exact same Parquet files the
Pandas example domain already produces (see ``storage/local.py``), so a
local dev pipeline can freely mix Pandas-mode and SQL-mode tables on the
same files.

This is what ``dev.yaml``'s ``sql_backend: duckdb`` selects (see
:mod:`platform_name.sql.factory`) -- the intent is that a new table's
``.sql`` file can be run and iterated on locally in milliseconds, with zero
Spark/Databricks dependency, before it ever needs to run for real against
``test``/``prod``.
"""
from __future__ import annotations

from typing import Any


class DuckDBSqlEngine:
    def __init__(self, connection: Any = None) -> None:
        import duckdb

        self.con = connection or duckdb.connect(database=":memory:")

    def register_view(self, name: str, path: str) -> None:
        relation = self.con.read_parquet(path)
        self.con.register(name, relation)

    def run_query(self, sql_text: str) -> Any:
        return self.con.sql(sql_text)

    def write_result(self, result: Any, output_path: str) -> None:
        from pathlib import Path

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        result.write_parquet(str(output_path))
