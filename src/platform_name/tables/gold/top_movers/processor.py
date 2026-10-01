"""Gold processor: daily top movers, computed entirely in SQL.

Demonstrates ``execution.mode: sql`` end to end via
:class:`~platform_name.tables.sql_base.BaseSqlProcessor`. The transformation
logic lives in query.sql, right next to this file -- a SQL-fluent analyst
can read and edit it without touching Python. Which engine actually runs
that query (DuckDB locally, Spark in test/prod) is an environment
configuration decision (see sql/factory.py), invisible to this file.
"""
from __future__ import annotations

from platform_name.tables.sql_base import BaseSqlProcessor

# See contract.py in this same directory for this table's DatasetContract.
# Not wired in here: BaseSqlProcessor's result is a DuckDBPyRelation or a
# Spark DataFrame depending on sql_backend, and quality/framework.py's
# validate_dataset_contract is pandas-specific -- it would fail against
# either. The contract documents the expected shape and is ready for a
# backend-aware validator once one exists.


class TopMoversProcessor(BaseSqlProcessor):
    sql_file = "query.sql"
