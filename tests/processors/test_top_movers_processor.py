"""Tests for the real, shipped TopMoversProcessor (execution.mode: sql).

Uses a mocked SqlEngine -- proves this specific table's read/transform/write
wiring and its actual query.sql content, without needing DuckDB or Spark
installed.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from platform_name.tables.gold.top_movers.processor import TopMoversProcessor


def test_registers_returns_view_from_resolved_path():
    engine = MagicMock()
    processor = TopMoversProcessor(
        sql_engine=engine, output_path="/data/gold/top_movers.parquet",
        returns="/data/gold/returns.parquet",
    )

    processor.read()

    engine.register_view.assert_called_once_with("returns", "/data/gold/returns.parquet")


def test_transform_runs_the_real_shipped_query():
    engine = MagicMock()
    processor = TopMoversProcessor(
        sql_engine=engine, output_path="/data/gold/top_movers.parquet",
        returns="/data/gold/returns.parquet",
    )

    processor.transform(None)

    executed_sql = engine.run_query.call_args[0][0]
    assert "RANK() OVER" in executed_sql
    assert "PARTITION BY trade_date" in executed_sql
    assert "ORDER BY ABS(daily_return) DESC" in executed_sql
    assert "FROM returns" in executed_sql


def test_write_persists_query_result_to_output_path():
    engine = MagicMock()
    processor = TopMoversProcessor(
        sql_engine=engine, output_path="/data/gold/top_movers.parquet",
        returns="/data/gold/returns.parquet",
    )
    fake_result = engine.run_query.return_value

    processor.write(fake_result)

    engine.write_result.assert_called_once_with(fake_result, "/data/gold/top_movers.parquet")


def test_full_run_lifecycle():
    engine = MagicMock()
    processor = TopMoversProcessor(
        sql_engine=engine, output_path="/data/gold/top_movers.parquet",
        returns="/data/gold/returns.parquet",
    )

    result = processor.run()

    engine.register_view.assert_called_once()
    engine.run_query.assert_called_once()
    engine.write_result.assert_called_once()
    assert result is engine.run_query.return_value


def test_requires_a_sql_engine():
    import pytest

    with pytest.raises(ValueError):
        TopMoversProcessor(sql_engine=None, output_path="x", returns="y")
