"""Tests for BaseSqlProcessor using a mocked SparkSession -- verifies the
temp-view registration, SQL-file resolution, and write contract without
requiring real PySpark installed.
"""
from __future__ import annotations

import sys
import textwrap
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from platform_name.tables.sql_base import BaseSqlProcessor


@pytest.fixture
def sql_processor_module(tmp_path: Path, monkeypatch):
    """Dynamically creates a real module + companion .sql file on disk,
    mirroring exactly how a table's processor.py + query.sql would sit
    next to each other in tables/<layer>/<table>/, then imports it."""
    package_dir = tmp_path / "fake_sql_table"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("")
    (package_dir / "processor.py").write_text(
        textwrap.dedent(
            """
            from platform_name.tables.sql_base import BaseSqlProcessor

            class FakeSqlProcessor(BaseSqlProcessor):
                sql_file = "query.sql"
            """
        )
    )
    (package_dir / "query.sql").write_text("SELECT security_id, daily_return FROM returns")

    monkeypatch.syspath_prepend(str(tmp_path))
    import importlib

    module = importlib.import_module("fake_sql_table.processor")
    yield module
    sys.modules.pop("fake_sql_table.processor", None)
    sys.modules.pop("fake_sql_table", None)


def test_read_registers_one_temp_view_per_input_path(sql_processor_module):
    sql_engine = MagicMock()
    processor = sql_processor_module.FakeSqlProcessor(
        sql_engine=sql_engine, output_path="/mnt/gold/risk", returns="/mnt/gold/returns"
    )

    processor.read()

    sql_engine.register_view.assert_called_once_with(
        "returns",
        "/mnt/gold/returns",
    )


def test_transform_executes_the_sql_file_contents(sql_processor_module):
    sql_engine = MagicMock()
    processor = sql_processor_module.FakeSqlProcessor(
        sql_engine=sql_engine,
        output_path="/mnt/gold/risk",
        returns="/mnt/gold/returns",
    )

    processor.transform(None)

    sql_engine.run_query.assert_called_once_with(
        "SELECT security_id, daily_return FROM returns"
    )


def test_write_saves_as_delta_with_overwrite_mode(sql_processor_module):
    sql_engine = MagicMock()
    processor = sql_processor_module.FakeSqlProcessor(
        sql_engine=sql_engine,
        output_path="/mnt/gold/risk",
        returns="/mnt/gold/returns",
    )
    result = MagicMock()

    processor.write(result)

    sql_engine.write_result.assert_called_once_with(
        result,
        "/mnt/gold/risk",
    )


def test_full_run_lifecycle_reads_transforms_and_writes(sql_processor_module):
    """End-to-end through BaseProcessor.run(): read -> transform -> write."""
    sql_engine = MagicMock()
    resulting_data = MagicMock()
    sql_engine.run_query.return_value = resulting_data

    processor = sql_processor_module.FakeSqlProcessor(
        sql_engine=sql_engine,
        output_path="/mnt/gold/risk",
        returns="/mnt/gold/returns",
    )

    result = processor.run()

    assert result is resulting_data
    sql_engine.register_view.assert_called_once_with(
        "returns",
        "/mnt/gold/returns",
    )
    sql_engine.run_query.assert_called_once_with(
        "SELECT security_id, daily_return FROM returns"
    )
    sql_engine.write_result.assert_called_once_with(
        resulting_data,
        "/mnt/gold/risk",
    )


def test_requires_a_spark_session_like_any_pyspark_processor(sql_processor_module):
    with pytest.raises(ValueError):
        sql_processor_module.FakeSqlProcessor(sql_engine=None, output_path="x", returns="y")
