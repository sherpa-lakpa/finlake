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
    spark = MagicMock()
    processor = sql_processor_module.FakeSqlProcessor(
        spark=spark, output_path="/mnt/gold/risk", returns="/mnt/gold/returns"
    )

    processor.read()

    spark.read.format.assert_called_with("delta")
    spark.read.format.return_value.load.assert_called_with("/mnt/gold/returns")
    spark.read.format.return_value.load.return_value.createOrReplaceTempView.assert_called_with(
        "returns"
    )


def test_transform_executes_the_sql_file_contents(sql_processor_module):
    spark = MagicMock()
    processor = sql_processor_module.FakeSqlProcessor(
        spark=spark, output_path="/mnt/gold/risk", returns="/mnt/gold/returns"
    )

    processor.transform(None)

    spark.sql.assert_called_once_with("SELECT security_id, daily_return FROM returns")


def test_write_saves_as_delta_with_overwrite_mode(sql_processor_module):
    spark = MagicMock()
    processor = sql_processor_module.FakeSqlProcessor(
        spark=spark, output_path="/mnt/gold/risk", returns="/mnt/gold/returns"
    )
    df = MagicMock()

    processor.write(df)

    df.write.format.assert_called_once_with("delta")
    df.write.format.return_value.mode.assert_called_once_with("overwrite")
    df.write.format.return_value.mode.return_value.save.assert_called_once_with("/mnt/gold/risk")


def test_full_run_lifecycle_reads_transforms_and_writes(sql_processor_module):
    """End-to-end through BaseProcessor.run(): read -> transform -> validate -> write."""
    spark = MagicMock()
    resulting_df = spark.sql.return_value
    processor = sql_processor_module.FakeSqlProcessor(
        spark=spark, output_path="/mnt/gold/risk", returns="/mnt/gold/returns"
    )

    result = processor.run()

    assert result is resulting_df
    resulting_df.write.format.assert_called_once_with("delta")


def test_requires_a_spark_session_like_any_pyspark_processor(sql_processor_module):
    with pytest.raises(ValueError):
        sql_processor_module.FakeSqlProcessor(spark=None, output_path="x", returns="y")
