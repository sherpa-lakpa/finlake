"""Tests for DeltaStorageAdapter using a mocked SparkSession.

These verify the adapter calls the right Spark DataFrameReader/Writer API
shape -- they don't require real PySpark or a real cluster, since the
whole point is testing *our* code's contract with Spark's API, not Spark
itself.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from platform_name.storage.delta import DeltaResultSinkUnavailable, DeltaStorageAdapter


def test_requires_a_spark_session():
    with pytest.raises(DeltaResultSinkUnavailable):
        DeltaStorageAdapter(spark=None)


def test_read_delta_calls_expected_spark_api():
    spark = MagicMock()
    adapter = DeltaStorageAdapter(spark=spark)

    adapter.read_delta("/mnt/silver/daily_prices")

    spark.read.format.assert_called_once_with("delta")
    spark.read.format.return_value.load.assert_called_once_with("/mnt/silver/daily_prices")


def test_write_delta_calls_expected_spark_api_with_default_overwrite_mode():
    spark = MagicMock()
    adapter = DeltaStorageAdapter(spark=spark)
    df = MagicMock()

    adapter.write_delta(df, "/mnt/gold/returns")

    df.write.format.assert_called_once_with("delta")
    df.write.format.return_value.mode.assert_called_once_with("overwrite")
    df.write.format.return_value.mode.return_value.save.assert_called_once_with("/mnt/gold/returns")


def test_write_delta_respects_explicit_mode():
    spark = MagicMock()
    adapter = DeltaStorageAdapter(spark=spark)
    df = MagicMock()

    adapter.write_delta(df, "/mnt/gold/returns", mode="append")

    df.write.format.return_value.mode.assert_called_once_with("append")


def test_read_delta_table_uses_unity_catalog_name():
    spark = MagicMock()
    adapter = DeltaStorageAdapter(spark=spark)

    adapter.read_delta_table("main.gold.returns")

    spark.read.table.assert_called_once_with("main.gold.returns")


def test_write_delta_table_uses_save_as_table():
    spark = MagicMock()
    adapter = DeltaStorageAdapter(spark=spark)
    df = MagicMock()

    adapter.write_delta_table(df, "main.gold.returns")

    df.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(
        "main.gold.returns"
    )


def test_merge_into_builds_expected_merge_chain():
    pytest.importorskip("delta", reason="merge_into requires the optional delta-spark dependency")
    spark = MagicMock()
    adapter = DeltaStorageAdapter(spark=spark)
    updates_df = MagicMock()

    with patch("delta.tables.DeltaTable") as MockDeltaTable:
        target_table = MockDeltaTable.forPath.return_value
        merge_builder = target_table.alias.return_value.merge.return_value
        merge_builder.whenMatchedUpdateAll.return_value = merge_builder
        merge_builder.whenNotMatchedInsertAll.return_value = merge_builder

        adapter.merge_into("/mnt/silver/security_master", updates_df, "target.id = updates.id")

        MockDeltaTable.forPath.assert_called_once_with(spark, "/mnt/silver/security_master")
        merge_builder.whenMatchedUpdateAll.assert_called_once()
        merge_builder.whenNotMatchedInsertAll.assert_called_once()
        merge_builder.execute.assert_called_once()
