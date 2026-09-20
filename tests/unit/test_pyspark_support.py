"""Tests for the PySpark execution-mode extension point.

These never require PySpark to actually be installed -- they test the
generic injection plumbing (ProcessorFactory <-> ExecutionContext <->
BasePySparkProcessor) using a stub session object, proving the mechanism
works without needing a real Spark cluster. Tests that genuinely need real
PySpark APIs are marked and skipped via `pytest.importorskip("pyspark")`.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from platform_name.common.config import Config
from platform_name.common.exceptions import ProcessorConstructionError
from platform_name.engine.enums import ExecutionMode, Layer, LoadStrategy, WriteMode
from platform_name.engine.models import ExecutionContext, TableDefinition
from platform_name.engine.processor_factory import EXECUTION_MODES_REQUIRING_SPARK, ProcessorFactory
from platform_name.tables.pyspark_base import BasePySparkProcessor


class _StubSparkSession:
    """Stands in for pyspark.sql.SparkSession in plumbing tests."""


class _FakeSparkProcessor(BasePySparkProcessor):
    def __init__(self, spark, output_path: str) -> None:
        super().__init__(spark)
        self.output_path = output_path


def _pyspark_table_definition(paths=None) -> TableDefinition:
    return TableDefinition(
        name="big_table",
        layer=Layer.SILVER,
        execution_mode=ExecutionMode.PYSPARK,
        load_strategy=LoadStrategy.FULL,
        write_mode=WriteMode.OVERWRITE,
        dependencies=(),
        description="",
        processor_module="tests.unit.test_pyspark_support",
        processor_class="_FakeSparkProcessor",
        metadata_path=None,
        metadata={},
        paths=paths or {"output": {"file": "big_table.parquet"}},
        config={},
    )


def _pandas_table_definition() -> TableDefinition:
    return TableDefinition(
        name="small_table",
        layer=Layer.SILVER,
        execution_mode=ExecutionMode.PANDAS,
        load_strategy=LoadStrategy.FULL,
        write_mode=WriteMode.OVERWRITE,
        dependencies=(),
        description="",
        processor_module="x",
        processor_class="Y",
        metadata_path=None,
        metadata={},
        paths={"output": {"file": "small_table.parquet"}},
        config={},
    )


def test_execution_modes_requiring_spark_are_pyspark_and_sql_only():
    assert EXECUTION_MODES_REQUIRING_SPARK == {ExecutionMode.PYSPARK, ExecutionMode.SQL}


def test_base_pyspark_processor_requires_a_session():
    with pytest.raises(ValueError):
        BasePySparkProcessor(spark=None)


def test_base_pyspark_processor_stores_session():
    stub = _StubSparkSession()
    processor = BasePySparkProcessor(spark=stub)
    assert processor.spark is stub


def test_factory_with_no_context_cannot_construct_pyspark_table(dev_environment):
    config = Config(dev_environment)
    factory = ProcessorFactory(config=config)  # no context at all

    with pytest.raises(ProcessorConstructionError) as exc_info:
        factory.create(_pyspark_table_definition())

    assert "spark" in str(exc_info.value)


def test_factory_with_empty_spark_context_cannot_construct_pyspark_table(dev_environment):
    config = Config(dev_environment)
    context = ExecutionContext(config=config, spark=None)
    factory = ProcessorFactory(config=config, context=context)

    with pytest.raises(ProcessorConstructionError) as exc_info:
        factory.create(_pyspark_table_definition())

    assert "spark" in str(exc_info.value)


def test_factory_injects_live_spark_session_into_pyspark_processor(dev_environment):
    config = Config(dev_environment)
    stub_spark = _StubSparkSession()
    context = ExecutionContext(config=config, spark=stub_spark)
    factory = ProcessorFactory(config=config, context=context)

    processor = factory.create(_pyspark_table_definition())

    assert isinstance(processor, BasePySparkProcessor)
    assert processor.spark is stub_spark


def test_pandas_table_never_receives_spark_even_with_live_session_in_context(dev_environment):
    """Negative test: proves no cross-contamination between execution
    modes. A pandas table sharing a context with a live Spark session must
    never see `spark` in its resolved constructor kwargs."""
    config = Config(dev_environment)
    context = ExecutionContext(config=config, spark=_StubSparkSession())
    factory = ProcessorFactory(config=config, context=context)

    kwargs = factory.build_constructor_arguments(_pandas_table_definition())

    assert "spark" not in kwargs


def test_pyspark_execution_mode_is_a_valid_metadata_value(dev_environment):
    """Confirms `execution.mode: pyspark` round-trips through metadata
    parsing exactly like `pandas` does -- no special metadata-loader
    handling needed for the new mode."""
    from platform_name.engine.metadata_loader import parse_table_definition

    raw = {
        "name": "big_table",
        "layer": "silver",
        "execution": {"mode": "pyspark"},
        "processor": {
            "module": "tests.unit.test_pyspark_support",
            "class": "_FakeSparkProcessor",
        },
        "paths": {"output": {"file": "big_table.parquet"}},
    }
    definition = parse_table_definition(raw, Path("silver/big_table/metadata.yaml"))
    assert definition.execution_mode == ExecutionMode.PYSPARK
