import dataclasses
from pathlib import Path

import pytest

from platform_name.engine.enums import ExecutionMode, Layer, LoadStrategy, WriteMode
from platform_name.engine.models import ExecutionContext, ExecutionResult, TableDefinition


def _make_definition(**overrides) -> TableDefinition:
    defaults = dict(
        name="daily_prices",
        layer=Layer.SILVER,
        execution_mode=ExecutionMode.PANDAS,
        load_strategy=LoadStrategy.UPSERT,
        write_mode=WriteMode.MERGE,
        dependencies=("bronze.market_prices",),
        description="test",
        processor_module="pkg.module",
        processor_class="Processor",
        metadata_path=Path("src/platform_name/tables/silver/daily_prices/metadata.yaml"),
        metadata={},
        paths={},
        config={},
    )
    defaults.update(overrides)
    return TableDefinition(**defaults)


def test_fully_qualified_name_and_alias():
    definition = _make_definition()
    assert definition.fully_qualified_name == "silver.daily_prices"
    assert definition.full_name == definition.fully_qualified_name


def test_table_definition_is_immutable():
    definition = _make_definition()
    with pytest.raises(dataclasses.FrozenInstanceError):
        definition.name = "changed"  # type: ignore[misc]


def test_execution_context_defaults_and_environment_inference():
    class DummyConfig:
        environment = "dev"

    context = ExecutionContext(config=DummyConfig())
    assert context.environment == "dev"
    assert context.run_id
    assert context.extra == {}


def test_execution_context_run_ids_are_unique():
    class DummyConfig:
        environment = "dev"

    ctx1 = ExecutionContext(config=DummyConfig())
    ctx2 = ExecutionContext(config=DummyConfig())
    assert ctx1.run_id != ctx2.run_id


def test_execution_result_lifecycle():
    result = ExecutionResult.start(table="gold.returns", layer="gold", run_id="run-1")
    assert result.status == "running"
    assert result.duration_seconds is None

    result.mark_success(row_count=10)
    assert result.status == "success"
    assert result.row_count == 10
    assert result.duration_seconds is not None
    assert result.to_dict()["table"] == "gold.returns"


def test_execution_result_failure():
    result = ExecutionResult.start(table="gold.returns", layer="gold", run_id="run-1")
    result.mark_failed("boom")
    assert result.status == "failed"
    assert result.error == "boom"
