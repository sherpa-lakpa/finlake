from pathlib import Path

import pytest

from platform_name.common.config import Config
from platform_name.common.exceptions import ProcessorConstructionError
from platform_name.engine.enums import ExecutionMode, Layer, LoadStrategy, WriteMode
from platform_name.engine.models import TableDefinition
from platform_name.engine.processor_factory import ProcessorFactory


def _definition(paths: dict, config: dict | None = None) -> TableDefinition:
    return TableDefinition(
        name="daily_prices",
        layer=Layer.SILVER,
        execution_mode=ExecutionMode.PANDAS,
        load_strategy=LoadStrategy.UPSERT,
        write_mode=WriteMode.MERGE,
        dependencies=("bronze.market_prices", "silver.security_master"),
        description="",
        processor_module="platform_name.tables.silver.daily_prices.processor",
        processor_class="DailyPricesProcessor",
        metadata_path=None,
        metadata={},
        paths=paths,
        config=config or {},
    )


def test_explicit_parameter_mapping_resolves_constructor_kwargs(dev_environment):
    config = Config(dev_environment)
    factory = ProcessorFactory(config=config)

    definition = _definition(
        paths={
            "market_prices": {"file": "market_prices.parquet", "root": "bronze", "parameter": "bronze_path"},
            "security_master": {
                "file": "security_master.parquet",
                "root": "silver",
                "parameter": "security_master_path",
            },
            "output": {"file": "daily_prices.parquet"},
        },
        config={"source": "historical"},
    )

    kwargs = factory.build_constructor_arguments(definition)

    assert kwargs["bronze_path"] == str(Path(config.layer_root("bronze")) / "market_prices.parquet")
    assert kwargs["security_master_path"] == str(
        Path(config.layer_root("silver")) / "security_master.parquet"
    )
    assert kwargs["output_path"] == str(Path(config.layer_root("silver")) / "daily_prices.parquet")
    assert kwargs["source"] == "historical"


def test_output_path_defaults_to_own_layer(dev_environment):
    config = Config(dev_environment)
    factory = ProcessorFactory(config=config)
    definition = _definition(paths={"output": {"file": "daily_prices.parquet"}})

    output_path = factory.resolve_output_path(definition)

    assert output_path == Path(config.layer_root("silver")) / "daily_prices.parquet"


def test_dependency_name_does_not_automatically_become_constructor_argument(dev_environment):
    """Core anti-pattern test: declaring a dependency alone must NOT inject
    any constructor argument. Only explicit `paths` entries do."""
    config = Config(dev_environment)
    factory = ProcessorFactory(config=config)
    # Note: dependencies include silver.security_master, but no `paths` entry
    # references it, so it must not appear anywhere in the resolved kwargs.
    definition = _definition(paths={"output": {"file": "daily_prices.parquet"}})

    kwargs = factory.build_constructor_arguments(definition)

    assert "security_master_path" not in kwargs
    assert "prices_path" not in kwargs
    assert not any("security_master" in str(v) for v in kwargs.values())


def test_missing_required_constructor_argument_raises_clear_error(dev_environment):
    config = Config(dev_environment)
    factory = ProcessorFactory(config=config)
    # daily_prices requires bronze_path, security_master_path, output_path;
    # only output_path is declared here.
    definition = _definition(paths={"output": {"file": "daily_prices.parquet"}})

    with pytest.raises(ProcessorConstructionError) as exc_info:
        factory.create(definition)

    message = str(exc_info.value)
    assert "bronze_path" in message
    assert "security_master_path" in message


def test_legacy_directory_input_infers_only_generic_parameter_name(dev_environment):
    config = Config(dev_environment)
    factory = ProcessorFactory(config=config)
    definition = TableDefinition(
        name="market_prices",
        layer=Layer.BRONZE,
        execution_mode=ExecutionMode.PANDAS,
        load_strategy=LoadStrategy.FULL,
        write_mode=WriteMode.OVERWRITE,
        dependencies=(),
        description="",
        processor_module="platform_name.tables.bronze.market_prices_historical.processor",
        processor_class="MarketPricesHistoricalProcessor",
        metadata_path=None,
        metadata={},
        paths={
            "input": {"directory": "historical", "root": "landing"},
            "output": {"file": "market_prices_historical.parquet"},
        },
        config={},
    )

    kwargs = factory.build_constructor_arguments(definition)

    assert kwargs["landing_path"] == str(Path(config.layer_root("landing")) / "historical")
