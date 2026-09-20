from pathlib import Path

import pytest

from platform_name.common.exceptions import ProcessorLoadError
from platform_name.engine.enums import ExecutionMode, Layer, LoadStrategy, WriteMode
from platform_name.engine.models import TableDefinition
from platform_name.engine.processor_loader import ProcessorLoader


def _definition(module: str | None, cls: str | None) -> TableDefinition:
    return TableDefinition(
        name="market_prices",
        layer=Layer.BRONZE,
        execution_mode=ExecutionMode.PANDAS,
        load_strategy=LoadStrategy.FULL,
        write_mode=WriteMode.OVERWRITE,
        dependencies=(),
        description="",
        processor_module=module,
        processor_class=cls,
        metadata_path=None,
        metadata={},
        paths={},
        config={},
    )


def test_loads_real_processor_class():
    loader = ProcessorLoader()
    definition = _definition(
        "platform_name.tables.bronze.market_prices_historical.processor",
        "MarketPricesHistoricalProcessor",
    )
    processor_class = loader.load(definition)
    assert processor_class.__name__ == "MarketPricesHistoricalProcessor"


def test_missing_module_raises():
    loader = ProcessorLoader()
    definition = _definition("platform_name.tables.does_not_exist", "Whatever")
    with pytest.raises(ProcessorLoadError):
        loader.load(definition)


def test_missing_class_raises():
    loader = ProcessorLoader()
    definition = _definition(
        "platform_name.tables.bronze.market_prices_historical.processor", "DoesNotExist"
    )
    with pytest.raises(ProcessorLoadError):
        loader.load(definition)


def test_missing_processor_declaration_raises():
    loader = ProcessorLoader()
    definition = _definition(None, None)
    with pytest.raises(ProcessorLoadError):
        loader.load(definition)
