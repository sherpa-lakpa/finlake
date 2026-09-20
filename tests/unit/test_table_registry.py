from pathlib import Path

import pytest

from platform_name.common.exceptions import RegistryError
from platform_name.engine.enums import Layer
from platform_name.engine.table_registry import TableRegistry


def test_discovers_all_shipped_tables(registry):
    assert set(registry.all_names()) == {
        "bronze.market_prices_historical",
        "bronze.market_prices_daily",
        "bronze.exchange_listings",
        "silver.security_master",
        "silver.daily_prices",
        "gold.returns",
        "gold.performance_summary",
        "gold.customer_risk",
        "gold.top_movers",
    }


def test_get_returns_table_definition(registry):
    definition = registry.get("gold.returns")
    assert definition.fully_qualified_name == "gold.returns"
    assert definition.layer == Layer.GOLD


def test_get_unknown_table_raises(registry):
    with pytest.raises(RegistryError):
        registry.get("gold.does_not_exist")


def test_has(registry):
    assert registry.has("bronze.market_prices_historical")
    assert not registry.has("bronze.does_not_exist")


def test_list_by_layer(registry):
    silver_tables = registry.list_by_layer(Layer.SILVER)
    assert {t.fully_qualified_name for t in silver_tables} == {
        "silver.security_master",
        "silver.daily_prices",
    }


def test_list_by_layer_accepts_string(registry):
    silver_tables = registry.list_by_layer("silver")
    assert len(silver_tables) == 2


def test_missing_metadata_root_raises(tmp_path):
    registry = TableRegistry(tmp_path / "does_not_exist")
    with pytest.raises(RegistryError):
        registry.load()


def test_duplicate_table_definition_raises(tmp_path):
    definition_yaml = (
        "name: returns\n"
        "layer: gold\n"
        "processor:\n"
        "  module: some.module\n"
        "  class: SomeClass\n"
        "paths:\n"
        "  output:\n"
        "    file: returns.parquet\n"
    )
    # Two separate roots, each independently satisfying the
    # <root>/gold/returns/metadata.yaml convention -- e.g. someone
    # accidentally pointed the registry at a parent directory containing
    # two full copies of the tables tree. Both resolve to the same
    # fully-qualified name ("gold.returns") and must be caught as a
    # duplicate, even though neither individually violates the
    # directory-naming convention.
    first_root = tmp_path / "checkout_one" / "gold" / "returns"
    first_root.mkdir(parents=True)
    (first_root / "metadata.yaml").write_text(definition_yaml)

    second_root = tmp_path / "checkout_two" / "gold" / "returns"
    second_root.mkdir(parents=True)
    (second_root / "metadata.yaml").write_text(definition_yaml)

    registry = TableRegistry(tmp_path)
    with pytest.raises(RegistryError):
        registry.load()
