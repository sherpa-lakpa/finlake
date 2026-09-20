"""Architecture tests enforcing the framework's core design rule:

    Never hardcode table-specific mappings in generic framework code.

These tests both (a) statically scan the generic engine modules for
business-specific vocabulary that should never appear there, and (b) prove,
behaviorally, that a brand-new table can be added purely via metadata + a
processor module without modifying any generic engine file (the "final
architectural test" from the project brief).
"""
from __future__ import annotations

import inspect
import textwrap
from pathlib import Path

from platform_name.common.config import Config
from platform_name.engine import (
    dependency_graph,
    metadata_loader,
    processor_factory,
    processor_loader,
    runner,
    table_registry,
    validation,
)
from platform_name.engine.processor_factory import ProcessorFactory
from platform_name.engine.table_registry import TableRegistry
from platform_name.tables import base as base_processor_module
from platform_name.tables import pyspark_base as pyspark_base_module
from platform_name.tables import sql_base as sql_base_module
from platform_name.storage import delta as delta_storage_module
from platform_name.storage import spark_parquet as spark_parquet_module
from platform_name.sql import base as sql_engine_base_module
from platform_name.sql import duckdb_engine as duckdb_engine_module
from platform_name.sql import factory as sql_engine_factory_module
from platform_name.sql import spark_engine as spark_sql_engine_module

_GENERIC_ENGINE_MODULES = [
    processor_factory,
    processor_loader,
    dependency_graph,
    table_registry,
    metadata_loader,
    runner,
    validation,
]

# tables/base.py, pyspark_base.py, sql_base.py, storage/delta.py,
# storage/spark_parquet.py, and the sql/ package are also generic
# (execution-mode-level, not table-level) components -- they must
# meet the same no-hardcoding bar as the engine/ modules above.
_GENERIC_TABLE_AND_STORAGE_MODULES = [
    base_processor_module,
    pyspark_base_module,
    sql_base_module,
    delta_storage_module,
    spark_parquet_module,
    sql_engine_base_module,
    duckdb_engine_module,
    sql_engine_factory_module,
    spark_sql_engine_module,
]

# Business/domain-specific vocabulary from the example domain that must never
# leak into generic engine code. If any of these appear, the framework has
# started special-casing a specific table (the exact anti-pattern the brief
# forbids).
_FORBIDDEN_BUSINESS_TERMS = [
    "daily_prices",
    "security_master",
    "market_prices",
    "prices_path",
    "security_master_path",
    "bronze_path",
    "returns_path",
    "risk_input_path",
    "customer_risk",
    "ticker",
    "asset_class",
    "top_movers",
    "movement_rank",
    "exchange_listings",
    "adj_close",
    "close_price",
    "yfinance",
    "since_default",
    "historical_path",
    "daily_path",
    "exchange_listings_path",
]


def test_generic_engine_modules_contain_no_business_specific_terms():
    violations = []
    for module in _GENERIC_ENGINE_MODULES + _GENERIC_TABLE_AND_STORAGE_MODULES:
        source = inspect.getsource(module)
        for term in _FORBIDDEN_BUSINESS_TERMS:
            if term in source:
                violations.append(f"{module.__name__} contains forbidden term '{term}'")

    assert not violations, "\n".join(violations)


def test_generic_engine_modules_contain_no_table_name_conditionals():
    """Scans for the literal anti-pattern shapes called out in the brief,
    e.g. `if table_name ==` or `if dependency ==`."""
    forbidden_patterns = [
        "if table_name ==",
        "if table ==",
        "if dependency ==",
        "if dep ==",
    ]
    violations = []
    for module in _GENERIC_ENGINE_MODULES + _GENERIC_TABLE_AND_STORAGE_MODULES:
        source = inspect.getsource(module)
        for pattern in forbidden_patterns:
            if pattern in source:
                violations.append(f"{module.__name__} contains forbidden pattern '{pattern}'")

    assert not violations, "\n".join(violations)


def test_adding_a_new_table_requires_no_generic_engine_changes(tmp_path, dev_environment, monkeypatch):
    """Behavioral proof of section 46's 'final architectural test': a brand
    new table with its own constructor parameter name can be added purely
    through metadata + a processor module.
    """
    package_dir = tmp_path / "new_tables"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("")
    (package_dir / "customer_risk_processor.py").write_text(
        textwrap.dedent(
            """
            import pandas as pd

            class CustomerRiskProcessor:
                def __init__(self, risk_input_path, output_path):
                    self.risk_input_path = risk_input_path
                    self.output_path = output_path

                def run(self):
                    return pd.DataFrame({"customer_id": ["C1", "C2"], "risk_score": [0.1, 0.9]})
            """
        )
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    metadata_dir = tmp_path / "metadata" / "gold" / "customer_risk"
    metadata_dir.mkdir(parents=True)
    (metadata_dir / "metadata.yaml").write_text(
        textwrap.dedent(
            """
            name: customer_risk
            layer: gold
            dependencies: []
            processor:
              module: new_tables.customer_risk_processor
              class: CustomerRiskProcessor
            paths:
              input:
                file: risk_features.parquet
                root: silver
                parameter: risk_input_path
              output:
                file: customer_risk.parquet
            """
        )
    )

    registry = TableRegistry(tmp_path / "metadata").load()
    definition = registry.get("gold.customer_risk")

    config = Config(dev_environment)
    factory = ProcessorFactory(config=config)  # the same, unmodified generic factory
    processor = factory.create(definition)
    result = processor.run()

    assert len(result) == 2
    assert set(result.columns) == {"customer_id", "risk_score"}


def test_bundle_generator_contains_no_business_specific_terms():
    """The deployment generator is a generic engine component too: it must
    never special-case a table by name any more than ProcessorFactory does.
    """
    from platform_name.deploy import bundle_generator

    source = inspect.getsource(bundle_generator)
    violations = [term for term in _FORBIDDEN_BUSINESS_TERMS if term in source]
    assert not violations, violations
