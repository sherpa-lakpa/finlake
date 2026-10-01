"""Every table lives in one folder containing exactly the same three
files: metadata.yaml, processor.py, contract.py. Enforced here so the
convention can't quietly erode as the number of tables grows -- one
consistent shape is what makes adding table #500 as unremarkable as adding
table #5.
"""
from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from platform_name.contracts.base import DatasetContract

REPO_ROOT = Path(__file__).resolve().parents[2]
TABLES_ROOT = REPO_ROOT / "src" / "platform_name" / "tables"


def _table_directories() -> list[Path]:
    return sorted(path.parent for path in TABLES_ROOT.rglob("metadata.yaml"))


def test_at_least_one_table_directory_is_discovered():
    assert _table_directories(), "no table directories found -- is TABLES_ROOT correct?"


@pytest.mark.parametrize("table_dir", _table_directories(), ids=lambda p: f"{p.parent.name}.{p.name}")
def test_every_table_folder_has_processor_and_contract(table_dir):
    assert (table_dir / "processor.py").exists(), f"{table_dir} is missing processor.py"
    assert (table_dir / "contract.py").exists(), f"{table_dir} is missing contract.py"


@pytest.mark.parametrize("table_dir", _table_directories(), ids=lambda p: f"{p.parent.name}.{p.name}")
def test_every_contract_module_exposes_a_dataset_contract(table_dir):
    layer = table_dir.parent.name
    table = table_dir.name
    module = importlib.import_module(f"platform_name.tables.{layer}.{table}.contract")

    assert hasattr(module, "CONTRACT"), f"{layer}.{table}'s contract.py must define CONTRACT"
    assert isinstance(module.CONTRACT, DatasetContract)


def test_no_central_contracts_definitions_module_exists():
    """The old contracts/definitions.py grew into a single file holding
    every table's contract -- exactly the messiness per-table contract.py
    files exist to avoid. contracts/base.py (the generic model) stays;
    concrete, table-specific contracts must not creep back into a shared
    module."""
    assert not (REPO_ROOT / "src" / "platform_name" / "contracts" / "definitions.py").exists()
