"""Architecture tests: metadata must be valid and the whole registry must
pass architecture validation *before* any processor executes.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from platform_name.common.config import Config
from platform_name.common.exceptions import MetadataError, RegistryError
from platform_name.engine.dependency_graph import DependencyGraph
from platform_name.engine.metadata_loader import load_table_definition
from platform_name.engine.table_registry import TableRegistry
from platform_name.engine.validation import ArchitectureValidator

REPO_ROOT = Path(__file__).resolve().parents[2]
# Metadata lives colocated with processors: src/platform_name/tables/<layer>/<table>/metadata.yaml
METADATA_ROOT = REPO_ROOT / "src" / "platform_name" / "tables"


def _write_metadata(tmp_path: Path, layer: str, table: str, content: str) -> Path:
    """Writes a metadata.yaml at the conventional
    <root>/<layer>/<table>/metadata.yaml location and returns its path."""
    table_dir = tmp_path / layer / table
    table_dir.mkdir(parents=True)
    path = table_dir / "metadata.yaml"
    path.write_text(content)
    return path


def test_shipped_metadata_all_parses_successfully():
    registry = TableRegistry(METADATA_ROOT).load()
    assert len(registry) == 9


def test_shipped_dependency_graph_has_no_cycles_or_missing_deps():
    registry = TableRegistry(METADATA_ROOT).load()
    DependencyGraph(registry).validate()  # should not raise


def test_full_architecture_validation_passes(dev_environment):
    """Every table except gold.customer_risk must always validate cleanly
    with no live spark/sql_engine in context -- including
    bronze.market_prices_daily (its yfinance call is lazily imported
    inside read(), never at construction time) and gold.top_movers
    (its processor module never imports PySpark directly).
    gold.customer_risk (a genuine PySpark DataFrame-API processor)
    requires the optional `pyspark` dependency to even import -- if it
    isn't installed, that's the one and only expected error, not a
    validator bug.
    """
    registry = TableRegistry(METADATA_ROOT).load()
    config = Config(dev_environment)
    report = ArchitectureValidator(registry, config).validate()

    try:
        import pyspark  # noqa: F401
    except ImportError:
        assert [e for e in report.errors if "customer_risk" not in e] == [], report.errors
    else:
        assert report.is_valid, report.errors


@pytest.mark.parametrize(
    "broken_yaml,expected_fragment",
    [
        ("layer: not_a_layer\nname: x\n", "layer"),
        ("name: x\nlayer: silver\nexecution:\n  mode: not_a_mode\n", "execution.mode"),
        ("name: x\nlayer: silver\nload:\n  strategy: not_a_strategy\n", "load.strategy"),
        ("layer: silver\n", "name"),
        ("name: x\n", "layer"),
    ],
)
def test_invalid_metadata_raises_clear_error(tmp_path, broken_yaml, expected_fragment):
    path = _write_metadata(tmp_path, "silver", "x", broken_yaml)

    with pytest.raises(MetadataError) as exc_info:
        load_table_definition(path)

    assert expected_fragment in str(exc_info.value)


def test_metadata_file_must_be_named_metadata_yaml(tmp_path):
    table_dir = tmp_path / "silver" / "right_name"
    table_dir.mkdir(parents=True)
    path = table_dir / "wrong_filename.yaml"
    path.write_text("name: right_name\nlayer: silver\n")

    with pytest.raises(MetadataError):
        load_table_definition(path)


def test_table_directory_name_must_match_name_field(tmp_path):
    """Catches the classic copy-paste mistake: duplicating a table's
    directory to start a new one and forgetting to update `name:` inside
    the copy."""
    path = _write_metadata(tmp_path, "silver", "actual_dir_name", "name: different_name\nlayer: silver\n")

    with pytest.raises(MetadataError) as exc_info:
        load_table_definition(path)

    assert "directory name" in str(exc_info.value)


def test_layer_directory_must_match_layer_field(tmp_path):
    path = _write_metadata(tmp_path, "silver", "x", "name: x\nlayer: gold\n")

    with pytest.raises(MetadataError) as exc_info:
        load_table_definition(path)

    assert "parent directory" in str(exc_info.value)


def test_malformed_dependency_format_rejected(tmp_path):
    path = _write_metadata(
        tmp_path, "gold", "x", "name: x\nlayer: gold\ndependencies:\n  - not_a_valid_dependency\n"
    )

    with pytest.raises(MetadataError):
        load_table_definition(path)


def test_processor_module_and_class_must_be_declared_together(tmp_path):
    path = _write_metadata(
        tmp_path,
        "gold",
        "x",
        textwrap.dedent(
            """
            name: x
            layer: gold
            processor:
              module: some.module
            """
        ),
    )

    with pytest.raises(MetadataError):
        load_table_definition(path)


def test_invalid_path_root_rejected(tmp_path):
    path = _write_metadata(
        tmp_path,
        "gold",
        "x",
        textwrap.dedent(
            """
            name: x
            layer: gold
            paths:
              input:
                file: a.parquet
                root: not_a_layer
            """
        ),
    )

    with pytest.raises(MetadataError):
        load_table_definition(path)


def test_unknown_top_level_key_rejected(tmp_path):
    path = _write_metadata(tmp_path, "gold", "x", "name: x\nlayer: gold\nnot_a_real_key: 1\n")

    with pytest.raises(MetadataError):
        load_table_definition(path)


def test_registry_ignores_non_metadata_yaml_files(tmp_path):
    """A stray *.yaml file that isn't named exactly `metadata.yaml` (e.g. a
    future companion config living in the same table directory) must not be
    picked up as a table definition."""
    table_dir = tmp_path / "gold" / "returns"
    table_dir.mkdir(parents=True)
    (table_dir / "metadata.yaml").write_text(
        "name: returns\nlayer: gold\n"
    )
    (table_dir / "some_other_config.yaml").write_text("unrelated: true\n")

    registry = TableRegistry(tmp_path).load()
    assert registry.all_names() == ("gold.returns",)
