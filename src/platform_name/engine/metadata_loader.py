"""Loads and validates table metadata YAML files into :class:`TableDefinition`
instances.

Metadata lives colocated with its processor, one directory per table:

    src/platform_name/tables/<layer>/<table>/metadata.yaml
    src/platform_name/tables/<layer>/<table>/processor.py

so a new engineer adding a table only ever needs to look in one place. This
module owns *parsing and validation only*. It does not discover files
(that's :mod:`table_registry`) and it does not resolve paths against an
environment (that's :mod:`processor_factory`).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from platform_name.common.exceptions import MetadataError
from platform_name.engine.enums import ExecutionMode, Layer, LoadStrategy, WriteMode
from platform_name.engine.models import TableDefinition

_VALID_TOP_LEVEL_KEYS = {
    "name",
    "layer",
    "execution",
    "load",
    "write_mode",
    "dependencies",
    "description",
    "processor",
    "paths",
    "processor_config",
}

# Default write mode per load strategy, used only when metadata omits
# `write_mode` explicitly. This is a sensible default, not a hardcoded
# table-specific mapping -- it applies uniformly to every table.
_DEFAULT_WRITE_MODE_BY_STRATEGY = {
    LoadStrategy.FULL: WriteMode.OVERWRITE,
    LoadStrategy.INCREMENTAL: WriteMode.APPEND,
    LoadStrategy.UPSERT: WriteMode.MERGE,
    LoadStrategy.SCD1: WriteMode.MERGE,
    LoadStrategy.SCD2: WriteMode.MERGE,
}


def load_raw_metadata(path: Path) -> dict[str, Any]:
    """Load a single metadata YAML file into a raw dict (no validation)."""
    if not path.exists():
        raise MetadataError("Table metadata file not found.", context={"path": str(path)})
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    except yaml.YAMLError as err:
        raise MetadataError(
            "Table metadata file is not valid YAML.",
            context={"path": str(path), "error": str(err)},
        ) from err

    if not isinstance(data, dict):
        raise MetadataError(
            "Table metadata file must contain a YAML mapping.",
            context={"path": str(path)},
        )
    return data


def _require(data: dict[str, Any], key: str, path: Path) -> Any:
    if key not in data or data[key] in (None, ""):
        raise MetadataError(
            f"Table metadata is missing required key '{key}'.",
            context={"path": str(path)},
        )
    return data[key]


def _validate_enum(value: str, enum_cls: type, field_name: str, path: Path) -> Any:
    try:
        return enum_cls(value)
    except ValueError as err:
        valid = [member.value for member in enum_cls]
        raise MetadataError(
            f"Invalid value for '{field_name}'.",
            context={"path": str(path), "value": value, "valid_values": valid},
        ) from err


def _validate_dependencies(raw_deps: Any, path: Path) -> tuple[str, ...]:
    if raw_deps is None:
        return ()
    if not isinstance(raw_deps, list):
        raise MetadataError(
            "Field 'dependencies' must be a list of fully-qualified table names.",
            context={"path": str(path), "value": raw_deps},
        )
    deps: list[str] = []
    for dep in raw_deps:
        if not isinstance(dep, str) or "." not in dep:
            raise MetadataError(
                "Each dependency must be a string in '<layer>.<table>' format.",
                context={"path": str(path), "value": dep},
            )
        layer_part, _, _ = dep.partition(".")
        if layer_part not in {layer.value for layer in Layer}:
            raise MetadataError(
                "Dependency references an unknown layer.",
                context={"path": str(path), "dependency": dep, "layer": layer_part},
            )
        deps.append(dep)
    return tuple(deps)


def _validate_paths(raw_paths: Any, path: Path) -> dict[str, Any]:
    if raw_paths is None:
        return {}
    if not isinstance(raw_paths, dict):
        raise MetadataError(
            "Field 'paths' must be a mapping of path-entry name to configuration.",
            context={"path": str(path)},
        )
    for entry_name, entry in raw_paths.items():
        if not isinstance(entry, dict):
            raise MetadataError(
                "Each entry under 'paths' must be a mapping.",
                context={"path": str(path), "entry": entry_name},
            )
        has_file = "file" in entry
        has_directory = "directory" in entry
        if entry_name != "output" and not (has_file or has_directory):
            raise MetadataError(
                "Path entry must declare either 'file' or 'directory'.",
                context={"path": str(path), "entry": entry_name},
            )
        if entry_name == "output" and not has_file:
            raise MetadataError(
                "Output path entry must declare 'file'.",
                context={"path": str(path), "entry": entry_name},
            )
        if entry_name != "output" and "root" not in entry:
            raise MetadataError(
                "Input path entry must declare 'root' (landing/bronze/silver/gold).",
                context={"path": str(path), "entry": entry_name},
            )
        root = entry.get("root")
        if root is not None and root not in {layer.value for layer in Layer}:
            raise MetadataError(
                "Path entry 'root' must be a valid layer name.",
                context={"path": str(path), "entry": entry_name, "root": root},
            )
    return raw_paths


def parse_table_definition(raw: dict[str, Any], path: Path) -> TableDefinition:
    """Validate a raw metadata dict and construct an immutable
    :class:`TableDefinition`.
    """
    unknown_keys = set(raw) - _VALID_TOP_LEVEL_KEYS
    if unknown_keys:
        raise MetadataError(
            "Table metadata contains unknown top-level keys.",
            context={"path": str(path), "unknown_keys": sorted(unknown_keys)},
        )

    name = _require(raw, "name", path)
    if not isinstance(name, str):
        raise MetadataError("Field 'name' must be a string.", context={"path": str(path)})

    layer_value = _require(raw, "layer", path)
    layer = _validate_enum(layer_value, Layer, "layer", path)

    execution_block = raw.get("execution") or {}
    if not isinstance(execution_block, dict):
        raise MetadataError(
            "Field 'execution' must be a mapping with an execution 'mode'.",
            context={"path": str(path)},
        )
    execution_mode_value = execution_block.get("mode", ExecutionMode.PANDAS.value)
    execution_mode = _validate_enum(execution_mode_value, ExecutionMode, "execution.mode", path)

    load_block = raw.get("load") or {}
    if not isinstance(load_block, dict):
        raise MetadataError(
            "Field 'load' must be a mapping with a 'strategy'.",
            context={"path": str(path)},
        )
    load_strategy_value = load_block.get("strategy", LoadStrategy.FULL.value)
    load_strategy = _validate_enum(load_strategy_value, LoadStrategy, "load.strategy", path)

    write_mode_value = raw.get("write_mode")
    if write_mode_value is None:
        write_mode = _DEFAULT_WRITE_MODE_BY_STRATEGY[load_strategy]
    else:
        write_mode = _validate_enum(write_mode_value, WriteMode, "write_mode", path)

    dependencies = _validate_dependencies(raw.get("dependencies"), path)

    description = raw.get("description", "") or ""
    if not isinstance(description, str):
        raise MetadataError("Field 'description' must be a string.", context={"path": str(path)})

    processor_block = raw.get("processor") or {}
    if not isinstance(processor_block, dict):
        raise MetadataError(
            "Field 'processor' must be a mapping with 'module' and 'class'.",
            context={"path": str(path)},
        )
    processor_module = processor_block.get("module")
    processor_class = processor_block.get("class")
    if bool(processor_module) != bool(processor_class):
        raise MetadataError(
            "Processor metadata must declare both 'module' and 'class' together.",
            context={"path": str(path)},
        )

    paths = _validate_paths(raw.get("paths"), path)
    processor_config = raw.get("processor_config") or {}
    if not isinstance(processor_config, dict):
        raise MetadataError(
            "Field 'processor_config' must be a mapping.", context={"path": str(path)}
        )

    fq_name_from_file = f"{layer.value}.{name}"

    # Metadata lives at `.../tables/<layer>/<table>/metadata.yaml`. Rather
    # than trusting `name:`/`layer:` in isolation, cross-check them against
    # the directory structure -- this catches the classic copy-paste mistake
    # (duplicating a table's directory to start a new one and forgetting to
    # update `name:` inside it) the same way the old flat-file convention's
    # filename check did.
    if path.name != "metadata.yaml":
        raise MetadataError(
            "Table metadata file must be named 'metadata.yaml'.",
            context={"path": str(path)},
        )
    table_dir_name = path.parent.name
    if table_dir_name != name:
        raise MetadataError(
            "The table's directory name must match its 'name' field.",
            context={"path": str(path), "directory_name": table_dir_name, "name_field": name},
        )
    layer_dir_name = path.parent.parent.name
    if layer_dir_name != layer.value:
        raise MetadataError(
            "The table's parent directory name must match its 'layer' field.",
            context={
                "path": str(path),
                "parent_directory": layer_dir_name,
                "layer_field": layer.value,
            },
        )

    return TableDefinition(
        name=name,
        layer=layer,
        execution_mode=execution_mode,
        load_strategy=load_strategy,
        write_mode=write_mode,
        dependencies=dependencies,
        description=description.strip(),
        processor_module=processor_module,
        processor_class=processor_class,
        metadata_path=path,
        metadata=dict(raw),
        paths=paths,
        config=dict(processor_config),
    )


def load_table_definition(path: Path) -> TableDefinition:
    """Load a single YAML file straight into a validated :class:`TableDefinition`."""
    raw = load_raw_metadata(path)
    return parse_table_definition(raw, path)
