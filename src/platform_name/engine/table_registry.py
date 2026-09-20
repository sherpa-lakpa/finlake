"""Table metadata registry.

Discovers table metadata YAML files on the filesystem, validates them via
:mod:`metadata_loader`, and exposes lookup/listing/filtering APIs. Tables are
never manually registered in Python code -- adding a new table means adding
a new ``metadata.yaml`` file next to its processor (under
``src/platform_name/tables/<layer>/<table>/``), nothing more.
"""
from __future__ import annotations

from pathlib import Path

from platform_name.common.exceptions import RegistryError
from platform_name.engine.enums import Layer
from platform_name.engine.metadata_loader import load_table_definition
from platform_name.engine.models import TableDefinition


class TableRegistry:
    """Discovers and holds all :class:`TableDefinition` instances for a
    metadata directory tree."""

    def __init__(self, metadata_root: str | Path) -> None:
        self.metadata_root = Path(metadata_root)
        self._tables: dict[str, TableDefinition] = {}
        self._loaded = False

    def load(self) -> "TableRegistry":
        """Discover and parse every ``metadata.yaml`` file under the
        metadata root (recursively -- one per table directory).

        Safe to call multiple times; subsequent calls reload from disk.
        """
        if not self.metadata_root.exists():
            raise RegistryError(
                "Metadata root directory does not exist.",
                context={"metadata_root": str(self.metadata_root)},
            )

        discovered: dict[str, TableDefinition] = {}
        for yaml_path in sorted(self.metadata_root.rglob("metadata.yaml")):
            definition = load_table_definition(yaml_path)
            fq_name = definition.fully_qualified_name
            if fq_name in discovered:
                raise RegistryError(
                    "Duplicate table definition detected.",
                    context={
                        "table": fq_name,
                        "first_path": str(discovered[fq_name].metadata_path),
                        "duplicate_path": str(yaml_path),
                    },
                )
            discovered[fq_name] = definition

        self._tables = discovered
        self._loaded = True
        return self

    def _ensure_loaded(self) -> None:
        if not self._loaded:
            self.load()

    def get(self, fully_qualified_name: str) -> TableDefinition:
        self._ensure_loaded()
        try:
            return self._tables[fully_qualified_name]
        except KeyError as err:
            raise RegistryError(
                "No table definition found for requested table.",
                context={
                    "table": fully_qualified_name,
                    "known_tables": sorted(self._tables),
                },
            ) from err

    def has(self, fully_qualified_name: str) -> bool:
        self._ensure_loaded()
        return fully_qualified_name in self._tables

    def list_tables(self) -> tuple[TableDefinition, ...]:
        self._ensure_loaded()
        return tuple(self._tables[name] for name in sorted(self._tables))

    def list_by_layer(self, layer: Layer | str) -> tuple[TableDefinition, ...]:
        self._ensure_loaded()
        layer_value = layer.value if isinstance(layer, Layer) else layer
        return tuple(
            definition
            for definition in self.list_tables()
            if definition.layer.value == layer_value
        )

    def all_names(self) -> tuple[str, ...]:
        self._ensure_loaded()
        return tuple(sorted(self._tables))

    def __len__(self) -> int:  # pragma: no cover - convenience only
        self._ensure_loaded()
        return len(self._tables)

    def __iter__(self):  # pragma: no cover - convenience only
        return iter(self.list_tables())
