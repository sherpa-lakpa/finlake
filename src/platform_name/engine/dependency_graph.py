"""Dependency graph: discovery, topological sorting, and validation.

The graph is built purely from ``dependencies`` declared in table metadata.
It never executes processors -- that is the :class:`~platform_name.engine.runner.TableRunner`'s
job. This module answers exactly one question: "in what order must tables
run to satisfy a target table's dependencies?"
"""
from __future__ import annotations

from platform_name.common.exceptions import DependencyGraphError
from platform_name.engine.enums import Layer
from platform_name.engine.table_registry import TableRegistry

# Layers are expected to depend only on the same or "earlier" layers.
_LAYER_ORDER = {Layer.LANDING: 0, Layer.BRONZE: 1, Layer.SILVER: 2, Layer.GOLD: 3}


class DependencyGraph:
    """Builds and queries the dependency graph for all tables in a registry."""

    def __init__(self, registry: TableRegistry) -> None:
        self.registry = registry

    def _edges(self, table_name: str) -> tuple[str, ...]:
        if not self.registry.has(table_name):
            raise DependencyGraphError(
                "Table referenced in the graph is not present in the registry.",
                context={"table": table_name, "known_tables": self.registry.all_names()},
            )
        return self.registry.get(table_name).dependencies

    def validate(self) -> None:
        """Validate the *entire* registry: missing dependencies, invalid
        layer ordering, and cycles. Raises on the first problem found.
        """
        for definition in self.registry.list_tables():
            for dependency in definition.dependencies:
                if not self.registry.has(dependency):
                    raise DependencyGraphError(
                        "Table declares a dependency that does not exist in the registry.",
                        context={
                            "table": definition.fully_qualified_name,
                            "missing_dependency": dependency,
                        },
                    )
                dep_definition = self.registry.get(dependency)
                if _LAYER_ORDER[dep_definition.layer] > _LAYER_ORDER[definition.layer]:
                    raise DependencyGraphError(
                        "Table depends on a table in a later medallion layer.",
                        context={
                            "table": definition.fully_qualified_name,
                            "table_layer": definition.layer.value,
                            "dependency": dependency,
                            "dependency_layer": dep_definition.layer.value,
                        },
                    )

        for definition in self.registry.list_tables():
            self.resolve(definition.fully_qualified_name)

    def resolve(self, table_name: str) -> list[str]:
        """Return the fully-qualified names of every table that must execute
        (in order) to satisfy ``table_name``, ending with ``table_name`` itself.
        """
        ordered: list[str] = []
        visited: set[str] = set()
        in_progress: set[str] = set()

        def visit(node: str, path: list[str]) -> None:
            if node in visited:
                return
            if node in in_progress:
                cycle = " -> ".join(path + [node])
                raise DependencyGraphError(
                    "Circular dependency detected.", context={"cycle": cycle}
                )

            in_progress.add(node)
            for dependency in self._edges(node):
                visit(dependency, path + [node])
            in_progress.discard(node)
            visited.add(node)
            ordered.append(node)

        visit(table_name, [])
        return ordered

    def upstream_of(self, table_name: str) -> list[str]:
        """Like :meth:`resolve` but excludes ``table_name`` itself."""
        return [name for name in self.resolve(table_name) if name != table_name]
