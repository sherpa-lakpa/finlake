"""Architecture-level validation.

Runs across an entire :class:`TableRegistry` to fail *early* (before any
processor executes) rather than partway through a pipeline run. Combines
metadata validation (already enforced at load time by
:mod:`metadata_loader`), dependency graph validation, and processor
loadability/constructibility validation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from platform_name.common.config import Config
from platform_name.common.exceptions import PlatformError
from platform_name.engine.dependency_graph import DependencyGraph
from platform_name.engine.processor_factory import (
    EXECUTION_MODES_REQUIRING_SPARK,
    EXECUTION_MODES_REQUIRING_SQL_ENGINE,
    ProcessorFactory,
)
from platform_name.engine.processor_loader import ProcessorLoader
from platform_name.engine.table_registry import TableRegistry


@dataclass
class ValidationReport:
    errors: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not self.errors

    def add(self, message: str) -> None:
        self.errors.append(message)

    def raise_if_invalid(self) -> None:
        if self.errors:
            formatted = "\n".join(f"- {e}" for e in self.errors)
            raise PlatformError(
                f"Architecture validation failed with {len(self.errors)} error(s):\n{formatted}"
            )


class ArchitectureValidator:
    """Validates a whole registry: dependency graph integrity plus, for every
    table, that its processor can be dynamically loaded and constructed from
    metadata alone (without requiring any physical input files to exist).

    For pandas-mode tables this always attempts real construction. For
    pyspark-mode tables, full construction additionally requires a live
    Spark session, and for sql-mode tables it requires a
    :class:`~platform_name.sql.base.SqlEngine` -- neither of which most CI
    environments will have. When ``context`` is omitted or carries neither,
    validation for those tables falls back to checking only that their
    metadata paths resolve to a valid shape, rather than reporting a
    spurious "missing spark"/"missing sql_engine" failure that has nothing
    to do with the table's actual metadata being correct. Pass a
    ``context`` with a real Spark session and/or SQL engine (e.g. when
    running this validator as a step inside an actual Databricks job) to
    get full construction checking for those tables too.
    """

    def __init__(self, registry: TableRegistry, config: Config, context: Any | None = None) -> None:
        self.registry = registry
        self.config = config
        self.context = context
        self.graph = DependencyGraph(registry)
        self.loader = ProcessorLoader()
        self.factory = ProcessorFactory(config=config, processor_loader=self.loader, context=context)

    def _spark_available(self) -> bool:
        return bool(self.context is not None and getattr(self.context, "spark", None) is not None)

    def _sql_engine_available(self) -> bool:
        return bool(self.context is not None and getattr(self.context, "sql_engine", None) is not None)

    def validate(self) -> ValidationReport:
        report = ValidationReport()

        try:
            self.graph.validate()
        except PlatformError as err:
            report.add(str(err))

        for definition in self.registry.list_tables():
            try:
                self.loader.load(definition)
            except PlatformError as err:
                report.add(str(err))
                continue

            needs_injected_dependency = (
                definition.execution_mode in EXECUTION_MODES_REQUIRING_SPARK
                and not self._spark_available()
            ) or (
                definition.execution_mode in EXECUTION_MODES_REQUIRING_SQL_ENGINE
                and not self._sql_engine_available()
            )
            try:
                if needs_injected_dependency:
                    # Can't fully construct without the live dependency
                    # this table's execution mode needs; still validate
                    # that declared paths have a valid shape.
                    self.factory.build_constructor_arguments(definition)
                else:
                    # Actually attempt construction (not just path
                    # resolution), so a table missing a required
                    # constructor argument is caught here, before any
                    # processor executes. Safe because processor
                    # constructors are expected to only store arguments,
                    # never perform I/O (I/O belongs in `read()`/`write()`)
                    # -- see tables/base.py.
                    self.factory.create(definition)
            except PlatformError as err:
                report.add(str(err))

        return report
