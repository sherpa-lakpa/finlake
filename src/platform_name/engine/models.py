"""Core data models: :class:`TableDefinition`, :class:`ExecutionContext`,
and :class:`ExecutionResult`.

These are pure data containers. They hold *metadata about* execution, never
business logic, and never perform I/O themselves.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from platform_name.engine.enums import ExecutionMode, Layer, LoadStrategy, WriteMode


@dataclass(frozen=True)
class TableDefinition:
    """Canonical, immutable description of WHAT a table is and HOW it should
    execute. Contains metadata only -- never business logic.
    """

    name: str
    layer: Layer

    execution_mode: ExecutionMode
    load_strategy: LoadStrategy
    write_mode: WriteMode

    dependencies: tuple[str, ...]

    description: str

    processor_module: str | None
    processor_class: str | None

    metadata_path: Path | None

    metadata: dict[str, Any] = field(default_factory=dict)
    paths: dict[str, Any] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)

    @property
    def fully_qualified_name(self) -> str:
        """e.g. ``bronze.orders``, ``silver.customer_orders``, ``gold.customer_metrics``."""
        return f"{self.layer.value}.{self.name}"

    @property
    def full_name(self) -> str:
        """Backward-compatible alias for :attr:`fully_qualified_name`."""
        return self.fully_qualified_name

    def __repr__(self) -> str:  # pragma: no cover - convenience only
        return (
            f"TableDefinition(name={self.fully_qualified_name!r}, "
            f"execution_mode={self.execution_mode.value!r}, "
            f"load_strategy={self.load_strategy.value!r}, "
            f"dependencies={self.dependencies!r})"
        )


@dataclass
class ExecutionContext:
    """Runtime context threaded through the engine during execution.

    Deliberately loose/extensible (a plain dict of ``extra`` values) rather
    than tightly coupled to any single execution platform. ``spark`` is
    ``None`` for local Pandas execution and would be populated with a
    ``SparkSession`` when running inside Databricks. ``sql_engine`` is the
    :class:`~platform_name.sql.base.SqlEngine` ``execution.mode: sql``
    tables run against -- DuckDB locally, Spark/Databricks in test/prod,
    selected by environment configuration (see
    :mod:`platform_name.sql.factory`), independent of ``spark`` itself.
    """

    config: Any
    environment: str | None = None
    catalog: str | None = None
    schema: str | None = None
    spark: Any = None
    sql_engine: Any = None
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    extra: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.environment is None and hasattr(self.config, "environment"):
            self.environment = self.config.environment


@dataclass
class ExecutionResult:
    """Structured record of a single table execution, suitable for logging,
    metrics pipelines, or persistence (Azure Monitor / Databricks in the
    future)."""

    table: str
    layer: str
    run_id: str
    status: str  # "success" | "failed" | "skipped"
    start_time: datetime
    end_time: datetime | None = None
    row_count: int | None = None
    error: str | None = None
    result: Any = None

    @property
    def duration_seconds(self) -> float | None:
        if self.end_time is None:
            return None
        return (self.end_time - self.start_time).total_seconds()

    @classmethod
    def start(cls, table: str, layer: str, run_id: str) -> "ExecutionResult":
        return cls(
            table=table,
            layer=layer,
            run_id=run_id,
            status="running",
            start_time=datetime.now(timezone.utc),
        )

    def mark_success(self, *, row_count: int | None, result: Any = None) -> "ExecutionResult":
        self.status = "success"
        self.end_time = datetime.now(timezone.utc)
        self.row_count = row_count
        self.result = result
        return self

    def mark_failed(self, error: str) -> "ExecutionResult":
        self.status = "failed"
        self.end_time = datetime.now(timezone.utc)
        self.error = error
        return self

    def mark_skipped(self, reason: str) -> "ExecutionResult":
        self.status = "skipped"
        self.end_time = datetime.now(timezone.utc)
        self.error = reason
        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "layer": self.layer,
            "run_id": self.run_id,
            "status": self.status,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "row_count": self.row_count,
            "error": self.error,
        }
