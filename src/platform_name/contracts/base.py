"""Dataset contract model.

A :class:`DatasetContract` declares the *shape* a dataset must have --
columns, required columns, expected dtypes, nullability, uniqueness, and
business keys -- independent of which layer (bronze/silver/gold) enforces
it. Contracts are reusable by any processor.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ColumnContract:
    """Expectations for a single column."""

    dtype: str | None = None
    nullable: bool = True
    unique: bool = False


@dataclass(frozen=True)
class DatasetContract:
    """Declarative expectations for an entire dataset."""

    name: str
    columns: dict[str, ColumnContract] = field(default_factory=dict)
    required_columns: tuple[str, ...] = ()
    business_keys: tuple[str, ...] = ()
    min_row_count: int = 0

    def __post_init__(self) -> None:
        if not self.required_columns:
            object.__setattr__(self, "required_columns", tuple(self.columns))
