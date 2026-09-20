"""Canonical enumerations shared across the engine.

Keeping these as simple ``str`` Enums means they serialize cleanly to/from
YAML metadata (the raw string values are what appear in the YAML files) while
still giving type safety and validation everywhere else in the codebase.
"""
from __future__ import annotations

from enum import Enum


class Layer(str, Enum):
    """Medallion architecture layer. Extensible: new layers can be added here
    without changing any other engine component, since all lookups go through
    this enum rather than hardcoded strings."""

    LANDING = "landing"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class ExecutionMode(str, Enum):
    """How a processor executes. PySpark/SQL are clean extension points;
    only PANDAS is fully implemented today."""

    PANDAS = "pandas"
    PYSPARK = "pyspark"
    SQL = "sql"


class LoadStrategy(str, Enum):
    """Describes *how new data should be reconciled with existing data*,
    independent of the physical write mechanism (see :class:`WriteMode`)."""

    FULL = "full"
    INCREMENTAL = "incremental"
    UPSERT = "upsert"
    SCD1 = "scd1"
    SCD2 = "scd2"


class WriteMode(str, Enum):
    """Describes the *physical* write mechanism used to persist output.

    Deliberately kept separate from :class:`LoadStrategy`: a table's load
    strategy might be ``upsert`` while, depending on the storage engine, the
    write mode used to realize that upsert could be ``merge`` (Delta/SQL
    MERGE) or ``overwrite`` (e.g. a Pandas implementation that recomputes the
    full merged result and overwrites the file each run).
    """

    OVERWRITE = "overwrite"
    APPEND = "append"
    MERGE = "merge"
