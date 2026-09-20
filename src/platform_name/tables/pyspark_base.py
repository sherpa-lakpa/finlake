"""PySpark processor base class.

Mirrors :class:`~platform_name.tables.base.BaseProcessor`'s
read/transform/validate/write/run lifecycle, but for PySpark DataFrames and
Spark-native storage (Delta), for tables whose data volume has outgrown a
single-node Pandas processor.

To move a table from Pandas to PySpark:

1. Set ``execution.mode: pyspark`` in that table's ``metadata.yaml``
   instead of ``pandas``.
2. Inherit the processor from :class:`BasePySparkProcessor` instead of
   :class:`~platform_name.tables.base.BaseProcessor`, and accept ``spark``
   as a constructor parameter.
3. Nothing else changes -- discovery, dependency resolution, path
   resolution, and orchestration are identical regardless of execution
   mode, because the engine (:mod:`platform_name.engine`) never branches on
   it except to decide whether to inject a Spark session (see
   :meth:`~platform_name.engine.processor_factory.ProcessorFactory.build_constructor_arguments`).

This module deliberately does **not** import ``pyspark`` at module level,
so environments without PySpark installed (local Pandas-only dev, this
repo's default test suite) never fail to import it. Only a concrete
subclass's ``transform()`` would import PySpark APIs, and only when it
actually runs.
"""
from __future__ import annotations

from typing import Any

from platform_name.tables.base import BaseProcessor


class BasePySparkProcessor(BaseProcessor):
    """Base class for tables executing on PySpark instead of Pandas.

    Requires a live ``SparkSession``, injected automatically by
    :class:`ProcessorFactory` when a table's ``execution_mode`` is
    ``pyspark`` (or ``sql``) and the active :class:`ExecutionContext`
    carries a real Spark session -- see ``engine/processor_factory.py``.
    Every subclass constructor must accept ``spark`` as a parameter for
    this injection to work; the factory's existing, generic
    required-argument check (used for every processor, not just PySpark
    ones) will raise a clear error naming ``spark`` as missing if no
    session is available, exactly the same way it reports any other
    missing constructor argument.
    """

    def __init__(self, spark: Any) -> None:
        if spark is None:
            raise ValueError(
                "BasePySparkProcessor requires a live `spark` SparkSession. "
                "Locally, this typically means no real Spark session was "
                "attached to the ExecutionContext -- see "
                "entrypoints/run_single_table.py for how one is obtained "
                "inside an actual Databricks job."
            )
        self.spark = spark
