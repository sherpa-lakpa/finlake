"""Generic processor factory: dependency/configuration injection.

``ProcessorFactory`` is the single place where table metadata paths are
resolved into concrete filesystem locations and injected into a processor's
constructor. It is intentionally "dumb": it only understands the *shape* of
the metadata contract --

    paths:
      <entry-name>:
        file: <filename>          # or: directory: <dirname>
        root: <landing|bronze|silver|gold>
        parameter: <constructor kwarg name>   # optional, see below

-- and never the *meaning* of any particular table, dependency, or
processor. See :mod:`platform_name.engine` module docstring / README for the
full rationale (dependency-vs-input distinction).
"""
from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any

from platform_name.common.config import Config
from platform_name.common.exceptions import ProcessorConstructionError
from platform_name.engine.enums import ExecutionMode
from platform_name.engine.models import TableDefinition
from platform_name.engine.processor_loader import ProcessorLoader

# Execution modes that expect a live Spark session injected as a `spark`
# constructor kwarg (see BasePySparkProcessor). This is keyed on the
# generic `execution_mode` field every table declares -- never on a table
# name -- so it applies uniformly and needs no per-table special-casing.
EXECUTION_MODES_REQUIRING_SPARK = frozenset({ExecutionMode.PYSPARK})

# Execution modes that expect a `sql_engine` kwarg instead (see
# BaseSqlProcessor) -- deliberately a *different* injected dependency than
# `spark` above, since a sql-mode table's engine might be DuckDB (no Spark
# involved at all) depending on environment configuration.
EXECUTION_MODES_REQUIRING_SQL_ENGINE = frozenset({ExecutionMode.SQL})

# Generic (non-business-specific) fallback parameter names used only when a
# path entry omits an explicit `parameter`. These names describe *shape*
# (an input path, a landing-zone path) never a specific table's vocabulary.
_GENERIC_FALLBACK_PARAMETER_BY_ROOT = {
    "landing": "landing_path",
}
_DEFAULT_GENERIC_PARAMETER = "input_path"


class ProcessorFactory:
    """Builds a ready-to-run processor instance for a given table.

    Responsibilities: load the processor class, resolve output/input paths
    declared in metadata, resolve environment configuration, validate
    constructor parameters, and instantiate the processor.

    Non-responsibilities (by design): no table-specific branching, no
    inference of constructor parameter names from dependency names, no
    knowledge of business domains, no execution of the processor, no
    workflow ordering.
    """

    def __init__(
        self,
        config: Config,
        processor_loader: ProcessorLoader | None = None,
        context: Any | None = None,
    ) -> None:
        self.config = config
        self.processor_loader = processor_loader or ProcessorLoader()
        # Optional: an ExecutionContext, used only to source a `spark`
        # session for pyspark/sql-mode tables (see
        # build_constructor_arguments below). Never required for pandas
        # tables -- everything in this class works exactly as before if
        # `context` is omitted.
        self.context = context

    # -- path resolution ---------------------------------------------------

    def _resolve_root(self, root_name: str) -> Path:
        return Path(self.config.layer_root(root_name))

    def resolve_output_path(self, table_definition: TableDefinition) -> Path | None:
        output_entry = table_definition.paths.get("output")
        if output_entry is None:
            return None
        root_name = output_entry.get("root", table_definition.layer.value)
        return self._resolve_root(root_name) / output_entry["file"]

    def resolve_input_paths(self, table_definition: TableDefinition) -> dict[str, Path]:
        """Returns a mapping of ``constructor_parameter_name -> resolved Path``
        for every non-output entry declared under ``paths`` in metadata.
        """
        resolved: dict[str, Path] = {}
        for entry_name, entry in table_definition.paths.items():
            if entry_name == "output":
                continue

            root_name = entry["root"]
            root_path = self._resolve_root(root_name)

            if "file" in entry:
                physical_path = root_path / entry["file"]
            else:
                physical_path = root_path / entry["directory"]

            parameter_name = entry.get("parameter")
            if not parameter_name:
                # Legacy support: only generic (non-business) names are inferred.
                parameter_name = _GENERIC_FALLBACK_PARAMETER_BY_ROOT.get(
                    root_name, _DEFAULT_GENERIC_PARAMETER
                )

            if parameter_name in resolved:
                raise ProcessorConstructionError(
                    "Two path entries resolved to the same constructor parameter.",
                    context={
                        "table": table_definition.fully_qualified_name,
                        "parameter": parameter_name,
                    },
                )
            resolved[parameter_name] = physical_path

        return resolved

    # -- construction --------------------------------------------------------

    def build_constructor_arguments(self, table_definition: TableDefinition) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}

        output_path = self.resolve_output_path(table_definition)
        if output_path is not None:
            kwargs["output_path"] = str(output_path)

        for parameter_name, path in self.resolve_input_paths(table_definition).items():
            kwargs[parameter_name] = str(path)

        # Explicit processor_config values are injected last so metadata
        # authors can override defaults, but they never collide silently:
        for key, value in table_definition.config.items():
            kwargs[key] = value

        # Spark session injection: generic behavior keyed on the table's
        # execution_mode field, identical for every pyspark table -- never
        # a table-specific branch. If no context (or no live spark
        # session on it) is available, `spark` is simply left unresolved,
        # and the normal required-argument check in `create()` reports it
        # as a missing constructor argument exactly like any other -- no
        # special-cased error handling needed for this to fail clearly.
        if table_definition.execution_mode in EXECUTION_MODES_REQUIRING_SPARK:
            spark = getattr(self.context, "spark", None) if self.context is not None else None
            if spark is not None:
                kwargs["spark"] = spark

        # sql_engine injection: same generic pattern, for execution.mode:
        # sql tables (see BaseSqlProcessor). Deliberately a *separate*
        # dependency from `spark` above -- the engine might be DuckDB,
        # with no Spark session involved anywhere.
        if table_definition.execution_mode in EXECUTION_MODES_REQUIRING_SQL_ENGINE:
            sql_engine = getattr(self.context, "sql_engine", None) if self.context is not None else None
            if sql_engine is not None:
                kwargs["sql_engine"] = sql_engine

        return kwargs

    def create(self, table_definition: TableDefinition) -> Any:
        """Instantiate the processor for the given table definition."""
        processor_class = self.processor_loader.load(table_definition)
        available_kwargs = self.build_constructor_arguments(table_definition)

        try:
            signature = inspect.signature(processor_class.__init__)
        except (TypeError, ValueError) as err:  # pragma: no cover - defensive
            raise ProcessorConstructionError(
                "Unable to inspect processor constructor signature.",
                context={
                    "table": table_definition.fully_qualified_name,
                    "processor": processor_class.__name__,
                    "error": str(err),
                },
            ) from err

        parameters = [p for name, p in signature.parameters.items() if name != "self"]
        accepts_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in parameters)
        named_parameters = {
            p.name: p for p in parameters if p.kind not in (
                inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL
            )
        }

        required_parameters = {
            name for name, p in named_parameters.items() if p.default is inspect.Parameter.empty
        }
        missing = sorted(required_parameters - set(available_kwargs))
        if missing:
            raise ProcessorConstructionError(
                f"Unable to construct processor for {table_definition.fully_qualified_name}.",
                context={
                    "processor": processor_class.__name__,
                    "missing_required_arguments": missing,
                    "arguments_supplied": sorted(available_kwargs),
                },
            )

        if accepts_kwargs:
            final_kwargs = available_kwargs
        else:
            # Only pass along arguments the constructor actually accepts;
            # anything else resolved from metadata (e.g. unused config) is
            # silently ignored rather than raising a TypeError, since
            # metadata may legitimately carry extra descriptive fields.
            final_kwargs = {
                name: value for name, value in available_kwargs.items() if name in named_parameters
            }

        try:
            return processor_class(**final_kwargs)
        except Exception as err:
            raise ProcessorConstructionError(
                f"Failed to instantiate processor for {table_definition.fully_qualified_name}.",
                context={
                    "processor": processor_class.__name__,
                    "arguments_supplied": sorted(final_kwargs),
                    "error": str(err),
                },
            ) from err
