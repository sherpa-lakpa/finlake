"""``TableRunner``: executes a table and its full upstream dependency chain.

The runner ties together the registry (metadata), the dependency graph
(ordering), the processor factory (construction), and processors
(execution), while recording structured :class:`ExecutionResult` records for
observability. It contains no table-specific logic.
"""
from __future__ import annotations

import logging
from pathlib import Path

from platform_name.common.exceptions import ExecutionError
from platform_name.engine.dependency_graph import DependencyGraph
from platform_name.engine.models import ExecutionContext, ExecutionResult
from platform_name.engine.processor_factory import ProcessorFactory
from platform_name.engine.table_registry import TableRegistry

logger = logging.getLogger("platform_name.runner")

# Metadata now lives colocated with processors under `tables/<layer>/<table>/
# metadata.yaml` (see engine/table_registry.py), so the default root is the
# installed `platform_name.tables` package itself -- this resolves correctly
# both from a repo checkout (editable install) and from a wheel installed on
# a Databricks cluster, since the metadata ships inside the package either
# way (see pyproject.toml package-data and docs/DEPLOYMENT.md).
_DEFAULT_METADATA_ROOT = Path(__file__).resolve().parents[1] / "tables"


class TableRunner:
    """Resolves dependencies and executes tables in the correct order."""

    def __init__(
        self,
        metadata_root: str | Path | None = None,
        registry: TableRegistry | None = None,
    ) -> None:
        if registry is not None:
            self.registry = registry
        else:
            root = Path(metadata_root) if metadata_root is not None else _DEFAULT_METADATA_ROOT
            self.registry = TableRegistry(root).load()

        self.graph = DependencyGraph(self.registry)

    def run(
        self,
        table_name: str,
        context: ExecutionContext,
        *,
        force_rerun: bool = False,
    ) -> ExecutionResult:
        """Execute ``table_name`` and, first, every upstream dependency it
        needs (in topological order). Returns the :class:`ExecutionResult`
        for ``table_name`` itself. All intermediate results are recorded on
        ``context.extra["execution_results"]``.
        """
        execution_order = self.graph.resolve(table_name)

        results: dict[str, ExecutionResult] = context.extra.setdefault("execution_results", {})
        factory = ProcessorFactory(config=context.config, context=context)

        final_result: ExecutionResult | None = None
        for name in execution_order:
            if name in results and not force_rerun:
                final_result = results[name]
                continue

            definition = self.registry.get(name)
            result = ExecutionResult.start(
                table=definition.fully_qualified_name,
                layer=definition.layer.value,
                run_id=context.run_id,
            )
            logger.info("Starting execution of %s (run_id=%s)", name, context.run_id)

            try:
                processor = factory.create(definition)
                output = processor.run()
                row_count = self._infer_row_count(output)
                result.mark_success(row_count=row_count, result=output)
                logger.info(
                    "Completed %s: status=%s row_count=%s duration=%.3fs",
                    name,
                    result.status,
                    result.row_count,
                    result.duration_seconds or 0.0,
                )
            except Exception as err:
                result.mark_failed(str(err))
                results[name] = result
                logger.error("Failed executing %s: %s", name, err)
                raise ExecutionError(
                    f"Execution failed for table '{name}'.",
                    context={
                        "table": name,
                        "run_id": context.run_id,
                        "error": str(err),
                    },
                ) from err

            results[name] = result
            final_result = result

        assert final_result is not None  # execution_order always includes table_name
        return final_result

    @staticmethod
    def _infer_row_count(output: object) -> int | None:
        if output is None:
            return None
        if hasattr(output, "__len__"):
            try:
                return len(output)  # type: ignore[arg-type]
            except TypeError:  # pragma: no cover - defensive
                return None
        return None
