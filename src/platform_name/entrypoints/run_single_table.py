"""Single-table production entry point.

This is what actually executes inside **one** Lakeflow Jobs task (Databricks'
current name for what was previously called a Databricks Workflows task --
same Jobs API underneath). It deliberately does NOT resolve or execute a
dependency chain the way
:class:`~platform_name.engine.runner.TableRunner` does for local
development -- in production, Lakeflow Jobs itself already
guarantees that every upstream task succeeded before this one starts (see
the generated job in ``resources/tables_job.yml``, built by
:mod:`platform_name.deploy.bundle_generator`).

This script's only job: resolve one table's metadata, build its processor
via the same generic :class:`ProcessorFactory` used everywhere else in the
framework, run it, and record a structured result. It contains no
table-specific logic -- the table to run is entirely a runtime parameter.
"""
from __future__ import annotations

import argparse
import sys
import uuid
from pathlib import Path

from platform_name.common.config import Config
from platform_name.common.exceptions import PlatformError
from platform_name.engine.models import ExecutionContext, ExecutionResult
from platform_name.engine.processor_factory import EXECUTION_MODES_REQUIRING_SQL_ENGINE, ProcessorFactory
from platform_name.engine.table_registry import TableRegistry
from platform_name.observability.result_sink import ResultSink, StdoutResultSink
from platform_name.sql.factory import build_sql_engine

# _REPO_ROOT = Path(__file__).resolve().parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[1]
# Metadata ships inside the installed package (see pyproject.toml
# package-data), so this resolves correctly both from a repo checkout and
# from a wheel installed on a Databricks cluster.
# DEFAULT_METADATA_ROOT = Path(__file__).resolve().parents[1] / "tables"
DEFAULT_METADATA_ROOT = _REPO_ROOT / "tables"
DEFAULT_CONFIG_ROOT = _REPO_ROOT / "configs" / "environments"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run exactly one table's processor. Intended to be invoked "
        "as a single Lakeflow Jobs task, one per table, with ordering "
        "guaranteed by task `depends_on` edges rather than by this script."
    )
    parser.add_argument(
        "--table", required=True, help="Fully-qualified table name, e.g. gold.returns"
    )
    parser.add_argument(
        "--env", required=True, help="Environment name matching configs/environments/<env>.yaml"
    )
    parser.add_argument(
        "--metadata-root",
        default=str(DEFAULT_METADATA_ROOT),
        help="Override the metadata directory (mainly for testing).",
    )
    parser.add_argument(
        "--config-path",
        default=None,
        help="Override the environment config file path (mainly for testing).",
    )
    return parser.parse_args(argv)


def _get_active_spark_session() -> object | None:
    """Best-effort attempt to attach to the SparkSession already running in
    this process, when running as a task on a Databricks (or any Spark)
    cluster. Returns ``None`` -- never raises -- when PySpark isn't
    installed (e.g. local Pandas-only development) or no session is
    available, so every pandas-mode table is completely unaffected.

    A Databricks ``python_wheel_task`` runs as a plain Python entry point
    (not a notebook), so unlike a notebook there is no ``spark`` global
    auto-injected -- but the JVM process it runs in already has a Spark
    context initialized, so ``SparkSession.builder.getOrCreate()`` attaches
    to that existing session rather than starting a new one.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return None
    try:
        return SparkSession.builder.getOrCreate()
    except Exception:
        return None


def run(
    table_name: str,
    environment: str,
    metadata_root: str | Path = DEFAULT_METADATA_ROOT,
    config_path: str | Path | None = None,
    sink: ResultSink | None = None,
) -> ExecutionResult:
    """Resolve, build, and execute exactly one table. Raises on failure
    (after recording the failed result) so the calling process -- and
    therefore the Databricks task -- exits non-zero and shows red.
    """
    resolved_config_path = Path(config_path) if config_path else DEFAULT_CONFIG_ROOT / f"{environment}.yaml"
    config = Config(resolved_config_path)
    registry = TableRegistry(metadata_root).load()
    definition = registry.get(table_name)

    active_sink = sink or StdoutResultSink()
    result = ExecutionResult.start(
        table=definition.fully_qualified_name,
        layer=definition.layer.value,
        run_id=str(uuid.uuid4()),
    )

    try:
        # `spark` is None for pandas-mode tables and in any environment
        # without PySpark installed; ProcessorFactory only ever reads it
        # for tables whose execution_mode actually requires it (see
        # processor_factory.py).
        active_spark = _get_active_spark_session()

        # `sql_engine` is selected by this environment's `sql_backend:`
        # config (DuckDB locally, Spark in test/prod -- see
        # sql/factory.py). Built only when THIS table's execution mode
        # actually needs one, so running a plain pandas-mode table against
        # test/prod config never requires a live Spark session just to
        # construct an engine it won't use. Building it inside this `try`
        # means a misconfigured backend (e.g. `sql_backend: spark` with no
        # Spark session available) is recorded as a normal failed
        # ExecutionResult for this table, not an unhandled crash.
        sql_engine = None
        if definition.execution_mode in EXECUTION_MODES_REQUIRING_SQL_ENGINE:
            sql_engine = build_sql_engine(config, spark=active_spark)

        context = ExecutionContext(config=config, spark=active_spark, sql_engine=sql_engine)
        result.run_id = context.run_id
        factory = ProcessorFactory(config=config, context=context)

        processor = factory.create(definition)
        output = processor.run()
        row_count = len(output) if hasattr(output, "__len__") else None
        result.mark_success(row_count=row_count)
    except Exception as err:
        result.mark_failed(str(err))
        active_sink.record(result)
        raise
    else:
        active_sink.record(result)
        return result


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        result = run(
            table_name=args.table,
            environment=args.env,
            metadata_root=args.metadata_root,
            config_path=args.config_path,
        )
    except PlatformError as err:
        # PlatformError subclasses already carry structured, actionable
        # context (table, processor, missing arguments, etc.) -- see
        # common/exceptions.py. Print as-is rather than re-wrapping.
        print(f"FAILED: {err}", file=sys.stderr)
        sys.exit(1)
    except Exception as err:  # pragma: no cover - defensive catch-all
        print(f"FAILED (unexpected error): {err}", file=sys.stderr)
        sys.exit(1)
    else:
        print(f"SUCCESS: {result.to_dict()}")


if __name__ == "__main__":
    main()
