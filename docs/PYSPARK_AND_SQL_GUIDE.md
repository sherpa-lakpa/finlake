# Writing PySpark and SQL transformations, and running them locally then in prod

This guide answers three concrete questions: *how do I write a table's
transformation logic in PySpark or SQL instead of Pandas*, *can the exact
same code run on my laptop and then in production*, and *how do I do that
without installing a JVM and DuckDB myself*. Short answers: yes for
PySpark/SQL portability (that's the whole point of how this is built), and
Docker handles the setup friction (see the end of this guide).

Two real, shipped tables in this repository demonstrate the whole story
end to end -- this isn't just illustrative code:

- **`gold.customer_risk`** (`tables/gold/customer_risk/`) --
  `execution.mode: pyspark`, a genuine PySpark DataFrame-API processor.
- **`gold.top_movers`** (`tables/gold/top_movers/`) -- `execution.mode: sql`,
  logic lives entirely in `query.sql` next to `processor.py`.

Both depend on `gold.returns` (the existing Pandas-mode table), proving a
mixed pipeline -- Pandas, PySpark, and SQL tables side by side, orchestrated
identically -- works exactly as designed.

If you haven't read [`docs/TUTORIAL.md`](TUTORIAL.md), read that first —
this assumes you understand `ProcessorFactory`, `TableDefinition`, and
`BaseProcessor` already.

## Three ways to write a table's logic, and when to reach for each

| | Base class | Local dev | Data volume | Best for |
|---|---|---|---|---|
| **Pandas** | `BaseProcessor` | Instant, no cluster | Single-node, in-memory | Small/medium tables, the fast default |
| **PySpark (DataFrame API)** | `BasePySparkProcessor` | Local `SparkSession` (needs a JVM — see Docker below) | Distributed, scales with cluster size | Large tables, complex joins/windows across big data |
| **SQL** | `BaseSqlProcessor` | DuckDB (`sql_backend: duckdb`, zero JVM) or Spark | Same as the selected backend | Transformations SQL-fluent analysts need to read/edit directly |

All three are selected the same way: set `execution.mode:` in the table's
`metadata.yaml` (`pandas`, `pyspark`, or `sql`), and inherit the matching
base class. `pyspark` tables always need a real Spark session. `sql`
tables need a `sql_engine` — **which backend that is comes from
environment configuration, not the table**:

```yaml
# configs/environments/dev.yaml
sql_backend: duckdb    # embedded, no cluster, fastest local iteration

# configs/environments/test.yaml and prod.yaml
sql_backend: spark      # Databricks cluster session in production
```

A `gold.top_movers`-style table's `query.sql` and `processor.py` never
change between environments — only which engine executes that query does,
decided entirely by `sql/factory.py` reading `sql_backend:`.

## The real `gold.customer_risk` table (PySpark)

```python
# tables/gold/customer_risk/processor.py
from pyspark.sql import DataFrame, functions as F

from platform_name.storage.spark_parquet import SparkParquetStorage
from platform_name.tables.pyspark_base import BasePySparkProcessor


class CustomerRiskProcessor(BasePySparkProcessor):
    """Risk score per security: mean absolute daily return."""

    def __init__(self, spark, returns_path: str, output_path: str) -> None:
        super().__init__(spark)
        self.returns_path = returns_path
        self.output_path = output_path
        self.storage = SparkParquetStorage(spark)

    def read(self) -> DataFrame:
        return self.storage.read_parquet(self.returns_path)

    def transform(self, data: DataFrame) -> DataFrame:
        return (
            data.filter(F.col("daily_return").isNotNull())
            .groupBy("security_id")
            .agg(F.avg(F.abs(F.col("daily_return"))).alias("risk_score"))
        )

    def write(self, data: DataFrame) -> None:
        self.storage.write_parquet(data, self.output_path)
```

```yaml
# tables/gold/customer_risk/metadata.yaml
name: customer_risk
layer: gold
execution:
  mode: pyspark
dependencies:
  - gold.returns
processor:
  module: platform_name.tables.gold.customer_risk.processor
  class: CustomerRiskProcessor
paths:
  input:
    file: returns.parquet
    root: gold
    parameter: returns_path
  output:
    file: customer_risk.parquet
```

Note this uses `SparkParquetStorage`, not `DeltaStorageAdapter` — plain
Parquet, matching exactly what the upstream Pandas-mode `gold.returns`
table already writes, so Spark can read it with zero format conversion and
no Delta Lake JAR configuration needed for a first PySpark table. Reach for
`DeltaStorageAdapter` instead once a table needs real Delta features
against ADLS/Unity Catalog storage.

**A gotcha worth knowing**: Spark's `.write.parquet(path)` creates a
*directory* at that path (containing part-files), not a single file — even
though the metadata says `file: customer_risk.parquet`. That's normal
Spark behavior and Spark reads it back correctly, but if you browse
`data/gold/` locally expecting one file, you'll find a folder instead. This
only matters if something else needs to read that output directly with a
different engine (nothing in this example domain does).

## The real `gold.top_movers` table (SQL)

```python
# tables/gold/top_movers/processor.py
from platform_name.tables.sql_base import BaseSqlProcessor

class TopMoversProcessor(BaseSqlProcessor):
    sql_file = "query.sql"
```

```sql
-- tables/gold/top_movers/query.sql
SELECT
    trade_date,
    security_id,
    daily_return,
    RANK() OVER (
        PARTITION BY trade_date
        ORDER BY ABS(daily_return) DESC
    ) AS movement_rank
FROM returns
WHERE daily_return IS NOT NULL
```

```yaml
# tables/gold/top_movers/metadata.yaml
name: top_movers
layer: gold
execution:
  mode: sql
dependencies:
  - gold.returns
processor:
  module: platform_name.tables.gold.top_movers.processor
  class: TopMoversProcessor
paths:
  returns:
    file: returns.parquet
    root: gold
    parameter: returns
  output:
    file: top_movers.parquet
```

The `parameter: returns` here is also the view name the SQL file queries
(`FROM returns`) — `BaseSqlProcessor` registers every input path as a view
named after its constructor keyword. A SQL-fluent analyst can open exactly
these two files and understand (or edit) the whole table without touching
Python.

## Running both locally, without installing a JVM or DuckDB yourself

This is what the Docker setup in this repository is for. PySpark's biggest
local setup cost is a working JVM — Docker removes that entirely.

```bash
docker compose build
docker compose run --rm dev python scripts/local_dev_pipeline.py
```

That script runs the *entire* mixed pipeline against `dev.yaml`: the
Pandas chain through `gold.performance_summary`, the PySpark
`gold.customer_risk` table (via a real local `SparkSession`), and the SQL
`gold.top_movers` table (via DuckDB, per `dev.yaml`'s `sql_backend:
duckdb`) — all sharing one `TableRunner` context, so `gold.returns`
computes once and both downstream tables reuse it, exactly like a single
Databricks job run.

Want a shell instead, to run tests or iterate interactively?

```bash
docker compose run --rm dev bash
# inside the container:
pytest
python scripts/local_dev_pipeline.py
```

Prefer not to use Docker? The same commands work with a local Python
environment as long as you have a JVM installed and run:

```bash
pip install -e ".[dev,spark,duckdb]"
```

## Running in production

`entrypoints/run_single_table.py` (the script each Lakeflow Jobs task
runs) auto-detects an already-running Spark session and builds the right
`sql_engine` from that environment's `sql_backend:` — both `gold.customer_risk`
and `gold.top_movers` run completely unmodified. A Databricks
`python_wheel_task` runs as a plain Python entry point in a JVM process
that already has Spark initialized, so
`SparkSession.builder.getOrCreate()` attaches to *that* existing session
rather than starting a new one — same code, a real cluster behind it
instead of `local[*]`. `configs/environments/test.yaml`/`prod.yaml` both
set `sql_backend: spark`, so `gold.top_movers` runs on real Databricks SQL
(via Spark) in those environments, never DuckDB — DuckDB is a local-only
convenience, deliberately not available as a `test`/`prod` option.

## The one case that genuinely doesn't run locally: Databricks SQL Warehouses

Everything above executes *through Spark or DuckDB*. That's different from
**Databricks SQL Warehouses**: a serverless, connector-based SQL endpoint
product (queried via `databricks-sql-connector`, a network connection to a
live warehouse) with no Spark session involved at all, and no local
equivalent to run offline.

This framework doesn't build that path today, and for medallion pipeline
transformations it usually isn't the right tool anyway — SQL Warehouses
are aimed at BI/interactive query workloads, not orchestrated ETL. If you
do have a genuine need for it, it would be a new execution mode with its
own injected object (a SQL connection, not a `spark` session or a
`sql_engine`) — a real but separate piece of work from what's built here.

## What stays the same regardless of execution mode

`TableRegistry` discovery, `DependencyGraph` ordering,
`ArchitectureValidator`'s pre-flight checks, the generated Lakeflow Jobs
task graph, and the whole triage story from `docs/DEPLOYMENT.md` apply
identically to Pandas, PySpark, and SQL tables. A mixed pipeline is not a
special case anywhere in `engine/`; it's the expected, ordinary shape of a
real platform as tables individually outgrow Pandas over time.
