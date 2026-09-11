# Databricks Lakeflow Spark Declarative Pipelines — Target Patterns

Reference for Phase 3 (target architecture) and Phase 4 (transformation map). Target is **Python / PySpark** declarative pipelines (Lakeflow Declarative Pipelines, formerly Delta Live Tables / DLT). Confirm the workspace's runtime and API surface with the user; both the `dlt` module and the newer `pyspark` pipelines decorators exist in the field — use whichever the target workspace standardizes on and keep the mapping consistent.

All transformation logic is the **DataFrame API** — no `spark.sql("…")`, no `.selectExpr`, no `F.expr` for logic with a typed API. The SQL snippets below are illustrative of intent; write them as `df.filter(...)`, `df.join(...)`, `df.groupBy().agg(...)`, `F.when(...)`. The only strings that stay SQL are `@dlt.expect_*` predicates.

Read alongside: `sdp-production.md` (hardening: determinism, expectations, parameters/secrets, testing, project skeleton) and `sdp-optimization.md` (performance: ingest-once, broadcast, pivots, windows, storage layout, compute).

## Medallion mapping from an Alteryx workflow

| Alteryx | Databricks layer | Construct |
|---|---|---|
| Input Data / Text Input (raw source) | Bronze | `@dlt.table` batch read, or `dlt.create_streaming_table` + Auto Loader (`cloudFiles`) for files landing / append-only |
| Select, Data Cleanse, Auto Field, Formula (standardization), dedupe (Unique), type casts | Silver | `@dlt.table` reading `dlt.read("bronze_x")`, with `@dlt.expect_*` |
| Join / enrichment, business-rule Formula, Filter (business), Summarize, Cross Tab | Gold | `@dlt.table` / `@dlt.materialized_view` |
| Output Data (create/overwrite) | Gold table | terminal `@dlt.table` |
| Output Data (update/insert / upsert) | Gold | `dlt.create_streaming_table` + `dlt.apply_changes` (AUTO CDC) |
| Reporting / Render / Email | out of pipeline | downstream Databricks SQL, dashboard, or job task |

Keep the Alteryx DAG topology. Each Alteryx tool becomes a transformation step; a chain of narrow tools (Select → Formula → Filter) can collapse into one table's transform function, but a tool feeding multiple branches should become its own named table so both branches can `dlt.read` it.

## Python API patterns

```python
import dlt
from pyspark.sql import functions as F

# --- Bronze: files landing -> streaming table ---
@dlt.table(name="bronze_orders", comment="Raw orders, Auto Loader ingest")
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{spark.conf.get('pipeline.schema_path')}/orders")
        .load(spark.conf.get("pipeline.orders_landing_path"))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

# --- Bronze: database source -> batch table ---
@dlt.table(name="bronze_customers")
def bronze_customers():
    return spark.read.table(spark.conf.get("pipeline.customers_source"))

# --- Silver: cleanse + expectations (from Alteryx Data Cleanse + Filter) ---
@dlt.table(name="silver_orders")
@dlt.expect_all({"valid_order_id": "order_id IS NOT NULL",
                 "non_negative_amount": "amount >= 0"})
@dlt.expect_all_or_drop({"has_customer": "customer_id IS NOT NULL"})
def silver_orders():
    df = dlt.read("bronze_orders")
    return (
        df.withColumn("order_id", F.trim(F.col("order_id")))
          .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
          .dropDuplicates(["order_id"])          # Alteryx Unique on [order_id]
    )

# --- Gold: join + business rules (from Alteryx Join + Formula + Summarize) ---
@dlt.table(name="gold_customer_revenue")
def gold_customer_revenue():
    o = dlt.read("silver_orders")
    c = dlt.read("silver_customers")
    joined = o.join(c, on="customer_id", how="inner")   # Alteryx Join 'Join' anchor
    return (
        joined
        .withColumn("net_amount",
                    F.when(F.col("status") == "returned", F.lit(0))
                     .otherwise(F.col("amount")))        # Alteryx Formula IF
        .groupBy("customer_id", "region")
        .agg(F.sum("net_amount").alias("total_revenue"),
             F.countDistinct("order_id").alias("order_count"))  # Alteryx Summarize
    )
```

### Reconstructing Alteryx Join's three anchors

```python
matched  = left.join(right, on=keys, how="inner")     # 'Join' anchor
left_only = left.join(right, on=keys, how="left_anti") # 'Left' anchor
right_only = right.join(left, on=keys, how="left_anti")# 'Right' anchor
```
Only build the anchors that are connected downstream. Apply the Join tool's `SelectConfiguration` (field drops/renames) to the `matched` result.

### CDC / upsert (Alteryx Output Data in update/insert mode)

```python
dlt.create_streaming_table("gold_dim_customer")

dlt.apply_changes(
    target="gold_dim_customer",
    source="silver_customers_cdc",
    keys=["customer_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=1,          # or 2 if the workflow keeps history
)
```

### Expectations — where they come from

| Alteryx source | Expectation |
|---|---|
| Filter (custom) that routes bad rows to a dead-end / error output | `@dlt.expect_all_or_drop` |
| Test tool / Message tool with `MessageType=Error` condition | `@dlt.expect_all_or_fail` |
| Error Message interface tool | `@dlt.expect_all_or_fail` |
| Filter that routes bad rows onward but is logically "should not happen" | `@dlt.expect_all` (warn only) |
| `ConvErrorLimit` / conversion-error tolerance | pipeline data-quality threshold + `expect` on cast success |
| Select implicit type change that could null-out values | `expect` that the cast column `IS NOT NULL` where source was not null |

## Project layout (Databricks Asset Bundles)

```
<project>/
  databricks.yml                 # bundle: targets (dev/staging/prod), variables
  resources/
    pipeline.yml                 # Lakeflow pipeline: libraries, catalog, schema, config
    job.yml                      # Lakeflow job wrapping the pipeline + schedule
  src/
    transformations/
      bronze.py                  # one module per layer, or per gold table
      silver.py
      gold.py
    utilities/
      formulas.py                # shared translations of reused Alteryx expressions
      windows.py                 # window-spec builders for Multi-Row / Running Total / Tile
    explorations/                # scratch notebooks, not in the pipeline
  tests/
    test_formulas.py             # unit tests for utilities/formulas.py
    reconcile.py                 # source vs target parity checks
```

- **Pipeline config** (`resources/pipeline.yml` `configuration:` block) holds every value that was an Alteryx workflow constant, analytic-app question, or batch-macro Control Parameter. Reference with `spark.conf.get("pipeline.<name>")`.
- **Bundle variables** (`databricks.yml`) hold per-environment values (catalog, schema, landing paths, source table names).
- **Secrets**: `dbutils.secrets.get(scope, key)` — map each Alteryx DCM connection / alias / inline credential to a `scope/key`. Never inline.

## Batch and iterative macros

- **Batch macro** (Control Parameters + Action tools): becomes a parameterized Python function. If it ran once per row of a driver table, prefer a single set-based transform (join/`groupBy`) over a loop. If it genuinely must run per parameter value (e.g. per file, per API endpoint), loop in Python and `unionByName` the results into one table.
- **Iterative macro**: no declarative equivalent. Options, in order of preference: (1) re-express as set-based logic (many iterative macros are running-total / fill-down / graph-walk patterns that map to window functions or `connect_by`/recursive CTE); (2) bounded Python `for` loop with a max-iterations guard, materializing each pass; (3) move outside the pipeline into a job task. Always flag as `needs-design`.

## Orchestration and non-declarative work

- Wrap the pipeline in a Lakeflow **job**; port the Alteryx Scheduler/Server cadence to the job schedule.
- Alteryx `Events` (pre/post Run Command, email): pre-run → an upstream job task; post-run success email → job notifications; post-run Run Command → a downstream task. Drop events that only made sense in Designer.
- Download / API / Run Command tools inside the workflow: move to an ingestion notebook/task that lands data for the Bronze layer to pick up.

## Streaming vs batch decision

Mark each Bronze table:

- **Streaming** (`spark.readStream` + `cloudFiles`, or a streaming Delta source) when the Alteryx input is a directory of files that accumulates, a log/event source, or was run incrementally.
- **Batch** (`spark.read`) when the Alteryx input is a full snapshot replaced each run, a small reference file, or a `Text Input`.

Downstream: a table is a streaming table if all its inputs are streaming and the transform is append-compatible; otherwise a materialized view. Summarize / Cross Tab / Join-to-latest generally force a materialized view.

## Declarative pipeline API surface

Two decorator modules are in the field. Confirm which one the target workspace standardizes on and use it consistently across the whole pipeline; do not mix.

| Concept | `dlt` module (established) | `pyspark.pipelines` (newer) |
|---|---|---|
| import | `import dlt` | `from pyspark import pipelines as dp` |
| materialized view / batch table | `@dlt.table` | `@dp.materialized_view` / `@dp.table` |
| streaming table | `dlt.create_streaming_table("t")` | `dp.create_streaming_table("t")` |
| non-materialized intermediate | `@dlt.view` | `@dp.temporary_view` |
| append flow into a streaming table | `@dlt.append_flow(target="t")` | `@dp.append_flow(target="t")` |
| AUTO CDC | `dlt.apply_changes(...)` | `dp.create_auto_cdc_flow(...)` |
| AUTO CDC from snapshots | `dlt.apply_changes_from_snapshot(...)` | `dp.create_auto_cdc_from_snapshot_flow(...)` |

Reading other pipeline datasets:

- Prefer `spark.read.table("<name>")` (batch) and `spark.readStream.table("<name>")` (streaming) with the bare dataset name. The legacy `dlt.read` / `dlt.read_stream` and the `LIVE.` prefix still work but are the older form.
- Use a **streaming** read of an upstream table only when that upstream is itself a streaming table and this transform is append-only.
- A step that exists only to be consumed once by the next step should be a **view** (`@dlt.view`), not a table — it is not materialized, so it costs no storage and no extra write. Collapse narrow linear chains into one function; make a table only at a branch point or where a materialized result is needed.

## Full-snapshot sources — `apply_changes_from_snapshot`

Many source workflows read a **full snapshot** each run (a nightly extract, a replaced file) with no CDC feed. To land that as a dimension without reprocessing everything and without losing deletes, use snapshot CDC instead of a plain overwrite:

```python
dlt.create_streaming_table("gold_dim_customer")

dlt.apply_changes_from_snapshot(
    target="gold_dim_customer",
    source="bronze_customer_snapshot",        # the latest full snapshot
    keys=["customer_id"],
    stored_as_scd_type=1,                      # 2 to keep history with __START_AT / __END_AT
    track_history_column_list=None,            # or a subset of columns to version on
)
```

Rows absent from the new snapshot are treated as deletes (SCD1) or closed out (SCD2). Use this wherever the source "Output" tool did a full replace of a keyed table and downstream consumers need current-state semantics or soft deletes.

`apply_changes` / `create_auto_cdc_flow` full options worth setting from the source logic:

- `sequence_by` — the ordering column for late/out-of-order rows (an `updated_at`, a load id). Required; pick the column the source relied on for "last write wins".
- `apply_as_deletes` / `apply_as_truncates` — predicates that mark a CDC row as a delete / full truncate (maps to a source "delete flag" column).
- `except_column_list` / `column_list` — restrict which columns land in the target.
- `stored_as_scd_type=2` adds `__START_AT` / `__END_AT`; downstream Gold reads the current row with `WHERE __END_AT IS NULL` (express as `.filter(F.col("__END_AT").isNull())`).

## Multiple inputs into one table — Union of N sources

A source Union (or several inputs feeding one output) that is **append-only** becomes one streaming table fed by several `@dlt.append_flow` functions — each flow reads a different source, they all write to the same target, and the target stays a streaming table:

```python
dlt.create_streaming_table("bronze_events")

for src in ["events_us", "events_eu", "events_apac"]:
    @dlt.append_flow(target="bronze_events", name=f"flow_{src}")
    def _flow(src=src):
        return (spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "json")
                .load(f"{spark.conf.get('pipeline.landing')}/{src}")
                .withColumn("_source", F.lit(src)))
```

If the union feeds a `groupBy` / pivot / join-to-latest, it must be a materialized view instead: `@dlt.table` returning `reduce(DataFrame.unionByName, [spark.read.table(s) for s in sources])` with `allowMissingColumns=True` when the source Union mode was "by name, warn on missing".

## Auto Loader options to carry from the source

When a Bronze table replaces a file-reading input tool, set these deliberately rather than taking defaults:

- `cloudFiles.format` and format options (delimiter, header, multiline, encoding) — copy from the source Input tool's file config.
- `cloudFiles.schemaLocation` — a per-table path under a pipeline schema dir.
- `cloudFiles.schemaEvolutionMode` — `addNewColumns` (default, fails then adds) or `rescue` (never fails; unexpected data goes to `_rescued_data`). Prefer `rescue` for production ingest and add an expectation on `_rescued_data IS NULL`.
- `cloudFiles.schemaHints` — pin the types the source declared, so numeric/date columns are not inferred as string.
- `cloudFiles.inferColumnTypes=true` only if there are no hints and the source relied on type inference (Alteryx Auto Field).
- `rescuedDataColumn` name — keep it explicit so expectations can reference it.
- File detection: directory listing (default) is fine for modest volumes; `cloudFiles.useNotifications=true` (file-event queue) for high file counts — note the cloud resources it needs.
