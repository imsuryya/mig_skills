# SDP Optimization Reference

Shared by every `*-to-sdp` skill. Apply during Phase 3 (target architecture) and Phase 7 of the deliverable (optimization design). Goal: the migrated Databricks Lakeflow pipeline is not just correct but **fast and cheap** — no accidental full shuffles, no re-reading a source per branch, no skew.

Companion: `sdp-production.md` (hardening), `databricks-sdp-target.md` (API patterns).

---

## 1. Ingest once, fan out from Delta

Source ETL tools often re-read the same input, or copy-paste a block per dimension. In the target:

- **Bronze is the only layer that touches the external system.** One `@dlt.table` per *distinct* source query. De-duplicate repeated identical reads (e.g. a lookup pulled once per region grain → one Bronze table).
- A source tool that feeds **multiple downstream branches** becomes its **own named table** so each branch does `dlt.read(...)` instead of recomputing.
- A copy-pasted block that runs N times over a dimension becomes **one parameterized transform** over a single materialized table that carries the dimension as a column (grain-explosion pattern), computed once.
- A linear chain of narrow tools (Select → Formula → Filter) collapses into **one** transform function — don't create a table per tool.

---

## 2. Joins

- **Broadcast every small side** explicitly: `df.join(F.broadcast(dim), keys, "left")` for lookup/reference/dimension tables and for small pre-aggregated metric tables in a wide assembly. Set `spark.sql.autoBroadcastJoinThreshold` deliberately; don't rely on the default guess.
- Reconstruct a source "join with 3 anchors" (matched / left-only / right-only) by building **only the anchors that are actually consumed downstream** — `inner` for matched, `left_anti` for the -only anchors. Check the DAG before writing all three.
- Cartesian / "append fields" with a 1-row source: `assert src.count() == 1` then `df.crossJoin(F.broadcast(src))`. Never a blind `crossJoin`.
- Chained multi-input joins: join on the **full shared key list** in one go; avoid intermediate re-partitioning by keeping the same key order.
- Watch for **join-key skew** (a dominant `KommuneNr`, a null-heavy key): enable AQE skew join handling; salt only if AQE isn't enough.

---

## 3. Aggregation & pivots

- `groupBy(*keys).agg(...)` — put all aggregates for one key set in a **single** `.agg()` call, not chained joins of separate group-bys.
- **Pivots must pass the explicit value list**: `df.groupBy(*g).pivot("col", ["a","b","c"]).agg(...)`. Without it Spark runs an extra distinct scan and the output column order is non-deterministic. The value list comes straight from the source tool's category definition (age bands, income bands, size intervals, gender codes, …).
- Source "cross tab, row-percentage" method: pivot to counts, then divide each cell by the row total (`reduce(add, [F.col(v) for v in values])`). One helper, reused.
- **Statistic definitions differ** — pick the one that matches the source:
  - population std dev → `F.stddev_pop` (Alteryx Summarize uses population; `F.stddev` is sample).
  - exact median → `F.percentile_approx(c, 0.5, 1_000_000)` (accuracy 1e6: one pass, deterministic, ≤1-unit error on large groups). Document the tolerance.
  - "round to a multiple of m" → `F.round(c / m) * m`, not `F.round(c, places)`.
- `count` vs `count(col)` vs `countDistinct` — match the source exactly; `count(col)` skips nulls.

---

## 4. Window functions

- Partition YoY / rank / running-total windows by the **full grain** so the work stays distributed. A window with no `partitionBy` (or a low-cardinality one) collapses every row to one partition — only acceptable for genuinely global running totals, and flag it.
- `orderBy` on a numeric-looking string column: cast first (`F.col("year").cast("int")`), otherwise lexicographic order silently breaks the sequence.
- Reuse one `Window` spec per (partition, order) rather than rebuilding it per column.
- `rowsBetween(Window.unboundedPreceding, 0)` for cumulative; `rowsBetween(-k, -1)` / `lag(col, k)` for look-back. Be explicit; the default frame changes with/without `orderBy`.

---

## 5. Storage layout

- **Partition** by a low-cardinality, high-selectivity column that downstream reads and the serving layer filter on (a region-type with ~5 values, a year). Do **not** partition by a high-cardinality key (municipality, id) — that creates the small-files problem.
- **Liquid clustering** (`CLUSTER BY`) is preferred over Hive partitioning + Z-order for new tables when the workspace supports it; otherwise `OPTIMIZE … ZORDER BY (<the columns downstream filter/join on>)` on a maintenance task.
- Table properties: `delta.autoOptimize.optimizeWrite=true`, `delta.autoOptimize.autoCompact=true` (or `delta.tuneFileSizesForRewrites=true` for large rewrite-heavy tables).
- Target file size ~128–256 MB; let `optimizeWrite` + `OPTIMIZE` handle it, don't hand-tune `maxRecordsPerFile` unless there is a proven problem.
- `VACUUM` on a schedule (retention per governance).

---

## 6. Compute

- **Photon on** for all SQL/DataFrame work.
- **Serverless pipeline** when the workload allows and any required cluster libraries (e.g. Sedona) are supported there; otherwise a right-sized job cluster with autoscaling `min..max`.
- Enable **AQE** (`spark.sql.adaptive.enabled=true`) — on by default on DBR, but state it: dynamic partition coalescing, skew join handling, local shuffle reader.
- `spark.sql.shuffle.partitions` = `auto` (AQE) rather than a fixed number.
- Cluster libs (Sedona `apache-sedona` + `geotools-wrapper`, or others) declared in `resources/pipeline.yml`, registered once in a pipeline init.

---

## 7. Anti-patterns to catch in the plan

| Anti-pattern | Fix |
|---|---|
| `.collect()` / `.toPandas()` on a non-tiny frame inside a transform | keep it in Spark; only `collect()` a single scalar (e.g. `max(year)`) and even then prefer a broadcast join |
| `df.count()` used for control flow on every table | use expectations / DQ metrics instead; reserve `count()` for the one Append-Fields guard |
| re-reading a Bronze/Silver table 5+ times in one function | read once into a local `df`, reuse |
| `withColumn` in a Python loop over hundreds of columns | build one `select(*exprs)` / `df.selectExpr`-free list comprehension of `F.col`/`F.when` |
| pivot without value list | pass the explicit list (§3) |
| `orderBy` without a following window/limit/write | drop it — it's a global sort with no effect on set-based output |
| partition by id / high-cardinality key | partition by the low-cardinality read/filter key; cluster by the id |
| one `@dlt.table` per source tool | collapse narrow linear chains; one table per branch point |
| UDF where a built-in exists | use `pyspark.sql.functions`; a Python UDF kills Photon and adds serialization cost |

---

## 8. Streaming & Auto Loader performance

- **Trigger sizing**: cap per-microbatch work with `cloudFiles.maxFilesPerTrigger` (default 1000) or `cloudFiles.maxBytesPerTrigger`. A source that dropped one big daily file needs a higher byte cap; a source with millions of tiny files needs a file cap plus notification-mode discovery.
- **File discovery**: directory listing rescans the path each trigger — fine up to ~tens of thousands of files. Past that use `cloudFiles.useNotifications=true` (cloud file-event queue); the plan names the queue/topic resource it requires.
- **Checkpoint & state**: each streaming table owns a checkpoint under the pipeline storage. Stateful ops (`dropDuplicates`, stream-stream join, windowed agg) grow the state store — always bound them with a watermark (`withWatermark("event_ts", "<lateness>")`) and use `dropDuplicatesWithinWatermark` for dedupe. An unbounded stateful stream is a cost leak; flag it.
- **Stream-stream joins** need a watermark on **both** sides and a time constraint in the join condition, or state grows forever. Most migrated "join to a slowly-changing lookup" cases are better as a stream-static join (static side re-read each microbatch) or `apply_changes` + a batch join in Gold.
- Don't set `spark.sql.shuffle.partitions` for streaming; let AQE/serverless manage it. Do set a sane `spark.sql.streaming.noDataMicroBatches.enabled` expectation for idle periods.
- Backfill of history: run once with a high `maxBytesPerTrigger` (or a one-off batch load into Bronze), then let the steady-state trigger take over.

## 9. Incremental materialized views (recomputation cost)

- A materialized view refreshes **incrementally** when the engine can (append-only upstream deltas, supported operations: projections, filters, many `groupBy` aggregations, some joins). Otherwise it does a **full recompute** every pipeline run — the dominant cost in most migrated pipelines.
- Keep the incremental path: read upstream as a delta (streaming table or a table with Change Data Feed), avoid non-deterministic expressions in the MV (see `sdp-production.md` §2), and avoid operations that force full recompute (e.g. some window functions, `distinct` on the whole row, self-joins). The plan states, per Gold MV, whether it is expected to refresh incrementally or fully, and why.
- Enable `delta.enableChangeDataFeed=true` on Silver tables that feed Gold MVs so downstream incremental refresh has a change source.
- A wide "assembly" Gold table joined from many small metric tables recomputes fully if any input changes — acceptable when inputs are small and broadcast; flag it if any input is large.
- For genuinely append-only Gold, use a **streaming table** (`@dlt.append_flow`), not an MV — it only ever processes new rows.

## 10. Delta table features & maintenance

- **Deletion vectors** (`delta.enableDeletionVectors=true`): merges/updates/deletes mark rows instead of rewriting files — large win for `apply_changes` targets and any table with frequent upserts. On by default on recent DBR; state it explicitly for CDC targets.
- **Predictive optimization** (managed `OPTIMIZE` / `VACUUM` / stats) at the catalog or schema level removes the need for a hand-rolled maintenance job — prefer it when the workspace has it; otherwise schedule the maintenance task (`sdp-production.md` §7).
- **Row tracking** (`delta.enableRowTracking=true`) for stable row ids across `OPTIMIZE` — useful when a downstream consumer or parity harness keys on row identity.
- **Data skipping**: stats are collected on the first N columns (`delta.dataSkippingNumIndexedCols`, default 32). Put the columns downstream filters/joins on **early** in the schema, or lower the count and rely on clustering. Long free-text columns should come after the indexed cut-off.
- `OPTIMIZE FULL` after a clustering-key change; `REORG TABLE … APPLY (PURGE)` after enabling deletion vectors on an existing table.
- Serverless pipelines apply many of these automatically; classic job clusters need them declared in table properties or the maintenance job.

## 11. What the plan must state (Phase 7)

- Per Bronze table: streaming or batch, and why.
- Per table: partition columns / cluster keys, and the downstream read pattern that justifies them.
- The broadcast list (every dim + small metric table).
- Every window's partition key and the cardinality argument that it stays distributed.
- Every pivot's explicit value list.
- Which statistic definitions were changed to match the source (`stddev_pop`, `percentile_approx`, round-to-multiple) and the resulting parity tolerance.
- Compute: serverless vs job cluster, Photon, autoscale range, required cluster libraries.
- The maintenance job (`OPTIMIZE` / `VACUUM`) cadence, or that predictive optimization covers it.
- Per streaming Bronze table: trigger cap (`maxFilesPerTrigger` / `maxBytesPerTrigger`) and file-discovery mode.
- Every stateful streaming op (dedupe, stream-stream join, windowed agg): its watermark and lateness bound.
- Per Gold materialized view: expected to refresh incrementally or fully recompute, and the reason.
- Deletion vectors on every CDC / upsert target; Change Data Feed on every Silver table feeding a Gold MV.
