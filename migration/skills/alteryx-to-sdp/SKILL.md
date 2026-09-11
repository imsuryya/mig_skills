---
name: alteryx-to-sdp
description: Exhaustively parse a local Alteryx workflow, macro, or analytic app and produce an end-to-end, production-grade, performance-optimized plan to rebuild it as a Databricks Lakeflow Spark Declarative Pipeline (SDP / DLT) in PySpark. Use for `.yxmd`, `.yxmc`, and `.yxwz` work that involves reading, documenting, or explaining an existing workflow tool by tool; capturing every filter condition, formula expression, join key, summarize action, container, and macro interface; tracing field-level lineage; or converting Alteryx data-prep logic to Databricks / Delta Live Tables / DLT / Lakeflow declarative pipelines, medallion architecture, or PySpark. Parses raw workflow XML directly and never skips a tool. All target code is the PySpark DataFrame API, not SQL strings.
---

# Alteryx to Databricks SDP Migration

Read an existing local Alteryx artifact and design its replacement as a Databricks Lakeflow Spark Declarative Pipeline (SDP / DLT) written in **PySpark**.

Three halves, all required:

1. **Exhaustive parse.** Account for every tool, every internal expression and condition, every connection, every container, and every referenced macro. Missing one makes the plan wrong.
2. **Production migration plan.** Turn the parsed model into an executable plan: medallion architecture, tool-by-tool transformation map, translated expressions, data-quality expectations, parameterization, orchestration, project skeleton, phased rollout, risk register.
3. **Optimization design.** The pipeline must be fast and cheap: ingest-once, partitioning/clustering, broadcast strategy, explicit pivot value lists, window partition discipline, streaming-vs-batch per table, compute sizing.

This skill **reads** Alteryx artifacts; it does not build, mutate, or run them. Running the source workflow is optional, only to capture reference outputs for parity testing (see `references/source-parity-harness.md`).

**Use `../alteryx-sdp-migrate` instead** when the job is to *execute* the migration rather than plan it — generating the pipeline code unit by unit, across a workflow too large to hold in one context (hundreds to 2,000+ tools), with persistent state, a gap register, and validation gating completion. That skill retrieves from this one's references rather than duplicating them; the parse semantics and translation tables below remain the source of truth for both.

## References

Skill-local (Alteryx side):

- `references/workflow-xml.md` — the `.yxmd` / `.yxmc` / `.yxwz` XML model.
- `references/tool-parse-reference.md` — what `Configuration` to extract per tool.
- `references/designer-tool-reference.yaml` — plugin names, palettes, anchor names.
- `references/formula-syntax.md` — Alteryx formula functions and null semantics.
- `references/alteryx-formula-to-spark.md` — Alteryx expression → PySpark translation table.
- `references/source-parity-harness.md` — optional golden-output capture with a local Alteryx Engine.

Shared (Databricks / SDP target — reused by every `*-to-sdp` skill):

- `../../references/databricks-sdp-target.md` — DLT/Lakeflow PySpark API patterns, medallion mapping, expectations syntax, CDC, DAB structure.
- `../../references/sdp-production.md` — production-hardening checklist: PySpark-not-SQL rule, determinism, expectations catalog, parameterization/secrets, CDC/SCD, streaming-vs-batch, orchestration, governance/PII, testing, project skeleton, deliverable checklist, and (§12–17) Databricks Asset Bundles deployment, pipeline settings, job orchestration, Unity Catalog governance, CI/CD, and observability.
- `../../references/sdp-optimization.md` — performance: ingest-once, joins/broadcast, aggregation/pivots, window discipline, storage layout, compute, anti-patterns, plus streaming/Auto Loader trigger sizing and state bounds, incremental-vs-full materialized-view cost, and Delta table features/maintenance (deletion vectors, predictive optimization, data skipping).

## Scope Intake

Before parsing, establish and record (state anything the user has not provided as an open question in the plan, never guess):

- Absolute path(s) to the primary `.yxmd` / `.yxmc` / `.yxwz` and every macro it references.
- Artifact type of each file: standard workflow, standard macro, batch macro, iterative macro, analytic app.
- Input sources (files, databases, APIs) and output targets, by tool.
- Run context: manual, Alteryx Scheduler / Server, chained apps, pre/post `Events`.
- Data volume and cadence, and whether each source is append-only / files landing (streaming candidate) or full snapshots (batch candidate).
- Target Databricks context: Unity Catalog catalog and schema, workspace, whether Databricks Asset Bundles (DAB) are in use, orchestration tool, secret scopes.
- Whether golden-output parity testing is in scope and whether a licensed Alteryx Engine is available.

## Phase 1 — Exhaustive Parse (Raw XML Only)

Parse the workflow XML directly as the source of truth — never a condensed view. Read `references/workflow-xml.md` and `references/tool-parse-reference.md`; use `references/designer-tool-reference.yaml` to resolve names.

For a large workflow, write a small parser script (Python `xml.etree`) that emits a structured inventory (nodes, configs, connections, topological order) rather than hand-transcribing hundreds of tools. Keep the script; cite it in the plan appendix.

Produce a **Workflow Inventory** covering:

- **Every node.** All `Node` elements by `ToolID`, including nested `ChildNodes`. Record `ToolID`, `GuiSettings Plugin`, `EngineSettings`, resolved tool name and palette, container membership and nesting, canvas position, enabled/disabled state (tool and enclosing container). A disabled tool still belongs in the inventory, flagged.
- **Every configuration.** Full `Configuration` intent per tool, cited from the XML — never invented. See `references/tool-parse-reference.md` for the per-tool key list (Select field lists with renames/type/size, Filter simple vs custom `Expression`, every Formula `FormulaField` name+expression+type+size, Multi-Row Formula group-by/num-rows/null-handling/expression, Summarize every `SummarizeField` field+action+rename, Join join-on fields and per-anchor `SelectConfiguration`, Union mode + `OutputOrder` + field mapping, Sort fields+direction, Sample mode/N/grouping, Unique key fields, Cross Tab / Transpose key/name/value/method, RegEx pattern+method+case, DateTime formats, Text To Columns delimiters/mode, Generate Rows init/condition/loop, Running Total / Tile / Rank config, Data Cleanse options, Input/Output Data file-or-connection + query + format options, Dynamic Rename / Dynamic Select expressions, interface / Action `Destination` XML paths).
- **Every expression, verbatim.** Filter conditions, all formula expressions, join keys, generate-rows expressions, message/error conditions, interface Action expressions, macro Condition tools. Keep original Alteryx syntax. Read `references/formula-syntax.md` for unfamiliar functions.
- **Every connection.** Each edge as origin `ToolID`+anchor, destination `ToolID`+anchor, connection `name`, `Wireless` flag. Reconstruct the full DAG and a valid topological order. Note branches, merges, multi-output-anchor tools (Join L/J/R, Filter T/F, Unique U/D), dead-end anchors.
- **Every macro.** Resolve each `.yxmc` path (relative to the workflow, then macro search paths) and parse recursively with this checklist. Capture the interface: `MacroInput` fields/anchors, `MacroOutput`, `Questions` / `Constants`, `Control Parameter` tools, every `Action` tool that rewrites inner XML. Classify: standard / batch (Control Parameters) / iterative (iteration count / convergence). A bundled Designer sample macro (e.g. `Cleanse.yxmc`) has no local file — document its known behavior and cite that.
- **Analytic app interface.** For `.yxwz`: `RuntimeProperties` `Questions` / `Actions`, root `Constants`, wizard fields, `Wiz_*`, chained-app `Events`.
- **Engine and runtime.** Effective AMP/E2 vs legacy E1 (`RunE2`, `RunWithE2`), `GlobalRecordLimit`, `ConvErrorLimit` + stop-on-error, runtime constants, `Events` (pre/post-run commands, email, run-command).
- **Determinism hazards.** `DateTimeNow` / `DateTimeToday`, `RAND` / `RandInt`, `Directory` + globs, `GetEnvironmentVariable`, `ReadRegistryString`, absolute local paths, hard-coded credentials / connection strings, sort-order-dependent logic (Sample first-N, Unique first-match, Multi-Row Formula, Running-Total==1 top-1 patterns), implicit type coercions.

End Phase 1 with a **coverage check**: nodes found vs documented, connections found vs documented, every macro file parsed. Counts must match before continuing.

## Phase 2 — Semantic Model and Lineage

- **Field-level lineage.** Per tool, input schema(s) → output schema(s): fields added, renamed, retyped, resized, dropped, reordered. Trace each output field back to its source column(s) and the expressions applied.
- **Logical stages.** Group the DAG into ingestion, cleansing/standardization, join/enrichment, business rules, aggregation, output.
- **Repeated-block detection.** Find copy-pasted container blocks that run once per dimension value (e.g. per region grain). These collapse to **one parameterized transform** over a grain-explosion table — the single biggest optimization lever. Record the instance→parameter mapping.
- **Semantic differences** (document, do not silently "fix"): Alteryx null vs Spark null (`"ABC" + Null()` → `"ABC"` in Alteryx, null in Spark; `Null()==Null()` → True in Alteryx), implicit string/number casts, `Sort` stability, `Sample` / `Unique` / dedupe tie-break ordering, Join emitting three anchors, `Append Fields` cartesian, `Find Replace` semantics, `CompareEpsilon`, timezone/locale in DateTime parsing, `Select` silent type changes, `Summarize` StdDev = population, `Median` exact vs approx, `Round(x, mult)` = round-to-multiple, `Contains` / `EndsWith` case-insensitive by default. Read the truth table in `references/formula-syntax.md` and the deltas in `references/alteryx-formula-to-spark.md`.
- **Tool-class map:** Multi-Row Formula / Running Total / Tile / Rank → Spark window functions; Cross Tab → `groupBy().pivot(col, [explicit values])`; Transpose → `unpivot` / `stack`; Generate Rows → `sequence` + `explode`; Summarize → `groupBy().agg()` (StdDev → `stddev_pop`, Median → `percentile_approx`); Join → `DataFrame.join` (reconstruct only the anchors used); Union → `unionByName(allowMissingColumns=True)`; Fuzzy Match / Make Group → no native equivalent (flag); Spatial → Apache Sedona (flag, drive-time distance has no equivalent); Reporting / Render / Email → out of pipeline (downstream job / BI); Predictive / ML → MLflow / Databricks ML or a Python model job (separate track).

## Phase 3 — Target Databricks SDP Architecture (PySpark)

Read `../../references/databricks-sdp-target.md`, then `../../references/sdp-production.md` and `../../references/sdp-optimization.md` in full and apply every item.

**3a. Medallion design.**

- **Bronze.** One `@dlt.table` per *distinct* source query (de-duplicate identical repeated reads). `spark.read.table(...)` + DataFrame `.filter`/`.join` (federation pushes down), or streaming table + Auto Loader for files landing. Preserve source columns + ingest metadata. Mark each table streaming or batch (`sdp-optimization.md` §1, `sdp-production.md` §6).
- **Silver.** Cleansing, standardization, casts, dedupe, enrichment/classification. One table per conformed entity + one **grain-explosion** table that replaces copy-pasted per-dimension blocks. `@dlt.expect_*` from Alteryx filters and clamps.
- **Gold.** Business rules and aggregation — one table per metric block as a parameterized function over the grain table; then a serving table = the wide join of the metric tables + the final Alteryx output schema (exact column order).
- **CDC / SCD.** Alteryx Output in update/insert mode, or a join-driven upsert → `dlt.apply_changes` (`sdp-production.md` §5).
- **Non-declarative work.** API/download/run-command tools, R/Python model scoring (ETS/ARIMA/predictive), reporting, spatial drive-time → job tasks around the pipeline, named in the plan.

**3b. Production hardening.** Apply `../../references/sdp-production.md`: PySpark-not-SQL (only `@dlt.expect` predicates and spatial callables may be strings); determinism fixes for every hazard from Phase 1 (explicit `Window` orders + tiebreaks, `run_date` parameter replacing `DateTimeToday()`); expectations catalog; parameterization + secret-scope references; streaming-vs-batch table; CDC/SCD (incl. `apply_changes_from_snapshot` for full-snapshot sources); orchestration; governance/PII; the DAB project skeleton and target/mode/run-as layout (§12); pipeline settings (§13); the job graph (§14); Unity Catalog objects and grants (§15); CI lint + deploy gates (§16); event-log queries and DQ alerts (§17).

**3c. Optimization.** Apply `../../references/sdp-optimization.md`: ingest-once and fan-out from Delta; broadcast every dim and small metric table; single `.agg()` per key set; explicit pivot value lists; `stddev_pop` / `percentile_approx` / round-to-multiple to match Alteryx; partition by low-cardinality read/filter keys, cluster by ids; Photon; serverless-vs-job-cluster; AQE; streaming trigger caps + watermark/state bounds for every stateful stream (§8); per-Gold-MV incremental-vs-full-recompute call (§9); Delta features/maintenance — deletion vectors on CDC targets, Change Data Feed on Silver feeding Gold MVs, predictive optimization vs a maintenance job (§10); the anti-pattern checklist.

## Phase 4 — Tool-by-Tool Transformation Map

One row for **every** source `ToolID`:

| ToolID | Alteryx tool | Container / caption | Verbatim config / expression | Target layer | Target construct (PySpark) | Expectations added | Confidence |

- **Target construct** is concrete PySpark DataFrame-API code or a precise pattern (`df.groupBy(*GK).agg(F.stddev_pop("x"))`, `F.row_number().over(Window.partitionBy(k).orderBy(o))==1`), never a vague phrase, never a SQL string.
- **Target layer** is Bronze / Silver / Silver-grain / Gold-metric / Gold-serve / Job / Drop.
- **Confidence**: `direct` (1:1), `adapted` (works with a documented semantic change), `needs-design` (needs a team decision — every order-dependent tool), `no-equivalent` (no native path — propose an approach or scope out).
- Translate every expression with `references/alteryx-formula-to-spark.md`. Note 1-based index shifts (`Left`, `Substring`), null-concatenation, date format-string conversion, regex dialect, case-insensitive `Contains`.
- Disabled tools and out-of-scope tools (reporting, spatial-without-a-library, predictive) still appear, with their disposition.

For a large workflow, generate this table programmatically from the Phase-1 parser output into a CSV companion file.

## Phase 5 — Data Quality, Parity, and Testing

- **Expectations catalog.** Consolidate every expectation with the Alteryx tool it came from (`sdp-production.md` §3).
- **Golden-output parity.** With an Alteryx Engine: run the source with `scripts/Invoke-AlteryxWorkflow.ps1` (pin any run-date), capture reference outputs, reconcile row counts + column hashes/aggregates vs the migrated gold tables (`references/source-parity-harness.md`). Without an Engine: reconcile against the last known-good output. Tolerances: integer/count exact, sums/means exact, approx median ±1, percentage-change ±0.01, spatial assignment ≥ 99.9%.
- **Unit tests** for every shared expression helper; **window tests** (grain break, null grain, tiebreak determinism, empty partition); **schema-contract tests** (each `unionByName` input set, the serving table column order); a reconciliation notebook.
- **Edge cases:** nulls, type coercions, ordering/tie-breaks, empty inputs, duplicate keys, timezone/locale, floating-point tolerance.

## Phase 6 — Deliverable

Write the plan as Markdown next to the source workflow (or where the user asks). Default to **two documents plus two companions**:

- **`summary-plan.md`** — source summary (every input, its filter, what it feeds), the transformation narrative stage by stage, and the shape/schema of the final output data.
- **`sdp-plan.md`** — the layer-by-layer Databricks SDP build in PySpark: for each layer, all sources in and all transformations out, with runnable DataFrame-API code, expectations, parameters, the semantic-delta handling, the optimization design, risks, DAB skeleton, and build order.
- **`workflow-inventory.md`** — Phase-1 exhaustive parse, one entry per ToolID, verbatim config, grouped by container in topological order.
- **`transformation-map.csv`** — Phase-4 map, one row per ToolID.

A single combined `migration-plan.md` is acceptable if the user prefers it; it then contains, in order: executive summary + scope + open questions; the exhaustive inventory + coverage check; semantic model + lineage + semantic-delta list; target architecture + DAB skeleton + parameters/secrets; tool-by-tool map; DQ expectations + parity/test plan; optimization design; deployment & operations (DAB targets, pipeline settings, job graph, UC objects/grants, CI/CD gates, observability queries/alerts); phased rollout; risk register; effort estimate.

If the user asks for the pipeline code itself, generate it against the plan as a follow-up; the plan is the primary deliverable.

## Shared Rules

- Parse from raw XML only. Cite configuration from the file; never assume it.
- Recurse into every referenced macro. An unparsed macro is an incomplete plan.
- **Target code is the PySpark DataFrame API.** No `spark.sql("…")`, no `.selectExpr`, no `F.expr` for logic with a typed API. The only allowed strings are `@dlt.expect_*` predicates and spatial-library Python callables' arguments.
- **No `F.current_date()` / `F.current_timestamp()` in transformations.** Every wall-clock reference becomes a `pipeline.run_date` parameter.
- Never expose or reproduce credentials, connection strings, DCM payloads, or tokens. Represent them as named secret-scope references.
- Preserve Alteryx semantics in the plan. Where Spark differs, state the Alteryx behavior, the Spark behavior, and the chosen resolution.
- Prefer native Spark / DLT. Flag anything needing an extra library (Sedona) or with no equivalent (Fuzzy Match, reporting, drive-time distance, some predictive tools).
- Inspect only the metadata and sampled data needed to understand the logic. Do not load full datasets into context.
- Do not dump full workflow XML, full generated code, or large data samples into the response. Summarize and point to the saved files.

## Completion and Response

Verify before reporting done:

- Every `ToolID` in every parsed file (nested and disabled included) appears in the inventory and the transformation map, or is listed out of scope with a reason.
- Every expression and condition is translated or explicitly flagged.
- The DAG and execution order are preserved in the target design.
- Every referenced macro was parsed.
- Determinism hazards, parameterization, data-quality expectations, the optimization design, and the parity approach are all defined.
- The deliverable files are saved at stated paths.

Summarize: files parsed (including macros), tool count and coverage-check result, the target architecture shape, counts of `direct` / `adapted` / `needs-design` / `no-equivalent`, where the plan was saved, and the top risks and open questions.
