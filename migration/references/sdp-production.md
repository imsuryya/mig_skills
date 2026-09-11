# SDP Production-Hardening Reference

Shared by every `*-to-sdp` skill. Apply during Phase 3 (target architecture) and Phase 6 (deliverable). The target is a **Python / PySpark** Databricks Lakeflow Spark Declarative Pipeline (SDP / DLT). Every item here is something a migration plan MUST address explicitly, not leave implied.

Companion: `databricks-sdp-target.md` (API patterns, medallion mapping), `sdp-optimization.md` (performance).

---

## 1. PySpark, not SQL strings

- All transformation logic is the **DataFrame API** (`df.filter`, `df.join`, `df.groupBy().agg()`, `F.when`, `Window`). No `spark.sql("…")`, no `.selectExpr`, no `F.expr` for logic that has a typed API.
- The only unavoidable SQL strings:
  - `@dlt.expect_*({"name": "<boolean predicate>"})` — DLT expectations take a string predicate; that is the only supported form. Keep them short; they are constraints, not transformations.
  - Spatial predicates use the library's Python callables (e.g. Sedona `ST_Intersects(a, b)`), never `F.expr("ST_...")`.
- Source extraction: read Unity Catalog tables with `spark.read.table(...)` and let Spark push `.filter`/`.join` down (Lakehouse Federation). Do not hand-write the extract SQL unless a construct cannot be expressed in the DataFrame API.
- CI lint (ruff/flake8 custom rule) fails the build on `spark.sql(`, `.selectExpr(`, `F.expr(` inside `transformations/`, and on `F.current_date()` / `F.current_timestamp()` (see §4).

---

## 2. Determinism

Source ETL tools (Alteryx, Informatica, SSIS, …) frequently depend on **row order** and **wall-clock time**. Spark preserves neither. Every migration plan lists each occurrence and its fix.

| Hazard | Where it appears | Required fix |
|---|---|---|
| Order-dependent dedupe / "first match" / "top-1 per group" | Unique, Sort→Sample first-N, Rank, running-total==1 patterns | `row_number().over(Window.partitionBy(keys).orderBy(<explicit TOTAL order incl. a unique tiebreak>)) == 1`. The tiebreak column is chosen and **documented per site** (default: all remaining columns ascending). Never `dropDuplicates` where the source kept a specific row. |
| Order-dependent running aggregates | Running Total, Multi-Row/lag/lead, cumulative sums | `F.sum/lag/lead(...).over(Window.partitionBy(group).orderBy(<explicit order>).rowsBetween(...))`. Assert the order key is unique within the partition; if not, add a tiebreak. |
| Sort feeding the above | standalone Sort tools | fold the Sort into the downstream `Window.orderBy`; a standalone Sort before an output becomes `df.orderBy(...)`. |
| Wall-clock time | `now()`, `today()`, `sysdate`, current-date filters, "last N years", "this month" | replace with a single pipeline parameter `pipeline.run_date` (+ derived `run_year`, `run_month`). No `current_date()`/`current_timestamp()` in transforms. Parity runs pin the parameter. |
| Non-seeded randomness | `rand()`, `randint()`, sampling | `F.rand(seed)` with a fixed seed from config; document that exact-match parity is impossible without it. |
| UUID / monotonic id | surrogate-key generation | `F.expr("uuid()")` is non-deterministic — use a hash of natural keys (`F.sha2(F.concat_ws('|', *keys), 256)`) or `apply_changes` with real keys. |

---

## 3. Data-quality expectations catalog

Translate every implicit source assumption into a declared expectation. Build a table in the plan: `expectation name | type | predicate | source tool it came from`.

| Source pattern | DLT expectation |
|---|---|
| Filter that routes bad rows to a dead end / error output | `@dlt.expect_all_or_drop` |
| Test/assert tool, "Error Message" with a hard condition | `@dlt.expect_all_or_fail` |
| Filter that keeps bad rows but is logically "should not happen" | `@dlt.expect_all` (warn only) |
| Value clamp (`if x < lo or x > hi then null`) | keep the clamp in the transform **and** add `@dlt.expect_all` that the clamped column is null-or-in-range |
| Implicit `inner join` that silently dropped null keys | `@dlt.expect_all` that each join key `IS NOT NULL` |
| Conversion-error tolerance (`ConvErrorLimit`, "abort after N bad rows") | a pipeline DQ counter on cast-null rate + `@dlt.expect` on cast-success columns; alert when the null count exceeds the source's tolerance |
| Uniqueness the source relied on (one row per key before a window) | a separate DQ check (`groupBy(keys).count().filter("count > 1")` must be empty) — DLT has no native unique constraint |

Expectations are **cheap and always-on**; prefer many small named ones over a few big ones so failures are legible in the pipeline event log and DQ dashboard.

---

## 4. Parameterization & secrets

- Every source constant, prompt/question value, batch-macro control parameter, and hard-coded literal (dates, thresholds, paths, file globs) becomes **pipeline configuration** (`resources/pipeline.yml` `configuration:` → `spark.conf.get("pipeline.<name>")`), never an inline literal in code.
- Per-environment values (catalog, schema, source scope, landing paths, workbook/output dir) are **bundle variables** in `databricks.yml`.
- Credentials / connection strings / DCM payloads / tokens: `dbutils.secrets.get(scope, key)`. Never inline, never in config, never echoed to logs. The plan lists each source connection → a `scope/key` reference.
- The run-date parameter (§2) is mandatory config for any pipeline with time-relative logic.

---

## 5. CDC / upsert / SCD

- Source "Output" in *update/insert* mode, or a join that drives an upsert → `dlt.create_streaming_table` + `dlt.apply_changes` (AUTO CDC), `keys=[…]`, `sequence_by=…`, `stored_as_scd_type=1` (or `2` if the source keeps history).
- Full-overwrite outputs → a terminal `@dlt.table` (batch) with `OutputOption=Overwrite` semantics.
- If the source has no CDC and reads full snapshots, the pipeline is full-refresh-safe and idempotent — state that explicitly.

---

## 6. Streaming vs batch decision (per Bronze table)

Mark each Bronze table:

- **Streaming** (`dlt.create_streaming_table` + Auto Loader `cloudFiles`, or a streaming Delta source) when the source input is a directory of files that accumulates, a log/event feed, or was run incrementally.
- **Batch** (`spark.read`) when the source input is a full snapshot replaced each run, a small reference table, or literal seed data.

Downstream: a table is a streaming table only if **all** inputs are streaming and the transform is append-compatible. Any `groupBy` / `pivot` / join-to-latest forces a **materialized view**.

---

## 7. Orchestration

- Wrap the pipeline in a Lakeflow **job**. Port the source scheduler's cadence to the job schedule.
- Pre/post events (run-command, email, conditional): pre-run → an upstream job task; success email → job notifications; post-run command → a downstream task. Drop events that only made sense in the source designer.
- Non-declarative work that can't live in SDP (API/download/run-command tools, R/Python model scoring, report/render/email, spatial drive-time) → separate job tasks around the pipeline, or a `foreachBatch` task. The plan names each one and where it goes.
- Job-level: failure notification, `max_retries`, timeout, and a maintenance task for `OPTIMIZE` / `VACUUM`.

---

## 8. Governance & PII

- All tables in Unity Catalog `catalog.<schema>`, one schema per medallion layer (`_bronze` / `_silver` / `_gold`) or a single schema with prefixes — be consistent.
- PII (names, birth dates, addresses, org numbers) stays confined to the Silver entity table that needs it, behind a restricted grant or column mask. Gold tables carry only aggregates / bands / derived flags.
- Grant `SELECT` on the serving tables to the consuming service principal only.
- UC captures table + column lineage automatically — no extra work, but verify it after the first run.

---

## 9. Testing & parity

- **Unit tests** for every shared expression helper in `utilities/` (one test per translated formula, covering the semantic-delta edge cases from `<source>-formula-to-spark.md`).
- **Window tests**: grain break, null grain value, tiebreak determinism, empty partition.
- **Schema-contract tests**: every `unionByName` input set; the final serving table's column list and order.
- **Golden-output parity** (`source-parity-harness.md` when the skill provides one): run the source with the run-date parameter pinned, capture reference outputs, reconcile — row counts per grain, exact match on integer/count columns, exact on sums/means, tolerance on approx median (±1) and percentage-change (±0.01), spatial assignment ≥ 99.9%. When no source engine is available, reconcile against the last known-good output.
- **Parallel run**: 2–4 production cycles side by side before cutover; the source artifact stays re-enableable as the rollback.

---

## 10. Project skeleton (Databricks Asset Bundles)

```
<project>/
  databricks.yml                 targets dev/prod; bundle vars (catalog, schema, source_scope, secret_scope, output_dir, ...)
  resources/
    pipeline.yml                 catalog/schema; libraries: glob src/transformations/**; configuration{ pipeline.* }; cluster libs (e.g. Sedona)
    job.yml                      tasks around the pipeline + schedule + notifications
  src/
    transformations/
      bronze.py                  one @dlt.table per distinct source
      silver_*.py                conformed entities, cleansing, dedupe, enrichment, expectations
      gold_*.py                  business metrics / aggregations / serving tables
    utilities/
      formulas.py                translated source expressions as named helpers
      windows.py                 yoy / first_per_key / running_total window builders
      pivots.py                  count_pivot / row_pct_pivot
      <domain>.py                shared domain helpers (spatial, cleanse, ...)
    <non_declarative>/           model scoring, exports, API ingestion — job tasks, not @dlt
  tests/
    test_*.py                    unit + window + schema-contract tests
    reconcile.py                 source-vs-target parity
  .github/workflows/ci.yml       lint (bans spark.sql / selectExpr / F.expr / current_date in transformations/), pytest, `databricks bundle validate`
```

---

## 11. Deliverable checklist (Phase 6)

The migration plan is production-ready only when it contains, in order:

1. Executive summary + scope + open questions.
2. Exhaustive source inventory with a coverage check (every tool/node/expression accounted for).
3. Semantic model + field-level lineage + the list of source→Spark semantic differences, each with its chosen resolution.
4. Target medallion architecture (Bronze/Silver/Gold tables), the DAB project skeleton, and the parameter + secret list.
5. Tool-by-tool transformation map — one row per source tool → concrete PySpark construct → target table → expectations added → confidence (`direct` / `adapted` / `needs-design` / `no-equivalent`).
6. Data-quality expectations catalog and the parity/test plan.
7. Optimization design (see `sdp-optimization.md`): partitioning/clustering, broadcast strategy, pivot value lists, window discipline, streaming-vs-batch table, compute sizing.
8. Phased rollout: build order, parallel-run window, cutover criteria, rollback.
9. Risk register: every `needs-design` / `no-equivalent`, every determinism hazard, every semantic difference.
10. Effort estimate by phase.
11. Deployment & operations: the DAB target layout (§12), pipeline settings (§13), the job graph (§14), the UC objects and grants (§15), the CI/CD gates (§16), and the observability queries/alerts (§17).

---

## 12. Databricks Asset Bundles (deployment)

The pipeline is deployed as a bundle, never by clicking in the UI. The plan specifies:

- `databricks.yml`: `bundle.name`; a `variables` block (catalog, schema, source_scope, secret_scope, landing/output paths, run cadence) with per-target overrides; `targets:` for `dev` and `prod` at minimum.
- Target `mode`: `development` for `dev` (resources prefixed with the deployer's username, pipelines set to `development: true`, schedules paused) and `production` for `prod` (no prefix, `development: false`, schedules active). Set `presets` (e.g. `pipelines_development: false`, `trigger_pause_status: PAUSED` on dev).
- `run_as`: a service principal for `prod` (not a user). The SP holds the UC grants from §15.
- `resources/`: one file per resource kind (`pipeline.yml`, `job.yml`). `pipeline.libraries` globs `src/transformations/**`. Do not inline transform code in YAML.
- Deploy flow: `databricks bundle validate -t <target>` → `databricks bundle deploy -t <target>` → `databricks bundle run -t <target> <job>`. `prod` deploy happens only from CI on merge to the main branch (§16).
- Never commit `.databricks/`, workspace state, or resolved secrets. Bundle state lives in the workspace.

## 13. Pipeline settings that matter

Set explicitly in `resources/pipeline.yml`; each has a correctness or cost consequence:

- `catalog` + `schema` (or `target`): the UC destination. One schema per medallion layer, or one schema with `bronze_`/`silver_`/`gold_` prefixes — match §8.
- `serverless: true` when the workload and any cluster libraries allow it; otherwise `clusters:` with `autoscale: { min_workers, max_workers }` and `photon: true`.
- `channel: CURRENT` for production (`PREVIEW` only to test an upcoming runtime).
- `development: false` in prod — prevents cluster reuse/retention shortcuts and makes runs behave like scheduled runs.
- `continuous: false` (triggered) unless the source truly needs always-on streaming; triggered pipelines start, process available data, and stop (cheaper).
- `configuration:` — every `pipeline.*` key from §4, including `pipeline.run_date`.
- `event_log:` → a Unity Catalog table (`catalog.schema.pipeline_event_log`) so operations can query it with SQL (§17).
- `notifications:` — email/webhook on `on-update-failure`, `on-flow-failure`.
- Expectations that use `expect_all_or_fail` and the DQ event stream require a pipeline edition that supports them (ADVANCED); state the required edition.

## 14. Job orchestration details

The pipeline runs inside a Lakeflow **job** (`resources/job.yml`):

- A `pipeline_task` for the SDP update. Upstream tasks for non-declarative pre-work (API pulls, file drops), downstream tasks for exports / report render / model scoring (§7).
- Task dependencies via `depends_on`; use a `condition_task` where the source had conditional branching, and `for_each_task` where a batch macro iterated over a parameter list.
- `schedule` (quartz cron + timezone) ported from the source scheduler, or `trigger.file_arrival` when the source was event-driven. `pause_status: UNPAUSED` only in the prod target.
- Resilience: `max_retries` with `min_retry_interval_millis`, `timeout_seconds`, `max_concurrent_runs: 1` for idempotent nightly loads, `health.rules` (e.g. `RUN_DURATION_SECONDS` threshold).
- `job_clusters` shared across tasks (not one cluster per task) unless isolation is needed; serverless tasks where possible.
- `email_notifications` / `webhook_notifications` on failure and on prolonged run; `queue.enabled: true` so overlapping triggers wait instead of dropping.
- `run_as` the prod service principal.

## 15. Unity Catalog governance details

The plan enumerates the UC objects to create and the grants:

- **Catalog / schemas**: created by the bundle or a one-time setup script. Schemas per layer.
- **External locations & storage credentials** for landing paths and any external tables; **volumes** (`catalog.schema.landing`) for files the pipeline reads — reference volumes by path, not DBFS.
- **Grants**: `USE CATALOG` / `USE SCHEMA` + `SELECT` on serving (Gold) tables to the consuming principals only; `ALL PRIVILEGES` on the pipeline's target schema to the run-as SP; no grants to `account users` on PII-bearing Silver tables.
- **Row filters / column masks**: where the source restricted or masked data, implement as UC row-filter / column-mask functions bound to the Silver table (not re-derived in every Gold query).
- **Tags**: tag PII columns and the pipeline's tables (`domain`, `pii`, `layer`) for discovery and policy.
- **Lineage**: UC records table + column lineage automatically once the pipeline runs — verify it after the first prod run; do not build a separate lineage doc.
- Predictive optimization enabled at catalog/schema level (ties to `sdp-optimization.md` §10).

## 16. CI/CD

`.github/workflows/ci.yml` (or equivalent) gates every change:

- **Lint**: ruff/flake8 with the custom rule banning `spark.sql(`, `.selectExpr(`, `F.expr(`, `F.current_date(`, `F.current_timestamp(` inside `src/transformations/` (§1, §2).
- **Unit tests**: `pytest` over `tests/` — formula helpers, window builders, schema contracts (§9).
- **Bundle validate**: `databricks bundle validate -t dev` on every PR.
- **Dry deploy**: `databricks bundle deploy -t dev` from a CI service principal on merge to a integration branch; smoke-run the pipeline on a small sample.
- **Prod deploy**: `databricks bundle deploy -t prod` runs only on merge to the release branch, via the prod SP, with manual approval.
- Pin the Databricks CLI version in CI. Secrets (CI SP tokens) come from the CI platform's secret store, never the repo.

## 17. Observability

- **Event log** (the UC table from §13): query `event_type = 'flow_progress'` for row counts and durations per flow; `event_type = 'flow_progress'` with `details:flow_progress.data_quality.expectations` for per-expectation pass/fail/drop counts each run. The plan includes the 2–3 SQL queries operations will use.
- **DQ dashboard / alert**: a scheduled query on the event log that alerts when any `expect_all_or_drop` drop rate exceeds the source's tolerance (from §3) or any `expect_all_or_fail` fires.
- **Lakehouse Monitoring** on the key Gold serving tables (snapshot profile + drift) when the consumer needs freshness/quality SLAs.
- **Job run alerts**: failure and SLA-breach notifications from §14 route to the owning team's channel.
- **Parity metric**: after cutover, keep the reconcile query (§9) running as a scheduled check for the parallel-run window and alert on divergence beyond tolerance.
