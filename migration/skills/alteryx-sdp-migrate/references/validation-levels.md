# Validation Levels

Four independent levels. Every check writes a row to `validations`, so status is
reportable per unit rather than in aggregate, and a failure names one component.

Run: `mig validate --unit U017` (all levels) or `--level code --level semantic`.
Structural is whole-migration and runs with any invocation that includes it.

## Contents

- [Structural](#structural)
- [Code](#code)
- [Semantic](#semantic)
- [Data](#data)
- [Verdicts and the completion gate](#verdicts-and-the-completion-gate)
- [Fixing a failure](#fixing-a-failure)

## Structural

Nothing was silently dropped; the DAG survived.

| Check | Fails when | Fix |
|-------|-----------|-----|
| `tool-coverage` | a wired data tool is in no unit and is not legitimately excludable | re-run `mig units`, or explain the exclusion; never leave a tool unaccounted |
| `no-tool-in-two-units` | a tool was assigned twice | rebuild units; a duplicated tool means duplicated logic in the target |
| `lakebridge-parity` | Lakebridge's census and the parse disagree on a tool type | find which side missed it before migrating anything further |
| `dependency-preservation` | a connection crosses a unit boundary with no declared dependency | rebuild units; generation order would be wrong |
| `io-accounted` | an Input/Output tool is in no unit | a pipeline boundary is missing from the target |
| `io-layer-assigned` | a unit holding an IO tool has no layer | `mig layer <unit> bronze\|gold\|job` |
| `no-open-blocking-gaps` | any blocking gap is open | resolve it, or escalate it explicitly with `gap resolve --escalate` |

`lakebridge-parity` records **skip**, not pass, when no report was ingested. A
check that never ran is never a pass.

## Code

The generated Python parses, resolves, and obeys the target-code rules.

| Check | Catches |
|-------|---------|
| `syntax` | anything that will not `ast.parse` |
| `names-resolve` | a name loaded but never bound anywhere in the module -- the usual shape of a hallucinated upstream DataFrame variable |
| `target-code-rules` | `spark.sql(`, `.selectExpr(`, `F.expr(`, `F.current_date/current_timestamp`, `.collect()` / `.toPandas()` inside a transform |
| `dlt-returns-dataframe` | a `@dlt.table` / `@dlt.view` function with no return value, which silently produces an empty table |
| `artifact-present` | no code was recorded for the unit at all |

`names-resolve` treats `spark`, `dlt`, and `dbutils` as provided by the runtime.

## Semantic

The Alteryx behavior actually reached the target.

| Check | Catches |
|-------|---------|
| `tool-constructs` | a tool class with no corresponding construct: `Filter` with no `.filter`/`.where`, `Join` with no `.join`, `Union` with no `.unionByName`, `CrossTab` with no `.pivot`, `MultiRowFormula`/`RunningTotal` with no `Window`, and so on |
| `aggregation-present` | `Summarize` present but no `.agg(` |
| `stddev-population` | `F.stddev` where Alteryx's `StdDev` is population -- use `F.stddev_pop` |
| `pivot-explicit-values` | `.pivot(col)` with no value list: non-deterministic column order and an extra scan |
| `deterministic-order` | an order-dependent tool (`MultiRowFormula`, `RunningTotal`, `Tile`, `Sample`, `Unique`) with no explicit ordering, or a window ordered on a single key with no tiebreak |
| `created-fields-present` | a field the Alteryx unit creates (Formula field, Multi-Row Formula output, Summarize rename) that appears nowhere in the code -- a dropped transformation |
| `clock-parameterized` | a unit with a `DateTimeToday`/`DateTimeNow` hazard whose code has no `run_date` parameter |
| `null-semantics-addressed` | recorded as **skip**, not pass: the unit depends on Alteryx null semantics (`"ABC" + Null()` keeps `"ABC"`; `Null()==Null()` is true) and the chosen resolution must be a recorded decision |

These are text-level checks over the generated module. They catch omission
reliably; they do not prove the logic is right. That is what data validation is
for.

## Data

Source and target agree on real data. This is the only level that can prove
correctness, and the only one that needs a running Alteryx engine or captured
reference output.

Set a manifest at `mig init --parity-manifest <file.json>`:

```json
{
  "U017": {
    "row_count": 148233,
    "columns": ["Regiontype", "Region", "Omsetningsar", "AntallOmsetninger"],
    "aggregates": { "AntallOmsetninger": 1029481, "Gjennomsnittspris": 4182933.55 },
    "tolerance":  { "Gjennomsnittspris": 0.01 },
    "actual": {
      "row_count": 148233,
      "columns": ["Regiontype", "Region", "Omsetningsar", "AntallOmsetninger"],
      "aggregates": { "AntallOmsetninger": 1029481, "Gjennomsnittspris": 4182933.55 }
    }
  }
}
```

`row_count`, `columns`, and `aggregates` are captured from Alteryx; `actual` is
written back from the migrated pipeline by a reconciliation notebook. Column
order is compared as well as membership -- the serving table's column order is
part of the contract.

Suggested tolerances: counts and integer sums exact; means exact to the stated
tolerance; approximate median ±1; percentage change ±0.01; spatial assignment
≥ 99.9%.

Without a manifest, or with expectations but no captured `actual`, the level
records **skip with the reason**. It never records pass. To capture reference
output, see `../../alteryx-to-sdp/references/source-parity-harness.md` and
`../alteryx-to-sdp/scripts/Invoke-AlteryxWorkflow.ps1`.

## Verdicts and the completion gate

`unit_verdict` returns:

- `incomplete` -- code and semantic validation have not both run
- `failed` -- at least one check failed
- `blocked` -- checks pass but a blocking gap is open
- `validated` -- everything ran, nothing failed, nothing blocking

`mig complete <unit>` refuses anything but `validated`. `--force` exists for the
case where the user has decided to accept a known limitation; it stamps
`[forced]` into the unit's notes, and it should never be used without telling
the user what is being accepted.

Whole-migration completion (`mig status`, `mig report`) additionally requires:
Lakebridge ingested, every unit complete, no open blocking gaps, structural +
code + semantic validation run with zero failures, data validation run (passing
or skipped with a reason), and zero unaccounted tools.

## Fixing a failure

1. Read the failing check's `detail`. It names the tool class, field, or line.
2. Retrieve the reference for it: `mig retrieve "<the construct> <the tool>"`.
3. If the cause is a missing fact, record a gap rather than guessing.
4. Update the decision, regenerate **only that unit**, `mig record`, revalidate.
5. `mig plan` to refresh the on-disk plan.

A failure in one unit never justifies regenerating the others.
