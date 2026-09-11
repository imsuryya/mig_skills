# The Lakebridge Gap Set

Lakebridge Analyzer and a raw-XML parse overlap. Where they overlap, the
Analyzer wins: it is deterministic, already run, and costs no context. **Parse
only what the Analyzer cannot produce -- no more than that.**

Re-deriving an Analyzer fact is not harmless. It burns context on a number you
already have, and it produces a second, differently-computed answer to the same
question, which then has to be reconciled.

## Contents

- [The split](#the-split)
- [What the AI parses](#what-the-ai-parses)
- [Scoping the Analyzer report](#scoping-the-analyzer-report)
- [Trust caveats](#trust-caveats)
- [When there is no Analyzer report](#when-there-is-no-analyzer-report)

## The split

Verified against a real Analyzer run (`alteryxanalysis.xlsx`) and a real
hand-built inventory over the same 1,533-node production workflow.

| Fact needed for migration | Analyzer | Parse it? |
|---|---|---|
| Object inventory + complexity band | Yes -- Job Details, LOW/MED/HIGH/VERY_HIGH per job | No |
| Tool / transformation frequency | Yes -- Transformations, Jobs Transformations Xref | No |
| Function census | Yes -- Functions, Functions by Job | No |
| Formula expressions, as text | Yes -- Transformation Expressions | Only to bind them to a `ToolID` (see below) |
| Source queries / SQL text, objects read | Yes -- SQL Statements (files appear as paths) | No |
| Checksums / change detection | Yes -- Combined Checksums, Legacy/Extended | No |
| **Tool-to-tool connection graph (the DAG)** | No | **Yes** |
| **Resolved execution order** | No | **Yes** |
| **Filter predicates** (expression + mode/operator) | No | **Yes** |
| **Select / field mapping** (order, rename, type, size, drop) | No | **Yes** |
| **Join keys**, per side, per field | No | **Yes** |
| **Summarize / aggregation config** | No | **Yes** |
| **Union / MultiRow / MultiField config** | No | **Yes** |
| **Output destinations** (format, path, mode) | No | **Yes** |
| **Macro resolution** -- what is inside each call | Only macros that happened to be in the scanned folder | **Yes**, incl. bundled/no-file-on-disk cases |
| **Container / grouping structure** | No | **Yes** |
| **Determinism hazards** | No | **Yes** |
| **Per-tool raw configuration** (catch-all) | No | **Yes** |
| **`ToolID`s at all** | No | **Yes** |
| Source table column types | No | No -- not in the workflow XML either; needs a live source connection |

For Alteryx, Lakebridge's support matrix lists **Analyzer only** -- not
Converter, not Reconcile. The whole right-hand column is permanent, not a gap
waiting on a newer release.

## What the AI parses

The gap set has one thing in common: it is all **per-`ToolID`, positional, or
relational**. The Analyzer reports *what a workflow contains*; migration needs
*what is wired to what, in what order, with which configuration*.

So the parse's job is the DAG and the per-tool configuration that hangs off it.
Two consequences:

- **Expressions.** The Analyzer lists expressions, but not bound to a `ToolID`
  or a position in the DAG, and codegen needs both. Take them from the parse
  (they are free -- you are already reading the node). Do not separately
  re-tabulate a function census from them; that sheet already exists.
- **Census.** The parse's own tool counts are a by-product, and they are worth
  keeping for exactly one purpose: cross-checking the Analyzer's independent
  count. Agreement is evidence neither side dropped a tool. Do not present the
  parse's census as a deliverable alongside the Analyzer's.

Inventory sheets that duplicate Analyzer output -- a Summary of tool-type
counts, a Formulas list, a complexity score, checksums -- should not be built.
Cite the Analyzer sheet instead.

## Scoping the Analyzer report

`--source-directory` sweeps a whole tree. On the reference run it pulled in ~23
objects from 8+ unrelated folders, so estate totals were not the workflow's
totals.

Point it at a directory holding only the target workflow and its macros, or
filter every Analyzer read by `sourceFile`. Never quote an estate-wide number as
if it described the workflow.

## Trust caveats

- **The connection columns can be present and empty.** The Item Node Info
  columns look like they should carry origin/destination, and on the reference
  run they were blank. Present-but-empty is not "no connections" -- the DAG
  always comes from the parse.
- **Macro coverage is incidental.** The Analyzer sees the macros that happened
  to sit in the scanned folder. Macro resolution is the parse's job: resolve
  against the workflow directory and the macro search paths, recurse, and record
  every unresolved macro as a blocking gap.
- File-based Inputs show up in SQL Statements as paths rather than SQL. That is
  the Analyzer being right, not missing something.

## When there is no Analyzer report

Parse the full set, and say so in the plan: the complexity band and the
independent cross-check are then unavailable, and the census is single-sourced.
That is a degraded run, not an equivalent one.
