---
name: alteryx-sdp-migrate
description: Execute a large Alteryx-to-Databricks migration end to end as a resumable, checkpointed, validation-gated process. Use for `.yxmd`, `.yxmc`, and `.yxwz` work that goes beyond planning into actually producing PySpark / Lakeflow SDP code, especially for workflows too large to hold in one context (hundreds to 2,000+ tools). Runs Lakebridge for estate facts, parses the workflow XML once into a SQLite migration state, cuts the DAG into dependency-ordered migration units, de-duplicates repeated blocks, retrieves only the repository skills each unit needs, records every gap and decision with its basis, generates code unit by unit, and gates completion on structural, semantic, code, and data validation. For plan-only work on a single workflow, use `alteryx-to-sdp` instead.
---

# Alteryx to Databricks SDP -- Executed Migration

`alteryx-to-sdp` produces a plan for one workflow. This skill *runs* a migration:
large, resumable, auditable, and validated.

The pipeline:

```
Alteryx workflow
  -> Lakebridge          estate facts (inventory, complexity, endpoints, census)
  -> deterministic parse only the gap set (ToolIDs, configs, expressions, DAG)
  -> units               dependency-ordered work items, duplicates collapsed
  -> skill retrieval     only the reference sections this unit needs
  -> gap resolution      AI used only where facts and skills run out
  -> generation          PySpark / SDP for one unit at a time
  -> validation          structural + semantic + code + data
  -> targeted fix        regenerate only the failing component, revalidate
```

Everything exhaustive is done by `engine/mig.py`. The model's job is judgment:
resolving gaps, choosing the target design, and writing the code for one small
unit at a time.

**The core rule:** extract once, retrieve narrowly, process small units, persist
state, validate, continue. The workflow XML is read exactly once and never
enters the conversation.

## Setup

Check tooling before starting; each missing piece degrades the run in a specific
way, and the user should know which.

| Tool | Purpose | Install | If missing |
|------|---------|---------|-----------|
| Lakebridge | estate facts + an independent tool census to cross-check the parse | `databricks labs install lakebridge` | run with `--no-lakebridge`; the parse still covers every tool, but the two-source structural check cannot run |
| RTK | compresses noisy third-party command output | `rtk init -g`, then restart | `mig` output is already compact; only third-party commands get noisy |
| Planning-with-Files | the on-disk plan format this skill writes | `/plugin marketplace add OthmanAdi/planning-with-files` | `mig plan` still writes `task_plan.md` / `findings.md` / `progress.md`; you just don't get its slash commands |

`mig` itself is stdlib-only Python 3.10+. No install, no network.

Set the engine path and the run directory once; every command below uses them:

```bash
MIG=<plugin-root>/engine/mig.py              # ../../engine/mig.py from this skill
export MIG_RUN=/path/to/migration-run        # or pass --run to every command
```

## Phase 1 -- Scope and extract (once)

Establish scope first. Record anything the user has not given as a gap rather
than assuming it: target catalog and schema, orchestration, secret scopes,
whether golden-output parity testing is in scope, data volume and cadence.

```bash
python "$MIG" init <workflow.yxmd> --catalog <cat> --schema <sch> \
    [--macro-path <dir>]... [--parity-manifest <file.json>]
python "$MIG" extract          # runs Lakebridge, then parses the XML
python "$MIG" units            # cut the DAG into migration units
python "$MIG" plan             # write the Planning-with-Files markdown
```

`extract` reports a **cross-check**: Lakebridge's tool census against the parse.
They must agree. A disagreement means one side missed something -- resolve it
before continuing; do not migrate from a census you know is wrong.

If Lakebridge was already run, skip re-running it:

```bash
python "$MIG" extract --lakebridge-json <analysis.json>
```

Read `references/pipeline-stages.md` for what each stage records and why.

## Phase 2 -- Work the units

```bash
python "$MIG" next             # units whose dependencies are complete
python "$MIG" context U017     # the pack for one unit
```

The context pack is the **only** workflow material you need for that unit:
verbatim expressions, compacted per-tool configuration, the unit's input and
output contract, its determinism hazards, its open gaps, decisions already
recorded, and the top reference sections retrieved for its specific tool mix.

Do not read the workflow XML. Do not open reference files wholesale -- if the
pack's retrieved sections are not enough, search rather than load:

```bash
python "$MIG" retrieve "apply_changes SCD2 deletes" --top-k 4
python "$MIG" show 1842 --raw  # one tool's full config, when truly needed
```

### Resolving gaps

Use AI judgment only where the facts and the retrieved skills genuinely run out:
missing information, ambiguous configuration, unsupported tools, ambiguous
expressions, unclear column mappings or types, complex tool behavior.

Record everything. The basis is what makes the migration auditable:

```bash
python "$MIG" decide "Cleanse macro -> trim + null-to-empty on 4 columns" \
    --unit U017 --basis mapping --confidence adapted
python "$MIG" gap add "Pivot header values are not in the config; need the \
    distinct ByggStatus values as of the cutover date" --kind missing-info --unit U017
python "$MIG" gap resolve 12 "User confirmed the four documented statuses" --by user
```

`--basis`: `fact` (from the parse or Lakebridge), `mapping` (from a repo skill),
`inference` (your judgment -- say so), `user` (they told you).

**Never** invent a value to get past a blocker. A blocking gap keeps its unit out
of `complete`, which is the point.

### Generating

Assign the layer, write the code for that unit only, and register it:

```bash
python "$MIG" layer U017 silver
python "$MIG" record U017 <out>/silver/u017_bygg_status.py --role sdp
```

Target-code rules, enforced by validation:

- PySpark DataFrame API only. No `spark.sql(...)`, `.selectExpr(...)`, or
  `F.expr(...)` for logic. Only `@dlt.expect_*` predicates and spatial-library
  callables may be strings.
- No `F.current_date()` / `F.current_timestamp()`. Wall-clock becomes a
  `pipeline.run_date` parameter.
- Every ordered operation gets an explicit `Window` order **with a tiebreak**.
- `.pivot()` always takes an explicit value list.
- Alteryx `StdDev` is population: `F.stddev_pop`.
- Never reproduce credentials or connection strings; use named secret references.

When a unit is marked `duplicate of Uxxx`, do not regenerate it. Reuse that
unit's transform as a parameterized function and record the parameter binding as
a decision. On a real 1,500-tool workflow this removed a third of the work.

### Validating

```bash
python "$MIG" validate --unit U017      # code + semantic + data + structural
python "$MIG" complete U017             # refuses unless validation passed
```

`complete` will not mark a unit done while a check fails or a blocking gap is
open. That refusal is the feature -- do not `--force` past it without saying so
to the user and recording why.

On failure, fix narrowly: read the failing check, retrieve the reference section
it points at, update the decision, regenerate **only** that unit, revalidate.
Never regenerate the whole workflow for a local failure.

Re-run `python "$MIG" plan` periodically so the on-disk plan tracks the
database.

## Phase 3 -- Finish

```bash
python "$MIG" validate --all --level structural
python "$MIG" status
python "$MIG" report
```

A migration is COMPLETE only when `mig status` says so. Its gate:

- Lakebridge analysis ingested
- every unit `complete`
- no open blocking gaps
- structural, semantic, and code validation all ran with zero failures
- data validation ran -- passing, or skipped with a recorded reason
- no unaccounted tools

Generated code is never, by itself, evidence of a successful migration.

## After a context reset

State lives on disk, so nothing is lost:

```bash
python "$MIG" status     # where the migration stands
python "$MIG" next       # what to do now
```

`state.db` is the source of truth; `.planning/<slug>/*.md` is a projection of it.
Never hand-edit the markdown -- change state through `mig` and re-run `mig plan`.

## What not to do

- Do not paste workflow XML, whole reference files, or generated code into the
  conversation. Point at paths.
- Do not load the full skill corpus. Retrieve.
- Do not re-analyze a resolved unit or re-extract an unchanged workflow.
- Do not mark a unit complete to keep momentum. Record the gap instead.
- Do not silently skip a tool. Every tool is in a unit, legitimately excluded
  with a reason, or reported as unaccounted -- there is no fourth option.

## References

- `references/pipeline-stages.md` -- what each stage does, records, and guarantees.
- `references/state-model.md` -- the `state.db` tables and how to query them.
- `references/validation-levels.md` -- every check, what it catches, how to fix it,
  and how to set up data parity.
- `references/tooling.md` -- Lakebridge, RTK, and Planning-with-Files: exact
  commands, what each contributes, and how the run degrades without it.

Alteryx semantics and the Databricks target design live in the sibling skill and
the shared references; this skill retrieves from them rather than restating them:

- `../alteryx-to-sdp/references/` -- workflow XML, per-tool parse reference,
  formula syntax, Alteryx-to-Spark translation, and `lakebridge-gap-set.md`:
  which facts come from the Analyzer and which only a parse can produce.
- `../../references/` -- SDP target patterns, production hardening, optimization.
