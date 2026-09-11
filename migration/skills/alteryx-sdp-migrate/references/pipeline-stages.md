# Pipeline Stages

What each stage does, what it writes to `state.db`, and what it guarantees. Read
this when a stage behaves unexpectedly or when deciding whether a stage needs to
be re-run.

## Contents

- [1. Extract](#1-extract)
- [2. Units](#2-units)
- [3. Skill retrieval](#3-skill-retrieval)
- [4. Gap resolution](#4-gap-resolution)
- [5. Planning](#5-planning)
- [6. Generation](#6-generation)
- [7. Validation and targeted fix](#7-validation-and-targeted-fix)
- [Re-running a stage](#re-running-a-stage)

## 1. Extract

Two independent sources, each doing what it is actually good at.

**Lakebridge Analyzer** supplies estate-level facts: which files exist, their
complexity band, a tool census, a function census, and the endpoints behind each
Input/Output tool (connection type, referenced objects, read/write action, and
the statement's shape). For Alteryx, Lakebridge's support matrix lists Analyzer
only -- not Converter, not Reconcile -- so it does not produce ToolIDs,
per-tool configuration, or the connection graph. Do not wait for it to.

The parse covers the complement of that list and nothing else -- the **gap
set**. `../../alteryx-to-sdp/references/lakebridge-gap-set.md` has it fact by
fact; the short form is that everything the Analyzer misses is per-`ToolID`,
positional, or relational. Expressions are the one deliberate overlap: the
Analyzer lists them, but not bound to a `ToolID`, and codegen needs the binding.

**The deterministic XML parse** (`engine/mig/sources/alteryx.py`) supplies tool-level facts:
every `Node` including nested `ChildNodes` and disabled tools, verbatim
`Configuration`, every expression, every connection with its anchors, macro
references resolved against the workflow directory and any `--macro-path`, and
determinism hazards.

Writes: `files`, `nodes`, `edges`, `expressions`, `hazards`, `lakebridge`, and
one `gaps` row per distinct unresolved macro.

Guarantees:

- Every node in every parsed document is in `nodes`, disabled and nested
  included. Disabled state is inherited from enclosing containers.
- `config_xml` is the verbatim subtree. Nothing is normalized away.
- Expressions are stored in original Alteryx syntax, untranslated.
- A macro that cannot be found on disk becomes a **blocking** gap, one per
  distinct macro rather than one per call site. Designer's bundled macros
  (`Cleanse.yxmc`, the Predictive Tools set) are recognized: their documented
  behavior is recorded and the gap is non-blocking, for confirmation.
- Analyzer rows are **scoped to this workflow** before anything reads them.
  `analyze --source-directory` sweeps a whole tree -- on the reference run,
  ~23 objects from 8+ unrelated folders -- so only rows whose `source_file`
  matches a file the parse actually walked count toward the census or supply
  endpoints. `mig extract` reports how many rows it ignored.
- The **cross-check** compares Lakebridge's census with the parse after
  normalizing both to bare tool names. Agreement is real evidence that neither
  side dropped anything; disagreement is a finding to resolve, not a warning to
  pass over.

The workflow XML is read here and never again.

## 2. Units

A migration unit is the unit of work, of planning, and of validation.

Cutting rules, in order:

1. Tools inside an Alteryx container become that container's unit. Containers
   are how the workflow's own author grouped it; that grouping is usually
   meaningful and always cheap to honor.
2. Loose tools group by weakly-connected component.
3. Groups larger than `--max-unit` (default 40) split along topological order,
   so each part's inputs precede it.
4. Groups smaller than `--min-unit` (default 3) merge into their dominant
   upstream unit, when that fits under the cap.

Then:

- **Dependencies.** Any connection crossing a unit boundary becomes a
  `unit_deps` row. Units are sequenced by a topological sort of that graph;
  cycles (iterative macros, feedback wiring) are broken deterministically by
  lowest id so ordering is stable across runs.
- **De-duplication.** Each unit gets a `block_hash` over the multiset of
  `(tool_name, config_hash)` plus its internal edge shape. Units sharing a hash
  are the same transformation: the first is migrated, the rest carry
  `dedupe_of` and are satisfied when it completes. On the 1,533-tool reference
  workflow this collapsed 97 units to 62 unique ones.

Writes: `units`, `unit_nodes`, `unit_deps`.

Guarantee: every tool is in exactly one unit, legitimately excluded with a
stated reason, or reported by `unaccounted_nodes` -- which fails structural
validation. Canvas comments, containers themselves, terminal Browse tools, and
unconnected tools are the only legitimate exclusions.

## 3. Skill retrieval

The repository's references are far larger than any unit needs. They are chunked
by heading, indexed with BM25 (k1=1.5, b=0.75), and queried per unit using that
unit's own tool census and function census, expanded through a term map
(`Summarize` also queries `groupBy agg stddev median percentile`, because the
reference indexes the Spark construct, not the Alteryx tool name).

`top_k` scales with the unit: a two-tool unit gets two sections, a forty-tool
unit spanning eight tool families gets six. A byte budget caps the result.

The index is cached in the run directory and rebuilt automatically when any
corpus file changes.

## 4. Gap resolution

Four kinds of thing, kept distinguishable forever via `decisions.basis`:

- `fact` -- from the parse or Lakebridge
- `mapping` -- from a repository skill
- `inference` -- model judgment, explicitly labelled as such
- `user` -- the user answered

Anything that cannot be resolved becomes a `gaps` row. Blocking gaps keep their
unit out of `complete` and fail structural validation. This is the mechanism that
makes "do not hallucinate, do not silently skip" enforceable rather than
aspirational.

Gap kinds: `missing-info`, `ambiguous-config`, `unsupported-tool`,
`ambiguous-expr`, `column-mapping`, `unclear-type`, `complex-behavior`.

## 5. Planning

`mig plan` projects `state.db` into the Planning-with-Files layout under
`.planning/<slug>/`: `task_plan.md` (phases and per-unit checkboxes),
`findings.md` (decision log, gap register, hazard summary), `progress.md`
(status counts, failing checks, artifacts, next ready units). A `.active_plan`
pointer names the current plan directory.

These files are **generated**. With ~100 units a hand-maintained checklist
diverges from reality within an hour, and a plan that disagrees with the state
is worse than no plan. Change state through `mig`, then re-run `mig plan`.

## 6. Generation

One unit at a time, from its context pack. The pack carries the unit's input and
output contract -- which upstream unit and anchor feeds each entry point, and
which downstream unit consumes each exit point -- so generated code composes
instead of drifting.

`mig record` attaches an artifact and moves the unit to `generated`. Assign a
layer with `mig layer` (bronze / silver / gold / job / drop); structural
validation requires every unit holding an IO tool to have one.

## 7. Validation and targeted fix

Four levels, run independently so a failure names one component. See
`validation-levels.md` for each check.

On failure: read the failing check, retrieve the reference section it points at,
update the decision, regenerate only that unit, revalidate. A localized failure
never justifies regenerating the workflow.

## Re-running a stage

- `extract` is idempotent for nodes and files (`INSERT OR REPLACE`), but appends
  to `edges`, `expressions`, and `hazards`. Re-extract into a **fresh run
  directory** rather than over an existing one.
- `units` fully rebuilds `units`, `unit_nodes`, and `unit_deps`, which discards
  unit status, layers, and `dedupe_of` links. Re-cutting units mid-migration
  loses completion state; change `--max-unit` before starting work, not after.
- `plan`, `validate`, and `report` are safe to re-run at any time. Each
  validation level clears its own prior rows for that unit first.

## Adding another source or target

The engine is source- and target-agnostic; everything platform-specific sits
behind two adapter contracts in `engine/mig/sources/` and `engine/mig/targets/`.
To add a platform:

1. Write the adapter. `sources/__init__.py` documents the contract and
   `sources/alteryx.py` is the reference implementation: `extract()` fills the
   same `nodes`/`edges`/`expressions`/`hazards` tables, `describe()` compacts one
   tool, and `semantic_checks()` asserts that platform's semantics survived.
2. Register it — one `register(module)` call at the bottom of the package
   `__init__`. Extension-based dispatch and `--source` then work automatically.
3. Add a skill under `skills/` holding that platform's reference material. The
   retrieval corpus picks up every directory in `skills/` with no code change.

The engine's own tests (`engine/tests/test_mig.py`, `TestAdapterContract`)
assert every registered adapter satisfies its contract, so a new adapter fails
loudly rather than subtly.
