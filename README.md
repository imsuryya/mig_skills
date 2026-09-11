# mig-skills

Agent skills for migrating ETL workloads to **Databricks Lakeflow Spark
Declarative Pipelines (SDP)** in PySpark.

## Install

```
/plugin marketplace add C:\Git\mig_mcp
/plugin install migration@mig-mcp
```

Then restart. Claude picks the right skill from what you ask; or invoke one
directly with `/migration:alteryx-to-sdp` or `/migration:alteryx-sdp-migrate`.

## Skills

| Skill | Use it to |
|-------|-----------|
| `alteryx-to-sdp` | **Plan.** Parse a workflow tool by tool, trace field-level lineage, produce a production-grade migration plan. |
| `alteryx-sdp-migrate` | **Execute.** Generate the pipeline across a workflow too large for one context, with persistent state and validation gating completion. |

## Layout

```
migration/
├── engine/              source- and target-agnostic migration engine (stdlib Python)
│   ├── mig.py           CLI entry point
│   ├── mig/
│   │   ├── sources/     one adapter per platform migrated FROM (alteryx)
│   │   ├── targets/     one adapter per platform migrated TO (databricks-sdp)
│   │   ├── retrieve.py  BM25 over the repo's own references, shared by both skills
│   │   ├── export.py    the single-workbook deliverable
│   │   ├── xlsx.py      xlsx read/write with no dependencies
│   │   └── rtk.py       routes engine-spawned commands through RTK when present
│   └── tests/
├── references/          shared target knowledge, reused by every skill
└── skills/
    ├── alteryx-to-sdp/       source knowledge + planning procedure
    └── alteryx-sdp-migrate/  execution procedure
```

The engine lives outside the skills on purpose: a second source platform is one
adapter, not a fork.

## How the execution skill works

```
Lakebridge (estate facts) + one-pass XML parse (tool facts)
  → dependency-ordered units, repeated blocks de-duplicated
  → BM25 retrieval of only the reference sections each unit needs
  → gap + decision register (facts stay distinguishable from inference)
  → code generated one unit at a time
  → structural / semantic / code / data validation gates completion
  → one .xlsx holding the analyzer report and every fact behind the migration
```

State lives in SQLite; the Planning-with-Files markdown is a projection of it, so
a migration survives context loss and resumes with `mig status` / `mig next`.

Measured on a real 1,533-tool workflow: parse in 1.5 s, Lakebridge cross-check
exact, 97 units of which 62 unique, context packs ~65% smaller than the raw
configuration XML.

## Using the engine directly

```bash
MIG=migration/engine/mig.py
export MIG_RUN=/path/to/run

python $MIG init <workflow.yxmd> --catalog <cat> --schema <sch>
python $MIG extract          # Lakebridge + XML parse → state.db
python $MIG units            # cut the DAG into migration units
python $MIG next             # what is ready to work
python $MIG context U013     # the only material the model needs for that unit
python $MIG validate --unit U013
python $MIG status           # resumes here after any context loss
python $MIG export           # <run>/migration-export.xlsx
```

Retrieval works standalone, with no run directory and no state — both skills use
it for lookups against the same index:

```bash
python $MIG retrieve "CrossTab key field method" --top-k 4
```

Tests: `cd migration/engine && python -m unittest discover -s tests` (91 tests,
no network, no API keys).

## The deliverable

`mig export` writes one workbook a reviewer can open without the repo, the
database, or an agent:

| Half | Sheets |
|------|--------|
| Engine (`MIG ` prefix, projected from `state.db`) | Overview, Files, Tools, Connections, Expressions, Hazards, Units, Unit Tools, Decisions, Gaps, Validations, Artifacts, Census Crosscheck, SQL Endpoints |
| Lakebridge | the analyzer's own sheets, copied verbatim under their original names |

Analyzer sheets keep their names because several reference their siblings by name
in formulas; renaming would break them silently. Decisions carry their `basis`
(fact / mapping / inference / user), so extracted truth stays separable from
model judgement in the deliverable exactly as it is in the database. `--full`
adds each tool's verbatim `<Configuration>` XML. If the analyzer workbook cannot
be found, the export rebuilds its essentials from the ingested JSON and says
`DEGRADED` rather than quietly shipping a thinner file.

## Adding a platform

1. Write an adapter in `engine/mig/sources/` (or `targets/`) — the contract is
   documented in that package's `__init__.py`, with `alteryx.py` as the
   reference implementation.
2. Register it with one `register()` call.
3. Add a skill under `skills/` for that platform's reference material; the
   retrieval corpus picks it up automatically.

`TestAdapterContract` asserts every registered adapter satisfies the contract.

## Optional tooling

Every one of these is optional and every one has a defined degraded mode — see
`migration/skills/alteryx-sdp-migrate/references/tooling.md` for what each
failure actually costs.

[**Lakebridge**](https://databrickslabs.github.io/lakebridge/) supplies estate
facts and an independent tool census to cross-check the parse — **Analyzer only**
for Alteryx; it does not convert Alteryx to PySpark. The only one of these that
the migration genuinely depends on.

[**RTK**](https://github.com/rtk-ai/rtk) compresses third-party command output.
`rtk init -g` installs a PreToolUse hook that covers commands run through the
Bash tool; the engine additionally routes the analyzer it spawns itself through
`rtk proxy`, since a `subprocess.run` never passes the hook. A wrapped run that
fails is retried bare, and `MIG_NO_RTK=1` opts out entirely.

[**Planning-with-Files**](https://github.com/OthmanAdi/planning-with-files) adds
slash commands over the plan files. `mig plan` writes them either way.

## Credits

The per-unit retrieval strategy follows
[**skill-retrieval**](https://github.com/moonlight-lupin/agent-skills/tree/main/plugins/skill-retrieval)
from `moonlight-lupin/agent-skills`: BM25 (k1=1.5, b=0.75) over chunked
reference material, top-K injected per turn instead of the whole corpus. That
plugin targets Hermes Agent and will not load in Claude Code — it needs the
`pre_llm_call` event and `plugin.yaml` — so `engine/mig/retrieve.py` implements
the technique directly, in stdlib, chunking Markdown by heading and YAML by
top-level entry.
