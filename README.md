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
│   │   └── targets/     one adapter per platform migrated TO (databricks-sdp)
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
```

Tests: `cd migration/engine && python -m unittest discover -s tests` (57 tests,
no network, no API keys).

## Adding a platform

1. Write an adapter in `engine/mig/sources/` (or `targets/`) — the contract is
   documented in that package's `__init__.py`, with `alteryx.py` as the
   reference implementation.
2. Register it with one `register()` call.
3. Add a skill under `skills/` for that platform's reference material; the
   retrieval corpus picks it up automatically.

`TestAdapterContract` asserts every registered adapter satisfies the contract.

## Optional tooling

[Lakebridge](https://databrickslabs.github.io/lakebridge/) supplies estate facts
and an independent tool census to cross-check the parse — **Analyzer only** for
Alteryx; it does not convert Alteryx to PySpark.
[RTK](https://github.com/rtk-ai/rtk) compresses third-party command output.
[Planning-with-Files](https://github.com/OthmanAdi/planning-with-files) adds
slash commands over the plan files. Each has a defined degraded mode — see
`migration/skills/alteryx-sdp-migrate/references/tooling.md`.
