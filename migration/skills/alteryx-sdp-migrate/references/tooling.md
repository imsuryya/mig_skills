# External Tooling

Three external tools sit around this skill. Each contributes something specific;
each has a defined degraded mode. Verify what is actually installed before
promising the user what the run will produce.

## Contents

- [Lakebridge](#lakebridge)
- [RTK](#rtk)
- [Planning-with-Files](#planning-with-files)
- [Skill retrieval](#skill-retrieval)
- [Checking the environment](#checking-the-environment)

## Lakebridge

Databricks Labs' open-source migration toolkit.

```bash
databricks labs install lakebridge
databricks labs lakebridge analyze \
    --source-directory ./workflows \
    --report-file ./reports/alteryx_estate.xlsx \
    --source-tech alteryx \
    --generate-json true
```

`mig extract` runs this for you (source directory defaults to the workflow's own
directory) and ingests the JSON. To reuse an existing report:
`mig extract --lakebridge-json <analysis.json>`.

**What it gives.** Per source file: `complexityLevel`, `type`, a `nodes` census
keyed by plugin, a `functionCall` census of Alteryx functions used, and
`sqlStatements` -- for each Input/Output tool, the connection type, the database
objects it reads or writes, and the statement itself. That endpoint inventory is
genuinely hard to reconstruct otherwise, and the function census tells you which
translation problems the workflow actually has before you open a single tool.

**What it does not give.** For Alteryx, Lakebridge's support matrix lists
**Analyzer only** -- not Converter, not Reconcile. There are no ToolIDs, no
per-tool configuration, and no connection graph in its output. It will not
convert Alteryx to PySpark. Anyone expecting a one-click conversion should be
told plainly that conversion and output validation remain engineering scope.

**Why both sources.** The tool census is an *independent* count. Comparing it
against the XML parse is real evidence that neither side dropped anything --
`mig extract` reports this as the cross-check, and structural validation records
it as `lakebridge-parity`.

**Degraded mode.** `mig extract --no-lakebridge`, or any failure to run it,
records a non-blocking gap. The parse still covers every tool; you lose the
endpoint inventory, the complexity banding, and the cross-check.

Requires the Databricks CLI on PATH, Python 3.10+, and Java 21+.

## RTK

`rtk-ai/rtk` -- a Rust CLI proxy that compresses command output before it reaches
the model, typically 60-90% on noisy commands.

```bash
brew install rtk            # or: cargo install --git https://github.com/rtk-ai/rtk
rtk init -g                 # installs the Claude Code PreToolUse hook
# restart the session
rtk gain                    # savings dashboard
```

With the hook installed, bash commands are rewritten transparently: `git status`
becomes `rtk git status`. Without it, wrap explicitly: `rtk pytest`,
`rtk git diff`, `rtk lint`.

Two things worth knowing:

- The hook only intercepts **bash**. Built-in Read/Grep/Glob bypass it entirely,
  so shell commands are the ones that benefit.
- On failure RTK keeps the full output for recall (`rtk recall <id>`), so
  compression does not cost you the diagnostic when something breaks.

**Where it matters here.** `mig` output is already compact by design -- that is
why the CLI has `--json`/`--compact` and prints summaries rather than dumps. RTK
earns its place on everything *around* the migration: `databricks` CLI calls,
`pytest`, linting, git, and the Lakebridge run itself.

**Degraded mode.** Nothing breaks. Keep third-party output small by hand:
`| head`, `--quiet`, and targeted queries rather than full dumps.

Config: `~/.config/rtk/config.toml`, `[hooks] exclude_commands = [...]`.

## Planning-with-Files

`OthmanAdi/planning-with-files` -- keeps an agent's plan on disk so it survives
`/clear`, compaction, and crashes.

```bash
/plugin marketplace add OthmanAdi/planning-with-files
```

Its layout: `task_plan.md` (phases and checkboxes), `findings.md` (research and
decisions), `progress.md` (session log), optionally under
`.planning/YYYY-MM-DD-slug/` with an `.active_plan` pointer. Slash commands:
`/plan`, `/plan-status`, `/plan-attest`, `/plan-doctor`.

**How this skill uses it.** `mig plan` writes exactly that layout, generated from
`state.db`. The database is authoritative because a ~100-unit checklist
maintained by hand drifts from reality within an hour, and a plan that disagrees
with the state is worse than no plan. If the plugin is installed, its commands
read these files normally.

**Degraded mode.** `mig plan` writes the files regardless; you lose only the
slash commands.

## Skill retrieval

The `agent-skills` skill-retrieval plugin is built for Hermes Agent
(`~/.hermes/plugins/`, a `pre_llm_call` hook) and is **not compatible with Claude
Code**. Its technique is what matters, and this skill implements it directly:
BM25 (k1=1.5, b=0.75) over heading-level chunks of the repository's own
references, top-K injected per unit instead of the whole corpus.

That lives in `engine/mig/retrieve.py` and needs nothing installed (`MIG` as defined in SKILL.md):

```bash
python "$MIG" index                               # (re)build, cached per run
python "$MIG" retrieve "apply_changes SCD2" --top-k 4
```

`mig context` calls it automatically with a query built from the unit's own tool
and function census. On the reference corpus this returns roughly 3% of the
available material per unit.

Add corpora with `--corpus <dir>` (repeatable). The index rebuilds automatically
when any indexed file changes.

## Checking the environment

```bash
command -v databricks && databricks labs lakebridge --help >/dev/null 2>&1 \
  && echo "lakebridge: ok" || echo "lakebridge: missing -> --no-lakebridge"
command -v rtk >/dev/null && rtk --version || echo "rtk: missing -> keep output small by hand"
python --version                       # 3.10+
python "$MIG" index            # proves the corpus resolves
```

State plainly which of these are present before starting a large run. The
difference between "Lakebridge cross-checked 1,533 tools" and "the parse found
1,533 tools, uncorroborated" is the kind of thing a migration owner needs to
know up front.
