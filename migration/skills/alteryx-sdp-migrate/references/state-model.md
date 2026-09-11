# State Model

`state.db` (SQLite, in the run directory) is the single source of truth. The
markdown under `.planning/` is a projection written by `mig plan`. Edit state
through the CLI; never hand-edit the markdown and expect it to stick.

Read this when you need a fact the CLI does not surface directly.

## Tables

| Table | One row per | Key columns |
|-------|-------------|-------------|
| `meta` | setting | `key`, `value` (workflow_path, scope, macro_paths, lakebridge_json, parity_manifest) |
| `files` | parsed document | `file_id` (`w0`, `m1`...), `path`, `kind`, `macro_type`, `tool_count`, `referenced_by` |
| `nodes` | Alteryx tool | `node_key` = `<file_id>:<ToolID>`, `tool_name`, `container`, `disabled`, `config_xml`, `config_hash`, `macro_path`, `is_io` |
| `edges` | connection | `origin`/`dest` node_key, `origin_anchor`/`dest_anchor`, `name`, `wireless` |
| `expressions` | verbatim expression | `node_key`, `kind`, `field`, `expr` |
| `hazards` | determinism/portability risk | `node_key`, `kind`, `detail` |
| `lakebridge` | analyzed source file | `node_census`, `func_census`, `statements` (all JSON) |
| `units` | migration unit | `unit_id`, `seq`, `title`, `kind`, `layer`, `status`, `dedupe_of`, `block_hash` |
| `unit_nodes` | unit-tool membership | `unit_id`, `node_key` |
| `unit_deps` | unit dependency | `unit_id`, `depends_on` |
| `decisions` | recorded decision | `basis`, `summary`, `confidence`, `unit_id`, `node_key` |
| `gaps` | open question | `kind`, `question`, `blocking`, `status`, `resolution`, `resolved_by` |
| `artifacts` | generated file | `unit_id`, `path`, `role`, `sha256` |
| `validations` | check result | `unit_id`, `level`, `check_name`, `status`, `detail` |

`node_key` is namespaced by file because `ToolID` is unique per document, not
across a workflow and its macros.

## Status values

`units.status`: `pending` -> `planned` -> `generated` -> `validated` ->
`complete`, with `blocked` (open blocking gap) and `failed` (validation failure)
off the path.

`gaps.status`: `open` | `resolved` | `escalated`. Escalated means a human
decision is required and has been handed over -- it still blocks completion.

`decisions.basis`: `fact` | `mapping` | `inference` | `user`. This is the column
that keeps extracted facts distinguishable from model inference forever.

`decisions.confidence`: `direct` (1:1) | `adapted` (works with a documented
semantic change) | `needs-design` (needs a team decision) | `no-equivalent`.

## Useful queries

```sql
-- Where is the migration actually stuck?
SELECT status, count(*) FROM units GROUP BY 1;
SELECT unit_id, title, notes FROM units WHERE status IN ('failed','blocked') ORDER BY seq;

-- Which tool types dominate the remaining work?
SELECT n.tool_name, count(*) c FROM units u
  JOIN unit_nodes un USING(unit_id) JOIN nodes n USING(node_key)
 WHERE u.status != 'complete' GROUP BY 1 ORDER BY c DESC LIMIT 15;

-- Every expression for one tool, verbatim.
SELECT kind, field, expr FROM expressions WHERE node_key = 'w0:1842';

-- Repeated blocks: the de-duplication payoff.
SELECT dedupe_of, count(*) c FROM units WHERE dedupe_of IS NOT NULL
 GROUP BY 1 ORDER BY c DESC;

-- Identical tool configurations across the whole workflow.
SELECT config_hash, tool_name, count(*) c FROM nodes
 WHERE is_container = 0 GROUP BY 1,2 HAVING c > 1 ORDER BY c DESC LIMIT 20;

-- What has been inferred rather than established?
SELECT unit_id, confidence, summary FROM decisions WHERE basis = 'inference';

-- All failing checks, most structural first.
SELECT unit_id, level, check_name, detail FROM validations
 WHERE status = 'fail' ORDER BY level, unit_id;

-- Determinism hazards still sitting in incomplete units.
SELECT h.kind, count(*) c FROM hazards h
  JOIN unit_nodes un USING(node_key) JOIN units u USING(unit_id)
 WHERE u.status != 'complete' GROUP BY 1 ORDER BY c DESC;

-- The unit dependency frontier.
SELECT u.unit_id, u.title FROM units u
 WHERE u.status = 'pending' AND NOT EXISTS (
   SELECT 1 FROM unit_deps d JOIN units x ON x.unit_id = d.depends_on
    WHERE d.unit_id = u.unit_id AND x.status NOT IN ('complete','validated'));
```

## Working with it directly

```bash
python -c "import sys;sys.path.insert(0,'scripts');from mig import db;
c=db.connect('$MIG_RUN');print([tuple(r) for r in c.execute('SELECT status,count(*) FROM units GROUP BY 1')])"
```

Prefer `mig` commands where one exists -- they keep `updated_at`, status
transitions, and duplicate-unit closure consistent. Drop to SQL for questions
the CLI does not answer, and keep the output small.

## Portability

`state.db` holds absolute paths to the workflow, macros, and generated
artifacts. Moving a run directory to another machine requires those paths to
still resolve, or a fresh `init` + `extract`. The database is otherwise
self-contained: stdlib `sqlite3`, no extensions.
