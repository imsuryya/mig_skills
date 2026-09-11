"""Persistent migration state.

SQLite is the single source of truth. The markdown plan files written by
``plan.py`` are a *projection* of this database, never the other way round --
that is what makes a migration resumable after context loss.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time

SCHEMA_VERSION = 1

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

-- One row per parsed Alteryx document (workflow + every macro it reaches).
CREATE TABLE IF NOT EXISTS files (
    file_id       TEXT PRIMARY KEY,   -- stable short id: w0, m1, m2 ...
    path          TEXT NOT NULL,
    kind          TEXT NOT NULL,      -- workflow | macro | app
    macro_type    TEXT,               -- standard | batch | iterative
    sha256        TEXT,
    tool_count    INTEGER DEFAULT 0,
    parsed_at     REAL,
    referenced_by TEXT                -- node_key of the macro node that pulled it in
);

-- One row per Node, including nested ChildNodes and disabled tools.
CREATE TABLE IF NOT EXISTS nodes (
    node_key     TEXT PRIMARY KEY,    -- "<file_id>:<ToolID>"
    file_id      TEXT NOT NULL,
    tool_id      TEXT NOT NULL,
    plugin       TEXT,                -- GuiSettings/@Plugin
    engine       TEXT,                -- EngineSettings/@EngineDllEntryPoint
    tool_name    TEXT,                -- resolved short name, e.g. "Formula"
    container    TEXT,                -- node_key of enclosing ToolContainer
    depth        INTEGER DEFAULT 0,
    disabled     INTEGER DEFAULT 0,   -- 1 if tool or any ancestor container is disabled
    caption      TEXT,
    x            REAL,
    y            REAL,
    config_xml   TEXT,                -- verbatim <Configuration> subtree
    config_hash  TEXT,                -- hash of normalised config, for dedupe
    macro_path   TEXT,                -- resolved macro this node invokes, if any
    is_container INTEGER DEFAULT 0,
    is_io        INTEGER DEFAULT 0    -- input/output tool: a pipeline boundary
);

-- Graph edges. Anchors matter: Join L/J/R, Filter T/F, Unique U/D.
CREATE TABLE IF NOT EXISTS edges (
    edge_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id       TEXT NOT NULL,
    origin        TEXT NOT NULL,      -- node_key
    origin_anchor TEXT,
    dest          TEXT NOT NULL,      -- node_key
    dest_anchor   TEXT,
    name          TEXT,
    wireless      INTEGER DEFAULT 0
);

-- Every verbatim expression, kept out of nodes so it stays cheap to query.
CREATE TABLE IF NOT EXISTS expressions (
    expr_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    node_key TEXT NOT NULL,
    kind     TEXT NOT NULL,           -- formula | filter | join_key | generate | action | ...
    field    TEXT,
    expr     TEXT NOT NULL
);

-- Determinism / portability hazards found during extract.
CREATE TABLE IF NOT EXISTS hazards (
    hazard_id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_key  TEXT NOT NULL,
    kind      TEXT NOT NULL,
    detail    TEXT
);

-- Lakebridge analyzer facts, one row per source file it inventoried.
CREATE TABLE IF NOT EXISTS lakebridge (
    lb_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT,
    name        TEXT,
    complexity  TEXT,
    type        TEXT,
    node_census TEXT,                 -- json {plugin: count}
    func_census TEXT,                 -- json {FUNC: count}
    statements  TEXT                  -- json [{nodeName, connectionType, objects, sql_head}]
);

-- Migration units: the unit of work, of planning, and of validation.
CREATE TABLE IF NOT EXISTS units (
    unit_id    TEXT PRIMARY KEY,
    seq        INTEGER,               -- topological order
    title      TEXT,
    kind       TEXT,                  -- container | macro | segment | io
    layer      TEXT,                  -- bronze | silver | gold | job | drop
    file_id    TEXT,
    tool_count INTEGER,
    status     TEXT DEFAULT 'pending',
      -- pending -> planned -> generated -> validated -> complete
      -- off-path: blocked (open blocking gap) | failed (validation failure)
    dedupe_of  TEXT,                  -- unit_id this is an exact structural duplicate of
    block_hash TEXT,
    notes      TEXT,
    updated_at REAL
);

CREATE TABLE IF NOT EXISTS unit_nodes (
    unit_id  TEXT NOT NULL,
    node_key TEXT NOT NULL,
    PRIMARY KEY (unit_id, node_key)
);

CREATE TABLE IF NOT EXISTS unit_deps (
    unit_id    TEXT NOT NULL,
    depends_on TEXT NOT NULL,
    via_edges  INTEGER DEFAULT 1,
    PRIMARY KEY (unit_id, depends_on)
);

-- Every decision, tagged by where it came from. This is the audit trail that
-- keeps extracted facts distinguishable from AI inference.
CREATE TABLE IF NOT EXISTS decisions (
    decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
    unit_id     TEXT,
    node_key    TEXT,
    basis       TEXT NOT NULL,        -- fact | mapping | inference | user
    summary     TEXT NOT NULL,
    detail      TEXT,
    confidence  TEXT,                 -- direct | adapted | needs-design | no-equivalent
    created_at  REAL
);

-- Unresolved questions. A unit with an open blocking gap can never be complete.
CREATE TABLE IF NOT EXISTS gaps (
    gap_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    unit_id     TEXT,
    node_key    TEXT,
    kind        TEXT NOT NULL,        -- missing-info | ambiguous-config | unsupported-tool |
                                      -- ambiguous-expr | column-mapping | unclear-type |
                                      -- complex-behavior
    question    TEXT NOT NULL,
    blocking    INTEGER DEFAULT 1,
    status      TEXT DEFAULT 'open',  -- open | resolved | escalated
    resolution  TEXT,
    resolved_by TEXT,                 -- fact | skill | user | inference
    created_at  REAL,
    resolved_at REAL
);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    unit_id     TEXT NOT NULL,
    path        TEXT NOT NULL,
    role        TEXT,                 -- pyspark | sdp | test | doc
    sha256      TEXT,
    written_at  REAL
);

CREATE TABLE IF NOT EXISTS validations (
    val_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    unit_id    TEXT,                  -- NULL = whole-migration check
    level      TEXT NOT NULL,         -- structural | semantic | code | data
    check_name TEXT NOT NULL,
    status     TEXT NOT NULL,         -- pass | fail | skip
    detail     TEXT,
    node_key   TEXT,
    ran_at     REAL
);

CREATE INDEX IF NOT EXISTS idx_nodes_file   ON nodes(file_id);
CREATE INDEX IF NOT EXISTS idx_nodes_hash   ON nodes(config_hash);
CREATE INDEX IF NOT EXISTS idx_edges_origin ON edges(origin);
CREATE INDEX IF NOT EXISTS idx_edges_dest   ON edges(dest);
CREATE INDEX IF NOT EXISTS idx_expr_node    ON expressions(node_key);
CREATE INDEX IF NOT EXISTS idx_un_unit      ON unit_nodes(unit_id);
CREATE INDEX IF NOT EXISTS idx_un_node      ON unit_nodes(node_key);
CREATE INDEX IF NOT EXISTS idx_val_unit     ON validations(unit_id, level);
CREATE INDEX IF NOT EXISTS idx_gaps_status  ON gaps(status, blocking);
"""


def connect(run_dir: str) -> sqlite3.Connection:
    os.makedirs(run_dir, exist_ok=True)
    con = sqlite3.connect(os.path.join(run_dir, "state.db"))
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    row = con.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    if row is None:
        con.execute(
            "INSERT INTO meta(key,value) VALUES('schema_version',?)", (str(SCHEMA_VERSION),)
        )
        con.commit()
    elif int(row["value"]) != SCHEMA_VERSION:
        raise SystemExit(
            "state.db schema v%s != engine v%s; start a new run directory."
            % (row["value"], SCHEMA_VERSION)
        )
    return con


def set_meta(con: sqlite3.Connection, key: str, value) -> None:
    if not isinstance(value, str):
        value = json.dumps(value)
    con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", (key, value))
    con.commit()


def get_meta(con: sqlite3.Connection, key: str, default=None):
    row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def get_meta_json(con: sqlite3.Connection, key: str, default=None):
    raw = get_meta(con, key)
    return json.loads(raw) if raw else default


def now() -> float:
    return time.time()
