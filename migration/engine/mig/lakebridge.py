"""Lakebridge Analyzer integration.

Lakebridge is the primary source of *estate-level* facts: file inventory,
complexity band, tool census, function census, and the source/target endpoints
behind every Input/Output tool. For Alteryx it is Analyzer-only -- the support
matrix does not list Alteryx under Converter or Reconcile -- so it never emits
ToolIDs, per-tool configuration, or the connection graph. Those come from
parse.py. Both feed the same state.db; neither is re-read by the model.

Command shape (Lakebridge >= 0.10):

    databricks labs lakebridge analyze \
        --source-directory <dir> \
        --report-file <out.xlsx> \
        --source-tech alteryx \
        --generate-json true
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess

from . import db

SQL_HEAD_CHARS = 400   # keep endpoints and shape, drop the 49-line column list


def analyzer_command(source_dir: str, report_file: str,
                     source_tech: str = "alteryx") -> list[str]:
    return [
        "databricks", "labs", "lakebridge", "analyze",
        "--source-directory", source_dir,
        "--report-file", report_file,
        "--source-tech", source_tech,
        "--generate-json", "true",
    ]


def run_analyzer(source_dir: str, out_dir: str, timeout: int = 1800,
                 source_tech: str = "alteryx"):
    """Run the analyzer. Returns (json_path, stderr) or (None, reason)."""
    if not source_tech:
        return None, "this source adapter has no Lakebridge source-tech"
    if not shutil.which("databricks"):
        return None, "databricks CLI not on PATH"
    os.makedirs(out_dir, exist_ok=True)
    report = os.path.join(out_dir, "lakebridge-analysis.xlsx")
    try:
        proc = subprocess.run(
            analyzer_command(source_dir, report, source_tech),
            capture_output=True, text=True, timeout=timeout,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        return None, "analyzer did not run: %s" % exc
    js = os.path.splitext(report)[0] + ".json"
    if os.path.isfile(js):
        return js, proc.stderr[-2000:]
    # Some builds name the json after the report stem in the same folder.
    for cand in os.listdir(out_dir):
        if cand.endswith(".json"):
            return os.path.join(out_dir, cand), proc.stderr[-2000:]
    return None, (proc.stderr or proc.stdout or "no json report produced")[-2000:]


def ingest(con, json_path: str):
    """Load an analyzer JSON report into state.db, keeping only what migration needs."""
    with open(json_path, encoding="utf-8", errors="replace") as fh:
        data = json.load(fh)

    inventory = data.get("inventory") or []
    rows = []
    for rec in inventory:
        statements = []
        for st in rec.get("sqlStatements") or []:
            sql = st.get("sql") or ""
            statements.append({
                "nodeName": st.get("nodeName"),
                "connectionType": st.get("connectionType"),
                "complexity": st.get("complexityLevel"),
                "lineCount": st.get("lineCount"),
                "objects": [o.get("object") for o in (st.get("objectRel") or []) if o.get("object")],
                "actions": sorted({o.get("action") for o in (st.get("objectRel") or []) if o.get("action")}),
                "sql_head": sql[:SQL_HEAD_CHARS],
                "sql_chars": len(sql),
            })
        rows.append((
            rec.get("sourceFile"), rec.get("name"), rec.get("complexityLevel"),
            rec.get("type"), json.dumps(rec.get("nodes") or {}),
            json.dumps(rec.get("functionCall") or {}), json.dumps(statements),
        ))

    con.execute("DELETE FROM lakebridge")
    con.executemany(
        "INSERT INTO lakebridge(source_file,name,complexity,type,node_census,"
        "func_census,statements) VALUES(?,?,?,?,?,?,?)", rows)
    db.set_meta(con, "lakebridge_run", data.get("runInfo") or {})
    db.set_meta(con, "lakebridge_json", os.path.abspath(json_path))
    con.commit()

    census = {}
    for r in rows:
        for k, v in json.loads(r[4]).items():
            census[k] = census.get(k, 0) + v
    return {
        "files": len(rows),
        "tools": sum(census.values()),
        "distinct_tool_types": len(census),
        "statements": sum(len(json.loads(r[6])) for r in rows),
    }


def in_scope(con):
    """The set of analyzer rows that describe *this* workflow.

    `analyze --source-directory` sweeps a whole tree, so a workflow sitting in a
    shared folder drags in every unrelated object beside it. Scope by the files
    the parse actually walked -- the workflow and the macros it reaches -- so an
    estate total is never mistaken for a workflow total.
    """
    parsed = {}
    for r in con.execute("SELECT file_id, path FROM files"):
        key = os.path.basename(os.path.normpath(r["path"].replace("\\", "/"))).lower()
        parsed[key] = r["file_id"]
    if not parsed:
        return None, 0, set()
    rows, out_of_scope, file_ids = [], 0, set()
    for row in con.execute("SELECT source_file, node_census FROM lakebridge"):
        src = (row["source_file"] or "").replace("\\", "/")
        fid = parsed.get(os.path.basename(src).lower())
        if fid is not None:
            rows.append(row)
            file_ids.add(fid)
        else:
            out_of_scope += 1
    return rows, out_of_scope, file_ids


def normalize_tool_name(name):
    """Reduce both sides' spelling of a tool to one comparable key.

    Lakebridge reports "AlteryxCrossTab" or "AlteryxBasePluginsGui.X.X"; our
    parse reports the trailing plugin segment, "CrossTab". Macro nodes arrive as
    "Macro:<file>" from Lakebridge and as a resolved path from the parse.
    """
    if name.startswith("Macro:"):
        return "macro:" + os.path.basename(name[6:].replace("\\", "/")).lower()
    base = name.split(".")[-1]
    if base.startswith("Alteryx") and len(base) > 7:
        base = base[7:]
    return base.lower()


def census_compare(con, allow_unscoped=False):
    """Per-tool-type census from both sides, every type, agreeing or not.

    ``cross_check`` keeps only the disagreements because that is all the model
    needs; the Excel export wants the whole table so a reviewer can see what
    matched as well as what did not.

    ``allow_unscoped`` covers a real failure mode: scoping matches analyzer rows
    to parsed files by basename, so a workflow analyzed under a renamed copy --
    an ASCII-safe name, say, because the original has non-ASCII characters --
    scopes to nothing and the comparison silently disappears. Reporting tools
    pass True to fall back to every analyzer row and say so via
    ``scope_matched``; validation leaves it False so the check still refuses to
    claim a pass it did not earn.
    """
    scoped, out_of_scope, file_ids = in_scope(con)
    if scoped is None:
        return None
    scope_matched = bool(scoped)
    if not scope_matched:
        if not allow_unscoped:
            return None
        scoped = list(con.execute("SELECT source_file, node_census FROM lakebridge"))
        out_of_scope, file_ids = 0, set()
    lb = {}
    for row in scoped:
        for k, v in json.loads(row["node_census"]).items():
            lb[k] = lb.get(k, 0) + v
    if not lb:
        return None

    # Compare like with like. The analyzer reports per file and does not descend
    # into macros, so count only the parsed tools belonging to the files it
    # actually reported on. Counting every parsed node instead makes any workflow
    # that calls a macro show a permanent false disagreement the size of the
    # expanded macro bodies -- a failure no amount of migration work can clear.
    if file_ids:
        marks = ",".join("?" * len(file_ids))
        cur = con.execute(
            "SELECT tool_name, count(*) c FROM nodes WHERE file_id IN (%s) "
            "GROUP BY 1" % marks, tuple(sorted(file_ids)))
    else:
        cur = con.execute("SELECT tool_name, count(*) c FROM nodes GROUP BY 1")
    parsed = {}
    for row in cur:
        parsed[row["tool_name"]] = parsed.get(row["tool_name"], 0) + row["c"]

    lb_n, p_n = {}, {}
    for k, v in lb.items():
        key = normalize_tool_name(k)
        lb_n[key] = lb_n.get(key, 0) + v
    for k, v in parsed.items():
        key = normalize_tool_name(k)
        p_n[key] = p_n.get(key, 0) + v

    rows = [{"tool": k, "lakebridge": lb_n.get(k, 0), "parsed": p_n.get(k, 0),
             "delta": p_n.get(k, 0) - lb_n.get(k, 0)}
            for k in sorted(set(lb_n) | set(p_n))]
    return {
        "rows": rows,
        "lakebridge_total": sum(lb_n.values()),
        "parsed_total": sum(p_n.values()),
        "files_out_of_scope": out_of_scope,
        "scope_matched": scope_matched,
    }


def cross_check(con):
    """Compare Lakebridge's census with our parse. Disagreement means one side
    missed something, and a silent miss is exactly what we must not ship."""
    cmp_ = census_compare(con)
    if cmp_ is None:
        return None
    return {
        "lakebridge_total": cmp_["lakebridge_total"],
        "parsed_total": cmp_["parsed_total"],
        "mismatched_types": [{"tool": r["tool"], "lakebridge": r["lakebridge"],
                              "parsed": r["parsed"]}
                             for r in cmp_["rows"] if r["delta"]],
        "files_out_of_scope": cmp_["files_out_of_scope"],
    }
