"""One workbook holding both halves of the migration record.

Two independent sources describe the same workflow, and reviewers keep having to
open them side by side: Lakebridge's analyzer report (estate facts -- inventory,
complexity, function census, SQL endpoints) and `state.db` (the exhaustive parse
plus every unit, decision, gap and validation the run produced). This merges
them into a single .xlsx.

Lakebridge sheets are copied *verbatim and under their original names*, because
several of them carry formulas that reference their siblings by name
('Jobs Transformations Xref'!...). Renaming would silently break those. The
engine's own sheets are prefixed "MIG " so the two halves stay legible apart,
and every sheet carries a `basis` where one applies -- fact, mapping, inference
or user -- so extracted truth is never confused with AI judgement.
"""
from __future__ import annotations

import datetime
import json
import os

from . import db, lakebridge, units, xlsx

MIG_PREFIX = "MIG "
CONFIG_LIMIT = 30000        # under Excel's 32767 cap, with room for the ellipsis

SCOPE_WARNING = (
    "WARNING: no analyzer row matched a parsed file by name, so this compares the "
    "whole analyzed folder against this workflow. Usually the workflow was analyzed "
    "under a renamed copy. The structural 'lakebridge-census' validation check "
    "cannot run in this state -- re-run extract with the analyzer pointed at the "
    "original filename to get a real cross-check.")


def _ts(value):
    """Epoch float -> 'YYYY-MM-DD HH:MM:SS'. Excel sorts the text form fine."""
    if not value:
        return None
    try:
        return datetime.datetime.fromtimestamp(float(value)).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, OSError, OverflowError):
        return str(value)


def find_report_xlsx(con, run):
    """Locate the analyzer's .xlsx: beside the ingested json, or in <run>/lakebridge."""
    candidates = []
    recorded = db.get_meta(con, "lakebridge_xlsx")
    if recorded:
        candidates.append(recorded)
    js = db.get_meta(con, "lakebridge_json")
    if js:
        candidates.append(os.path.splitext(js)[0] + ".xlsx")
        folder = os.path.dirname(js)
        if os.path.isdir(folder):
            candidates += [os.path.join(folder, f) for f in sorted(os.listdir(folder))
                           if f.lower().endswith(".xlsx")]
    candidates.append(os.path.join(run, "lakebridge", "lakebridge-analysis.xlsx"))
    lb_dir = os.path.join(run, "lakebridge")
    if os.path.isdir(lb_dir):
        candidates += [os.path.join(lb_dir, f) for f in sorted(os.listdir(lb_dir))
                       if f.lower().endswith(".xlsx")]
    for c in candidates:
        if os.path.isfile(c):
            return c
    return None


# --------------------------------------------------------------------- mig sheets


def _node_lookup(con):
    """node_key -> (tool label, unit_id). Used to make edge/expression rows readable."""
    tools = {r["node_key"]: "%s (%s)" % (r["tool_name"] or "?", r["tool_id"])
             for r in con.execute("SELECT node_key, tool_name, tool_id FROM nodes")}
    unit_of = {r["node_key"]: r["unit_id"]
               for r in con.execute("SELECT node_key, unit_id FROM unit_nodes")}
    return tools, unit_of


def _overview(con, run, lb_source, complete):
    scope = db.get_meta_json(con, "scope", {}) or {}
    counts = {r["status"]: r["c"] for r in con.execute(
        "SELECT status, count(*) c FROM units GROUP BY 1")}
    one = lambda q: con.execute(q).fetchone()[0]  # noqa: E731
    rows = [["Field", "Value"],
            ["Generated", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
            ["Run directory", run],
            ["Source workflow", db.get_meta(con, "workflow_path")],
            ["Source / target", "%s -> %s" % (db.get_meta(con, "source"),
                                              db.get_meta(con, "target"))],
            ["Migration complete", "YES" if complete["complete"] else "NO"]]
    for b in complete["blockers"]:
        rows.append(["  blocker", b])
    rows += [[None, None], ["Parsed facts (state.db)", None],
             ["Files parsed", one("SELECT count(*) FROM files")],
             ["Tools parsed", one("SELECT count(*) FROM nodes")],
             ["Connections", one("SELECT count(*) FROM edges")],
             ["Expressions", one("SELECT count(*) FROM expressions")],
             ["Hazards", one("SELECT count(*) FROM hazards")],
             [None, None], ["Migration work", None],
             ["Units", one("SELECT count(*) FROM units")],
             ["Units by status", ", ".join("%s %d" % kv for kv in sorted(counts.items()))],
             ["Tools assigned to units", one("SELECT count(DISTINCT node_key) FROM unit_nodes")],
             ["Decisions recorded", one("SELECT count(*) FROM decisions")],
             ["Gaps open / total", "%d / %d" % (
                 one("SELECT count(*) FROM gaps WHERE status='open'"),
                 one("SELECT count(*) FROM gaps"))],
             ["Artifacts generated", one("SELECT count(*) FROM artifacts")]]

    try:
        unaccounted = units.unaccounted_nodes(con)
        rows.append(["Tools unaccounted for", len(unaccounted)])
    except Exception as exc:                      # never let a report fail the export
        rows.append(["Tools unaccounted for", "not computed: %s" % exc])

    rows += [[None, None], ["Lakebridge", None],
             ["Analyzer workbook", lb_source or "(not merged)"],
             ["Analyzer json", db.get_meta(con, "lakebridge_json") or "(none ingested)"]]
    cmp_ = lakebridge.census_compare(con, allow_unscoped=True)
    if cmp_:
        rows += [["Census: Lakebridge vs parsed",
                  "%d vs %d" % (cmp_["lakebridge_total"], cmp_["parsed_total"])],
                 ["Disagreeing tool types",
                  sum(1 for r in cmp_["rows"] if r["delta"])],
                 ["Analyzer files out of scope", cmp_["files_out_of_scope"]]]
        if not cmp_["scope_matched"]:
            rows.append(["Census scoping", SCOPE_WARNING])

    if scope:
        rows += [[None, None], ["Scope", None]]
        rows += [["  " + k, v if isinstance(v, (str, int, float)) else json.dumps(v)]
                 for k, v in sorted(scope.items())]
    return xlsx.Sheet(MIG_PREFIX + "Overview", rows, autofilter=False)


def _table(con, name, header, sql, rowfn):
    rows = [header]
    rows.extend(rowfn(r) for r in con.execute(sql))
    return xlsx.Sheet(MIG_PREFIX + name, rows)


def mig_sheets(con, run, lb_source, complete, full=False):
    tools, unit_of = _node_lookup(con)
    sheets = [_overview(con, run, lb_source, complete)]

    sheets.append(_table(
        con, "Files",
        ["file_id", "kind", "macro_type", "path", "tool_count", "sha256",
         "referenced_by", "parsed_at"],
        "SELECT * FROM files ORDER BY file_id",
        lambda r: [r["file_id"], r["kind"], r["macro_type"], r["path"], r["tool_count"],
                   r["sha256"], r["referenced_by"], _ts(r["parsed_at"])]))

    node_cols = ["node_key", "file_id", "tool_id", "tool_name", "plugin", "caption", "unit_id",
                 "container", "depth", "disabled", "is_container", "is_io", "macro_path",
                 "engine", "x", "y", "config_hash", "config_chars"]
    if full:
        node_cols.append("config_xml")

    def node_row(r):
        row = [r["node_key"], r["file_id"], r["tool_id"], r["tool_name"], r["plugin"],
               r["caption"], unit_of.get(r["node_key"]), r["container"], r["depth"],
               r["disabled"], r["is_container"], r["is_io"], r["macro_path"], r["engine"],
               r["x"], r["y"], r["config_hash"], len(r["config_xml"] or "")]
        if full:
            cfg = r["config_xml"] or ""
            row.append(cfg[:CONFIG_LIMIT] + ("..." if len(cfg) > CONFIG_LIMIT else ""))
        return row

    sheets.append(_table(con, "Tools", node_cols,
                         "SELECT * FROM nodes ORDER BY file_id, CAST(tool_id AS INTEGER)",
                         node_row))

    sheets.append(_table(
        con, "Connections",
        ["file_id", "origin", "origin_tool", "origin_anchor", "dest", "dest_tool",
         "dest_anchor", "name", "wireless"],
        "SELECT * FROM edges ORDER BY file_id, edge_id",
        lambda r: [r["file_id"], r["origin"], tools.get(r["origin"]), r["origin_anchor"],
                   r["dest"], tools.get(r["dest"]), r["dest_anchor"], r["name"],
                   r["wireless"]]))

    sheets.append(_table(
        con, "Expressions",
        ["node_key", "tool", "unit_id", "kind", "field", "expression"],
        "SELECT * FROM expressions ORDER BY node_key, expr_id",
        lambda r: [r["node_key"], tools.get(r["node_key"]), unit_of.get(r["node_key"]),
                   r["kind"], r["field"], r["expr"]]))

    sheets.append(_table(
        con, "Hazards",
        ["hazard_id", "node_key", "tool", "unit_id", "kind", "detail"],
        "SELECT * FROM hazards ORDER BY kind, hazard_id",
        lambda r: [r["hazard_id"], r["node_key"], tools.get(r["node_key"]),
                   unit_of.get(r["node_key"]), r["kind"], r["detail"]]))

    deps = {}
    for r in con.execute("SELECT unit_id, depends_on FROM unit_deps ORDER BY depends_on"):
        deps.setdefault(r["unit_id"], []).append(r["depends_on"])
    arts = {}
    for r in con.execute("SELECT unit_id, path FROM artifacts ORDER BY path"):
        arts.setdefault(r["unit_id"], []).append(os.path.basename(r["path"]))
    open_gaps = {r["unit_id"]: r["c"] for r in con.execute(
        "SELECT unit_id, count(*) c FROM gaps WHERE status='open' GROUP BY 1")}
    vals = {}
    for r in con.execute(
            "SELECT unit_id, status, count(*) c FROM validations GROUP BY 1,2"):
        vals.setdefault(r["unit_id"], {})[r["status"]] = r["c"]

    def unit_row(r):
        v = vals.get(r["unit_id"], {})
        return [r["seq"], r["unit_id"], r["title"], r["kind"], r["layer"], r["status"],
                r["file_id"], r["tool_count"], r["dedupe_of"],
                ", ".join(deps.get(r["unit_id"], [])), open_gaps.get(r["unit_id"], 0),
                v.get("pass", 0), v.get("fail", 0), v.get("skip", 0),
                ", ".join(arts.get(r["unit_id"], [])), r["block_hash"], r["notes"],
                _ts(r["updated_at"])]

    sheets.append(_table(
        con, "Units",
        ["seq", "unit_id", "title", "kind", "layer", "status", "file_id", "tool_count",
         "dedupe_of", "depends_on", "open_gaps", "val_pass", "val_fail", "val_skip",
         "artifacts", "block_hash", "notes", "updated_at"],
        "SELECT * FROM units ORDER BY seq", unit_row))

    sheets.append(_table(
        con, "Unit Tools", ["unit_id", "seq", "node_key", "tool", "layer"],
        "SELECT un.unit_id, un.node_key, u.seq, u.layer FROM unit_nodes un "
        "LEFT JOIN units u ON u.unit_id=un.unit_id ORDER BY u.seq, un.node_key",
        lambda r: [r["unit_id"], r["seq"], r["node_key"], tools.get(r["node_key"]),
                   r["layer"]]))

    sheets.append(_table(
        con, "Decisions",
        ["decision_id", "unit_id", "node_key", "tool", "basis", "confidence", "summary",
         "detail", "created_at"],
        "SELECT * FROM decisions ORDER BY decision_id",
        lambda r: [r["decision_id"], r["unit_id"], r["node_key"], tools.get(r["node_key"]),
                   r["basis"], r["confidence"], r["summary"], r["detail"],
                   _ts(r["created_at"])]))

    sheets.append(_table(
        con, "Gaps",
        ["gap_id", "unit_id", "node_key", "tool", "kind", "blocking", "status", "question",
         "resolution", "resolved_by", "created_at", "resolved_at"],
        "SELECT * FROM gaps ORDER BY status, blocking DESC, gap_id",
        lambda r: [r["gap_id"], r["unit_id"], r["node_key"], tools.get(r["node_key"]),
                   r["kind"], r["blocking"], r["status"], r["question"], r["resolution"],
                   r["resolved_by"], _ts(r["created_at"]), _ts(r["resolved_at"])]))

    sheets.append(_table(
        con, "Validations",
        ["val_id", "unit_id", "level", "check", "status", "detail", "node_key", "ran_at"],
        "SELECT * FROM validations ORDER BY unit_id IS NOT NULL, unit_id, level, check_name",
        lambda r: [r["val_id"], r["unit_id"] or "(migration)", r["level"], r["check_name"],
                   r["status"], r["detail"], r["node_key"], _ts(r["ran_at"])]))

    sheets.append(_table(
        con, "Artifacts", ["artifact_id", "unit_id", "role", "path", "sha256", "written_at"],
        "SELECT * FROM artifacts ORDER BY unit_id, path",
        lambda r: [r["artifact_id"], r["unit_id"], r["role"], r["path"], r["sha256"],
                   _ts(r["written_at"])]))

    cmp_ = lakebridge.census_compare(con, allow_unscoped=True)
    if cmp_:
        rows = [["tool", "lakebridge", "parsed", "delta", "agrees"]]
        rows += [[r["tool"], r["lakebridge"], r["parsed"], r["delta"],
                  "yes" if r["delta"] == 0 else "NO"] for r in cmp_["rows"]]
        rows.append(["TOTAL", cmp_["lakebridge_total"], cmp_["parsed_total"],
                     cmp_["parsed_total"] - cmp_["lakebridge_total"], None])
        if not cmp_["scope_matched"]:
            rows.append([None, None, None, None, None])
            rows.append([SCOPE_WARNING, None, None, None, None])
        sheets.append(xlsx.Sheet(MIG_PREFIX + "Census Crosscheck", rows))

    stmt_rows = [["source_file", "node", "connection_type", "complexity", "line_count",
                  "objects", "actions", "sql_chars", "sql_head"]]
    for r in con.execute("SELECT source_file, statements FROM lakebridge"):
        for st in json.loads(r["statements"] or "[]"):
            stmt_rows.append([r["source_file"], st.get("nodeName"), st.get("connectionType"),
                              st.get("complexity"), st.get("lineCount"),
                              ", ".join(st.get("objects") or []),
                              ", ".join(st.get("actions") or []),
                              st.get("sql_chars"), st.get("sql_head")])
    if len(stmt_rows) > 1:
        sheets.append(xlsx.Sheet(MIG_PREFIX + "SQL Endpoints", stmt_rows))
    return sheets


def lakebridge_fallback_sheets(con):
    """Rebuild the essentials from the ingested json when the .xlsx is gone.

    Degraded on purpose: the analyzer's own formatting and formulas are lost, but
    the facts the migration actually used are all in state.db, so the export is
    never silently missing the Lakebridge half.
    """
    rows = [["source_file", "name", "type", "complexity"]]
    census, funcs = {}, {}
    for r in con.execute("SELECT * FROM lakebridge ORDER BY source_file"):
        rows.append([r["source_file"], r["name"], r["type"], r["complexity"]])
        for k, v in json.loads(r["node_census"] or "{}").items():
            census[k] = census.get(k, 0) + v
        for k, v in json.loads(r["func_census"] or "{}").items():
            funcs[k] = funcs.get(k, 0) + v
    if len(rows) == 1:
        return []
    out = [xlsx.Sheet("LB Job Details", rows)]
    out.append(xlsx.Sheet("LB Transformations",
                          [["Transformation Type", "# of Occurrences"]]
                          + [[k, v] for k, v in sorted(census.items(), key=lambda kv: -kv[1])]))
    if funcs:
        out.append(xlsx.Sheet("LB Functions",
                              [["Function", "# of Calls"]]
                              + [[k, v] for k, v in sorted(funcs.items(), key=lambda kv: -kv[1])]))
    return out


def build(con, run, out_path, lb_xlsx=None, include_lakebridge=True, full=False,
          complete=None):
    """Write the merged workbook. Returns a summary dict."""
    complete = complete or {"complete": False, "blockers": ["completion not evaluated"]}
    lb_source, lb_sheets, degraded = None, [], None

    if include_lakebridge:
        path = lb_xlsx or find_report_xlsx(con, run)
        if path and os.path.isfile(path):
            try:
                taken = set()
                for name, rows in xlsx.read(path):
                    if rows:
                        # Original names on purpose: sibling formulas resolve by name.
                        lb_sheets.append(xlsx.Sheet(xlsx.safe_sheet_name(name, taken), rows))
                lb_source = os.path.abspath(path)
            except Exception as exc:
                degraded = "could not read %s (%s); rebuilt from state.db" % (path, exc)
                lb_sheets = []
        else:
            degraded = "analyzer .xlsx not found; rebuilt from state.db"
        if not lb_sheets:
            lb_sheets = lakebridge_fallback_sheets(con)
            if not lb_sheets and degraded:
                degraded = "no Lakebridge data in this run"

    sheets = mig_sheets(con, run, lb_source, complete, full=full)
    # Keep the engine's sheets first: this workbook is read as a migration
    # record, and the analyzer report is the supporting evidence behind it.
    taken = {s.name.lower() for s in sheets}
    for sh in lb_sheets:
        sh.name = xlsx.safe_sheet_name(sh.name, taken)
    sheets.extend(lb_sheets)

    os.makedirs(os.path.dirname(os.path.abspath(out_path)) or ".", exist_ok=True)
    xlsx.write(out_path, sheets)
    return {
        "path": os.path.abspath(out_path),
        "sheets": len(sheets),
        "mig_sheets": len(sheets) - len(lb_sheets),
        "lakebridge_sheets": len(lb_sheets),
        "lakebridge_source": lb_source,
        "rows": sum(len(s.rows) for s in sheets),
        "bytes": os.path.getsize(out_path),
        "degraded": degraded,
    }
