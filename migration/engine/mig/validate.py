"""Validation. Generating code is not evidence that the migration is correct.

Four levels, run independently so a failure names one component rather than
condemning the whole migration:

  structural -- nothing was silently dropped; the DAG survived
  semantic   -- the Alteryx behavior actually made it into the target
  code       -- the Python parses, resolves, and obeys the target-code rules
  data       -- source and target agree on real data (when a parity manifest exists)

Every check writes a row to `validations`, so `mig status` and the final report
can state validation status per unit rather than in aggregate.
"""
from __future__ import annotations

import ast
import builtins
import json
import os
import re
from collections import defaultdict

from . import db, lakebridge, sources, targets, units

SAFE_BUILTINS = set(dir(builtins)) | {"__name__", "__file__", "__doc__"}


class _CheckCtx:
    """Handed to source/target adapter hooks so they can record their own checks
    without knowing anything about the validations table."""

    def __init__(self, **kw):
        self.__dict__.update(kw)


def _adapters(con):
    return (sources.get(db.get_meta(con, "source", "alteryx")),
            targets.get(db.get_meta(con, "target", "databricks-sdp")))


def _record(con, unit_id, level, check, status, detail=None, node_key=None):
    con.execute(
        "INSERT INTO validations(unit_id,level,check_name,status,detail,node_key,ran_at) "
        "VALUES(?,?,?,?,?,?,?)", (unit_id, level, check, status, detail, node_key, db.now()))


def _clear(con, unit_id, level=None):
    if unit_id is None and level is None:
        con.execute("DELETE FROM validations")
    elif unit_id is None:
        con.execute("DELETE FROM validations WHERE unit_id IS NULL AND level=?", (level,))
    elif level is None:
        con.execute("DELETE FROM validations WHERE unit_id=?", (unit_id,))
    else:
        con.execute("DELETE FROM validations WHERE unit_id=? AND level=?", (unit_id, level))


# --------------------------------------------------------------------------
# structural
# --------------------------------------------------------------------------

def structural(con):
    _clear(con, None, "structural")

    total = con.execute("SELECT count(*) c FROM nodes").fetchone()["c"]
    placed = con.execute("SELECT count(DISTINCT node_key) c FROM unit_nodes").fetchone()["c"]
    excluded = units.excluded_nodes(con)
    missing = units.unaccounted_nodes(con)
    if not missing and placed + len(excluded) == total:
        _record(con, None, "structural", "tool-coverage", "pass",
                "%d tools: %d in units, %d excluded with a stated reason"
                % (total, placed, len(excluded)))
    else:
        _record(con, None, "structural", "tool-coverage", "fail",
                "%d tools: %d in units, %d legitimately excluded, %d unaccounted (e.g. %s)"
                % (total, placed, len(excluded), len(missing),
                   ", ".join("%s %s" % (m["node_key"], m["tool"]) for m in missing[:5])
                   or "none"))

    dup = con.execute(
        "SELECT node_key, count(*) c FROM unit_nodes GROUP BY 1 HAVING c>1").fetchall()
    _record(con, None, "structural", "no-tool-in-two-units",
            "pass" if not dup else "fail",
            "ok" if not dup else "%d tool(s) assigned twice, e.g. %s"
            % (len(dup), dup[0]["node_key"]))

    cc = lakebridge.cross_check(con)
    if cc is None:
        _record(con, None, "structural", "lakebridge-parity", "skip",
                "no Lakebridge report ingested; run `mig extract --lakebridge-json`")
    elif not cc["mismatched_types"]:
        _record(con, None, "structural", "lakebridge-parity", "pass",
                "Lakebridge and the XML parse agree on all %d tools" % cc["parsed_total"])
    else:
        _record(con, None, "structural", "lakebridge-parity", "fail",
                "census disagreement: %s" % json.dumps(cc["mismatched_types"][:8]))

    # Every cross-unit edge must be represented by a declared unit dependency.
    owner = {r["node_key"]: r["unit_id"] for r in
             con.execute("SELECT unit_id,node_key FROM unit_nodes")}
    deps = {(r["unit_id"], r["depends_on"]) for r in
            con.execute("SELECT unit_id,depends_on FROM unit_deps")}
    missing = set()
    for e in con.execute("SELECT origin,dest FROM edges"):
        uo, ud = owner.get(e["origin"]), owner.get(e["dest"])
        if uo and ud and uo != ud and (ud, uo) not in deps:
            missing.add((ud, uo))
    _record(con, None, "structural", "dependency-preservation",
            "pass" if not missing else "fail",
            "every cross-unit connection has a declared dependency"
            if not missing else "%d undeclared: %s" % (len(missing), sorted(missing)[:5]))

    # Boundaries: every IO tool must end up in a unit with a layer assigned.
    io_unplaced = con.execute(
        "SELECT count(*) c FROM nodes n LEFT JOIN unit_nodes un USING(node_key) "
        "WHERE n.is_io=1 AND un.unit_id IS NULL").fetchone()["c"]
    _record(con, None, "structural", "io-accounted",
            "pass" if io_unplaced == 0 else "fail",
            "all input/output tools belong to a unit" if io_unplaced == 0
            else "%d input/output tool(s) in no unit" % io_unplaced)

    io_nolayer = con.execute(
        "SELECT count(DISTINCT u.unit_id) c FROM units u JOIN unit_nodes un USING(unit_id) "
        "JOIN nodes n USING(node_key) WHERE n.is_io=1 AND (u.layer IS NULL OR u.layer='')"
    ).fetchone()["c"]
    _record(con, None, "structural", "io-layer-assigned",
            "pass" if io_nolayer == 0 else "fail",
            "every boundary unit has a target layer" if io_nolayer == 0
            else "%d unit(s) hold IO tools but have no layer assigned" % io_nolayer)

    # Unresolved macros are unknown semantics; they can never pass silently.
    open_block = con.execute(
        "SELECT count(*) c FROM gaps WHERE status='open' AND blocking=1").fetchone()["c"]
    _record(con, None, "structural", "no-open-blocking-gaps",
            "pass" if open_block == 0 else "fail",
            "no blocking gaps open" if open_block == 0
            else "%d blocking gap(s) still open" % open_block)

    con.commit()
    return summary(con, level="structural")


# --------------------------------------------------------------------------
# code
# --------------------------------------------------------------------------

def _bound_names(tree):
    bound = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Name) and isinstance(n.ctx, (ast.Store, ast.Del)):
            bound.add(n.id)
        elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bound.add(n.name)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                a = n.args
                for arg in list(a.args) + list(a.posonlyargs) + list(a.kwonlyargs):
                    bound.add(arg.arg)
                if a.vararg:
                    bound.add(a.vararg.arg)
                if a.kwarg:
                    bound.add(a.kwarg.arg)
        elif isinstance(n, (ast.Import, ast.ImportFrom)):
            for al in n.names:
                bound.add((al.asname or al.name).split(".")[0])
        elif isinstance(n, ast.ExceptHandler) and n.name:
            bound.add(n.name)
        elif isinstance(n, (ast.Global, ast.Nonlocal)):
            bound.update(n.names)
    return bound


def _dropped_with_reason(con, unit_id):
    """A unit deliberately layered 'drop' needs no target code.

    The migration model allows a tool to be legitimately excluded with a
    stated reason. We honour that here only when the reason is actually on
    record -- i.e. a doc artifact is attached -- so a unit cannot be waved
    through by setting its layer alone. Returns the doc path, or None.
    """
    row = con.execute("SELECT layer FROM units WHERE unit_id=?", (unit_id,)).fetchone()
    if not row or (row["layer"] or "") != "drop":
        return None
    doc = con.execute(
        "SELECT path FROM artifacts WHERE unit_id=? AND role='doc'", (unit_id,)).fetchone()
    if doc and os.path.isfile(doc["path"]):
        return doc["path"]
    return None


def code(con, unit_id, artifact_paths=None):
    _clear(con, unit_id, "code")
    _source, target = _adapters(con)
    paths = artifact_paths or [r["path"] for r in con.execute(
        "SELECT path FROM artifacts WHERE unit_id=? AND role IN ('pyspark','sdp')", (unit_id,))]
    paths = [p for p in paths if os.path.isfile(p)]
    if not paths:
        doc = _dropped_with_reason(con, unit_id)
        if doc:
            _record(con, unit_id, "code", "artifact-present", "skip",
                    "unit layered 'drop'; reason recorded in %s" % os.path.basename(doc))
            con.commit()
            return summary(con, unit_id, "code")
        _record(con, unit_id, "code", "artifact-present", "fail",
                "no generated code recorded for this unit")
        con.commit()
        return summary(con, unit_id, "code")

    for path in paths:
        with open(path, encoding="utf-8", errors="replace") as fh:
            src = fh.read()
        label = os.path.basename(path)

        try:
            tree = ast.parse(src, filename=path)
            _record(con, unit_id, "code", "syntax:%s" % label, "pass", "parses as Python")
        except SyntaxError as exc:
            _record(con, unit_id, "code", "syntax:%s" % label, "fail",
                    "line %s: %s" % (exc.lineno, exc.msg))
            continue

        bound = _bound_names(tree) | SAFE_BUILTINS | set(target.runtime_globals)
        unresolved = sorted({
            n.id for n in ast.walk(tree)
            if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load) and n.id not in bound
        })
        _record(con, unit_id, "code", "names-resolve:%s" % label,
                "pass" if not unresolved else "fail",
                "all referenced names are defined or imported" if not unresolved
                else "undefined name(s): %s" % ", ".join(unresolved[:12]))

        hits = []
        for check, pat, why in target.forbidden:
            for m in pat.finditer(src):
                line = src[:m.start()].count("\n") + 1
                hits.append("%s at line %d (%s)" % (check, line, why))
        _record(con, unit_id, "code", "target-code-rules:%s" % label,
                "pass" if not hits else "fail",
                "code obeys the %s target rules" % target.name if not hits
                else "; ".join(hits[:8]))

        target.code_checks(_CheckCtx(
            tree=tree, src=src, label=label,
            record=lambda c, s, d: _record(con, unit_id, "code", c, s, d)))

    con.commit()
    return summary(con, unit_id, "code")


# --------------------------------------------------------------------------
# semantic
# --------------------------------------------------------------------------

def _created_fields(con, unit_id):
    """Field names the Alteryx unit creates. If a created field never appears in
    the generated code, a transformation was dropped."""
    out = set()
    for r in con.execute(
            "SELECT e.kind, e.field FROM unit_nodes un JOIN expressions e USING(node_key) "
            "WHERE un.unit_id=? AND e.kind IN ('formula','aggregate')", (unit_id,)):
        f = (r["field"] or "").strip()
        if f and not f.startswith("*"):
            out.add(f)
    return out


def semantic(con, unit_id, artifact_paths=None):
    _clear(con, unit_id, "semantic")
    source, _target = _adapters(con)
    paths = artifact_paths or [r["path"] for r in con.execute(
        "SELECT path FROM artifacts WHERE unit_id=? AND role IN ('pyspark','sdp')", (unit_id,))]
    paths = [p for p in paths if os.path.isfile(p)]
    if not paths:
        doc = _dropped_with_reason(con, unit_id)
        if doc:
            _record(con, unit_id, "semantic", "artifact-present", "skip",
                    "unit layered 'drop'; reason recorded in %s" % os.path.basename(doc))
            con.commit()
            return summary(con, unit_id, "semantic")
        _record(con, unit_id, "semantic", "artifact-present", "fail",
                "no generated code recorded for this unit")
        con.commit()
        return summary(con, unit_id, "semantic")

    src = ""
    for p in paths:
        with open(p, encoding="utf-8", errors="replace") as fh:
            src += fh.read() + "\n"

    census = defaultdict(int)
    for r in con.execute(
            "SELECT n.tool_name, count(*) c FROM unit_nodes un JOIN nodes n USING(node_key) "
            "WHERE un.unit_id=? AND n.disabled=0 GROUP BY 1", (unit_id,)):
        census[r["tool_name"]] = r["c"]

    # Generic: every tool class must show its target construct somewhere.
    missing = []
    for tool, count in sorted(census.items()):
        req = source.required_constructs.get(tool)
        if req and not req[0].search(src):
            missing.append("%s x%d -> no %s in the code" % (tool, count, req[1]))
    _record(con, unit_id, "semantic", "tool-constructs",
            "pass" if not missing else "fail",
            "every tool class has its target construct" if not missing
            else "; ".join(missing[:8]))

    # Generic: every field the source unit creates must exist in the target.
    created = _created_fields(con, unit_id)
    if created:
        absent = sorted(f for f in created if f not in src)
        _record(con, unit_id, "semantic", "created-fields-present",
                "pass" if not absent else "fail",
                "all %d created field(s) appear in the code" % len(created) if not absent
                else "%d of %d created field(s) missing: %s"
                     % (len(absent), len(created), ", ".join(absent[:10])))

    # Platform semantics (null handling, aggregate definitions, ordering rules,
    # hazard follow-through) belong to the source adapter.
    exprs = [r["expr"] for r in con.execute(
        "SELECT e.expr FROM unit_nodes un JOIN expressions e USING(node_key) "
        "WHERE un.unit_id=?", (unit_id,))]
    hazards = [r["kind"] for r in con.execute(
        "SELECT DISTINCT h.kind FROM unit_nodes un JOIN hazards h USING(node_key) "
        "WHERE un.unit_id=?", (unit_id,))]
    source.semantic_checks(_CheckCtx(
        src=src, census=dict(census), expressions=exprs, hazards=hazards,
        record=lambda c, s, d: _record(con, unit_id, "semantic", c, s, d)))

    con.commit()
    return summary(con, unit_id, "semantic")


# --------------------------------------------------------------------------
# data
# --------------------------------------------------------------------------

def data(con, unit_id, manifest_path=None):
    """Compare against captured Alteryx reference output when one exists.

    A manifest is JSON: {"<unit_id>": {"row_count": N, "columns": [...],
    "aggregates": {"col": value}, "tolerance": {"col": 0.01}}}
    Absent evidence, this records `skip` with the reason -- never `pass`.
    """
    _clear(con, unit_id, "data")
    manifest_path = manifest_path or db.get_meta(con, "parity_manifest")
    if not manifest_path or not os.path.isfile(manifest_path):
        _record(con, unit_id, "data", "parity-manifest", "skip",
                "no parity manifest; capture Alteryx reference output to enable "
                "data validation (see references/validation-levels.md)")
        con.commit()
        return summary(con, unit_id, "data")

    with open(manifest_path, encoding="utf-8") as fh:
        man = json.load(fh)
    exp = man.get(unit_id)
    if not exp:
        _record(con, unit_id, "data", "parity-manifest", "skip",
                "manifest has no expectations for this unit")
        con.commit()
        return summary(con, unit_id, "data")

    actual = exp.get("actual")
    if not actual:
        _record(con, unit_id, "data", "parity-actual", "skip",
                "manifest has expectations but no captured target results; run the "
                "reconciliation notebook and write results back into the manifest")
        con.commit()
        return summary(con, unit_id, "data")

    if "row_count" in exp:
        ok = actual.get("row_count") == exp["row_count"]
        _record(con, unit_id, "data", "row-count", "pass" if ok else "fail",
                "source %s vs target %s" % (exp["row_count"], actual.get("row_count")))
    if exp.get("columns"):
        missing = [c for c in exp["columns"] if c not in (actual.get("columns") or [])]
        order_ok = (actual.get("columns") or [])[:len(exp["columns"])] == exp["columns"]
        _record(con, unit_id, "data", "schema", "pass" if not missing and order_ok else "fail",
                "column set and order match" if not missing and order_ok
                else "missing %s; order_match=%s" % (missing[:8], order_ok))
    for col, want in (exp.get("aggregates") or {}).items():
        got = (actual.get("aggregates") or {}).get(col)
        tol = (exp.get("tolerance") or {}).get(col, 0)
        if got is None:
            _record(con, unit_id, "data", "aggregate:%s" % col, "fail", "not captured")
        else:
            ok = abs(float(got) - float(want)) <= float(tol)
            _record(con, unit_id, "data", "aggregate:%s" % col, "pass" if ok else "fail",
                    "source %s vs target %s (tolerance %s)" % (want, got, tol))
    con.commit()
    return summary(con, unit_id, "data")


# --------------------------------------------------------------------------

def summary(con, unit_id=None, level=None):
    q = "SELECT level,status,count(*) c FROM validations WHERE 1=1"
    args = []
    if unit_id is not None:
        q += " AND unit_id=?"
        args.append(unit_id)
    if level:
        q += " AND level=?"
        args.append(level)
    q += " GROUP BY 1,2"
    out = defaultdict(lambda: {"pass": 0, "fail": 0, "skip": 0})
    for r in con.execute(q, args):
        out[r["level"]][r["status"]] = r["c"]
    return {k: dict(v) for k, v in out.items()}


def failures(con, unit_id=None):
    q = "SELECT * FROM validations WHERE status='fail'"
    args = []
    if unit_id is not None:
        q += " AND unit_id=?"
        args.append(unit_id)
    return [dict(r) for r in con.execute(q + " ORDER BY level, check_name", args)]


def unit_verdict(con, unit_id):
    """A unit is validated only when nothing failed and no blocking gap is open."""
    fails = failures(con, unit_id)
    blocking = con.execute(
        "SELECT count(*) c FROM gaps g LEFT JOIN unit_nodes un ON g.node_key=un.node_key "
        "WHERE g.status='open' AND g.blocking=1 AND (g.unit_id=? OR un.unit_id=?)",
        (unit_id, unit_id)).fetchone()["c"]
    levels = summary(con, unit_id)
    ran = set(levels)
    required = {"code", "semantic"}
    if not required <= ran:
        return "incomplete", "not yet validated at: %s" % ", ".join(sorted(required - ran))
    if fails:
        return "failed", "%d failing check(s): %s" % (
            len(fails), "; ".join("%s/%s" % (f["level"], f["check_name"]) for f in fails[:5]))
    if blocking:
        return "blocked", "%d open blocking gap(s)" % blocking
    return "validated", "all checks pass"
