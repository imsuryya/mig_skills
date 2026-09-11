"""Build the smallest context that is still sufficient to migrate one unit.

This is where token efficiency is actually won or lost. The raw XML for a
40-tool unit runs to tens of thousands of characters, most of it cached
metadata, canvas styling, and field lists that repeat the schema verbatim. A
context pack keeps what changes behavior -- expressions, keys, actions, modes,
renames, boundaries -- and counts the rest.

Every pack states its own provenance: facts came from the parse, mappings from
the repo's skills, and anything else is an open question, never a guess.
"""
from __future__ import annotations

import json
import os
import re
from collections import Counter, defaultdict

from . import db, retrieve, sources

MAX_EXPR_PER_TOOL = 25
_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def source_for(con):
    """The source adapter this run was started with."""
    return sources.get(db.get_meta(con, "source", "alteryx"))


def describe(node, source=None):
    """Compact, behavior-bearing summary of one tool. The adapter knows how."""
    return (source or sources.get("alteryx")).describe(node)


# --------------------------------------------------------------------------

def unit_census(con, unit_id, source=None):
    source = source or source_for(con)
    known_funcs = getattr(source, "func_terms", {}) or {}
    tools = Counter()
    for r in con.execute(
            "SELECT n.tool_name FROM unit_nodes u JOIN nodes n USING(node_key) "
            "WHERE u.unit_id=?", (unit_id,)):
        tools[r["tool_name"]] += 1
    funcs = Counter()
    for r in con.execute(
            "SELECT e.expr FROM unit_nodes u JOIN expressions e USING(node_key) "
            "WHERE u.unit_id=?", (unit_id,)):
        for tok in {w.upper() for w in _WORD.findall(r["expr"] or "")}:
            if tok in known_funcs:
                funcs[tok] += 1
    return tools, funcs


def boundaries(con, unit_id):
    """Which dataframes enter this unit and which leave it -- the contract the
    generated code must satisfy."""
    members = {r["node_key"] for r in con.execute(
        "SELECT node_key FROM unit_nodes WHERE unit_id=?", (unit_id,))}
    owner = {}
    for r in con.execute("SELECT unit_id,node_key FROM unit_nodes"):
        owner[r["node_key"]] = r["unit_id"]
    inbound, outbound = [], []
    for r in con.execute("SELECT * FROM edges"):
        o, d = r["origin"], r["dest"]
        if d in members and o not in members:
            inbound.append({"from_unit": owner.get(o), "from_node": o,
                            "from_anchor": r["origin_anchor"],
                            "into_node": d, "into_anchor": r["dest_anchor"]})
        elif o in members and d not in members:
            outbound.append({"to_unit": owner.get(d), "to_node": d,
                             "to_anchor": r["dest_anchor"],
                             "from_node": o, "from_anchor": r["origin_anchor"]})
    return inbound, outbound




_NODENAME_ID = re.compile(r"_(\d+)$")


def lakebridge_endpoints(con, unit_id):
    """Endpoint facts Lakebridge resolved for *this unit's* IO tools.

    Lakebridge names statement nodes `<Tool>_<ToolID>` (FileInput_1023), so the
    trailing id joins its rows to our parse. Without this filter every unit would
    carry all 37 of the workflow's statements, which is exactly the kind of
    whole-workflow context this design exists to avoid.
    """
    want = {r["tool_id"] for r in con.execute(
        "SELECT n.tool_id FROM unit_nodes u JOIN nodes n USING(node_key) "
        "WHERE u.unit_id=? AND n.is_io=1", (unit_id,))}
    if not want:
        return []
    out, unmatched = [], []
    for row in con.execute("SELECT statements FROM lakebridge"):
        for st in json.loads(row["statements"] or "[]"):
            m = _NODENAME_ID.search(st.get("nodeName") or "")
            rec = {"node": st.get("nodeName"), "connection": st.get("connectionType"),
                   "objects": st.get("objects"), "actions": st.get("actions"),
                   "complexity": st.get("complexity"),
                   "statement_chars": st.get("sql_chars"),
                   "statement_head": st.get("sql_head")}
            if m and m.group(1) in want:
                out.append(rec)
            elif not m:
                unmatched.append(rec)
    # A statement whose node name carries no id can't be attributed; surface it
    # rather than dropping it silently.
    return out or unmatched[:3]


def build_pack(con, unit_id, corpus_roots, cache_path, top_k=retrieve.DEFAULT_TOP_K,
               budget=retrieve.DEFAULT_BUDGET, include_endpoints=True):
    u = con.execute("SELECT * FROM units WHERE unit_id=?", (unit_id,)).fetchone()
    if not u:
        raise SystemExit("no such unit: %s" % unit_id)
    u = dict(u)

    nodes = [dict(r) for r in con.execute(
        "SELECT n.* FROM unit_nodes un JOIN nodes n USING(node_key) WHERE un.unit_id=? "
        "ORDER BY CAST(n.tool_id AS INTEGER)", (unit_id,))]

    exprs = defaultdict(list)
    for r in con.execute(
            "SELECT e.* FROM unit_nodes un JOIN expressions e USING(node_key) "
            "WHERE un.unit_id=?", (unit_id,)):
        exprs[r["node_key"]].append((r["kind"], r["field"], r["expr"]))

    hazards = defaultdict(list)
    for r in con.execute(
            "SELECT h.* FROM unit_nodes un JOIN hazards h USING(node_key) "
            "WHERE un.unit_id=?", (unit_id,)):
        hazards[r["node_key"]].append((r["kind"], r["detail"]))

    gaps = [dict(r) for r in con.execute(
        "SELECT g.* FROM gaps g LEFT JOIN unit_nodes un ON g.node_key=un.node_key "
        "WHERE g.status='open' AND (g.unit_id=? OR un.unit_id=?)", (unit_id, unit_id))]

    decisions = [dict(r) for r in con.execute(
        "SELECT basis,summary,confidence FROM decisions WHERE unit_id=?", (unit_id,))]

    inbound, outbound = boundaries(con, unit_id)
    source = source_for(con)
    tools, funcs = unit_census(con, unit_id, source)

    deps = []
    for r in con.execute(
            "SELECT d.depends_on, u.title, u.status, u.layer FROM unit_deps d "
            "JOIN units u ON u.unit_id=d.depends_on WHERE d.unit_id=? ORDER BY 1", (unit_id,)):
        deps.append(dict(r))

    idx = retrieve.load_index(corpus_roots, cache_path)
    query = retrieve.query_for_unit(dict(tools), dict(funcs), source=source)
    # Scale retrieval to the unit. A 2-tool unit does not need six reference
    # sections; a 40-tool unit spanning eight tool families does.
    effective_k = max(2, min(top_k, 1 + len(tools)))
    skills = retrieve.search(idx, query, top_k=effective_k, budget=budget)

    tool_rows = []
    for n in nodes:
        row = {
            "node": n["node_key"], "tool_id": n["tool_id"], "tool": n["tool_name"],
            "disabled": bool(n["disabled"]),
            "caption": n["caption"],
            "config": describe(n, source),
        }
        e = exprs.get(n["node_key"], [])
        if e:
            row["expressions"] = [
                {"kind": k, "field": f, "expr": x} for k, f, x in e[:MAX_EXPR_PER_TOOL]]
            if len(e) > MAX_EXPR_PER_TOOL:
                row["expressions_truncated"] = len(e) - MAX_EXPR_PER_TOOL
        h = hazards.get(n["node_key"], [])
        if h:
            row["hazards"] = [{"kind": k, "match": d} for k, d in h]
        if n["macro_path"]:
            row["macro"] = n["macro_path"]
        tool_rows.append(row)

    pack = {
        "unit": {k: u[k] for k in ("unit_id", "seq", "title", "kind", "layer",
                                   "tool_count", "status", "dedupe_of")},
        "provenance": {
            "facts": "parsed from the workflow XML; verbatim",
            "mappings": "retrieved from this repository's skills",
            "inference": "anything not covered above MUST be recorded as a gap, not assumed",
        },
        "depends_on": deps,
        "inputs": inbound,
        "outputs": outbound,
        "tool_census": dict(tools),
        "function_census": dict(funcs),
        "tools": tool_rows,
        "open_gaps": [{"gap_id": g["gap_id"], "kind": g["kind"], "blocking": bool(g["blocking"]),
                       "question": g["question"]} for g in gaps],
        "decisions_so_far": decisions,
        "retrieved_skills": skills,
        "retrieval_query": query[:300],
    }
    if u["dedupe_of"]:
        pack["reuse"] = (
            "This unit is structurally identical to %s. Do not regenerate it: reuse that "
            "unit's transform as a parameterized function and record the parameter "
            "binding." % u["dedupe_of"])
    if include_endpoints and any(n["is_io"] for n in nodes):
        pack["lakebridge_endpoints"] = lakebridge_endpoints(con, unit_id)
    return pack


def render_markdown(pack):
    """Compact human/model-readable rendering. Markdown costs fewer tokens than
    pretty-printed JSON for the same facts."""
    u = pack["unit"]
    L = []
    a = L.append
    a("# Migration unit %s -- %s" % (u["unit_id"], u["title"]))
    a("")
    a("seq %s | kind %s | %s tool(s) | status %s%s" % (
        u["seq"], u["kind"], u["tool_count"], u["status"],
        " | DUPLICATE of %s" % u["dedupe_of"] if u["dedupe_of"] else ""))
    if pack.get("reuse"):
        a("")
        a("> **Reuse:** " + pack["reuse"])
    a("")
    a("Facts below are parsed verbatim from the workflow. Mappings are retrieved from "
      "this repo's skills. Anything neither covers is a gap -- record it, never guess.")

    if pack["depends_on"]:
        a("")
        a("## Depends on")
        for d in pack["depends_on"]:
            a("- %s (%s) -- %s" % (d["depends_on"], d["status"], d["title"]))

    if pack["inputs"]:
        a("")
        a("## Inputs (dataframes entering this unit)")
        for i in pack["inputs"]:
            a("- from %s node %s anchor `%s` -> into node %s anchor `%s`" % (
                i["from_unit"], i["from_node"], i["from_anchor"], i["into_node"], i["into_anchor"]))

    if pack["outputs"]:
        a("")
        a("## Outputs (dataframes leaving this unit)")
        for o in pack["outputs"]:
            a("- node %s anchor `%s` -> %s node %s anchor `%s`" % (
                o["from_node"], o["from_anchor"], o["to_unit"], o["to_node"], o["to_anchor"]))

    a("")
    a("## Tools")
    for t in pack["tools"]:
        head = "### %s  `%s`%s" % (t["tool_id"], t["tool"],
                                   "  (DISABLED)" if t["disabled"] else "")
        if t.get("caption"):
            head += "  -- %s" % t["caption"]
        a("")
        a(head)
        if t.get("config"):
            a("- config: %s" % t["config"])
        if t.get("macro"):
            a("- macro: %s" % t["macro"])
        for e in t.get("expressions", []):
            a("- %s `%s`: `%s`" % (e["kind"], e["field"], e["expr"]))
        if t.get("expressions_truncated"):
            a("- (+%d more expressions -- run `mig show %s` for the rest)"
              % (t["expressions_truncated"], t["node"]))
        for h in t.get("hazards", []):
            a("- HAZARD %s: `%s`" % (h["kind"], h["match"]))

    if pack.get("lakebridge_endpoints"):
        a("")
        a("## Source/target endpoints (Lakebridge)")
        for e in pack["lakebridge_endpoints"][:12]:
            a("- %s via %s, objects %s, actions %s (%s chars of SQL)" % (
                e["node"], e["connection"], e["objects"], e["actions"], e["statement_chars"]))

    if pack["open_gaps"]:
        a("")
        a("## Open gaps -- resolve or escalate, do not guess")
        for g in pack["open_gaps"]:
            a("- [%s%s] #%s %s" % (g["kind"], ", BLOCKING" if g["blocking"] else "",
                                   g["gap_id"], g["question"]))

    if pack["decisions_so_far"]:
        a("")
        a("## Decisions already recorded")
        for d in pack["decisions_so_far"]:
            a("- (%s/%s) %s" % (d["basis"], d["confidence"] or "-", d["summary"]))

    a("")
    a("## Retrieved skills (top %d of this repo's corpus)" % len(pack["retrieved_skills"]))
    for s in pack["retrieved_skills"]:
        a("")
        a("### %s  --  %s" % (os.path.basename(s["path"]), s["heading"]))
        a("")
        a(s["text"])
    return "\n".join(L)
