"""`mig` -- the command surface the skill drives.

Every command is built to be read by a model: compact by default, `--json` when
a machine needs it, and never a wall of output. That is the same goal RTK has
for third-party commands; here it is a property of the tool itself, so the
output stays small whether or not RTK is installed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys

if __package__ in (None, ""):        # allow `python engine/mig/cli.py`
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from mig import (context, db, export, lakebridge, plan, retrieve, sources, units,
                     validate)
else:
    from . import (context, db, export, lakebridge, plan, retrieve, sources, units,
                   validate)

# Workflow field names are routinely non-ASCII (this was built against a
# Norwegian workflow) and the Windows console defaults to cp1252, which raises
# on them. Force UTF-8 so output never dies on a column name.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

_HERE = os.path.dirname(os.path.abspath(__file__))                # .../engine/mig
ENGINE_ROOT = os.path.dirname(_HERE)                              # .../engine
PLUGIN_ROOT = os.path.dirname(ENGINE_ROOT)                        # .../migration
SKILLS_DIR = os.path.join(PLUGIN_ROOT, "skills")


def default_corpus():
    """Shared target references plus every installed skill's own references.

    A new skill dropped into `skills/` joins the retrieval corpus with no code
    change -- which is the point of keeping the engine outside the skills.
    """
    roots = [os.path.join(PLUGIN_ROOT, "references")]
    if os.path.isdir(SKILLS_DIR):
        roots += [os.path.join(SKILLS_DIR, d) for d in sorted(os.listdir(SKILLS_DIR))
                  if os.path.isdir(os.path.join(SKILLS_DIR, d))]
    return roots


def _run_dir(args):
    d = args.run or os.environ.get("MIG_RUN")
    if not d:
        raise SystemExit("no run directory: pass --run <dir> or set MIG_RUN")
    return os.path.abspath(d)


def _corpus(args):
    roots = [r for r in default_corpus() if os.path.exists(r)]
    for extra in (args.corpus or []):
        roots.append(os.path.abspath(extra))
    return roots


def _cache(run_dir):
    return os.path.join(run_dir, "skill-index.json")


def _emit(args, obj, lines=None):
    if getattr(args, "json", False) or lines is None:
        json.dump(obj, sys.stdout, indent=None if getattr(args, "compact", False) else 2,
                  default=str)
        sys.stdout.write("\n")
    else:
        sys.stdout.write("\n".join(lines) + "\n")


def _sha_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for c in iter(lambda: fh.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()[:16]


# --------------------------------------------------------------------------

def cmd_init(args):
    run = _run_dir(args)
    con = db.connect(run)
    wf = os.path.abspath(args.workflow)
    if not os.path.isfile(wf):
        raise SystemExit("workflow not found: %s" % wf)
    source = sources.get(args.source) if args.source else sources.for_file(wf)
    db.set_meta(con, "workflow_path", wf)
    db.set_meta(con, "source", source.name)
    db.set_meta(con, "target", args.target)
    db.set_meta(con, "macro_paths", [os.path.abspath(p) for p in (args.macro_path or [])])
    scope = {
        "catalog": args.catalog, "schema": args.schema,
        "run_date_param": "pipeline.run_date",
        "notes": args.note or "",
    }
    db.set_meta(con, "scope", scope)
    if args.parity_manifest:
        db.set_meta(con, "parity_manifest", os.path.abspath(args.parity_manifest))
    _emit(args, {"run_dir": run, "workflow": wf, "source": source.name,
                 "target": args.target, "scope": scope},
          ["run dir : %s" % run, "workflow: %s" % wf,
           "source  : %s   target: %s" % (source.name, args.target),
           "scope   : catalog=%s schema=%s" % (args.catalog, args.schema),
           "",
           "next: mig extract --run %s" % run])


def cmd_extract(args):
    run = _run_dir(args)
    con = db.connect(run)
    wf = args.workflow or db.get_meta(con, "workflow_path")
    if not wf:
        raise SystemExit("no workflow recorded; run `mig init` first")
    macro_paths = db.get_meta_json(con, "macro_paths", []) + [
        os.path.abspath(p) for p in (args.macro_path or [])]
    source = sources.get(db.get_meta(con, "source", "alteryx"))

    lb_info, lb_note = None, None
    js = args.lakebridge_json
    if js is None and not args.no_lakebridge:
        src_dir = args.lakebridge_dir or os.path.dirname(wf)
        js, lb_note = lakebridge.run_analyzer(
            src_dir, os.path.join(run, "lakebridge"),
            source_tech=source.lakebridge_source_tech)
    if js and os.path.isfile(js):
        lb_info = lakebridge.ingest(con, js)
    elif not args.no_lakebridge:
        con.execute(
            "INSERT INTO gaps(kind,question,blocking,created_at) VALUES(?,?,0,?)",
            ("missing-info",
             "Lakebridge analyzer did not run (%s). Estate-level facts (complexity "
             "bands, endpoint inventory, function census) are unavailable; the XML "
             "parse still covers every tool, but the two-source structural "
             "cross-check cannot run." % (lb_note or "not attempted"), db.now()))
        con.commit()

    stats = source.extract(con, wf, macro_paths)
    cc = lakebridge.cross_check(con)
    out = {"parse": stats, "lakebridge": lb_info, "cross_check": cc}
    lines = ["parsed %(files)s file(s): %(nodes)s tools, %(edges)s connections, "
             "%(expressions)s expressions, %(hazards)s hazards" % stats]
    if lb_info:
        lines.append("lakebridge: %(files)s file(s), %(tools)s tools, %(statements)s statements"
                     % lb_info)
    if cc:
        lines.append("cross-check: lakebridge %d vs parsed %d -- %s" % (
            cc["lakebridge_total"], cc["parsed_total"],
            "agree" if not cc["mismatched_types"]
            else "%d type(s) disagree" % len(cc["mismatched_types"])))
        if cc["files_out_of_scope"]:
            lines.append("  (%d analyzer file(s) outside this workflow ignored; "
                         "point --lakebridge-dir at a folder holding only the "
                         "workflow and its macros)" % cc["files_out_of_scope"])
    else:
        lines.append("cross-check: skipped (no lakebridge report)")
    if stats["unresolved_macros"]:
        lines.append("unresolved macro call sites: %d (see `mig gaps`)"
                     % stats["unresolved_macros"])
    lines += ["", "next: mig units --run %s" % run]
    _emit(args, out, lines)


def cmd_units(args):
    run = _run_dir(args)
    con = db.connect(run)
    stats = units.build(con, max_unit=args.max_unit, min_unit=args.min_unit,
                        keep_viewers=args.keep_viewers)
    _emit(args, stats,
          ["%(units)d units (%(unique_units)d unique, %(duplicate_units)d duplicates to reuse)"
           % stats,
           "%(tools_in_units)d of %(tools_total)d tools in units; %(tools_excluded)d excluded "
           "with a stated reason" % stats,
           "%(cross_unit_edges)d cross-unit connections" % stats,
           "", "next: mig plan --run %s" % run])


def cmd_plan(args):
    run = _run_dir(args)
    con = db.connect(run)
    res = plan.write(con, run, args.slug)
    _emit(args, res, ["plan written: %s" % res["dir"],
                      "  " + ", ".join(res["files"]),
                      "%d/%d units complete" % (res["complete"], res["units"])])


def cmd_next(args):
    run = _run_dir(args)
    con = db.connect(run)
    rs = units.ready(con, limit=args.limit)
    if not rs:
        remaining = con.execute(
            "SELECT count(*) c FROM units WHERE status NOT IN ('complete','validated')"
        ).fetchone()["c"]
        msg = ("all units complete" if remaining == 0 else
               "%d unit(s) remain but none are ready -- their dependencies are not "
               "complete, or a cycle needs breaking; see `mig status`" % remaining)
        _emit(args, {"ready": [], "note": msg}, [msg])
        return
    _emit(args, {"ready": rs},
          ["%-6s %-4s %-5s %s" % ("unit", "n", "dup", "title")] +
          ["%-6s %-4d %-5s %s" % (u["unit_id"], u["tool_count"], u["dedupe_of"] or "-",
                                  u["title"][:60]) for u in rs])


def cmd_context(args):
    run = _run_dir(args)
    con = db.connect(run)
    pack = context.build_pack(con, args.unit, _corpus(args), _cache(run),
                              top_k=args.top_k, budget=args.budget,
                              include_endpoints=not args.no_endpoints)
    if args.json:
        _emit(args, pack)
    else:
        sys.stdout.write(context.render_markdown(pack) + "\n")


def cmd_retrieve(args):
    run = _run_dir(args) if (args.run or os.environ.get("MIG_RUN")) else None
    idx = retrieve.load_index(_corpus(args), _cache(run) if run else None)
    hits = retrieve.search(idx, args.query, top_k=args.top_k, budget=args.budget,
                           path_filter=args.path_filter)
    if args.json:
        _emit(args, {"query": args.query, "hits": hits})
        return
    for h in hits:
        sys.stdout.write("\n## %s -- %s  (score %.1f)\n\n%s\n"
                         % (os.path.basename(h["path"]), h["heading"], h["score"], h["text"]))


def cmd_show(args):
    run = _run_dir(args)
    con = db.connect(run)
    key = args.node if ":" in args.node else "w0:%s" % args.node
    n = con.execute("SELECT * FROM nodes WHERE node_key=?", (key,)).fetchone()
    if not n:
        raise SystemExit("no such node: %s" % key)
    n = dict(n)
    exprs = [dict(r) for r in con.execute(
        "SELECT kind,field,expr FROM expressions WHERE node_key=?", (key,))]
    hz = [dict(r) for r in con.execute(
        "SELECT kind,detail FROM hazards WHERE node_key=?", (key,))]
    out = {"node": {k: n[k] for k in n if k != "config_xml"},
           "description": context.describe(n), "expressions": exprs, "hazards": hz}
    if args.raw:
        out["config_xml"] = n["config_xml"]
    _emit(args, out)


def cmd_decide(args):
    run = _run_dir(args)
    con = db.connect(run)
    con.execute(
        "INSERT INTO decisions(unit_id,node_key,basis,summary,detail,confidence,created_at) "
        "VALUES(?,?,?,?,?,?,?)",
        (args.unit, args.node, args.basis, args.summary, args.detail, args.confidence, db.now()))
    con.commit()
    _emit(args, {"recorded": True, "unit": args.unit, "basis": args.basis},
          ["recorded %s decision for %s" % (args.basis, args.unit or "global")])


def cmd_gap(args):
    run = _run_dir(args)
    con = db.connect(run)
    if args.gap_action == "add":
        cur = con.execute(
            "INSERT INTO gaps(unit_id,node_key,kind,question,blocking,created_at) "
            "VALUES(?,?,?,?,?,?)",
            (args.unit, args.node, args.kind, args.question,
             0 if args.non_blocking else 1, db.now()))
        con.commit()
        _emit(args, {"gap_id": cur.lastrowid},
              ["gap #%d recorded (%s)" % (cur.lastrowid, args.kind)])
    elif args.gap_action == "resolve":
        con.execute(
            "UPDATE gaps SET status=?, resolution=?, resolved_by=?, resolved_at=? "
            "WHERE gap_id=?",
            ("escalated" if args.escalate else "resolved", args.resolution,
             args.by, db.now(), args.gap_id))
        con.commit()
        _emit(args, {"gap_id": args.gap_id, "status": "escalated" if args.escalate else "resolved"},
              ["gap #%d %s" % (args.gap_id, "escalated" if args.escalate else "resolved")])
    else:
        q = "SELECT gap_id,unit_id,node_key,kind,blocking,status,question,resolution FROM gaps"
        if not args.all:
            q += " WHERE status='open'"
        rows = [dict(r) for r in con.execute(q + " ORDER BY blocking DESC, gap_id")]
        _emit(args, {"gaps": rows},
              ["#%-4d %-18s %-8s %s %s" % (
                  r["gap_id"], r["kind"], r["status"],
                  "BLOCKING" if r["blocking"] else "        ", r["question"][:110])
               for r in rows] or ["no gaps"])


def cmd_unrecord(args):
    """Detach an artifact from a unit.

    `record` can attach but nothing could detach, so an attribution recorded in
    error -- a shared helper credited to a unit it turns out not to implement --
    could not be corrected through the CLI, only by editing state.db by hand.
    """
    run = _run_dir(args)
    con = db.connect(run)
    if args.path:
        p = os.path.abspath(args.path)
        cur = con.execute("DELETE FROM artifacts WHERE unit_id=? AND path=?",
                          (args.unit, p))
    else:
        cur = con.execute("DELETE FROM artifacts WHERE unit_id=?", (args.unit,))
    n = cur.rowcount
    left = con.execute("SELECT count(*) c FROM artifacts WHERE unit_id=?",
                       (args.unit,)).fetchone()["c"]
    if not left:
        con.execute("UPDATE units SET status=?, updated_at=? WHERE unit_id=? "
                    "AND status='generated'", ("pending", db.now(), args.unit))
    con.commit()
    _emit(args, {"unit": args.unit, "removed": n, "remaining": left},
          ["%s: removed %d artifact(s), %d remaining" % (args.unit, n, left)])
    con.close()


def cmd_record(args):
    run = _run_dir(args)
    con = db.connect(run)
    p = os.path.abspath(args.path)
    if not os.path.isfile(p):
        raise SystemExit("artifact not found: %s" % p)
    con.execute("DELETE FROM artifacts WHERE unit_id=? AND path=?", (args.unit, p))
    con.execute(
        "INSERT INTO artifacts(unit_id,path,role,sha256,written_at) VALUES(?,?,?,?,?)",
        (args.unit, p, args.role, _sha_file(p), db.now()))
    if args.layer:
        con.execute("UPDATE units SET layer=? WHERE unit_id=?", (args.layer, args.unit))
    con.execute("UPDATE units SET status=?, updated_at=? WHERE unit_id=?",
                ("generated", db.now(), args.unit))
    con.commit()
    _emit(args, {"unit": args.unit, "path": p, "status": "generated"},
          ["%s -> %s (%s)" % (args.unit, p, args.role),
           "", "next: mig validate --unit %s --run %s" % (args.unit, run)])


def cmd_layer(args):
    run = _run_dir(args)
    con = db.connect(run)
    con.execute("UPDATE units SET layer=?, updated_at=? WHERE unit_id=?",
                (args.layer, db.now(), args.unit))
    con.commit()
    _emit(args, {"unit": args.unit, "layer": args.layer},
          ["%s -> %s" % (args.unit, args.layer)])


def cmd_validate(args):
    run = _run_dir(args)
    con = db.connect(run)
    out = {}
    if args.unit:
        targets = [args.unit]
    elif args.all:
        targets = [r["unit_id"] for r in con.execute(
            "SELECT unit_id FROM units WHERE status IN ('generated','failed','validated','complete') "
            "ORDER BY seq")]
    else:
        targets = []

    levels = args.level or ["structural", "code", "semantic", "data"]
    if "structural" in levels:
        out["structural"] = validate.structural(con)
    for uid in targets:
        if "code" in levels:
            validate.code(con, uid)
        if "semantic" in levels:
            validate.semantic(con, uid)
        if "data" in levels:
            validate.data(con, uid)
        verdict, why = validate.unit_verdict(con, uid)
        status = {"validated": "validated", "failed": "failed",
                  "blocked": "blocked", "incomplete": "generated"}[verdict]
        con.execute("UPDATE units SET status=?, notes=?, updated_at=? WHERE unit_id=?",
                    (status, why, db.now(), uid))
        out[uid] = {"verdict": verdict, "why": why, "levels": validate.summary(con, uid)}
    con.commit()

    fails = validate.failures(con, args.unit if args.unit else None)
    lines = []
    for k, v in out.items():
        if k == "structural":
            for lvl, st in v.items():
                lines.append("structural: %d pass %d fail %d skip"
                             % (st.get("pass", 0), st.get("fail", 0), st.get("skip", 0)))
        else:
            lines.append("%s: %s -- %s" % (k, v["verdict"], v["why"]))
    if fails:
        lines.append("")
        lines.append("failures:")
        for f in fails[:25]:
            lines.append("  [%s/%s] %s: %s" % (f["unit_id"] or "-", f["level"],
                                               f["check_name"], (f["detail"] or "")[:150]))
    _emit(args, out, lines)


def cmd_complete(args):
    run = _run_dir(args)
    con = db.connect(run)
    verdict, why = validate.unit_verdict(con, args.unit)
    if verdict != "validated" and not args.force:
        raise SystemExit(
            "refusing to mark %s complete: %s (%s). Fix and re-validate, or record the "
            "reason as a gap." % (args.unit, verdict, why))
    con.execute("UPDATE units SET status='complete', notes=?, updated_at=? WHERE unit_id=?",
                (why + (" [forced]" if args.force else ""), db.now(), args.unit))
    # Structural duplicates of a completed unit are satisfied by its code.
    n = con.execute(
        "UPDATE units SET status='complete', notes=? WHERE dedupe_of=? AND status!='complete'",
        ("satisfied by %s (identical block)" % args.unit, args.unit)).rowcount
    con.commit()
    _emit(args, {"unit": args.unit, "verdict": verdict, "duplicates_closed": n},
          ["%s complete%s" % (args.unit, " (+%d duplicate unit(s) closed)" % n if n else "")])


def cmd_status(args):
    run = _run_dir(args)
    con = db.connect(run)
    counts = {r["status"]: r["c"] for r in con.execute(
        "SELECT status, count(*) c FROM units GROUP BY 1")}
    tools = con.execute("SELECT count(*) c FROM nodes").fetchone()["c"]
    total_u = sum(counts.values())
    done = counts.get("complete", 0) + counts.get("validated", 0)
    gaps = {r["status"]: r["c"] for r in con.execute(
        "SELECT status, count(*) c FROM gaps GROUP BY 1")}
    blocking = con.execute(
        "SELECT count(*) c FROM gaps WHERE status='open' AND blocking=1").fetchone()["c"]
    val = validate.summary(con)
    rdy = units.ready(con, limit=5)
    out = {"tools": tools, "units": counts, "complete": done, "total_units": total_u,
           "gaps": gaps, "blocking_gaps": blocking, "validation": val,
           "ready": [{"unit_id": u["unit_id"], "title": u["title"],
                      "tool_count": u["tool_count"]} for u in rdy],
           "migration_complete": _is_complete(con)}
    lines = [
        "tools   : %d" % tools,
        "units   : %d/%d complete  (%s)" % (
            done, total_u, ", ".join("%s %d" % (k, v) for k, v in sorted(counts.items()))),
        "gaps    : %d open, %d blocking" % (gaps.get("open", 0), blocking),
        "validate: " + (", ".join(
            "%s %dp/%df/%ds" % (lv, st.get("pass", 0), st.get("fail", 0), st.get("skip", 0))
            for lv, st in sorted(val.items())) or "not run"),
        "ready   : " + (", ".join(u["unit_id"] for u in rdy) or "none"),
        "COMPLETE: %s" % ("yes" if out["migration_complete"]["complete"]
                          else "no -- " + "; ".join(out["migration_complete"]["blockers"][:4])),
    ]
    _emit(args, out, lines)


def _is_complete(con):
    """The completion gate. Code existing is never sufficient."""
    blockers = []
    if not con.execute("SELECT 1 FROM nodes LIMIT 1").fetchone():
        blockers.append("nothing extracted")
    if not db.get_meta(con, "lakebridge_json"):
        blockers.append("no Lakebridge analysis ingested")
    incomplete = con.execute(
        "SELECT count(*) c FROM units WHERE status!='complete'").fetchone()["c"]
    if incomplete:
        blockers.append("%d unit(s) not complete" % incomplete)
    bg = con.execute(
        "SELECT count(*) c FROM gaps WHERE status='open' AND blocking=1").fetchone()["c"]
    if bg:
        blockers.append("%d blocking gap(s) open" % bg)
    v = validate.summary(con)
    for level in ("structural", "code", "semantic"):
        st = v.get(level)
        if not st:
            blockers.append("%s validation never ran" % level)
        elif st.get("fail", 0):
            blockers.append("%s validation has %d failure(s)" % (level, st["fail"]))
    if not v.get("data"):
        blockers.append("data validation never ran (record a skip with a reason if "
                        "no reference output exists)")
    elif v["data"].get("fail", 0):
        blockers.append("data validation has %d failure(s)" % v["data"]["fail"])
    return {"complete": not blockers, "blockers": blockers}


def cmd_report(args):
    run = _run_dir(args)
    con = db.connect(run)
    comp = _is_complete(con)
    rows = [dict(r) for r in con.execute("SELECT * FROM units ORDER BY seq")]
    L = ["# Migration report", "",
         "Source workflow: `%s`" % db.get_meta(con, "workflow_path"), "",
         "**Status: %s**" % ("COMPLETE" if comp["complete"] else "NOT COMPLETE"), ""]
    if not comp["complete"]:
        L.append("Outstanding:")
        L += ["- %s" % b for b in comp["blockers"]]
        L.append("")

    L += ["## Coverage", ""]
    tools = con.execute("SELECT count(*) c FROM nodes").fetchone()["c"]
    placed = con.execute("SELECT count(DISTINCT node_key) c FROM unit_nodes").fetchone()["c"]
    ex = units.excluded_nodes(con)
    L.append("- %d tools parsed from %d file(s)" % (
        tools, con.execute("SELECT count(*) c FROM files").fetchone()["c"]))
    L.append("- %d tools assigned to %d migration units" % (placed, len(rows)))
    L.append("- %d tools excluded, by reason:" % len(ex))
    from collections import Counter
    for reason, n in Counter(e["reason"] for e in ex).most_common():
        L.append("  - %s: %d" % (reason, n))
    missing = units.unaccounted_nodes(con)
    if missing:
        L.append("- **%d tool(s) UNACCOUNTED** -- wired on the canvas but in no unit: %s"
                 % (len(missing), ", ".join("%s (%s)" % (m["node_key"], m["tool"])
                                            for m in missing[:20])))
    else:
        L.append("- 0 tools unaccounted for")
    cc = lakebridge.cross_check(con)
    if cc:
        L.append("- Lakebridge cross-check: %d vs %d parsed, %d disagreeing tool type(s)"
                 % (cc["lakebridge_total"], cc["parsed_total"], len(cc["mismatched_types"])))
    L.append("")

    L += ["## Units", "",
          "| unit | title | tools | layer | status | duplicate of |",
          "|------|-------|-------|-------|--------|--------------|"]
    for r in rows:
        L.append("| %s | %s | %d | %s | %s | %s |" % (
            r["unit_id"], (r["title"] or "").replace("|", "/")[:60], r["tool_count"],
            r["layer"] or "-", r["status"], r["dedupe_of"] or "-"))
    L.append("")

    L += ["## Validation", "",
          "| unit | level | check | status | detail |",
          "|------|-------|-------|--------|--------|"]
    for v in con.execute(
            "SELECT * FROM validations ORDER BY unit_id IS NOT NULL, unit_id, level, check_name"):
        L.append("| %s | %s | %s | %s | %s |" % (
            v["unit_id"] or "-", v["level"], v["check_name"], v["status"],
            (v["detail"] or "").replace("|", "/")[:140]))
    L.append("")

    L += ["## Open and escalated gaps", ""]
    any_g = False
    for g in con.execute("SELECT * FROM gaps WHERE status!='resolved' ORDER BY blocking DESC"):
        any_g = True
        L.append("- **#%d** (%s%s) %s" % (g["gap_id"], g["kind"],
                                          ", blocking" if g["blocking"] else "", g["question"]))
    if not any_g:
        L.append("None.")
    L.append("")

    L += ["## Decisions by basis", ""]
    for d in con.execute("SELECT basis, count(*) c FROM decisions GROUP BY 1 ORDER BY 2 DESC"):
        L.append("- %s: %d" % (d["basis"], d["c"]))
    L.append("")
    L.append("Facts come from the Lakebridge report and the XML parse; mappings from this "
             "repository's skills; inferences are listed above and in `findings.md`.")

    out_path = args.out or os.path.join(run, "migration-report.md")
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")
    _emit(args, {"report": out_path, "complete": comp},
          ["report: %s" % out_path,
           "status: %s" % ("COMPLETE" if comp["complete"] else
                           "NOT COMPLETE -- " + "; ".join(comp["blockers"][:5]))])


def cmd_export(args):
    run = _run_dir(args)
    con = db.connect(run)
    out = args.out or os.path.join(run, "migration-export.xlsx")
    res = export.build(con, run, out, lb_xlsx=args.lakebridge_xlsx,
                       include_lakebridge=not args.no_lakebridge, full=args.full,
                       complete=_is_complete(con))
    lines = ["wrote %s (%.1f KB)" % (res["path"], res["bytes"] / 1024.0),
             "  %d sheets: %d from state.db, %d from Lakebridge"
             % (res["sheets"], res["mig_sheets"], res["lakebridge_sheets"]),
             "  %d rows total" % res["rows"]]
    if res["lakebridge_source"]:
        lines.append("  analyzer report merged from %s" % res["lakebridge_source"])
    if res["degraded"]:
        lines.append("  DEGRADED: %s" % res["degraded"])
    _emit(args, res, lines)


def cmd_index(args):
    run = _run_dir(args) if (args.run or os.environ.get("MIG_RUN")) else None
    idx = retrieve.build_index(_corpus(args), _cache(run) if run else None)
    _emit(args, {"chunks": len(idx["docs"]), "vocab": len(idx["idf"]),
                 "roots": idx["roots"]},
          ["indexed %d chunks / %d terms from:" % (len(idx["docs"]), len(idx["idf"]))] +
          ["  " + r for r in idx["roots"]])


# --------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(prog="mig", description=__doc__)
    p.add_argument("--run", help="run directory holding state.db")
    p.add_argument("--json", action="store_true", help="machine-readable output")
    p.add_argument("--compact", action="store_true", help="single-line json")
    p.add_argument("--corpus", action="append", help="extra skill corpus root (repeatable)")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("init", help="record scope and the source workflow")
    s.add_argument("workflow")
    s.add_argument("--macro-path", action="append", help="extra macro search path")
    s.add_argument("--source", choices=sources.available(),
                   help="source platform adapter (default: inferred from the file type)")
    s.add_argument("--target", default="databricks-sdp",
                   help="target platform adapter (default: databricks-sdp)")
    s.add_argument("--catalog"); s.add_argument("--schema")
    s.add_argument("--parity-manifest"); s.add_argument("--note")
    s.set_defaults(func=cmd_init)

    s = sub.add_parser("extract", help="run Lakebridge + parse the XML into state.db")
    s.add_argument("--workflow")
    s.add_argument("--macro-path", action="append")
    s.add_argument("--lakebridge-json", help="use an existing analyzer JSON report")
    s.add_argument("--lakebridge-dir", help="source directory to analyze")
    s.add_argument("--no-lakebridge", action="store_true")
    s.set_defaults(func=cmd_extract)

    s = sub.add_parser("units", help="partition the DAG into dependency-ordered units")
    s.add_argument("--max-unit", type=int, default=units.DEFAULT_MAX_UNIT)
    s.add_argument("--min-unit", type=int, default=units.DEFAULT_MIN_UNIT)
    s.add_argument("--keep-viewers", action="store_true")
    s.set_defaults(func=cmd_units)

    s = sub.add_parser("plan", help="write the Planning-with-Files markdown")
    s.add_argument("--slug")
    s.set_defaults(func=cmd_plan)

    s = sub.add_parser("next", help="units whose dependencies are complete")
    s.add_argument("--limit", type=int, default=5)
    s.set_defaults(func=cmd_next)

    s = sub.add_parser("context", help="context pack for one unit (facts + retrieved skills)")
    s.add_argument("unit")
    s.add_argument("--top-k", type=int, default=retrieve.DEFAULT_TOP_K)
    s.add_argument("--budget", type=int, default=retrieve.DEFAULT_BUDGET)
    s.add_argument("--no-endpoints", action="store_true")
    s.set_defaults(func=cmd_context)

    s = sub.add_parser("retrieve", help="BM25 search over the repo's skills")
    s.add_argument("query")
    s.add_argument("--top-k", type=int, default=retrieve.DEFAULT_TOP_K)
    s.add_argument("--budget", type=int, default=retrieve.DEFAULT_BUDGET)
    s.add_argument("--path-filter")
    s.set_defaults(func=cmd_retrieve)

    s = sub.add_parser("show", help="full detail for one tool")
    s.add_argument("node"); s.add_argument("--raw", action="store_true")
    s.set_defaults(func=cmd_show)

    s = sub.add_parser("decide", help="record a migration decision and its basis")
    s.add_argument("summary")
    s.add_argument("--unit"); s.add_argument("--node"); s.add_argument("--detail")
    s.add_argument("--basis", choices=["fact", "mapping", "inference", "user"], required=True)
    s.add_argument("--confidence",
                   choices=["direct", "adapted", "needs-design", "no-equivalent"])
    s.set_defaults(func=cmd_decide)

    s = sub.add_parser("gap", help="record, list, or resolve an open question")
    gs = s.add_subparsers(dest="gap_action", required=True)
    a = gs.add_parser("add")
    a.add_argument("question")
    a.add_argument("--kind", required=True,
                   choices=["missing-info", "ambiguous-config", "unsupported-tool",
                            "ambiguous-expr", "column-mapping", "unclear-type",
                            "complex-behavior"])
    a.add_argument("--unit"); a.add_argument("--node")
    a.add_argument("--non-blocking", action="store_true")
    a = gs.add_parser("list"); a.add_argument("--all", action="store_true")
    a = gs.add_parser("resolve")
    a.add_argument("gap_id", type=int)
    a.add_argument("resolution")
    a.add_argument("--by", choices=["fact", "skill", "user", "inference"], required=True)
    a.add_argument("--escalate", action="store_true")
    s.set_defaults(func=cmd_gap)

    s = sub.add_parser("record", help="attach generated code to a unit")
    s.add_argument("unit"); s.add_argument("path")
    s.add_argument("--role", default="sdp", choices=["pyspark", "sdp", "test", "doc"])
    s.add_argument("--layer")
    s.set_defaults(func=cmd_record)

    s = sub.add_parser("unrecord", help="detach an artifact from a unit")
    s.add_argument("unit")
    s.add_argument("path", nargs="?",
                   help="artifact to detach; omit to detach all for the unit")
    s.set_defaults(func=cmd_unrecord)

    s = sub.add_parser("layer", help="assign a medallion layer to a unit")
    s.add_argument("unit")
    s.add_argument("layer", choices=["bronze", "silver", "gold", "job", "drop"])
    s.set_defaults(func=cmd_layer)

    s = sub.add_parser("validate", help="run validation levels")
    s.add_argument("--unit"); s.add_argument("--all", action="store_true")
    s.add_argument("--level", action="append",
                   choices=["structural", "semantic", "code", "data"])
    s.set_defaults(func=cmd_validate)

    s = sub.add_parser("complete", help="mark a unit complete (validated units only)")
    s.add_argument("unit"); s.add_argument("--force", action="store_true")
    s.set_defaults(func=cmd_complete)

    s = sub.add_parser("status", help="one-screen migration state")
    s.set_defaults(func=cmd_status)

    s = sub.add_parser("report", help="write the final auditable report")
    s.add_argument("--out")
    s.set_defaults(func=cmd_report)

    s = sub.add_parser("export", help="one .xlsx: Lakebridge report + parsed/AI facts")
    s.add_argument("--out", help="default <run>/migration-export.xlsx")
    s.add_argument("--lakebridge-xlsx", help="analyzer report to merge (default: auto-locate)")
    s.add_argument("--no-lakebridge", action="store_true",
                   help="engine sheets only")
    s.add_argument("--full", action="store_true",
                   help="include each tool's verbatim <Configuration> XML")
    s.set_defaults(func=cmd_export)

    s = sub.add_parser("index", help="(re)build the skill retrieval index")
    s.set_defaults(func=cmd_index)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    main()
