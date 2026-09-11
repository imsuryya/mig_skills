"""Source adapter: Alteryx (.yxmd / .yxmc / .yxwz).

Implements the contract in `sources/__init__.py`. Everything the engine
knows about Alteryx tools, expressions, and semantics lives here.

Parsing notes.

Lakebridge gives estate-level facts (file inventory, complexity, tool census,
function census, source/target endpoints). It does *not* emit ToolIDs, per-tool
configuration, or the connection graph -- so those come from here, parsed once
from the raw XML straight into state.db. After this stage nothing re-reads the
workflow, and the raw XML never enters the model context.
"""
from __future__ import annotations

import hashlib
import os
import re
import xml.etree.ElementTree as ET

from .. import db

name = "alteryx"
extensions = (".yxmd", ".yxmc", ".yxwz")
lakebridge_source_tech = "alteryx"

# Tools that are a pipeline boundary (a read from or write to the outside world).
IO_TOOLS = {
    "DbFileInput", "DbFileOutput", "TextInput", "AlteryxSelect_Input",
    "DirectoryTool", "Download", "MongoDBInput", "MongoDBOutput",
    "AzureDataLakeInput", "AzureDataLakeOutput", "SharePointInput",
    "SharePointOutput", "InputData", "OutputData", "MacroInput", "MacroOutput",
}

# Tools that produce no data path: canvas furniture and terminal viewers.
NON_DATA_TOOLS = {"Comment", "TextBox", "ToolContainer", "BrowseV2", "Browse", "ExplorerBox"}

# Macros that ship with Designer and so have no file next to the workflow. Their
# behavior is documented rather than parsed; flagged for confirmation, not blocking.
BUNDLED_MACROS = {
    "cleanse.yxmc": "Data Cleansing -- optional null replacement, whitespace trim, "
                    "punctuation/digit/letter stripping, and case conversion per "
                    "selected field, driven by the node's own Configuration",
    "ets.yxmc": "Predictive Tools ETS -- exponential smoothing forecast model; no "
                "native Spark equivalent, migrate as a separate ML/job track",
    "ts_forecast.yxmc": "Predictive Tools TS Forecast -- applies a fitted time-series "
                        "model to produce forecasts; separate ML/job track",
    "ts_covariate_forecast.yxmc": "Predictive Tools covariate forecast; separate ML/job track",
}

# Determinism / portability hazards, matched against verbatim configuration text.
HAZARD_PATTERNS = [
    ("nondeterministic-clock", re.compile(r"\bDateTime(Now|Today)\s*\(", re.I)),
    ("nondeterministic-random", re.compile(r"\b(RandInt|Rand)\s*\(", re.I)),
    ("environment-dependent", re.compile(r"\b(GetEnvironmentVariable|ReadRegistryString)\s*\(", re.I)),
    ("absolute-local-path", re.compile(r"[A-Za-z]:\\\\|[A-Za-z]:/|\\\\\\\\[A-Za-z0-9_.-]+\\\\")),
    ("embedded-credential", re.compile(r"(pwd|password|uid|user\s*id)\s*=", re.I)),
    ("alteryx-null-semantics", re.compile(r"\bNull\s*\(\s*\)")),
    ("case-insensitive-default", re.compile(r"\b(Contains|StartsWith|EndsWith)\s*\(", re.I)),
]

_WS = re.compile(r"\s+")


def _short_name(plugin: str, engine: str, macro: str) -> str:
    if macro:
        return "Macro:" + os.path.basename(macro)
    if plugin:
        return plugin.split(".")[-1]
    if engine:
        return engine
    return "Unknown"


def _canonical(elem: ET.Element) -> str:
    """Stable text form of a Configuration subtree, for duplicate detection."""
    parts = []

    def walk(e: ET.Element):
        attrs = ";".join("%s=%s" % (k, e.attrib[k]) for k in sorted(e.attrib))
        text = _WS.sub(" ", (e.text or "")).strip()
        parts.append("<%s|%s|%s>" % (e.tag, attrs, text))
        for c in e:
            walk(c)
        parts.append("</%s>" % e.tag)

    walk(elem)
    return "".join(parts)


def _sha(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", "replace")).hexdigest()[:16]


def _file_sha(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


# --------------------------------------------------------------------------
# Expression extraction, per tool family.
# --------------------------------------------------------------------------

def _extract_expressions(short: str, cfg: ET.Element):
    """Yield (kind, field, expr) for every verbatim expression in a tool."""
    out = []
    if short == "Formula":
        for ff in cfg.iter("FormulaField"):
            out.append(("formula", ff.get("field"), ff.get("expression") or ""))
    elif short in ("Filter",):
        mode = cfg.findtext("Mode") or ""
        expr = cfg.findtext("Expression") or ""
        if mode.lower() == "simple":
            simple = cfg.find("Simple")
            if simple is not None:
                expr = "%s %s %s" % (
                    simple.findtext("Field") or "",
                    simple.findtext("Operator") or "",
                    simple.findtext("Operands/Operand") or "",
                )
        if expr:
            out.append(("filter", mode or "Custom", expr))
    elif short in ("MultiRowFormula", "MultiFieldFormula"):
        expr = cfg.findtext("Expression") or ""
        field = cfg.findtext("CreateField_Name") or cfg.findtext("UpdateField_Name") or ""
        if expr:
            out.append(("formula", field, expr))
    elif short in ("Join", "JoinMultiple"):
        for ji in cfg.iter("JoinInfo"):
            side = ji.get("connection") or ""
            for f in ji.findall("Field"):
                out.append(("join_key", side, f.get("field") or ""))
    elif short == "Summarize":
        for sf in cfg.iter("SummarizeField"):
            out.append((
                "aggregate",
                sf.get("rename") or sf.get("field"),
                "%s(%s)" % (sf.get("action") or "", sf.get("field") or ""),
            ))
    elif short == "Sort":
        for f in cfg.iter("Field"):
            if f.get("order"):
                out.append(("sort_key", f.get("field"), f.get("order")))
    elif short == "Unique":
        for f in cfg.findall("UniqueFields/Field"):
            out.append(("dedupe_key", f.get("field"), f.get("field") or ""))
    elif short == "CrossTab":
        out.append((
            "pivot",
            cfg.findtext("HeaderField") or (cfg.find("HeaderField").get("field") if cfg.find("HeaderField") is not None else ""),
            "header=%s data=%s methods=%s group=%s" % (
                (cfg.find("HeaderField").get("field") if cfg.find("HeaderField") is not None else ""),
                (cfg.find("DataField").get("field") if cfg.find("DataField") is not None else ""),
                ",".join(m.get("method") or "" for m in cfg.iter("Method")),
                ",".join(f.get("field") or "" for f in cfg.findall("GroupFields/Field")),
            ),
        ))
    elif short == "GenerateRows":
        for tag, kind in (("InitValue", "generate"), ("Condition", "generate"), ("LoopExpression", "generate")):
            v = cfg.findtext(tag)
            if v:
                out.append((kind, tag, v))
    elif short in ("DynamicRename", "DynamicSelect"):
        for tag in ("Expression", "RenameExpression", "Formula"):
            v = cfg.findtext(tag)
            if v:
                out.append(("rename_expr", cfg.findtext("RenameMode") or tag, v))
    elif short == "RegEx":
        out.append(("regex", cfg.findtext("Field") or "", cfg.findtext("RegExExpression") or ""))

    # Catch-all: any Expression element not already captured (Action tools,
    # Message/Test conditions, Condition tools inside macros).
    if not out:
        for e in cfg.iter("Expression"):
            if e.text and e.text.strip():
                out.append(("expression", e.tag, e.text.strip()))
    return [(k, f or "", x) for k, f, x in out if x]


def _find_hazards(text: str):
    return [(kind, pat.search(text).group(0)) for kind, pat in HAZARD_PATTERNS if pat.search(text)]


# --------------------------------------------------------------------------
# Macro resolution
# --------------------------------------------------------------------------

def _resolve_macro(macro_attr: str, base_dir: str, search_paths) -> str | None:
    if not macro_attr:
        return None
    cand = macro_attr.replace("\\", os.sep).replace("/", os.sep)
    for root in [base_dir] + list(search_paths):
        p = os.path.normpath(os.path.join(root, cand))
        if os.path.isfile(p):
            return p
    return None


# --------------------------------------------------------------------------
# Document parse
# --------------------------------------------------------------------------

class ParseResult:
    def __init__(self):
        self.files = []          # dicts for `files`
        self.nodes = []
        self.edges = []
        self.exprs = []
        self.hazards = []
        self.unresolved_macros = []   # (node_key, macro_attr)


def parse_document(path, file_id, kind, con_result, search_paths, referenced_by=None):
    """Parse one .yxmd/.yxmc/.yxwz into ParseResult buffers. Returns macro refs."""
    tree = ET.parse(path)
    root = tree.getroot()
    base_dir = os.path.dirname(os.path.abspath(path))
    macro_refs = []   # (node_key, resolved_path)
    count = 0

    def walk(container_elem, parent_key, parent_disabled, depth):
        nonlocal count
        for node in container_elem.findall("Node"):
            tool_id = node.get("ToolID") or ""
            node_key = "%s:%s" % (file_id, tool_id)
            gui = node.find("GuiSettings")
            plugin = gui.get("Plugin") if gui is not None else None
            eng = node.find("EngineSettings")
            engine = eng.get("EngineDllEntryPoint") if eng is not None else None
            macro_attr = eng.get("Macro") if eng is not None else None
            short = _short_name(plugin or "", engine or "", macro_attr or "")

            cfg = node.find("Properties/Configuration")
            cfg_xml = ET.tostring(cfg, encoding="unicode") if cfg is not None else ""
            cfg_hash = _sha(_canonical(cfg)) if cfg is not None else _sha(short)

            self_disabled = False
            if cfg is not None:
                d = cfg.find("Disabled")
                self_disabled = bool(d is not None and (d.get("value") or "").lower() == "true")
            disabled = parent_disabled or self_disabled

            caption = None
            if cfg is not None:
                caption = cfg.findtext("Caption")
            if not caption:
                ann = node.find("Properties/Annotation")
                if ann is not None:
                    caption = (ann.findtext("Name") or "").strip() or None

            pos = gui.find("Position") if gui is not None else None
            x = float(pos.get("x")) if pos is not None and pos.get("x") else None
            y = float(pos.get("y")) if pos is not None and pos.get("y") else None

            is_container = short == "ToolContainer"
            macro_path = None
            if macro_attr:
                macro_path = _resolve_macro(macro_attr, base_dir, search_paths)
                if macro_path:
                    macro_refs.append((node_key, macro_path))
                else:
                    con_result.unresolved_macros.append((node_key, macro_attr))

            con_result.nodes.append({
                "node_key": node_key, "file_id": file_id, "tool_id": tool_id,
                "plugin": plugin, "engine": engine, "tool_name": short,
                "container": parent_key, "depth": depth,
                "disabled": int(disabled), "caption": caption, "x": x, "y": y,
                "config_xml": cfg_xml, "config_hash": cfg_hash,
                "macro_path": macro_path, "is_container": int(is_container),
                "is_io": int(short in IO_TOOLS),
            })
            count += 1

            if cfg is not None and not is_container:
                for kind_, field, expr in _extract_expressions(short, cfg):
                    con_result.exprs.append((node_key, kind_, field, expr))
                for hk, detail in _find_hazards(cfg_xml):
                    con_result.hazards.append((node_key, hk, detail[:200]))

            child = node.find("ChildNodes")
            if child is not None:
                walk(child, node_key, disabled, depth + 1)

    nodes_elem = root.find("Nodes")
    if nodes_elem is not None:
        walk(nodes_elem, None, False, 0)

    conns = root.find("Connections")
    if conns is not None:
        for c in conns.findall("Connection"):
            o, d = c.find("Origin"), c.find("Destination")
            if o is None or d is None:
                continue
            con_result.edges.append({
                "file_id": file_id,
                "origin": "%s:%s" % (file_id, o.get("ToolID")),
                "origin_anchor": o.get("Connection"),
                "dest": "%s:%s" % (file_id, d.get("ToolID")),
                "dest_anchor": d.get("Connection"),
                "name": c.get("name"),
                "wireless": int((c.get("Wireless") or "").lower() == "true"),
            })

    macro_type = None
    if kind == "macro":
        has_control = any(n["tool_name"] == "ControlParam" for n in con_result.nodes
                          if n["file_id"] == file_id)
        props = root.find("Properties/RuntimeProperties")
        iterative = props is not None and props.find("Iterative") is not None
        macro_type = "batch" if has_control else ("iterative" if iterative else "standard")

    con_result.files.append({
        "file_id": file_id, "path": os.path.abspath(path), "kind": kind,
        "macro_type": macro_type, "sha256": _file_sha(path),
        "tool_count": count, "parsed_at": db.now(), "referenced_by": referenced_by,
    })
    return macro_refs


def extract(con, workflow_path, search_paths=(), max_macro_depth=6):
    """Parse the workflow and every macro it reaches, into state.db."""
    res = ParseResult()
    kind_for_ext = {".yxmd": "workflow", ".yxmc": "macro", ".yxwz": "app"}
    root_kind = kind_for_ext.get(os.path.splitext(workflow_path)[1].lower(), "workflow")

    seen = {}                       # abs path -> file_id
    queue = [(os.path.abspath(workflow_path), root_kind, None, 0)]
    next_macro = [0]

    while queue:
        path, kind, ref_by, depth = queue.pop(0)
        if path in seen:
            continue
        if kind == "workflow" or kind == "app":
            file_id = "w0"
        else:
            next_macro[0] += 1
            file_id = "m%d" % next_macro[0]
        seen[path] = file_id
        refs = parse_document(path, file_id, kind, res, search_paths, ref_by)
        if depth < max_macro_depth:
            for node_key, mpath in refs:
                if os.path.abspath(mpath) not in seen:
                    queue.append((os.path.abspath(mpath), "macro", node_key, depth + 1))

    cur = con.cursor()
    cur.executemany(
        "INSERT OR REPLACE INTO files(file_id,path,kind,macro_type,sha256,tool_count,"
        "parsed_at,referenced_by) VALUES(:file_id,:path,:kind,:macro_type,:sha256,"
        ":tool_count,:parsed_at,:referenced_by)", res.files)
    cur.executemany(
        "INSERT OR REPLACE INTO nodes(node_key,file_id,tool_id,plugin,engine,tool_name,"
        "container,depth,disabled,caption,x,y,config_xml,config_hash,macro_path,"
        "is_container,is_io) VALUES(:node_key,:file_id,:tool_id,:plugin,:engine,:tool_name,"
        ":container,:depth,:disabled,:caption,:x,:y,:config_xml,:config_hash,:macro_path,"
        ":is_container,:is_io)", res.nodes)
    cur.executemany(
        "INSERT INTO edges(file_id,origin,origin_anchor,dest,dest_anchor,name,wireless) "
        "VALUES(:file_id,:origin,:origin_anchor,:dest,:dest_anchor,:name,:wireless)", res.edges)
    cur.executemany(
        "INSERT INTO expressions(node_key,kind,field,expr) VALUES(?,?,?,?)", res.exprs)
    cur.executemany(
        "INSERT INTO hazards(node_key,kind,detail) VALUES(?,?,?)", res.hazards)
    con.commit()

    # Every macro we could not find on disk is a gap -- an unparsed macro means
    # unknown semantics, which we must never invent. One gap per *distinct*
    # macro, not per call site: 75 Cleanse.yxmc nodes are one question.
    by_macro = {}
    for node_key, macro_attr in res.unresolved_macros:
        by_macro.setdefault(macro_attr, []).append(node_key)
    for macro_attr, keys in sorted(by_macro.items()):
        base = os.path.basename(macro_attr.replace("\\", "/")).lower()
        known = BUNDLED_MACROS.get(base)
        if known:
            question = (
                "Macro '%s' (%d call site%s) ships with Designer and has no local file. "
                "Documented behavior: %s. Confirm this matches the installed version."
                % (macro_attr, len(keys), "s" if len(keys) != 1 else "", known))
            blocking, kind = 0, "complex-behavior"
        else:
            question = (
                "Macro '%s' (%d call site%s, e.g. %s) was not found on disk or in the "
                "macro search paths. Its semantics are unknown -- supply the file, add a "
                "--macro-path, or describe its behavior."
                % (macro_attr, len(keys), "s" if len(keys) != 1 else "", keys[0]))
            blocking, kind = 1, "missing-info"
        con.execute(
            "INSERT INTO gaps(node_key,kind,question,blocking,created_at) VALUES(?,?,?,?,?)",
            (keys[0], kind, question, blocking, db.now()))
    con.commit()

    return {
        "files": len(res.files),
        "nodes": len(res.nodes),
        "edges": len(res.edges),
        "expressions": len(res.exprs),
        "hazards": len(res.hazards),
        "unresolved_macros": len(res.unresolved_macros),
    }


# --------------------------------------------------------------------------
# Compact tool description (replaces raw Configuration XML in context packs)
# --------------------------------------------------------------------------

MAX_FIELDS_LISTED = 12
MAX_EXPR_PER_TOOL = 25


def _fields(cfg, xpath, attr="field"):
    return [e.get(attr) for e in cfg.findall(xpath) if e.get(attr)]


def _fmt_list(items, cap=MAX_FIELDS_LISTED):
    items = [i for i in items if i]
    if len(items) <= cap:
        return ", ".join(items)
    return "%s, ... (+%d more)" % (", ".join(items[:cap]), len(items) - cap)


def describe(node):
    """One compact, behavior-bearing line (or few) per tool, from its config."""
    name = node["tool_name"]
    xml = node["config_xml"] or ""
    if not xml:
        return ""
    try:
        cfg = ET.fromstring(xml)
    except ET.ParseError:
        return "(unparseable configuration)"

    if name == "AlteryxSelect":
        sel, des, ren, ret = [], [], [], []
        for f in cfg.findall("SelectFields/SelectField"):
            fld = f.get("field")
            if fld == "*Unknown":
                des.append("*Unknown deselected" if f.get("selected") == "False" else "*Unknown kept")
                continue
            if (f.get("selected") or "True") == "False":
                des.append(fld)
            else:
                sel.append(fld)
            if f.get("rename"):
                ren.append("%s->%s" % (fld, f.get("rename")))
            if f.get("type"):
                ret.append("%s:%s%s" % (fld, f.get("type"),
                                        "(%s)" % f.get("size") if f.get("size") else ""))
        out = ["keeps %d field(s)" % len(sel)]
        if des:
            out.append("drops: %s" % _fmt_list(des))
        if ren:
            out.append("renames: %s" % _fmt_list(ren))
        if ret:
            out.append("retypes: %s" % _fmt_list(ret))
        if (cfg.findtext("OrderChanged") or cfg.find("OrderChanged") is not None and
                cfg.find("OrderChanged").get("value") == "True"):
            out.append("column order changed")
        return "; ".join(out)

    if name in ("Join", "JoinMultiple"):
        by_pos = cfg.get("joinByRecordPos") == "True"
        sides = {}
        for ji in cfg.findall("JoinInfo"):
            sides[ji.get("connection")] = _fields(ji, "Field")
        anchors = sorted({c.get("outputConnection")
                          for c in cfg.findall("SelectConfiguration/Configuration")
                          if c.get("outputConnection")})
        return "join by %s on L(%s) = R(%s); output anchors configured: %s" % (
            "record position" if by_pos else "field",
            _fmt_list(sides.get("Left", [])), _fmt_list(sides.get("Right", [])),
            ", ".join(anchors) or "unspecified")

    if name == "Summarize":
        groups, acts = [], []
        for sf in cfg.findall("SummarizeFields/SummarizeField"):
            act = (sf.get("action") or "")
            if act.lower() == "groupby":
                groups.append(sf.get("field"))
            else:
                acts.append("%s(%s)%s" % (act, sf.get("field"),
                                          "->" + sf.get("rename") if sf.get("rename") else ""))
        return "group by [%s]; aggregates: %s" % (_fmt_list(groups), _fmt_list(acts, 16))

    if name == "CrossTab":
        return ("pivot: group [%s], header field [%s], data field [%s], method [%s] "
                "-- header values are NOT in the config; the explicit pivot value list "
                "must come from the data or the source output schema" % (
                    _fmt_list(_fields(cfg, "GroupFields/Field")),
                    (cfg.find("HeaderField").get("field") if cfg.find("HeaderField") is not None else "?"),
                    (cfg.find("DataField").get("field") if cfg.find("DataField") is not None else "?"),
                    ",".join(m.get("method") or "" for m in cfg.findall("Methods/Method"))))

    if name == "Unique":
        return "unique on [%s]; U anchor = first row per key, D anchor = the rest " \
               "(tie-break follows incoming row order)" % _fmt_list(_fields(cfg, "UniqueFields/Field"))

    if name == "Sort":
        return "sort by %s" % _fmt_list(
            ["%s %s" % (f.get("field"), f.get("order")) for f in cfg.findall("SortInfo/Field")], 20)

    if name == "Sample":
        return "mode=%s N=%s grouped by [%s]" % (
            cfg.findtext("Mode"), cfg.findtext("N"),
            _fmt_list(_fields(cfg, "GroupFields/Field")))

    if name == "Union":
        return "mode=%s, output=%s, on missing field=%s, explicit input order=[%s]" % (
            cfg.findtext("Mode"), cfg.findtext("ByName_OutputMode"),
            cfg.findtext("ByName_ErrorMode"),
            ", ".join(c.text or "" for c in cfg.findall("OutputOrder/Connection")))

    if name == "MultiRowFormula":
        return ("creates [%s] type %s(%s); %s rows of context; group by [%s]; "
                "rows outside the group: %s" % (
                    cfg.findtext("CreateField_Name") or cfg.findtext("UpdateField_Name"),
                    cfg.findtext("CreateField_Type"), cfg.findtext("CreateField_Size"),
                    (cfg.find("NumRows").get("value") if cfg.find("NumRows") is not None else "?"),
                    _fmt_list(_fields(cfg, "GroupByFields/Field")),
                    cfg.findtext("OtherRows")))

    if name == "MultiFieldFormula":
        return "applies one expression to fields [%s] of type %s (copy=%s)" % (
            _fmt_list([f.get("name") for f in cfg.findall("Fields/Field")
                       if f.get("selected") != "False"]),
            cfg.findtext("FieldType"), cfg.findtext("CopyOutput"))

    if name == "RunningTotal":
        return "running total over [%s] grouped by [%s]" % (
            _fmt_list(_fields(cfg, "RunningTotalFields/Field")),
            _fmt_list(_fields(cfg, "GroupByFields/Field")))

    if name == "DynamicRename":
        return "rename mode=%s; affects [%s]" % (
            cfg.findtext("RenameMode"),
            _fmt_list([f.get("name") for f in cfg.findall("Fields/Field")
                       if f.get("selected") != "False"]))

    if name == "TextToColumns":
        return "split [%s] on delimiter %r into %s columns (root %s), extra=%s" % (
            cfg.findtext("Field"),
            (cfg.find("Delimeters").get("value") if cfg.find("Delimeters") is not None else ""),
            (cfg.find("NumFields").get("value") if cfg.find("NumFields") is not None else "?"),
            cfg.findtext("RootName"), cfg.findtext("ErrorHandling"))

    if name in ("DbFileInput", "DbFileOutput"):
        f = cfg.find("File")
        raw = (f.text or "") if f is not None else ""
        # aka:CONN|||query -- keep the connection alias and the query shape, never
        # a credential, and never the full column list.
        alias, _, tail = raw.partition("|||")
        return "%s via connection %r; statement %d chars%s" % (
            "reads" if name == "DbFileInput" else "writes",
            alias.strip()[:80], len(tail),
            "; format=%s" % f.get("FileFormat") if f is not None and f.get("FileFormat") else "")

    if name == "TextInput":
        rows = cfg.findall("Data/r")
        cols = [f.get("name") for f in cfg.findall("Fields/Field")]
        return "literal table: %d row(s), columns [%s]" % (len(rows), _fmt_list(cols))

    if name == "Filter":
        return "mode=%s" % (cfg.findtext("Mode") or "Custom")

    if name.startswith("Macro:"):
        return "macro invocation; see the gap register for its resolved semantics"

    # Generic: name the elements that carry behavior, not the whole tree.
    bits = []
    for child in cfg:
        if child.tag in ("Annotation", "Passwords", "CachedCosmeticName", "MetaInfo",
                         "Style", "Position"):
            continue
        val = (child.text or "").strip() or child.get("value") or ""
        if val:
            bits.append("%s=%s" % (child.tag, val[:80]))
        elif len(child):
            bits.append("%s[%d]" % (child.tag, len(child)))
    return "; ".join(bits[:14]) or "(no behavior-bearing configuration)"


# --------------------------------------------------------------------------
# Retrieval vocabulary
# --------------------------------------------------------------------------

# Tool name -> extra retrieval query terms. Retrieval by tool name alone misses
# the section that actually explains the migration, because the reference
# indexes the *Spark* construct.
tool_terms = {
    "Formula": "formula expression translate function null",
    "MultiRowFormula": "window lag lead row order partition multi-row",
    "MultiFieldFormula": "multi field formula loop columns",
    "Filter": "filter where predicate expectation boolean",
    "Join": "join left right inner anchors unmatched broadcast",
    "JoinMultiple": "join multiple union broadcast",
    "Summarize": "groupBy agg aggregate stddev median percentile count",
    "CrossTab": "pivot groupBy pivot values explicit crosstab",
    "Transpose": "unpivot stack melt transpose",
    "Unique": "dedupe distinct dropDuplicates row_number tiebreak",
    "Sort": "orderBy sort stability determinism",
    "Sample": "limit sample first n row_number ordering",
    "Union": "unionByName allowMissingColumns schema",
    "AlteryxSelect": "select rename cast type column order",
    "DynamicRename": "rename dynamic columns expression",
    "DynamicSelect": "select dynamic type columns",
    "RunningTotal": "window cumulative sum rowsBetween running total",
    "Tile": "ntile window tile bucket",
    "RecordID": "monotonically_increasing_id row_number surrogate",
    "GenerateRows": "sequence explode generate rows loop",
    "DbFileInput": "bronze ingest read table autoloader streaming source",
    "DbFileOutput": "gold write target output table apply_changes",
    "TextInput": "literal createDataFrame static seed table",
    "RegEx": "regexp_extract regexp_replace rlike regex dialect",
    "DateTime": "to_date date_format timestamp parse locale timezone",
    "TextToColumns": "split explode delimiter columns",
    "DataCleansing": "trim null replace whitespace punctuation case cleanse",
    "Macro:Cleanse.yxmc": "trim null replace whitespace punctuation case cleanse",
    "FuzzyMatch": "fuzzy match no equivalent flag",
    "MakeGroup": "grouping connected components no equivalent",
    "AppendFields": "cross join cartesian append fields",
    "FindReplace": "join lookup find replace contains",
    "RunningSum": "window cumulative",
    "CreatePoints": "spatial sedona point geometry",
    "Spatial": "sedona spatial geometry drive time",
}

func_terms = {
    "DATETIMETODAY": "run_date parameter current_date determinism",
    "DATETIMENOW": "run_date parameter current_timestamp determinism",
    "CONTAINS": "contains case insensitive lower like",
    "TONUMBER": "cast double number conversion",
    "TOSTRING": "cast string format conversion",
    "ISNULL": "isNull null semantics coalesce",
    "NULL": "null semantics concatenation coalesce",
    "ROUND": "round multiple scale bround",
    "MEDIAN": "percentile_approx median exact",
    "STDEV": "stddev_pop population sample",
    "LEFT": "substring index 1-based left",
    "RIGHT": "substring index right length",
    "SUBSTRING": "substring index 1-based offset",
    "REPLACE": "replace regexp_replace translate",
    "DATETIMEADD": "date_add interval add months",
}


# --------------------------------------------------------------------------
# Tool classes the engine needs to know about
# --------------------------------------------------------------------------

annotation_tools = {"Comment", "TextBox", "ExplorerBox"}
grouping_tools = {"ToolContainer"}
viewer_tools = {"BrowseV2", "Browse"}
non_data_tools = annotation_tools | grouping_tools


# --------------------------------------------------------------------------
# Semantic validation: Alteryx tool -> required Spark construct
# --------------------------------------------------------------------------

required_constructs = {
    "Filter":          (re.compile(r"\.\s*(filter|where)\s*\("), "filter/where"),
    "Join":            (re.compile(r"\.\s*join\s*\("), "join"),
    "JoinMultiple":    (re.compile(r"\.\s*join\s*\("), "join"),
    "Summarize":       (re.compile(r"\.\s*groupBy\s*\("), "groupBy"),
    "CrossTab":        (re.compile(r"\.\s*pivot\s*\("), "pivot"),
    "Transpose":       (re.compile(r"\b(stack|unpivot|melt)\b"), "unpivot/stack"),
    "Unique":          (re.compile(r"\b(row_number|dropDuplicates)\b"),
                        "row_number/dropDuplicates"),
    "Sort":            (re.compile(r"\.\s*(orderBy|sort)\s*\("), "orderBy"),
    "Union":           (re.compile(r"\.\s*unionByName\s*\("), "unionByName"),
    "MultiRowFormula": (re.compile(r"\bWindow\b"), "Window"),
    "RunningTotal":    (re.compile(r"\bWindow\b"), "Window"),
    "Tile":            (re.compile(r"\b(ntile|Window)\b"), "ntile/Window"),
    "GenerateRows":    (re.compile(r"\b(sequence|explode)\b"), "sequence/explode"),
    "Sample":          (re.compile(r"\b(limit|row_number)\b"), "limit/row_number"),
    "TextToColumns":   (re.compile(r"\b(split|regexp_extract)\b"), "split"),
    "RegEx":           (re.compile(r"\b(regexp_extract|regexp_replace|rlike)\b"), "regexp_*"),
    "AppendFields":    (re.compile(r"\.\s*(crossJoin|join)\s*\("), "crossJoin"),
    "RecordID":        (re.compile(r"\b(row_number|monotonically_increasing_id)\b"),
                        "row_number/monotonically_increasing_id"),
}

ORDER_DEPENDENT = ("MultiRowFormula", "RunningTotal", "Tile", "Sample", "Unique")


def semantic_checks(ctx):
    """Alteryx-specific semantics that must survive into the target.

    ctx: .src (generated code), .census {tool: count}, .expressions [str],
         .hazards [kind], .record(check, status, detail)
    """
    src, census = ctx.src, ctx.census

    if census.get("Summarize"):
        ok = bool(re.search(r"\.\s*agg\s*\(", src))
        ctx.record("aggregation-present", "pass" if ok else "fail",
                   "groupBy().agg() present" if ok else "Summarize present but no .agg(")

    # Alteryx StdDev is population; Spark's stddev defaults to sample.
    if re.search(r"\bStdDev\b", " ".join(ctx.expressions)):
        ok = "stddev_pop" in src
        ctx.record("stddev-population", "pass" if ok else "fail",
                   "uses stddev_pop to match Alteryx" if ok
                   else "Alteryx StdDev is population; use F.stddev_pop, not F.stddev")

    # A pivot with no explicit value list is non-deterministic in column order
    # and forces an extra scan.
    if census.get("CrossTab"):
        bare = re.findall(r"\.\s*pivot\s*\(\s*([^),]+)\s*\)", src)
        ctx.record("pivot-explicit-values", "pass" if not bare else "fail",
                   "pivot value lists are explicit" if not bare
                   else "%d pivot(s) with no explicit value list" % len(bare))

    # Ordered operations need a deterministic order, including a tiebreak.
    ordered = sum(census.get(t, 0) for t in ORDER_DEPENDENT)
    if ordered:
        wins = re.findall(r"Window\s*\.\s*partitionBy\([^)]*\)\s*\.\s*orderBy\(([^)]*)\)", src)
        has_order = bool(wins) or bool(re.search(r"\.\s*orderBy\s*\(", src))
        if not has_order:
            ctx.record("deterministic-order", "fail",
                       "%d order-dependent tool(s) but no explicit ordering in the code"
                       % ordered)
        elif wins and not any("," in w for w in wins):
            ctx.record("deterministic-order", "fail",
                       "window orderBy has a single key; Alteryx row order is a total "
                       "order, so add a tiebreak or the result is non-deterministic")
        else:
            ctx.record("deterministic-order", "pass",
                       "ordered operations have an explicit, tie-broken order")

    # Hazards found at extract time must have been handled, not inherited.
    if "nondeterministic-clock" in ctx.hazards:
        ok = re.search(r"run_date|runDate|RUN_DATE", src) is not None
        ctx.record("clock-parameterized", "pass" if ok else "fail",
                   "wall-clock replaced by a run_date parameter" if ok
                   else "unit uses DateTimeToday/Now but the code has no run_date parameter")
    if "alteryx-null-semantics" in ctx.hazards:
        ctx.record("null-semantics-addressed", "skip",
                   "unit relies on Alteryx Null() semantics (string concat keeps the "
                   "non-null side; Null()==Null() is true) -- confirm the chosen "
                   "resolution is recorded as a decision")
