"""Break a workflow into dependency-ordered migration units.

A 1,500-tool workflow cannot be migrated in one context, and cutting it
arbitrarily produces units whose inputs are undefined. So we cut along the
structure the workflow already has -- Alteryx containers, then weakly-connected
segments -- and record the unit-level dependency graph so work happens in an
order where every input already exists.

The second job here is de-duplication. Copy-pasted container blocks (the same
metric computed once per region, the same cleanse macro 75 times) collapse to
one unit that is migrated once and referenced N times. On real workflows this
is the single largest token saving available.
"""
from __future__ import annotations

import hashlib
from collections import defaultdict, deque

from . import db, sources

DEFAULT_MAX_UNIT = 40
DEFAULT_MIN_UNIT = 3


def _load_graph(con):
    nodes = {}
    for r in con.execute("SELECT * FROM nodes"):
        nodes[r["node_key"]] = dict(r)
    edges = [dict(r) for r in con.execute("SELECT * FROM edges")]
    # Keep only edges whose endpoints exist (a connection can name a tool that
    # was deleted from the canvas but left in Connections).
    edges = [e for e in edges if e["origin"] in nodes and e["dest"] in nodes]
    return nodes, edges


def _topo_order(keys, succ, pred):
    """Kahn's algorithm; cycles (iterative macros, feedback wiring) are broken
    deterministically by lowest key so the order is stable across runs."""
    indeg = {k: len(pred.get(k, ())) for k in keys}
    ready = deque(sorted(k for k in keys if indeg[k] == 0))
    order, seen = [], set()
    remaining = set(keys)
    while remaining:
        while ready:
            k = ready.popleft()
            if k in seen:
                continue
            seen.add(k)
            remaining.discard(k)
            order.append(k)
            for s in sorted(succ.get(k, ())):
                indeg[s] -= 1
                if indeg[s] == 0 and s not in seen:
                    ready.append(s)
        if remaining:
            # Cycle: release the smallest remaining node and continue.
            k = min(remaining)
            indeg[k] = 0
            ready.append(k)
    return order


def _top_container(node_key, nodes):
    """Outermost container ancestor -- the natural unit boundary on a canvas."""
    cur, last = nodes[node_key]["container"], None
    while cur and cur in nodes:
        last = cur
        cur = nodes[cur]["container"]
    return last


def _block_hash(unit_nodes, nodes, internal_edges):
    """Structural fingerprint: which tools, with which configs, wired how.
    Two units with the same hash are the same transformation."""
    tools = sorted(
        "%s/%s" % (nodes[k]["tool_name"], nodes[k]["config_hash"]) for k in unit_nodes
    )
    idx = {k: i for i, k in enumerate(sorted(unit_nodes))}
    shape = sorted(
        "%d-%s>%d-%s" % (idx[o], oa or "", idx[d], da or "")
        for o, oa, d, da in internal_edges
    )
    return hashlib.sha1(("|".join(tools) + "##" + "|".join(shape)).encode()).hexdigest()[:16]


def _source(con):
    return sources.get(db.get_meta(con, "source", "alteryx"))


def build(con, max_unit=DEFAULT_MAX_UNIT, min_unit=DEFAULT_MIN_UNIT, keep_viewers=False):
    nodes, edges = _load_graph(con)
    src = _source(con)

    skip = set(src.non_data_tools) | (set() if keep_viewers else set(src.viewer_tools))
    data_keys = [k for k, n in nodes.items() if n["tool_name"] not in skip]
    data_set = set(data_keys)

    succ, pred = defaultdict(set), defaultdict(set)
    for e in edges:
        if e["origin"] in data_set and e["dest"] in data_set:
            succ[e["origin"]].add(e["dest"])
            pred[e["dest"]].add(e["origin"])

    order = _topo_order(data_keys, succ, pred)
    pos = {k: i for i, k in enumerate(order)}

    # ---- group assignment -------------------------------------------------
    # 1. Every tool inside a container joins that container's group.
    # 2. Loose tools join a group per weakly-connected component.
    groups = defaultdict(list)
    parent = {}

    def find(a):
        while parent.get(a, a) != a:
            parent[a] = parent.get(parent[a], parent[a])
            a = parent[a]
        return a

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    loose = [k for k in data_keys if _top_container(k, nodes) is None]
    for k in loose:
        parent.setdefault(k, k)
    loose_set = set(loose)
    for e in edges:
        if e["origin"] in loose_set and e["dest"] in loose_set:
            union(e["origin"], e["dest"])

    for k in data_keys:
        cont = _top_container(k, nodes)
        groups["C" + cont if cont else "L" + find(k)].append(k)

    # ---- split oversized groups along topological order -------------------
    raw_units = []
    for gid, members in groups.items():
        members.sort(key=lambda k: pos[k])
        if len(members) <= max_unit:
            raw_units.append((gid, members))
            continue
        for i in range(0, len(members), max_unit):
            chunk = members[i:i + max_unit]
            raw_units.append(("%s#%d" % (gid, i // max_unit + 1), chunk))

    # ---- coalesce runt units into their dominant predecessor --------------
    node_unit = {}
    for gid, members in raw_units:
        for k in members:
            node_unit[k] = gid
    sizes = {gid: len(m) for gid, m in raw_units}
    members_by = {gid: list(m) for gid, m in raw_units}

    for gid, _raw_members in sorted(raw_units, key=lambda x: len(x[1])):
        # Use the unit's CURRENT membership, not its original raw members: a
        # unit that has already absorbed others must carry those nodes with it
        # when it is itself absorbed, or they stay pointed at an emptied gid.
        members = members_by[gid]
        if sizes.get(gid, 0) == 0 or sizes[gid] >= min_unit:
            continue
        upstream = defaultdict(int)
        for k in members:
            for p in pred.get(k, ()):
                u = node_unit.get(p)
                if u and u != gid:
                    upstream[u] += 1
        if not upstream:
            continue
        host = max(sorted(upstream), key=lambda u: upstream[u])
        if sizes[host] + sizes[gid] > max_unit:
            continue
        members_by[host].extend(members)
        sizes[host] += sizes[gid]
        sizes[gid] = 0
        for k in members:
            node_unit[k] = host
        members_by[gid] = []

    final = [(gid, m) for gid, m in members_by.items() if m]

    # ---- name, order, fingerprint ----------------------------------------
    unit_ids, records = {}, []
    for gid, members in final:
        members.sort(key=lambda k: pos[k])
        cont_key = gid[1:].split("#")[0] if gid.startswith("C") else None
        caption = nodes[cont_key]["caption"] if cont_key in nodes else None
        seg = gid.split("#")[1] if "#" in gid else None
        title = caption or ("Segment from %s" % nodes[members[0]]["tool_name"])
        if seg:
            title = "%s (part %s)" % (title, seg)
        kind = "container" if gid.startswith("C") else "segment"
        if all(nodes[k]["is_io"] for k in members):
            kind = "io"
        if any(nodes[k]["macro_path"] for k in members) and len(members) == 1:
            kind = "macro"
        records.append((gid, members, title, kind, cont_key))

    # Stable unit ids in topological order of their earliest member.
    records.sort(key=lambda r: pos[r[1][0]])
    for i, (gid, members, title, kind, cont_key) in enumerate(records, 1):
        unit_ids[gid] = "U%03d" % i

    internal_by_unit = defaultdict(list)
    cross = set()
    for e in edges:
        uo, ud = node_unit.get(e["origin"]), node_unit.get(e["dest"])
        if uo is None or ud is None:
            continue
        if uo == ud:
            internal_by_unit[uo].append(
                (e["origin"], e["origin_anchor"], e["dest"], e["dest_anchor"]))
        else:
            cross.add((unit_ids[ud], unit_ids[uo]))

    cur = con.cursor()
    cur.execute("DELETE FROM units")
    cur.execute("DELETE FROM unit_nodes")
    cur.execute("DELETE FROM unit_deps")

    by_hash = {}
    rows, link_rows = [], []
    for seq, (gid, members, title, kind, cont_key) in enumerate(records, 1):
        uid = unit_ids[gid]
        bh = _block_hash(members, nodes, internal_by_unit.get(gid, []))
        dedupe_of = by_hash.get(bh)
        if dedupe_of is None:
            by_hash[bh] = uid
        file_id = nodes[members[0]]["file_id"]
        rows.append((uid, seq, title, kind, None, file_id, len(members),
                     "pending", dedupe_of, bh, None, db.now()))
        link_rows.extend((uid, k) for k in members)

    cur.executemany(
        "INSERT INTO units(unit_id,seq,title,kind,layer,file_id,tool_count,status,"
        "dedupe_of,block_hash,notes,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    cur.executemany("INSERT INTO unit_nodes(unit_id,node_key) VALUES(?,?)", link_rows)
    cur.executemany(
        "INSERT OR IGNORE INTO unit_deps(unit_id,depends_on) VALUES(?,?)", sorted(cross))
    con.commit()

    # Re-sequence units so unit order is a valid topological order of the unit
    # DAG, not just of their first member.
    resequence(con)

    covered = len(link_rows)
    total = len(nodes)
    dupes = sum(1 for r in rows if r[8])
    return {
        "units": len(rows),
        "duplicate_units": dupes,
        "unique_units": len(rows) - dupes,
        "tools_in_units": covered,
        "tools_total": total,
        "tools_excluded": total - covered,
        "cross_unit_edges": len(cross),
    }


def resequence(con):
    ids = [r["unit_id"] for r in con.execute("SELECT unit_id FROM units")]
    succ, pred = defaultdict(set), defaultdict(set)
    for r in con.execute("SELECT unit_id, depends_on FROM unit_deps"):
        succ[r["depends_on"]].add(r["unit_id"])
        pred[r["unit_id"]].add(r["depends_on"])
    order = _topo_order(ids, succ, pred)
    con.executemany(
        "UPDATE units SET seq=? WHERE unit_id=?",
        [(i, u) for i, u in enumerate(order, 1)])
    con.commit()


def ready(con, limit=None):
    """Units whose dependencies are all complete and that are not themselves done."""
    done = {r["unit_id"] for r in con.execute(
        "SELECT unit_id FROM units WHERE status IN ('complete','validated')")}
    deps = defaultdict(set)
    for r in con.execute("SELECT unit_id, depends_on FROM unit_deps"):
        deps[r["unit_id"]].add(r["depends_on"])
    out = []
    for r in con.execute(
            "SELECT * FROM units WHERE status NOT IN ('complete','validated') ORDER BY seq"):
        if deps[r["unit_id"]] - done:
            continue
        out.append(dict(r))
        if limit and len(out) >= limit:
            break
    return out


def excluded_nodes(con):
    """Tools legitimately left out of every unit, each with its reason.

    Only tools that are *defensibly* excludable count: canvas furniture,
    containers (their children are migrated), terminal viewers, and tools with
    no connection at all. A wired data tool that is missing from every unit is
    not an exclusion -- it is a silent skip, and `unaccounted_nodes` reports it
    so structural validation can fail on it.
    """
    src = _source(con)
    placed = {r["node_key"] for r in con.execute("SELECT node_key FROM unit_nodes")}
    wired = set()
    for r in con.execute("SELECT origin, dest FROM edges"):
        wired.add(r["origin"])
        wired.add(r["dest"])
    out = []
    for r in con.execute("SELECT node_key, tool_name FROM nodes"):
        if r["node_key"] in placed:
            continue
        name = r["tool_name"]
        if name in src.annotation_tools:
            reason = "canvas annotation"
        elif name in src.grouping_tools:
            reason = "container (its children are migrated)"
        elif name in src.viewer_tools:
            reason = "terminal viewer, no data path"
        elif r["node_key"] not in wired:
            reason = "unconnected on the canvas"
        else:
            continue        # wired data tool -> unaccounted, not excluded
        out.append({"node_key": r["node_key"], "tool": name, "reason": reason})
    return out


def unaccounted_nodes(con):
    """Wired data tools that reached no migration unit. Must always be empty."""
    placed = {r["node_key"] for r in con.execute("SELECT node_key FROM unit_nodes")}
    legit = {e["node_key"] for e in excluded_nodes(con)}
    return [{"node_key": r["node_key"], "tool": r["tool_name"]}
            for r in con.execute("SELECT node_key, tool_name FROM nodes")
            if r["node_key"] not in placed and r["node_key"] not in legit]
