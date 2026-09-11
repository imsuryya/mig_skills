"""BM25 retrieval over the repository's own skill corpus.

The reference material in this repo is far too large to load wholesale: the
Alteryx and Databricks references together run to thousands of lines, and the
tool reference alone is 2,500. Loading all of it per migration unit would cost
more than the unit is worth.

So the corpus is chunked by heading, indexed with BM25 (k1=1.5, b=0.75), and
queried per unit using that unit's own tool and function census. The model sees
a handful of sections chosen for the transformation actually in front of it.

Pure stdlib, no index server, no embeddings -- the index for this repo builds in
well under a second and is cached in the run directory.
"""
from __future__ import annotations

import json
import math
import os
import re
import time

TOKEN = re.compile(r"[A-Za-z_][A-Za-z0-9_]+")
K1, B = 1.5, 0.75
MAX_CHUNK_CHARS = 2600
DEFAULT_TOP_K = 6
DEFAULT_BUDGET = 9000     # chars of retrieved material per call

def _chunk_markdown(path, text):
    """Split on headings, keeping the heading path as retrievable context."""
    lines = text.splitlines()
    chunks, cur, heads = [], [], {}
    title = os.path.basename(path)

    def flush():
        if not cur:
            return
        body = "\n".join(cur).strip()
        if not body:
            return
        head = " > ".join(heads[k] for k in sorted(heads) if heads[k])
        for i in range(0, len(body), MAX_CHUNK_CHARS):
            piece = body[i:i + MAX_CHUNK_CHARS]
            chunks.append({
                "path": path, "title": title,
                "heading": head or title,
                "text": piece,
            })

    for ln in lines:
        m = re.match(r"^(#{1,6})\s+(.*)$", ln)
        if m:
            flush()
            cur = []
            lvl = len(m.group(1))
            heads = {k: v for k, v in heads.items() if k < lvl}
            heads[lvl] = m.group(2).strip()
            cur.append(ln)
        else:
            cur.append(ln)
    flush()
    return chunks


YAML_KEY = re.compile(r"^[A-Za-z0-9_.\-]+:")
YAML_ITEM = re.compile(r"^-\s+\S")
# What to call a sequence entry, in order of preference.
YAML_LABEL = re.compile(r"^\s*(?:-\s+)?(tool_name|name|plugin_name|id)\s*:\s*(\S.*?)\s*$")


def _yaml_label(lines, fallback):
    for ln in lines[:8]:
        m = YAML_LABEL.match(ln)
        if m:
            return m.group(2)
    return fallback


def _chunk_yaml(path, text):
    """One chunk per top-level entry.

    Two shapes appear in this repo and both must index. A *mapping* chunks per
    top-level key. A *sequence* chunks per `- ` item -- which is how
    `designer-tool-reference.yaml` lists its plugins, and which the key-only
    split missed entirely: the largest reference in the corpus produced zero
    chunks and was silently absent from retrieval. Sequence entries are titled by
    their own `tool_name`/`name`, so a query for a tool finds that tool.
    """
    chunks, cur, key = [], [], None
    title = os.path.basename(path)

    def flush():
        if cur and key:
            body = "\n".join(cur).strip()
            if body:
                for i in range(0, len(body), MAX_CHUNK_CHARS):
                    chunks.append({"path": path, "title": title,
                                   "heading": "%s: %s" % (title, key),
                                   "text": body[i:i + MAX_CHUNK_CHARS]})

    for ln in text.splitlines():
        if YAML_ITEM.match(ln):
            flush()
            cur = [ln]
            key = _yaml_label(cur, "entry %d" % (len(chunks) + 1))
        elif YAML_KEY.match(ln):
            flush()
            key, cur = ln.split(":", 1)[0], [ln]
        else:
            cur.append(ln)
            # A sequence item's label usually sits on a later line than its dash.
            if key and key.startswith("entry "):
                key = _yaml_label(cur, key)
    flush()
    return chunks


def build_index(roots, cache_path=None):
    docs = []
    latest = 0.0
    for root in roots:
        if os.path.isfile(root):
            paths = [root]
        else:
            paths = []
            for dirpath, dirnames, filenames in os.walk(root):
                dirnames[:] = [d for d in dirnames if not d.startswith(".")]
                for fn in sorted(filenames):
                    if fn.lower().endswith((".md", ".yaml", ".yml")):
                        paths.append(os.path.join(dirpath, fn))
        for p in sorted(paths):
            try:
                latest = max(latest, os.path.getmtime(p))
                with open(p, encoding="utf-8", errors="replace") as fh:
                    text = fh.read()
            except OSError:
                continue
            docs.extend(_chunk_yaml(p, text) if p.lower().endswith((".yaml", ".yml"))
                        else _chunk_markdown(p, text))

    for d in docs:
        d["tokens"] = [t.lower() for t in TOKEN.findall(d["heading"] + "\n" + d["text"])]

    df = {}
    for d in docs:
        for t in set(d["tokens"]):
            df[t] = df.get(t, 0) + 1
    n = max(len(docs), 1)
    idf = {t: math.log(1 + (n - c + 0.5) / (c + 0.5)) for t, c in df.items()}
    avgdl = sum(len(d["tokens"]) for d in docs) / n

    index = {
        "built_at": time.time(),
        "corpus_mtime": latest,
        "roots": [os.path.abspath(r) for r in roots],
        "avgdl": avgdl,
        "idf": idf,
        "docs": [{
            "path": d["path"], "heading": d["heading"], "text": d["text"],
            "len": len(d["tokens"]),
            "tf": _tf(d["tokens"]),
        } for d in docs],
    }
    if cache_path:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as fh:
            json.dump(index, fh)
    return index


def _tf(tokens):
    out = {}
    for t in tokens:
        out[t] = out.get(t, 0) + 1
    return out


def load_index(roots, cache_path):
    if cache_path and os.path.isfile(cache_path):
        try:
            with open(cache_path, encoding="utf-8") as fh:
                idx = json.load(fh)
            newest = 0.0
            for root in idx.get("roots", []):
                if os.path.isfile(root):
                    newest = max(newest, os.path.getmtime(root))
                elif os.path.isdir(root):
                    for dp, dn, fns in os.walk(root):
                        dn[:] = [d for d in dn if not d.startswith(".")]
                        for fn in fns:
                            if fn.lower().endswith((".md", ".yaml", ".yml")):
                                newest = max(newest, os.path.getmtime(os.path.join(dp, fn)))
            if newest <= idx.get("corpus_mtime", 0) + 1e-6:
                return idx
        except (OSError, ValueError, KeyError):
            pass
    return build_index(roots, cache_path)


def search(index, query, top_k=DEFAULT_TOP_K, budget=DEFAULT_BUDGET, path_filter=None):
    q = [t.lower() for t in TOKEN.findall(query)]
    if not q:
        return []
    idf, avgdl = index["idf"], index["avgdl"] or 1.0
    scored = []
    for i, d in enumerate(index["docs"]):
        if path_filter and path_filter not in d["path"].replace("\\", "/"):
            continue
        tf, dl = d["tf"], d["len"] or 1
        s = 0.0
        for t in q:
            f = tf.get(t)
            if not f:
                continue
            s += idf.get(t, 0.0) * (f * (K1 + 1)) / (f + K1 * (1 - B + B * dl / avgdl))
        if s > 0:
            scored.append((s, i))
    scored.sort(key=lambda x: (-x[0], x[1]))

    out, used = [], 0
    for s, i in scored[:top_k * 3]:
        d = index["docs"][i]
        if used + len(d["text"]) > budget and out:
            continue
        out.append({"score": round(s, 3), "path": d["path"],
                    "heading": d["heading"], "text": d["text"]})
        used += len(d["text"])
        if len(out) >= top_k:
            break
    return out


def query_for_unit(tool_census, func_census=None, extra="", source=None):
    """Turn a unit's census into a retrieval query weighted by what it contains.

    The term maps come from the source adapter: querying the corpus for
    "Summarize" alone misses the section that explains the migration, because
    the reference indexes the *Spark* construct.
    """
    tool_terms = getattr(source, "tool_terms", {}) or {}
    func_terms = getattr(source, "func_terms", {}) or {}
    parts = []
    for tool, count in sorted((tool_census or {}).items(), key=lambda x: -x[1]):
        weight = 1 + min(int(math.log2(count + 1)), 3)
        parts.extend([tool] * weight)
        terms = tool_terms.get(tool)
        if terms:
            parts.extend([terms] * weight)
    for fn, count in sorted((func_census or {}).items(), key=lambda x: -x[1])[:12]:
        parts.append(fn)
        if func_terms.get(fn):
            parts.append(func_terms[fn])
    if extra:
        parts.append(extra)
    return " ".join(parts)
