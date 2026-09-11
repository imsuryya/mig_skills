"""Source adapters: one per ETL platform being migrated *from*.

The engine is source-agnostic. Everything that knows what an Alteryx tool is
lives behind this contract, so adding SSIS or DataStage means writing one
adapter, not forking the engine.

A source adapter is a module exposing:

    name                    str    -- registry key, e.g. "alteryx"
    extensions              tuple  -- file suffixes it claims, e.g. (".yxmd", ".yxmc")
    lakebridge_source_tech  str    -- value for `--source-tech`, or None
    extract(con, path, search_paths) -> dict
                                   -- parse into nodes/edges/expressions/hazards/gaps
    describe(node)          str    -- one compact, behavior-bearing line for a tool
    tool_terms              dict   -- tool name -> extra retrieval query terms
    func_terms              dict   -- function name -> extra retrieval query terms
    non_data_tools          set    -- canvas furniture; inventoried, never migrated
    viewer_tools            set    -- terminal viewers with no data path
    required_constructs     dict   -- tool -> (compiled regex, human label)
    semantic_checks(ctx)           -- source-specific semantic validation

See `alteryx.py` for the reference implementation and `ctx` contract.
"""
from __future__ import annotations

import os

from . import alteryx

_REGISTRY = {}


def register(module) -> None:
    _REGISTRY[module.name] = module


def get(name: str):
    try:
        return _REGISTRY[name]
    except KeyError:
        raise SystemExit(
            "unknown source %r; available: %s" % (name, ", ".join(sorted(_REGISTRY))))


def for_file(path: str):
    """Pick the adapter that claims this file's extension."""
    ext = os.path.splitext(path)[1].lower()
    for mod in _REGISTRY.values():
        if ext in mod.extensions:
            return mod
    raise SystemExit(
        "no source adapter handles %r; known extensions: %s"
        % (ext, ", ".join(sorted(e for m in _REGISTRY.values() for e in m.extensions))))


def available():
    return sorted(_REGISTRY)


register(alteryx)
