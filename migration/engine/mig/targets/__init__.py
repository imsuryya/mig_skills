"""Target adapters: one per platform being migrated *to*.

Keeps target-side rules (which code constructs are forbidden, what the generated
artifact must look like) out of the engine, so a second target is an adapter
rather than a fork.

A target adapter is a module exposing:

    name       str   -- registry key, e.g. "databricks-sdp"
    language   str   -- artifact language, e.g. "python"
    forbidden  list  -- (check_name, compiled regex, why) triples applied to
                        generated code by validate.code
    runtime_globals set -- names the runtime provides, so name resolution does
                        not flag them as undefined
    code_checks(ctx) -- target-specific structural checks over the parsed module
"""
from __future__ import annotations

from . import databricks_sdp

_REGISTRY = {}


def register(module) -> None:
    _REGISTRY[module.name] = module


def get(name: str):
    try:
        return _REGISTRY[name]
    except KeyError:
        raise SystemExit(
            "unknown target %r; available: %s" % (name, ", ".join(sorted(_REGISTRY))))


def available():
    return sorted(_REGISTRY)


register(databricks_sdp)
