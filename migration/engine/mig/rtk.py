"""RTK integration for the commands the engine spawns itself.

RTK (`rtk-ai/rtk`) is a CLI proxy that compresses third-party command output
before it reaches a model. On Claude Code it installs a PreToolUse hook
(`rtk init -g`) that rewrites Bash commands transparently -- which covers every
command the *skill* tells the model to run, but not one the engine runs behind
its back.

That is the gap this closes. `lakebridge.run_analyzer` spawns the analyzer
through `subprocess.run`, so it never passes through the Bash tool and the hook
never sees it -- and the analyzer is the noisiest third-party command in the
pipeline. Routing it through `rtk proxy` puts it back under RTK's accounting.

`proxy` is deliberate: RTK optimizes commands it recognises (git, ls, cargo) and
`databricks` is not one of them, so `proxy` -- documented as raw passthrough
with tracking -- is the form that cannot change the command's behaviour. RTK
stays entirely optional; every helper here degrades to plain invocation.
"""
from __future__ import annotations

import os
import shutil

# Set MIG_NO_RTK=1 to force direct invocation even where rtk is installed.
DISABLE_ENV = "MIG_NO_RTK"


def available():
    """Path to the rtk binary, or None. Honours the opt-out."""
    if os.environ.get(DISABLE_ENV, "").strip() not in ("", "0", "false", "no"):
        return None
    return shutil.which("rtk")


def wrap(cmd):
    """(command, used_rtk). Prefixes `rtk proxy` when rtk is installed."""
    if not cmd:
        return cmd, False
    if available():
        return ["rtk", "proxy"] + list(cmd), True
    return list(cmd), False


def status():
    """What to tell the user about RTK's involvement in this run."""
    path = available()
    if path:
        return {"available": True, "path": path,
                "note": "third-party commands routed through `rtk proxy`"}
    if os.environ.get(DISABLE_ENV, "").strip() not in ("", "0", "false", "no"):
        return {"available": False, "path": None,
                "note": "disabled by %s" % DISABLE_ENV}
    return {"available": False, "path": None,
            "note": "not installed; third-party output not compressed "
                    "(`rtk init -g` to enable)"}
