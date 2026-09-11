#!/usr/bin/env python
"""Entry point: `python scripts/mig.py <command> ...`

Stdlib only, so it runs wherever the workflow files are without a pip install.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mig.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main() or 0)
