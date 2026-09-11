"""Target adapter: Databricks Lakeflow Spark Declarative Pipelines (PySpark).

Rules here are about the *target* artifact, independent of which ETL platform
the logic came from. Source-specific semantics live in `mig/sources/`.
"""
from __future__ import annotations

import ast
import re

name = "databricks-sdp"
language = "python"

# Names the DLT/Spark runtime injects, so name resolution must not flag them.
runtime_globals = {"spark", "dlt", "dbutils", "sc", "sqlContext"}

# Constructs the target-code rules forbid: logic must use the typed DataFrame
# API, so SQL strings and untyped expressions are failures, not style notes.
forbidden = [
    ("sql-string", re.compile(r"\bspark\s*\.\s*sql\s*\("),
     "SQL string in transformation logic; use the DataFrame API"),
    ("selectExpr", re.compile(r"\.\s*selectExpr\s*\("),
     "selectExpr uses SQL strings; use typed select/withColumn"),
    ("F.expr", re.compile(r"\bF\s*\.\s*expr\s*\("),
     "F.expr uses a SQL string; use the typed API"),
    ("wallclock", re.compile(r"\bF\s*\.\s*current_(date|timestamp)\s*\("),
     "wall-clock call makes the pipeline non-reproducible; use the run_date parameter"),
    ("collect-in-transform", re.compile(r"\.\s*(collect|toPandas)\s*\("),
     "driver-side materialization inside a pipeline transform"),
]


def code_checks(ctx):
    """Target-specific structural checks over the parsed module.

    ctx: .tree (ast.Module), .src (str), .label (str), .record(check, status, detail)
    """
    # A @dlt.table function that returns nothing produces an empty table.
    empty = []
    for n in ast.walk(ctx.tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            dec_src = " ".join(ast.unparse(d) for d in n.decorator_list)
            if "dlt.table" in dec_src or "dlt.view" in dec_src:
                if not any(isinstance(x, ast.Return) and x.value is not None
                           for x in ast.walk(n)):
                    empty.append(n.name)
    ctx.record("dlt-returns-dataframe:%s" % ctx.label,
               "pass" if not empty else "fail",
               "every dlt dataset function returns a DataFrame" if not empty
               else "no return value: %s" % ", ".join(empty))
