# Source-Workflow Parity Harness (Optional)

Use this only to capture **reference outputs** from the source Alteryx workflow so the migrated Databricks pipeline can be validated against them. It is optional and requires a local, licensed Alteryx Designer / Engine. It is never used to modify the workflow.

If no Engine is available, skip this and define parity against an existing known-good sample output the user provides instead.

## Bundled scripts

Run with PowerShell from the skill's `scripts/` directory, or with absolute paths.

- `scripts/Get-DesignerVersion.ps1` — discover the installed Designer/Engine version. Record it in the plan; engine version affects some tool behavior.
- `scripts/Find-DesignerSampleWorkflows.ps1` — locate the Designer install root and sample workflows.
- `scripts/Invoke-AlteryxWorkflow.ps1` — run a workflow with `AlteryxEngineCmd.exe` and capture stdout/stderr, timeout state, and the true exit code.

The scripts discover the install from explicit arguments, environment variables (`ALTERYX_DESIGNER_ROOT`, `ALTERYX_ENGINE_EXE`), `InstallInfo.ini`, registry entries, and common `Program Files` paths. If discovery fails, pass `-DesignerRoot` or `-EnginePath`.

## Capturing reference outputs

1. Copy the workflow to a scratch location so the original is untouched.
2. Edit only the output tools in the copy to write deterministic, comparable formats (CSV or a Parquet/YXDB you can read from Spark) to a known folder. Leave all transformation logic unchanged.
3. Run it:

   ```powershell
   powershell -ExecutionPolicy Bypass -File .\scripts\Invoke-AlteryxWorkflow.ps1 C:\scratch\workflow_copy.yxmd -Json
   ```

4. Verify: exit code `0`, substantive engine diagnostics (not banner-only), and that each expected output file was actually written. Treat banner-only output or missing files as an indeterminate failed run.
5. For configuration-only validation without processing records, add `updateMode="Full"` to the root `AlteryxDocument` of the copy, run, then discard the copy (see `workflow-xml.md`).

## Reconciliation against the migrated pipeline

In a Databricks notebook (`tests/reconcile.py`):

- **Row counts** per final output, source vs target.
- **Column-level checksums**: `df.select(F.sum(F.xxhash64(*cols)))` or per-column `F.sum(F.hash(col))`, compared after sorting-insensitive aggregation.
- **Aggregate checks**: sum/min/max/count-distinct of key numeric and id columns.
- **Full anti-join** on business keys for small outputs to list exact row differences.
- **Tolerances**: exact for ids and counts; `abs(a-b) <= epsilon` for floating-point measures (mirror any Alteryx `CompareEpsilon`).

Record expected, tolerated, and unexplained differences in the plan's parity section. Common legitimate differences: ordering, dedupe tie-breaks, timezone handling, and float rounding — each should be a documented, accepted deviation, not a silent one.
