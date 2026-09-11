# Worked example — "Helårsboligomsetninger siste ti år"

A real Alteryx workflow migrated with this skill. Norwegian full-year housing-transaction
statistics for the last ten years, feeding two dashboard workbooks.

**Source shape:** 1,533 nodes = 1,430 processing tools + 102 Tool Containers + 1 comment ·
1,681 connections · legacy E1 engine · 37 `Input Data` (one SQL Server alias) collapsing to
15 distinct sources · 2 Excel outputs · macros: `Cleanse.yxmc` ×75 (standard sample),
`ETS.yxmc` / `TS_Forecast.yxmc` ×1 (R time-series).

**Key structural finding:** ten "statistic block" containers each appear **8×** (4 region
grains × 2 dwelling-type aggregation modes), copy-pasted. In the target they become **one
parameterised transform** over a single grain-explosion table — the count collapses from
1,430 tools to ~35 SDP tables + ~15 utility helpers.

**Confidence tally (1,430 tools):** direct 266 · adapted 1,068 · needs-design 72 (all
`Multi-Row Formula` + `Unique` — order-dependent) · no-equivalent 11 (9 spatial + 2 forecast
macros).

## Files

| File | Corresponds to |
|---|---|
| `helaarsbolig-summary.md` | Phase 6 `summary-plan.md` — source catalogue, transformation narrative, output schema |
| `helaarsbolig-sdp-plan.md` | Phase 6 `sdp-plan.md` — layer-by-layer PySpark build (Bronze / Silver / Gold), semantic-delta handling, optimization, risks |

These are **trimmed** for illustration. A full run also produces `workflow-inventory.md`
(one entry per ToolID, ~400 KB here) and `transformation-map.csv` (one row per ToolID).
