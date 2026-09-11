# Summary Plan (example) — Helårsboligomsetninger siste ti år

Trimmed illustration of a Phase-6 `summary-plan.md`. Parsed from raw XML.
1,533 nodes = 1,430 tools + 102 containers + 1 comment · 1,681 connections · acyclic ·
legacy E1 engine · `ConvErrorLimit=10` non-stop · no workflow constants · empty `Events`.

---

## Part A — Source data

All 37 `Input Data` tools use one Alteryx alias `aka:D1` (a SQL Server instance,
`FileFormat="23"`). The 37 reads collapse to **15 distinct logical sources**; 18 are
duplicate small geography lookups pulled once per region-grain block.

| # | Source | Filter | Feeds |
|---|---|---|---|
| S1 | `Matrikkelen_Omsetning_Verdiobjekt` ⋈ `Matrikkelen_Eier` ⋈ `Matrikkelen_Omsetning` (ToolID 1) | `OmsetningType=1 & KunNyeEiere=1 & EiendomAnvendelse='B' & OmsetningTinglystDato >= '01.01.2003'` | the transaction **spine** for every statistic block |
| S2 | `Matrikkelen_Omsetning_Verdiobjekt` ⋈ `Matrikkelen_Omsetning` (ToolID 1983) | `KjoperSelger='K' & Omsetning>0 & AndelProsent>=25 & KunNyeEiere=1 & EiendomAnvendelse='B' & HeleAndelenOmsatt=1` | first-time buyers |
| S3 | `Matrikkelen_Eier` full (1810) | — | company-vs-private buyer classification |
| S4 | `Matrikkelen_Bruksenhet` ⋈ `Matrikkelen_Bygg` (1368) | `BruksenhetType='B'` | usable area + build year per VerdiobjektId |
| S5 | + `X_ByggType` (1584) | join on `ByggType` | cabin / apartment classification |
| S6 | `Matrikkelen_Bruksenhet` ⋈ `Matrikkelen_Bygg` (1775/1788/1873/1886, 4 filter variants) | mixed | hyttetype classification, build-permit stats |
| S7 | `Matrikkelen_Bygg` ⋈ `Krets_Grunnkrets` (813) | `Eksist=1 & Left(ByggType,1)=1` | holiday-home building stock |
| S8 | `N_Befolkning.dbo.Kjopskapasitet_Aar` (1043, wide) | — | buyer gross-income bands |
| S9 | `N_Befolkning.dbo.KjonnOgOpprinnelse` (348 / 1816) | — | first name → gender / Norwegian-or-foreign |
| S10 | `Krets_Kommune` ⋈ `Krets_Fylke` | — | municipality labels (read 9×) |
| S11 | `Krets_Fylke` | — | county labels (read 9×) |
| S12 | `Krets_DelOmraade` ⋈ `Krets_Kommune` | — | district labels (read 3×) |
| S13 | `Krets_Grunnkrets` (2230), `Krets_Postnummer` (826) — incl. `Polygon` | — | **spatial** point-in-polygon assignment |
| S14 | `X_ByggEndring` (1911), `X_EiendomBoligType` (1978) | — | code → name lookups |
| S15 | `Verdsetting_BoligData` (2249) | — | modelled dwelling values + bands |

Plus 4 `Text Input` seed tables (the 127-row `ByggType → Hyttetype` map, ×3).

**Source-side hazards:** `OmsetningTinglystDato >= '01.01.2003'` hard-coded in S1;
`select top 10000000000` full snapshots; S6 read 4× with different filters (reproduce
exactly, do not merge); polygon CRS must be confirmed (S7 `Create Points` uses `EPSG:32633`).

---

## Part B — Transformations

One transaction **spine** (S1), cleansed and enriched, then fanned out into **10 statistic
blocks**, each computed **8×** = 4 region grains (`1.Land / 2.Fylke / 3.Kommune / 4.Delområde`)
× 2 `Hyttetype` modes (rolled-up `'Alle hytter'` split by ownership form, vs detailed type).
Everything is finally `Union`-ed into one long table (one row per
`Regiontype × Region × Hyttetype × Eieform × Omsetningsår`) and written to Excel.

**Stage 1 — spine build:** price filter `[Omsetning] != 0 && < 70000000`;
`Tinglysningsår = Left([OmsetningTinglystDato],4)`; drop transactions where a buyer surname
== a seller surname (container 41); co-purchase flag `CountDistinct(EierId) per OmsetningId ≥ 2`
(container 2358); company-vs-private buyer (`EndsWith(Kjoper,' AS'|' ASA'|' ANS')`,
case-insensitive — container 1824); assumed origin from first name (container 397).

**Stage 2 — enrichment:** usable area + build year + m²-price + transaction-size bands
(container 868); latest-year buyer gross income + 14 bands (container 1045); cabin/apartment
classification `IF AntallEiendommer>=3 OR AntallEtasjer>=3 OR BorettsAndelNr not null →
'Fritidsleilighet' ELSEIF Bruksareal>=180 → 'Hytte'` (containers 1588/1589/1576/1583/1585);
first-time buyer = owner's first-ever purchase + age 20-39 (container 1990).

**Stage 3 — the 10 statistic blocks (×8 each):**

| Block | Computes |
|---|---|
| PRISER ETC. | count, count YoY %, avg/median/stddev(pop) price, avg BRA, m²-price, YoY %; weighted centroid + convex hull (spatial) |
| ANTALL KJØPERE | distinct buyers, buyers per transaction |
| ALDER OG KJØNN | gender share, age-band share (Cross Tab method `XRow` = row %), avg age, avg/median straight-line buyer→object distance |
| ANTATT OPPRINNELSE | Norwegian / foreign buyer share |
| MAKSPRIS | highest-price transaction per (year, grain) via `Sort desc` + `RunningTotal(1)==1`; excludes company buyers |
| Z-score | `Abs((Standardavvik pris − Avg_Omsetning) / StdDev_Omsetning)` per (Hyttetype, year) |
| OMSETNINGSINTERVALLER | Cross Tab of 11 transaction-size bands |
| BRUTTOINNTEKT | Cross Tab of 14 income bands + avg/median income |
| BOSTEDSKOMMUNE EIERE | buyer municipality-of-residence + straight-line distance |
| Andel førstegangsomsetning | `Førstegangsomsatt = (OmsetningTinglystDato == min per VerdiobjektId) AND DiffByggeårOmsÅr <= 6`; construction-year bands |

**Stage 4 — building stock / build permits:** container 831 (spatial point-in-polygon to
count `ByggId`), container 1972 "Tilbygg" (103 tools — parse `ByggStatushistorikk`, cross-tab
`Rammetillatelse / Igangsettingstillatelse / Fullførte` × `ANTBYGG / ANTBOLIGER / BRA /
SNITTBOLIG_M2`).

**Stage 5 — assembly:** `Join Multiple` + `Union` on `(Regiontype, Region, Hyttetype,
Omsetningsår)`; region labels resolved via S10/S11/S12 joins; final `Select` pins the
~135-column output order.

**Stage 6 — forecast:** monthly transaction series → `ETS` (R exponential smoothing,
12 periods, auto) → `TS_Forecast` → cumulative `Running Total` → workbook 2; a year-end
projection row is also unioned into workbook 1.

**Semantic deltas that change the result in Spark:** string `+` ignores nulls (labels);
`null==null` is True (Multi-Row grain guard); `Median` exact vs `percentile_approx`;
`StdDev` = population in Summarize; `Round(x,10000)` = round to multiple; `Contains/EndsWith`
case-insensitive; `Unique` / `RunningTotal==1` / `Sample` depend on input order;
`DateTimeToday()` in ~30 expressions.

---

## Part C — Output data

**Workbook 1** `Omsetningstabell - Helårsboliger - Siste ti år.xlsx` (Sheet1): one row per
`Regiontype × Region × Hyttetype × Eieform × Omsetningsår` for 10 years + a current-year
projection row. **7 dimensions** + **~128 measures** in groups: volume, price, max price,
11 transaction-size bands, buyer gender/age, buyer origin, buyer entity (foretak/private),
14 income bands, buyer distance, new-build share + 12 construction-year bands, 24 build-permit
columns, dwelling value + 16 value bands, map (centroid + convex hull GeoJSON), metadata.

**Workbook 2** `12-måneders prognoser antall omsetninger - Helårsboliger.xlsx` (Sheet1):
one row per forecast month; `Periode`, `Antall omsetninger` (cumulative forecast),
`forecast_high_95`, `forecast_low_95`. Scope: `Land / Hele Norge / Alle helårsboliger`.

Both write `OutputOption=Overwrite` with a header row. In Databricks these become Delta gold
tables; the `.xlsx` files are produced by a downstream export task (or the dashboard is
repointed to Delta / Databricks SQL).
