# SDP Plan (example) — Helårsboligomsetninger, PySpark

Trimmed illustration of a Phase-6 `sdp-plan.md`. All transformation logic is the PySpark
DataFrame API; the only SQL strings are `@dlt.expect_*` predicates.

## Layer model

| Layer | Schema | Contents |
|---|---|---|
| Bronze | `hbo_bronze` | ~19 `@dlt.table`, one per distinct source S1–S15 (18 duplicate lookups de-duplicated) + seed |
| Silver | `hbo_silver` | `silver_omsetning_spine` (cleansed + enriched fact) + `silver_eier`, `silver_bygg_enhet`, `silver_hyttetype`, `silver_firsttime`, `silver_income`, geo dims, `silver_geo_assign` (Sedona) |
| Silver-grain | `hbo_silver` | `silver_omsetning_grain` — spine × 5 region grains × 2 hyttetype modes. **Replaces the Alteryx 8× copy-paste.** |
| Gold-metric | `hbo_gold` | one `@dlt.table` per statistic block, each a parameterised fn over `silver_omsetning_grain` |
| Gold-serve | `hbo_gold` | `gold_omsetningstabell` (135-col wide join) + `gold_prognose_12mnd` |
| Job | — | `forecast_job.py` (ETS replacement), `excel_export.py` |
| Drop | — | 13 `Browse`, 1 `Comment`, 102 `Tool Container` |

## Parameters (`resources/pipeline.yml` → `spark.conf`)

`run_date`, `run_year`, `window_years` (10), `hist_start` (`2003-01-01`), `price_cap`
(70_000_000), `bra_min/max` (15/600), `m2price_min/max` (2000/150000),
`firsttime_year_diff` (6), `crs` (`EPSG:32633`), `source_scope`, `secret_scope`.
No `F.current_date()` anywhere — CI lint enforces it.

## Shared helpers (`utilities/`)

```python
from pyspark.sql import functions as F, Window
from functools import reduce

def label(*parts):                                   # Alteryx str '+' ignores nulls
    return F.concat_ws("", *[F.coalesce(x if isinstance(x, F.Column) else F.lit(x), F.lit("")) for x in parts])

def left_year(col):  return F.substring(col.cast("string"), 1, 4)
def round_to(col, m): return F.round(col / F.lit(m)) * F.lit(m)
def pct_change(cur, prev): return F.when(prev.isNotNull() & (prev != 0), (cur/prev - F.lit(1)) * F.lit(100))

def naeringskjop(kjoper):                             # case-insensitive (Alteryx default)
    low = F.lower(F.coalesce(kjoper, F.lit("")))
    return (low.rlike(r"( as| asa| ans)$") | low.rlike(r" (as|asa|ans) ")).cast("int")

def yoy(df, metric, alias):
    w = Window.partitionBy("Regiontype", "Region", "Hyttetype").orderBy(F.col("Omsetningsaar").cast("int"))
    return df.withColumn(alias, pct_change(F.col(metric), F.lag(metric).over(w)))

def first_per_key(df, keys, order_cols):              # Unique / RunningTotal==1 / top-1 (explicit tiebreak)
    w = Window.partitionBy(*keys).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")

def row_pct_pivot(df, group, header, values):         # Cross Tab method 'XRow'
    piv = df.groupBy(*group).pivot(header, values).agg(F.count(F.lit(1))).na.fill(0)
    total = reduce(lambda a, b: a + b, [F.col(v) for v in values])
    for v in values:
        piv = piv.withColumn(v, F.when(total != 0, F.col(v) / total))
    return piv

GK = ["Regiontype", "Region", "Hyttetype", "Omsetningsaar"]
```

## Bronze (representative)

```python
import dlt
def src(t): return spark.read.table(f"{SRC}.{t}")

@dlt.table(name="bronze_omsetning_spine", table_properties={"delta.autoOptimize.optimizeWrite": "true"})
@dlt.expect_all_or_fail({"has_rows": "count(1) > 0"})
def bronze_omsetning_spine():
    ov = src("Matrikkelen_Omsetning_Verdiobjekt")
    e  = src("Matrikkelen_Eier").select("EierId", "EierNavn", "PostNr", "Kjonn",
             F.col("KommuneNr").alias("KommuneNr1"), F.col("Lat").alias("Lat1"), F.col("Long").alias("Long1"))
    o  = src("Matrikkelen_Omsetning").select("OmsetningId", "AntallBoenheter", "AntallBygg",
             "Selger", "Kjoper", F.col("KommuneNrK").alias("KommuneNr2"), "OmsetningObjekt", "Lat", "Long")
    return (ov.filter((F.col("OmsetningType") == 1) & (F.col("KunNyeEiere") == 1)
                      & (F.col("EiendomAnvendelse") == "B")
                      & (F.col("OmsetningTinglystDato") >= F.lit(HIST_START)))
              .join(e, "EierId", "inner").join(o, "OmsetningId", "inner")
              .withColumn("_ingest_ts", F.to_timestamp(F.lit(RUN_DATE))))
```

The 9 identical `Krets_Kommune ⋈ Krets_Fylke` reads → one `bronze_dim_kommune`. The 127-row
`ByggType → Hyttetype` map → one `spark.createDataFrame` seed table.

## Silver — spine (excerpt)

```python
@dlt.table(name="silver_omsetning_spine", partition_cols=["_oms_year"])
@dlt.expect_all({"price_in_range": "Omsetning > 0 AND Omsetning < 70000000",
                 "bra_sane": "Sum_Bruksareal IS NULL OR Sum_Bruksareal BETWEEN 15 AND 600"})
@dlt.expect_all_or_drop({"has_omsetningid": "OmsetningId IS NOT NULL"})
def silver_omsetning_spine():
    b = (dlt.read("bronze_omsetning_spine")
         .filter((F.col("Omsetning") != 0) & (F.col("Omsetning") < F.lit(PRICE_CAP)))     # ToolID 12
         .withColumn("Tinglysningsaar", left_year(F.col("OmsetningTinglystDato")))        # ToolID 7
         .withColumn("Fylkesnummer", F.substring("KommuneNr", 1, 2))
         .withColumn("_oms_year", left_year(F.col("OmsetningTinglystDato")).cast("int")))

    # Container 41 — drop buyer-surname == seller-surname
    sn = F.element_at(F.split(F.col("EierNavn"), r"\s+"), 1)
    sellers = b.filter(F.col("KjoperSelger") == "S").select("OmsetningId", sn.alias("_s"))
    buyers  = b.filter(F.col("KjoperSelger") == "K").select("OmsetningId", sn.alias("_b"))
    bad = (sellers.join(buyers, "OmsetningId")
                  .withColumn("_same", (F.upper("_s") == F.upper("_b")).cast("int"))
                  .groupBy("OmsetningId").agg(F.max("_same").alias("x")).filter("x = 1").select("OmsetningId"))
    b = b.join(bad, "OmsetningId", "left_anti")

    # enrichment joins (broadcast the small sides)
    b = (b.join(F.broadcast(dlt.read("silver_eier").select("EierId", "Foretak")), "EierId", "left")
           .join(dlt.read("silver_bygg_enhet"), "VerdiobjektId", "left")
           .join(F.broadcast(dlt.read("silver_income")), "EierId", "left")
           .join(dlt.read("silver_hyttetype"), "VerdiobjektId", "left")
           .join(dlt.read("silver_firsttime").select("OmsetningId", "Forstegangskjoper"), "OmsetningId", "left")
           .withColumn("Sum_Bruksareal",
                       F.when(F.col("Sum_Bruksareal").between(BRA_MIN, BRA_MAX), F.col("Sum_Bruksareal")))
           .withColumn("Kvadratmeterpris_BRA", F.col("Omsetning") / F.col("Sum_Bruksareal")))
    return b
```

## Silver — grain explosion (kills the 8× copy-paste)

```python
@dlt.table(name="silver_omsetning_grain", partition_cols=["Regiontype"])
def silver_omsetning_grain():
    s = (dlt.read("silver_omsetning_spine")
         .join(dlt.read("silver_geo_assign"), "VerdiobjektId", "left")
         .withColumn("Omsetningsaar", F.col("Tinglysningsaar")))
    grains = [("1.Land", F.lit("Hele Norge")), ("2.Fylke", F.col("Fylkesnummer")),
              ("3.Kommune", F.col("KommuneNr")), ("4.Delomrade", F.col("DelomraadeNr")),
              ("Postnummer", F.col("PostNr"))]
    def project(rt, rx, mode):
        hytte = (F.when(F.col("Fritidsboligtype").contains("Borettslag"), F.lit("Alle hytter, Borettslag"))
                  .when(F.col("Fritidsboligtype").contains("Selveier"), F.lit("Alle hytter, Selveier"))
                  .otherwise(F.lit("Alle hytter"))) if mode == "rollup" else F.col("Fritidsboligtype")
        return s.withColumn("Regiontype", F.lit(rt)).withColumn("Region", rx).withColumn("Hyttetype", hytte)
    parts = [project(rt, rx, m) for (rt, rx) in grains for m in ("rollup", "detail")]
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), parts)
```

## Gold-metric (representative: PRISER ETC.)

```python
@dlt.table(name="gold_m_priser")
@dlt.expect_all({"count_positive": "`Antall omsetninger` > 0"})
def gold_m_priser():
    df = dlt.read("silver_omsetning_grain").dropDuplicates(["OmsetningId", "Tinglysningsaar", "Hyttetype"])
    agg = df.groupBy(*GK).agg(
        F.count("OmsetningId").alias("Antall omsetninger"),
        F.avg("Omsetning").alias("Gjennomsnittspris"),
        F.percentile_approx("Omsetning", 0.5, 1_000_000).alias("Medianpris"),      # exact Median -> approx
        F.stddev_pop("Omsetning").alias("Standardavvik pris"),                     # Summarize StdDev = population
        F.avg("Kvadratmeterpris_BRA").alias("Gjennomsnittlig kvadratmeterpris (BRA)"))
    for m, a in [("Antall omsetninger", "Antall omsetninger, endring (pst)"),
                 ("Gjennomsnittspris", "Gjennomsnittspris, endring (pst)"),
                 ("Medianpris", "Medianpris, endring (pst)")]:
        agg = yoy(agg, m, a)
    return agg.filter(F.col("Omsetningsaar").cast("int") >= F.lit(RUN_YEAR - WINDOW_Y))
```

`gold_m_makspris` = `first_per_key(df.filter(naeringskjop("Kjoper") == 0),
["Omsetningsaar","Regiontype","Region","Hyttetype"], [F.col("Omsetning").desc(), F.col("OmsetningId").asc()])`.
`gold_m_alder_kjonn` / `_opprinnelse` / `_bruttoinntekt` / `_oms_intervaller` use
`row_pct_pivot` / `count_pivot` with **explicit value lists**.

## Gold-serve

`gold_omsetningstabell` = `gold_m_priser` left-joined (broadcast) to the other 12 metric
tables on `GK`, region names resolved via the geo dims, `Eieform` defaulted, metadata columns
added, projection rows unioned from `forecast_job`, final `.select(*FINAL_COLS)` in the exact
135-column order.

## Semantic-delta handling

| Alteryx | PySpark fix |
|---|---|
| `str + null` keeps the string | `label()` = `concat_ws("", *coalesce(p, ""))` |
| `null == null` → True (Multi-Row grain guard) | window `partitionBy(grain)` makes the guard implicit |
| exact `Median` | `F.percentile_approx(c, 0.5, 1_000_000)` (document ±1 tolerance) |
| Summarize `StdDev` = population | `F.stddev_pop` |
| `Round(x, 10000)` | `round_to()` = `F.round(c/10000)*10000` |
| `Contains/EndsWith` case-insensitive | `naeringskjop()` lower-cases + `rlike` |
| `Unique` / top-1 depend on input order | `first_per_key()` with explicit total order + tiebreak |
| `DateTimeToday()` (~30 sites) | `RUN_DATE` / `RUN_YEAR` params; CI lint bans `F.current_date()` |
| Cross Tab `XRow` = row-% | `row_pct_pivot()` |
| spatial `Intersects`, `EPSG:32633`, drive-time | Sedona `ST_Intersects` / `ST_Transform` callables; drive-time flagged no-equivalent |

## Optimization

- Bronze is the only layer touching SQL Server; `silver_omsetning_grain` materialised once,
  read by all 13 `gold_m_*`.
- Broadcast every geo dim + every small `gold_m_*` in the wide assembly.
- Partition `silver_omsetning_spine` by `_oms_year`; grain + gold tables by `Regiontype`
  (5 values). Cluster/Z-order by `Region, Omsetningsaar`.
- Every pivot passes its explicit band list. YoY windows partition by the full grain.
- `percentile_approx` accuracy 1e6 (one pass). Photon on. AQE on. `optimizeWrite` + a
  scheduled `OPTIMIZE` / `VACUUM` job.

## Risks

R1 — 72 order-dependent tools (Multi-Row ×32, Unique ×40): per-site tiebreak sign-off.
R2 — drive-time `Distance` (ToolID 1364) has no Spark equivalent: external routing table or drop.
R3 — Sedona vs Alteryx boundary handling: nearest-polygon fallback; reconcile ≥99.9%.
R4 — `ETS` / `TS_Forecast` are R models: Python re-implementation with a back-test acceptance band.

## Build order

Bundle + UC schemas + Sedona → Bronze (row-count check) → Silver core + geo → grain →
Gold metrics block by block (each reconciles to its Alteryx container) → Gold-serve
(135-col parity) → forecast job → export + schedule → 2–4 cycle parallel run → cutover.
