# Alteryx Expression to Spark Translation

Translate Alteryx formula-engine expressions (Filter, Formula, Multi-Row Formula, Generate Rows, Condition, Error Message, Action) to PySpark (`pyspark.sql.functions as F`) or Spark SQL. Use `formula-syntax.md` for the full source function list and Alteryx null semantics.

Record every translation with its confidence: `direct`, `adapted` (semantic caveat noted), or `needs-design`.

## Core semantic differences (always flag these)

| Topic | Alteryx | Spark | Handling |
|---|---|---|---|
| String + Null | `"ABC" + Null()` → `"ABC"` | `concat` with null → `null` | Use `F.concat_ws("", ...)` or wrap operands in `F.coalesce(col, F.lit(""))` |
| Number + Null | `1 + Null()` → `Null` | same (`null`) | direct |
| Null comparison | `Null() == Null()` → `True`; other comparisons with Null → `False` | `null = null` → `null` (not true); filters drop nulls | Use `eqNullSafe` / `<=>` where Alteryx relied on `Null()==Null()`; add explicit `isNull` branches |
| String index | 1-based (`Left`, `Substring` start at 1) | 0-based; `F.substring` is 1-based but `substr` semantics differ | Shift indices; verify each call |
| Integer division | `/` always returns double | `/` returns double; `div` for int | direct, but check downstream casts |
| Implicit cast | strings auto-coerce to numbers in math | needs explicit `cast` | add `F.col(x).cast("double")` |
| Boolean type | numeric `1`/`0` | true boolean | cast conditions to boolean |
| Field ref | `[Field Name]` | `F.col("Field Name")` | strip brackets |
| Row ref | `[Row-1:Field]`, `[Row+1:Field]` | `F.lag/F.lead over Window` | see Multi-Row Formula below |
| Current field | `[_CurrentField_]` | loop over field list in Python | Multi-Field Formula |

## Conditional

| Alteryx | Spark |
|---|---|
| `IF c THEN t ELSEIF c2 THEN t2 ELSE f ENDIF` | `F.when(c, t).when(c2, t2).otherwise(f)` |
| `IIF(b, x, y)` | `F.when(b, x).otherwise(y)` |
| `Switch(v, def, c1, r1, c2, r2)` | `F.when(v == c1, r1).when(v == c2, r2).otherwise(def)` |
| `value IN ("a","b")` | `F.col("value").isin("a", "b")` |
| `value NOT IN (...)` | `~F.col("value").isin(...)` |

## String

| Alteryx | Spark |
|---|---|
| `Uppercase(s)` / `LowerCase(s)` | `F.upper` / `F.lower` |
| `TitleCase(s)` | `F.initcap` (verify per-word behavior) |
| `Trim(s)` / `TrimLeft` / `TrimRight` | `F.trim` / `F.ltrim` / `F.rtrim`; with a char set → `F.regexp_replace` |
| `Left(s, n)` | `F.substring(s, 1, n)` |
| `Right(s, n)` | `F.substring(s, -n, n)` or `F.expr("right(s, n)")` |
| `Substring(s, start, len)` | `F.substring(s, start + 1, len)` (1-based shift) |
| `Length(s)` | `F.length` |
| `Contains(s, t)` | `F.col("s").contains(t)` (case-insensitive by default in Alteryx → use `F.instr(F.lower(s), lower(t)) > 0`) |
| `StartsWith` / `EndsWith` | `F.col("s").startswith` / `.endswith` (mind case-insensitive default) |
| `Replace(s, a, b)` | `F.regexp_replace(s, F.lit(quoted_a), b)` (Alteryx `Replace` is literal, not regex) |
| `ReplaceChar(s, y, z)` | `F.translate(s, y, z)` |
| `REGEX_Replace(s, pat, rep, icase)` | `F.regexp_replace(s, pat, rep)` — translate `\1` backrefs to `$1`; add `(?i)` for icase; Java regex dialect |
| `REGEX_Match(s, pat)` | `F.col("s").rlike(pat)` (Alteryx matches whole string → anchor with `^...$`) |
| `REGEX_CountMatches(s, pat)` | `F.size(F.split(s, pat)) - 1` or `F.expr("regexp_count(s, pat)")` (DBR 13.3+) |
| `FindString(s, t)` | `F.instr(s, t) - 1` (Alteryx returns 0-based, -1 if absent → adjust) |
| `GetLeft(s, d)` / `GetRight(s, d)` | `F.substring_index(s, d, 1)` / `F.substring_index(s, d, -1)` |
| `GetWord(s, n)` | `F.split(F.trim(s), "\\s+")[n]` |
| `PadLeft(s, n, c)` / `PadRight` | `F.lpad` / `F.rpad` |
| `Trim`/case + punctuation (Data Cleanse) | compose `F.regexp_replace` + `F.trim` + `F.initcap` |
| `MD5_UTF8(s)` / `MD5_ASCII` | `F.md5(F.col("s").cast("binary"))` |
| `PadLeft`, `ReverseString`, `StripQuotes` | `F.lpad`, `F.reverse`, `F.regexp_replace(s, '^"|"$', '')` |
| `Contains`/`FindString` case flag | Alteryx default is case-insensitive; make it explicit in Spark |

## DateTime

Alteryx datetimes are ISO strings `yyyy-MM-dd HH:mm:ss`. Alteryx format tokens are **not** Java tokens.

| Alteryx | Spark |
|---|---|
| `DateTimeParse(s, fmt)` | `F.to_timestamp(s, java_fmt)` — convert `fmt`: `%Y`→`yyyy`, `%m`→`MM`, `%d`→`dd`, `%H`→`HH`, `%M`→`mm`, `%S`→`ss`, `%p`→`a`, `%b`→`MMM`, `%A`→`EEEE` |
| `DateTimeFormat(dt, fmt)` | `F.date_format(dt, java_fmt)` (same token conversion) |
| `DateTimeAdd(dt, i, "days")` | `F.col("dt") + F.expr(f"INTERVAL {i} DAYS")` or `F.date_add` / `F.add_months` |
| `DateTimeDiff(a, b, "days")` | `F.datediff(a, b)`; for months/years use `F.months_between` / arithmetic; seconds → `unix_timestamp` diff |
| `DateTimeNow()` / `DateTimeToday()` | `F.current_timestamp()` / `F.current_date()` — **determinism hazard**, pin to a pipeline run timestamp parameter |
| `DateTimeYear/Month/Day/Hour(dt)` | `F.year` / `F.month` / `F.dayofmonth` / `F.hour` |
| `DateTimeTrim(dt, "day")` | `F.date_trunc("day", dt)` |
| `DateTimeFirstOfMonth()` / `LastOfMonth()` | `F.trunc(dt, "month")` / `F.last_day(dt)` |
| `ToDate(x)` / `ToDateTime(x)` | `F.to_date` / `F.to_timestamp` |
| Timezone (`DateTimeToUTC`, `tz` args) | `F.to_utc_timestamp` / `F.from_utc_timestamp`; set `spark.sql.session.timeZone` |

## Math / Conversion / Test

| Alteryx | Spark |
|---|---|
| `Abs`, `Ceil`, `Floor`, `Round`, `Sqrt`, `Exp`, `Log`, `Log10`, `Pow`, `Mod` | `F.abs`, `F.ceil`, `F.floor`, `F.round`, `F.sqrt`, `F.exp`, `F.log`, `F.log10`, `F.pow`, `F.col%F.col` or `F.expr("mod(a,b)")` |
| `Round(x, mult)` | `F.round(x / mult) * mult` (Alteryx rounds to a multiple) |
| `CEIL(x, mult)` / `FLOOR(x, mult)` | `F.ceil(x/mult)*mult` / `F.floor(x/mult)*mult` |
| `Average(a,b,c)` (row-wise) | `(F.coalesce(a,0)+...) / n` or `F.expr` over an array |
| `Min(a,b,c)` / `Max(...)` (row-wise) | `F.least(a,b,c)` / `F.greatest(a,b,c)` |
| `ToNumber(s)` | `F.col("s").cast("double")` (add `regexp_replace` for separators) |
| `ToString(x, dec)` | `F.format_number(x, dec)` or `F.col("x").cast("string")` |
| `ToString(x, dec, thousands)` | `F.format_number(x, dec)` (adds thousands sep) |
| `IsNull(v)` | `F.col("v").isNull()` |
| `IsEmpty(v)` | `F.col("v").isNull() | (F.col("v") == "")` |
| `IsNumber(v)` / `IsString(v)` | schema-level; usually resolved by target types |
| `IsInteger(v)` | `F.col("v").cast("int").isNotNull()` on a string source |
| `Coalesce(a,b,c)` | `F.coalesce(a, b, c)` |
| `Null()` | `F.lit(None)` |
| `MD5_*`, `Soundex` | `F.md5`, `F.soundex` |
| `CompareEpsilon(a,b,e)` | `F.abs(a - b) <= e` |
| `BinaryAnd/Or/Xor`, `ShiftLeft/Right` | `F.expr("a & b")`, `|`, `^`, `shiftleft`, `shiftright` |
| `UuidCreate()` | `F.expr("uuid()")` — determinism hazard |
| `RAND()` / `RandInt(n)` | `F.rand()` / `F.floor(F.rand()*(n+1))` — determinism hazard, set a seed |

## Windowed tools

| Alteryx tool | Spark |
|---|---|
| Multi-Row Formula `[Row-k:F]` | `F.lag("F", k).over(Window.partitionBy(group).orderBy(order))`; `Row+k` → `F.lead` |
| Running Total | `F.sum("F").over(Window.partitionBy(group).orderBy(order).rowsBetween(Window.unboundedPreceding, 0))` |
| Tile (equal records) | `F.ntile(n).over(Window.partitionBy(group).orderBy(order))` |
| Rank | `F.row_number` / `F.rank` / `F.dense_rank` over a Window |
| Sample "First N per group" | `F.row_number().over(...) <= N` then filter |
| Unique (first match) | `F.row_number().over(Window.partitionBy(keys).orderBy(tiebreak)) == 1` — **make the tie-break explicit**, Alteryx relied on input order |

## Aggregation / reshape tools

| Alteryx tool | Spark |
|---|---|
| Summarize | `df.groupBy(*group).agg(F.sum(...).alias(...), F.countDistinct(...), F.concat_ws(sep, F.collect_list(...)), ...)` |
| Cross Tab | `df.groupBy(*group).pivot("header").agg(F.first("data") / F.sum(...))` |
| Transpose | `df.selectExpr(*keys, "stack(k, 'c1', c1, 'c2', c2) as (Name, Value)")` |
| Generate Rows | `F.explode(F.sequence(start, stop, step))` or `spark.range` joined in |
| Text To Columns (to rows) | `F.explode(F.split(col, delim))` |
| Text To Columns (to cols) | `F.split(col, delim)` then index, or `F.regexp_extract_all` |

## No native equivalent — flag as `no-equivalent` / `needs-design`

- **Fuzzy Match / Make Group** → `dbldatagen`/custom Levenshtein UDF, `F.levenshtein`, or Zingg/Splink; design decision.
- **Spatial tools** → Apache Sedona / Databricks spatial functions; separate track.
- **Reporting: Table, Layout, Render, Email, Charting** → out of pipeline; move to Databricks SQL / dashboards / a downstream job.
- **Predictive / ML / Text Mining tools** → MLflow + Spark ML / Databricks ML; separate migration track.
- **Download / API / Run Command tools** → ingestion job or `foreachBatch` task outside the declarative pipeline.
- **Iterative macros** → bounded Python loop or restructured set-based logic.
