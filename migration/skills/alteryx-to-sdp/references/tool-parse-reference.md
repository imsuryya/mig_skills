# Per-Tool Parse Reference

What to extract from each tool's `Configuration` during Phase 1. This is a discovery aid, not a schema. Always confirm the exact XML shape from the file you are parsing. Use `designer-tool-reference.yaml` for plugin names and anchor names, and `workflow-xml.md` for the shared node/connection/container model.

For every tool, always capture: `ToolID`, `GuiSettings Plugin`, `EngineSettings`, annotation `Name` and `AnnotationText`, container membership, enabled/disabled state, and cached `MetaInfo` (as a hint to expected output schema, not ground truth).

## Inputs and Outputs

- **Input Data** (`DbFileInput`): `File` path or connection string, `FileFormat`, delimiter / code page / header row / start line, sheet name or table, `Query` (SQL), ODBC/OLEDB/DCM reference, `RecordLimit`, `SearchSubDirs`, wildcard. Record the connection as a named reference; never reproduce credentials.
- **Output Data** (`DbFileOutput`): destination `File` or connection, `FileFormat`, `MaxRecords`, output options (overwrite / append / create / update — this drives CDC choice), delimiter/header, table name, pre/post SQL.
- **Text Input** (`TextInput`): `Fields` and every `Row` of literal data. Reproduce the data (it becomes a seed table or literal DataFrame).
- **Directory**: search path, wildcard, `SearchSubDirs`, output fields. Determinism hazard — the file list is environment-bound.
- **Date Time Now** (`DateTimeNow`): output field name. Determinism hazard.

## Preparation

- **Select** (`AlteryxSelect`): the ordered `SelectField` list — for each: `field`, `selected` (true/false), `rename`, `type`, `size`, `description`. Also `SelectFields` unknown-field default (`*Unknown` selected/deselected). Captures drops, renames, reorders, and silent type/size changes.
- **Filter** (`Filter`): `Mode` (`Simple` or `Custom`). Simple: `Field`, `Operator`, `Operands`. Custom: the `Expression`. Both output anchors (`True`, `False`) — check which are connected.
- **Formula** (`Formula`): every `FormulaField`: `field` (name), `expression`, `type`, `size`. Order matters — later fields can reference earlier ones in the same tool.
- **Multi-Row Formula** (`MultiRowFormula`): `UpdateField` vs new field (`field`, `type`, `size`), `NumRows`, `Direction`, `GroupByFields`, behavior for rows that don't exist (`0`, `NULL`, or `Empty`), and the `Expression` (uses `Row-1:Field` / `Row+1:Field` syntax → window `lag`/`lead`).
- **Multi-Field Formula** (`MultiFieldFormula`): selected `Field` list, `Expression` (uses `[_CurrentField_]`), whether it creates new fields or updates in place, `CopyOutput` naming.
- **Formula-like conditions:** always keep the raw expression text.
- **Sort** (`Sort`): ordered `SortInfo` `Field` + `Order` (`Ascending`/`Descending`) list.
- **Sample** (`Sample`): `Mode` (First N, Last N, Skip first N, 1 in N, Random 1 in N, First N%, etc.), `N`, `GroupFields`. Ordering-dependent.
- **Unique** (`Unique`): key `Field` list. First-match wins → tie-break is ordering-dependent. Anchors `Unique`, `Duplicates`.
- **Record ID** (`RecordID`): field name, start value, type, size, position (first/last).
- **Auto Field** (`AutoField`): selected fields — retypes strings to smallest fitting type. Translate as explicit casts after profiling.
- **Data Cleanse / Data Cleanse Pro**: selected fields, and every enabled option (leading/trailing/dup whitespace, remove nulls → 0 / blank, punctuation, letters, numbers, case conversion). Each option is an explicit Spark transform.
- **Generate Rows** (`GenerateRows`): new field name/type/size, `InitExpression`, `ConditionExpression`, `LoopExpression`. → `sequence()` + `explode()` or `spark.range`.
- **Tile** (`Tile`): `Method` (equal records / equal sum / smart / manual / unique), `NumTiles`, `TileField`, `SumField`, `GroupFields`. → `ntile()` window or bucketing.
- **Running Total** (`RunningTotal`): `GroupByFields`, ordered `RunningTotalField` list. → `sum().over(Window)`.
- **Rank** (not always present natively): ranking type, sort fields, group fields.

## Join

- **Join** (`Join`): `JoinInfo` for `Left` and `Right` — the ordered join `Field` list per side (positional if none). `SelectConfiguration` — the post-join `SelectField` list controlling output fields and renames on the `Join` anchor. Three output anchors: `Left` (left-only), `Join` (matched, inner), `Right` (right-only). Reconstruct whichever are connected.
- **Join Multiple** (`JoinMultiple`): input list, `JoinByRecordPosition` or by-field, `Field` per input, `OutputJoinOnly`, fill-nulls option.
- **Union** (`Union`): `Mode` (`ByName`, `ByName_Warn`, `ByPosition`), `SetOutputOrder`, `ByName_ErrorMode`, per-field mapping when manual. → `unionByName(allowMissingColumns=...)`.
- **Append Fields** (`AppendFields`): source vs targets, `SelectConfiguration`. Cartesian product with a safety limit — flag row-count risk.
- **Find Replace** (`FindReplace`): find field, replace field, append vs replace, whole-string vs any-part, case sensitivity, `MultipleMatches`. → join or `regexp_replace` depending on mode.
- **Fuzzy Match** (`FuzzyMatch`): match style, match fields with match function (Jaro, Levenshtein, etc.) and thresholds. **No native equivalent** — flag.

## Parse

- **RegEx** (`RegEx`): `Field`, `RegExExpression`, `CaseInsensitive`, `Method` (`Match`, `Parse`, `Replace`, `Tokenize`), replace-with string, `NumFields` and output field names for Parse, tokenize output mode.
- **DateTime** (`DateTime`): direction (string→date or date→string), `Format` string(s), input/output field names, language. Alteryx format specifiers differ from Spark — see `alteryx-formula-to-spark.md`.
- **Text To Columns** (`TextToColumns`): `Field`, `Delimeters`, split to columns vs rows, `NumFields`, extra-characters handling, leave/split quoted, `Advanced`.
- **XML Parse** / **JSON Parse**: field, parse depth, output mode, `ChildValues`.

## Transform

- **Summarize** (`Summarize`): every `SummarizeField`: `field`, `action` (GroupBy, Sum, Count, CountDistinct, CountNonNull, Min, Max, Avg, Median, Mode, StdDev, Var, First, Last, Concat with separator, spatial actions), `rename`. Group-by fields are `action="GroupBy"` entries.
- **Cross Tab** (`CrossTab`): `GroupFields`, `HeaderField` (becomes columns), `DataField`, `Methods` (Sum/Concat/First/etc.), missing-value fill, header prefix/cleaning. → `groupBy().pivot()`.
- **Transpose** (`Transpose`): key `Field` list, data `Field` list, `HandleNulls`. → unpivot / `stack()`.
- **Count Records** (`CountRecords` macro): output field name. → `count()`.
- **Weighted Average**, **Running Total**, **Make Columns / Make Group / Arrange**: capture grouping, value, and layout fields.

## Interface, Macros, Apps

- **Macro Input** (`MacroInput`): template `Fields`, anchor `Name`, `Abbrev`, whether it shows field map. Defines the macro's input contract.
- **Macro Output** (`MacroOutput`): anchor `Name`, `Abbrev`.
- **Control Parameter** (`ControlParam`): question name. Presence ⇒ batch macro; each param feeds `Action` tools.
- **Action** (`Action`): `ToolId` it drives, `Type`, `Expression`, `Destination` (an XML path into a target tool, e.g. `12/Configuration/Expression`), `Mapping`, `Mode`. These mutate inner tool XML per run — translate to a parameterized function argument.
- **Condition** (`Condition`): the test `Expression`, `True`/`False` connection anchors. → Python `if` around which flow is built.
- **Question interface tools** (Drop Down, List Box, Text Box, Numeric Up Down, Date, File Browse, Folder Browse, Check Box, Radio Button, Tree, Map): question `Name`, default value, choice list / data source. Each becomes a pipeline config parameter.
- **Error Message** (`Error`): condition `Expression`, message. → `@dlt.expect_or_fail`.
- **Detour / Detour End / Block Until Done / Message**: capture but usually collapses to ordering / expectations in Spark.

## Iterative and Batch Macros

- **Iterative macro**: root `RuntimeProperties` — `Iterative` input/output anchor names, `MaxIterations`, convergence (`Iteration Output` feeding back). Requires explicit redesign — bounded Python loop or restructured set logic. Flag as `needs-design`.
- **Batch macro**: Control Parameters + Action tools. Translate to a function called once per parameter value, or a broadcast-join / partitioned transform. Flag row-multiplication behavior.

## Always Also Record

- The AMP/E2 vs E1 selection (`RunE2`, `RunWithE2`).
- Root `Constants` (workflow-level user constants and engine constants used in expressions).
- `Events` (pre/post-run Run Command, email, conditional events).
- `GlobalRecordLimit`, `ConvErrorLimit`, `ConvErrorLimit_Stop`, `CancelOnError`, `DisableAllOutput`.
- Any tool reading environment state: `GetEnvironmentVariable`, `ReadRegistryString`, `%temp%` / UNC / absolute paths.
