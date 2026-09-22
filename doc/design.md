# Design for the Trino Virtual Schema adapter

## Data Types

Trino types are translated to Exasol types when the virtual schema's column metadata is read. The adapter overrides only a few cases in `TrinoColumnMetadataReader.mapJdbcType`; everything else falls through to `BaseColumnMetadataReader` from `virtual-schema-common-jdbc`.

| Trino type                                                                       | Exasol type            | Notes                                                                             |
|----------------------------------------------------------------------------------|------------------------|-----------------------------------------------------------------------------------|
| `BOOLEAN`                                                                        | `BOOLEAN`              |                                                                                   |
| `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`                                       | `DECIMAL(p, 0)`        |                                                                                   |
| `REAL`, `DOUBLE`                                                                 | `DOUBLE`               | `REAL` widens from 4 to 8 bytes                                                   |
| `DECIMAL(p, s)`                                                                  | `DECIMAL(p, s)`        | falls back to max `VARCHAR` if `p > 36` (Trino max is 38)                         |
| `VARCHAR(n)`                                                                     | `VARCHAR(n)`           | unbounded `VARCHAR` becomes max `VARCHAR`                                         |
| `CHAR(n)`                                                                        | `CHAR(n)`              | larger sizes fall back to `VARCHAR`                                               |
| `DATE`                                                                           | `DATE`                 |                                                                                   |
| `TIMESTAMP(p)`                                                                   | `TIMESTAMP(min(p, 9))` | Trino supports up to picoseconds (p ≤ 12); capped to nanoseconds                  |
| `TIMESTAMP(p) WITH TIME ZONE`                                                    | `VARCHAR(100)`         | literal including the zone; disables timestamp arithmetic and predicate push-down |
| `TIME(p)`                                                                        | `VARCHAR(100)`         |                                                                                   |
| `TIME(p) WITH TIME ZONE`                                                         | `VARCHAR(100)`         |                                                                                   |
| `NUMBER`                                                                         | max `VARCHAR`          | non-standard Trino type                                                           |
| `JSON`, `JSON2016`, `IPADDRESS`, `UUID`                                          | max `VARCHAR`          |                                                                                   |
| `ARRAY`, `MAP`, `ROW`                                                            | max `VARCHAR`          | cast to `VARCHAR` at push-down time                                               |
| `INTERVAL DAY TO SECOND`, `INTERVAL YEAR TO MONTH`                               | max `VARCHAR`          |                                                                                   |
| `GEOMETRY`, `SPHERICAL_GEOGRAPHY`, `BING_TILE`, `KDB_TREE`                       | max `VARCHAR`          |                                                                                   |
| `HYPER_LOG_LOG`, `P4_HYPER_LOG_LOG`, `QDIGEST`, `TDIGEST`, `SET_DIGEST`, `COLOR` | max `VARCHAR`          |                                                                                   |

### Unsupported types

| Trino type  | Reason                                                                                                                                                                  |
|-------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `VARBINARY` | Exasol has no native binary type, and a hex-encoded `VARCHAR` representation cannot round-trip back to bytes. Columns of this type are dropped from the virtual schema. |

---

## Scalar Functions

### `SECONDS_BETWEEN`, `MINUTES_BETWEEN`, `HOURS_BETWEEN`, `DAYS_BETWEEN`, `MONTHS_BETWEEN`, `YEARS_BETWEEN`

Exasol supports functions that return the difference between two dates in a specific unit as a fractional value. For example, `MINUTES_BETWEEN` of two timestamps two hours apart returns 120. For more details check the [Exasol scalar functions manual](https://docs.exasol.com/sql_references/functions/all_functions.htm).

Trino has `date_diff(unit, timestamp1, timestamp2)`, but it returns whole-unit counts only. Exasol's `*_BETWEEN` functions return fractional differences, so the semantics do not match 1:1.

**Design**: Do not support these capabilities.

#### Other considered solutions

* Use `date_diff`:
  Returns only completed units and would lose fractional precision compared to Exasol.

* Use custom arithmetic on timestamps:
  Would require adapter-specific rewrites and careful timezone/precision handling for every affected function.

---

### `ROUND`

Exasol rounds `0.5` up (round half away from zero). Trino uses banker's rounding (round half to nearest even), so `ROUND(0.5)` returns `0` in Trino but `1` in Exasol.

**Design**: Do not support this capability to avoid silent result differences.

---

### `CONCAT`

Exasol's `CONCAT` accepts boolean arguments and converts them to strings. Trino's `CONCAT` only accepts `VARCHAR` and `VARBINARY`, so passing a boolean column causes a type error.

**Design**: Do not support this capability. Users can use the `||` operator instead, which Trino handles correctly for string concatenation.

---

### `INSTR`

Exasol's `INSTR(string, substring [, position [, occurrence]])` supports optional starting position and occurrence count parameters. Trino has `STRPOS(string, substring)` which only finds the first occurrence and has no position or occurrence parameters.

**Design**: Do not support this capability. A partial mapping covering only the two-argument form would silently produce wrong results when the optional parameters are used.

---

### `COLOGNE_PHONETIC`

Exasol's `COLOGNE_PHONETIC` function returns the Kölner Phonetik code of a string. Trino has no equivalent phonetic function.

**Design**: Do not support this capability.

---

### Geospatial functions (`ST_*`)

Trino has native geospatial support via the `io.trino.plugin.geospatial` plugin, but the Exasol virtual schema API's geospatial functions do not map cleanly to Trino's `ST_*` function signatures.

**Design**: Do not support these capabilities. Geospatial queries should be expressed directly via Trino SQL if needed.

---

### `TO_DATE`, `TO_TIMESTAMP`

Filtering by day is a common pattern, and `WHERE TO_DATE(TRACK_DATETIME) = TO_DATE(NOW())` is the natural way to express it in Exasol. Exasol pushes a `WHERE` clause down only if *every* expression in it is covered by a reported capability, so leaving these functions disabled meant the whole filter stayed on the Exasol side and the full virtual table was imported.

**Design**: Support both capabilities and translate them in `TrinoSqlGenerationVisitor`.

Capabilities are reported per function, not per signature, so enabling `TO_DATE` also means the two-argument form gets pushed down. There is no way to decline an individual expression: the capability report is a separate request that carries no query, and once Exasol hands over a push-down request the adapter must render all of it or fail the query — the generated SQL replaces the delegated part wholesale and Exasol re-applies nothing. Every form Exasol can send therefore has to be rendered correctly.

| Form | Rendering |
|---|---|
| `TO_DATE(expr)` | `CAST(expr AS DATE)` |
| `TO_TIMESTAMP(expr)` | `CAST(expr AS TIMESTAMP(9))` |
| `TO_DATE(str, format)` | `CAST(DATE_PARSE(str, translated) AS DATE)` |
| `TO_TIMESTAMP(str, format)` | `CAST(DATE_PARSE(str, translated) AS TIMESTAMP(9))` |

where `translated` is the Exasol format model rewritten for Trino, as described under *Format models* below. `TO_TIMESTAMP` casts to `TIMESTAMP(9)` — Exasol's maximum precision — because a bare `TIMESTAMP` means `timestamp(3)` in Trino and would silently round microsecond columns, while Exasol's `TO_TIMESTAMP` of a timestamp is an identity that keeps the full precision.

#### Time zones

Trino casts zone-carrying timestamps (`TIMESTAMP(p) WITH TIME ZONE`) using the value's *own* time zone: `CAST(TIMESTAMP '2023-05-01 05:30:00 Asia/Tokyo' AS DATE)` yields `2023-05-01` regardless of the Trino session time zone (verified on Trino 483 under `UTC` and `America/Los_Angeles` sessions; these have been the semantics since the timestamp overhaul around Trino 351). The plain cast is therefore correct for zone-carrying columns too, and for zone-free `TIMESTAMP(p)` columns both sides take the date part of a wall clock, so the cast is exact.

Push-down does change behavior for zone-carrying columns in one respect: they are mapped to `VARCHAR(100)` in the virtual schema and their rendered form includes the zone (for example `2023-05-01 05:30:00.000 UTC`), so *local* evaluation of `TO_DATE` on such a column fails with a data exception under the default `NLS_DATE_FORMAT`. The pushed-down cast succeeds instead and yields the value in its original zone. Turning that error into a result is deliberate — reproducing the local error would make the push-down pointless for exactly the columns it targets.

A second, unrelated shift affects `NOW()` and `CURRENT_TIMESTAMP`: Exasol evaluates them in the Exasol session time zone, Trino in the Trino session time zone, so pushing the expression down moves where the clock is read. This is not specific to `TO_DATE` — `CURRENT_TIMESTAMP` is already a reported capability, so any pushed-down predicate using it behaves this way, including the `>= CURRENT_DATE AND < ADD_DAYS(CURRENT_DATE, 1)` range form.

#### Format models

Exasol format models (`'YYYY-MM-DD'`) differ from Trino's `DATE_PARSE` specifiers (`'%Y-%m-%d'`). `TrinoDateFormat` translates the elements that have a counterpart: `YYYY`, `MONTH`, `MON`, `MM`, `DDD`, `DD`, `DAY`, `DY`, `HH24`, `HH12`, `HH`, `MI`, `SS`, `FF1` to `FF9`, `AM`, `PM`, and the separators `-/.,:;` and space. Note that Exasol's `HH` means `HH24` (unlike Oracle, where it means `HH12`), and that every `FF` precision maps to `%f`, which parses one to nine fractional digits and truncates the result to millisecond precision.

Anything else — two-digit years (`YY`, which Exasol completes with the current century while Trino's `%y` uses a fixed 1970–2069 window), ISO year and week numbers, quarters, Julian days, timezone offsets, quoted literal text — raises `E-VSTR-7`, and a format argument that is not a string literal raises `E-VSTR-8`. Rejecting is deliberate: an approximate translation would silently produce wrong dates, which is worse than a query that fails with an actionable message.

#### Parsing leniency

Exasol parses more leniently than Trino: it accepts values with extra trailing datetime components (`TO_DATE('2023-05-01 14:30:00', 'YYYY-MM-DD')` works, as does the single-argument `TO_DATE('2023-05-01 14:30:00')`) and values missing trailing format elements (`TO_TIMESTAMP('2023-05-01 14:30', 'YYYY-MM-DD HH24:MI:SS')` works). Trino's `DATE_PARSE` and `CAST` reject both (`malformed at " 14:30:00"`, `is too short`). Such values therefore fail with a query error when the expression is pushed down, where local evaluation would have returned a result — loudly, never silently different. Digit padding is lenient on both sides (`'2023-5-1'` parses against `YYYY-MM-DD` and `%Y-%m-%d` alike), and arbitrary trailing text is an error on both sides.

#### Session formats

The single-argument forms on a string parse with the session's `NLS_DATE_FORMAT` / `NLS_TIMESTAMP_FORMAT` when Exasol evaluates them locally. These session settings are invisible to the adapter — a push-down request carries no session parameters — so the pushed-down `CAST` always applies Trino's ISO parsing, which matches the Exasol *defaults* (`YYYY-MM-DD` and `YYYY-MM-DD HH24:MI:SS.FF6`) but not a session that changed them. Such a session gets different results depending on whether the predicate is pushed down; the explicit two-argument form with a format model behaves consistently. The same applies to `NLS_DATE_LANGUAGE`: Exasol parses month and day names (`MON`, `MONTH`, `DY`, `DAY`) in the session's date language, while Trino's `%b`, `%M`, `%a`, `%W` are English-only, so non-English sessions fail on the pushed-down parse.

#### Other considered solutions

* Refuse to push down the forms that deviate from local evaluation and let Exasol evaluate them locally:
  Not expressible. The adapter cannot decline part of a push-down request, and omitting a predicate from the generated Trino query would return too many rows.

* Enable `TO_DATE` and translate only the single-argument form:
  The two-argument form would still be pushed down, because capabilities are per function.

---

### Currently not implemented

The following functions are disabled because no implementation has been attempted yet. Each may be implementable in a future version:

| Function | Reason for deferral |
|---|---|
| `DUMP` | No direct Trino equivalent |
| `EDIT_DISTANCE` | Trino has no built-in Levenshtein function |
| `INSERT` | No direct Trino equivalent |
| `LOCATE` | Trino has `STRPOS` but argument order and behaviour differ |
| `REGEXP_INSTR` | Trino has no direct equivalent |
| `REGEXP_SUBSTR` | Trino has `REGEXP_EXTRACT` but semantics differ |
| `SOUNDEX` | No Trino equivalent |
| `SPACE` | No direct Trino equivalent; expressible as `REPEAT(' ', n)` |
| `UNICODE` / `UNICODECHR` | No direct Trino equivalents |
| `DBTIMEZONE` / `SESSIONTIMEZONE` | Exasol session concepts, not meaningful in Trino |
| `FROM_POSIX_TIME` | Trino has `FROM_UNIXTIME` but timezone handling needs verification |
| `IS_NUMBER`, `IS_BOOLEAN`, `IS_DATE`, `IS_TIMESTAMP`, `IS_DSINTERVAL`, `IS_YMINTERVAL` | Exasol type-check predicates; no Trino equivalent |
| `TO_CHAR`, `TO_NUMBER` | Exasol conversion functions; Trino uses `CAST` and `FORMAT`. `TO_CHAR` additionally needs Exasol's numeric format models, which have no Trino counterpart |
| `BIT_*` | Trino uses `bitwise_*` functions with different names |
| `HASH_SHA1`, `HASH_SHA256`, `HASH_SHA512`, `HASH_TIGER`, `HASHTYPE_*` | Trino has `SHA1`, `SHA256`, `SHA512` but return types differ |
| `NULLIFZERO` / `ZEROIFNULL` | Expressible with `NULLIF`/`COALESCE`; not native in Trino |
| `MIN_SCALE` | No Trino equivalent |
| `NUMTOYMINTERVAL` / `NUMTODSINTERVAL` / `TO_YMINTERVAL` / `TO_DSINTERVAL` | Exasol interval conversion functions; no Trino equivalent |
| `JSON_VALUE` | Trino has `JSON_VALUE` but with different syntax |
| `CONVERT_TZ` | Expressible via `AT TIME ZONE`; not mapped yet |
| `CAST` | Handled at the Exasol level; push-down not required |
| `SYS_GUID` | Exasol-specific; Trino has `UUID()` |
| `SYSTIMESTAMP` / `SYSDATE` | Exasol-specific; use `CURRENT_TIMESTAMP` / `CURRENT_DATE` instead |
| `CURRENT_STATEMENT` / `CURRENT_SESSION` / `CURRENT_USER` | Exasol session metadata; not meaningful in Trino |
