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
| `TO_CHAR`, `TO_DATE`, `TO_NUMBER`, `TO_TIMESTAMP` | Exasol conversion functions; Trino uses `CAST` and `FORMAT` |
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
