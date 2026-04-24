# Trino SQL Dialect User Guide

[Trino](https://trino.io/) is an open-source, distributed SQL query engine for running interactive analytic queries
against data sources of all sizes.

## Uploading the JDBC Driver to Exasol BucketFS

1. Download the [Trino JDBC driver](https://trino.io/docs/current/client/jdbc.html).

   Use a driver version compatible with your Trino coordinator. The Maven coordinates are `io.trino:trino-jdbc`.
2. Upload the driver to BucketFS,
   see [BucketFS documentation](https://docs.exasol.com/db/latest/administration/on-premise/bucketfs/accessfiles.htm).

   Hint: Put the driver into folder `default/drivers/jdbc/` to register it
   for [ExaLoader](#registering-the-jdbc-driver-for-exaloader), too.

## Registering the JDBC driver for ExaLoader

In order to enable the ExaLoader to fetch data from the external database you must register the driver for ExaLoader as
described in
the [Installation procedure for JDBC drivers](https://github.com/exasol/docker-db/#installing-custom-jdbc-drivers).

1. ExaLoader expects the driver in BucketFS folder `default/drivers/jdbc`.<br />
   If you uploaded the driver for UDF to a different folder, then you need
   to [upload](#uploading-the-jdbc-driver-to-exasol-bucketfs) the driver again.
2. Additionally you need to create file `settings.cfg` and [upload](#uploading-the-jdbc-driver-to-exasol-bucketfs) it to
   the same folder in BucketFS:

```
DRIVERNAME=TRINO_JDBC_DRIVER
JAR=<trino-jdbc-jar>
DRIVERMAIN=io.trino.jdbc.TrinoDriver
PREFIX=jdbc:trino:
FETCHSIZE=100000
INSERTSIZE=-1
```

| Variable           | Description               |
|--------------------|---------------------------|
| `<trino-jdbc-jar>` | E.g. `trino-jdbc-480.jar` |

## Installing the Adapter Script

[Upload](https://docs.exasol.com/db/latest/administration/on-premise/bucketfs/accessfiles.htm) the latest available
release of [Trino Virtual Schema JDBC Adapter](https://github.com/exasol/trino-virtual-schema/releases) to Bucket FS.

Then create a schema to hold the adapter script.

```sql
CREATE SCHEMA ADAPTER;
```

The SQL statement below creates the adapter script, defines the Java class that serves as entry point and tells the UDF
framework where to find the libraries (JAR files) for Virtual Schema and database driver.

```sql
--/
CREATE
OR REPLACE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER AS
  %scriptclass com.exasol.adapter.RequestDispatcher;
%jar /buckets/<BFS service>/<bucket>/virtual-schema-dist-13.0.0-trino-0.1.0.jar;
%jar /buckets/<BFS service>/<bucket>/trino-jdbc-<trino-driver-version>.jar;
/
```

## Defining a Named Connection

Define the connection to the Trino coordinator as shown below. We recommend using TLS to secure the connection.

```sql
CREATE
OR REPLACE CONNECTION TRINO_CONNECTION
TO 'jdbc:trino://<host>:<port>'
USER '<user>'
IDENTIFIED BY '<password>';
```

Do not include catalog or schema in the URL. The adapter already specifies them via `CATALOG_NAME` and `SCHEMA_NAME`
properties and always generates fully-qualified `catalog.schema.table` names in pushdown queries. Including them in the
URL would be redundant.

See the [Trino JDBC driver documentation](https://trino.io/docs/current/client/jdbc.html) for the full list of supported
connection parameters, including authentication options such as Kerberos, JWT, or OAuth2.

| Variable | Description                                                                                                                                                              |
|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `<host>` | Hostname or IP address of the Trino coordinator.                                                                                                                         |
| `<port>` | Port of the Trino coordinator, default is `8080` (HTTP) or `8443` (HTTPS), see also [Developer's guide](../developers_guide/developers_guide.md#finding-the-trino-port). |

See also [Allowing external connections](../developers_guide/developers_guide.md#allowing-external-connections) in the
Developer's guide.

## Creating a Virtual Schema

Use the following SQL command in Exasol database to create a Trino Virtual Schema:

```sql
CREATE
VIRTUAL SCHEMA <virtual schema name>
  USING ADAPTER.JDBC_ADAPTER
  WITH
  CATALOG_NAME = '<catalog name>'
  SCHEMA_NAME = '<schema name>'
  CONNECTION_NAME = 'TRINO_CONNECTION';
```

| Variable                | Description                                                                                                    |
|-------------------------|----------------------------------------------------------------------------------------------------------------|
| `<virtual schema name>` | Name of the virtual schema you want to use.                                                                    |
| `<catalog name>`        | Name of the Trino catalog, see [Trino catalogs](https://trino.io/docs/current/overview/concepts.html#catalog). |
| `<schema name>`         | Name of the schema inside the Trino catalog.                                                                   |

See also section [Remote logging](../developers_guide/developers_guide.md#remote-logging) in the developers guide.

For additional parameters coming from the base library see
also [Adapter Properties for JDBC-Based Virtual Schemas](https://github.com/exasol/virtual-schema-common-jdbc#adapter-properties-for-jdbc-based-virtual-schemas).

## Trino Identifiers

In contrast to Exasol, Trino does not treat identifiers as specified in the SQL standard. Trino folds unquoted
identifiers to lower case instead of upper case. The adapter has two modes for handling this:

### Automatic Identifier conversion

This is the default mode for handling identifiers, but identifier conversion can also be set explicitly using the
following property:

```sql
ALTER
VIRTUAL SCHEMA <virtual schema name> SET TRINO_IDENTIFIER_MAPPING = 'CONVERT_TO_UPPER';
```

In this mode you do not have to care about identifier handling. Everything will work as expected out of the box as long
as you **do not use quoted identifiers** (in the Trino schema as well as in the Exasol Virtual Schema). More
specifically everything will work as long as there are no identifiers in the Trino catalog that contain upper case
characters. If that is the case an error is thrown when creating or refreshing the virtual schema.
Regardless of this, you can create or refresh the virtual schema by specifying the adapter to ignore this particular
error as shown below:

```sql
CREATE
VIRTUAL SCHEMA <virtual schema name>
  USING ADAPTER.JDBC_ADAPTER
  WITH
  CATALOG_NAME = '<catalog name>'
  SCHEMA_NAME = '<schema name>'
  CONNECTION_NAME = 'TRINO_CONNECTION'
  IGNORE_ERRORS = 'TRINO_UPPERCASE_TABLES';
```

You can also set this property to an existing virtual schema:

```sql
ALTER
VIRTUAL SCHEMA trino SET IGNORE_ERRORS = 'TRINO_UPPERCASE_TABLES';
```

However, you **will not be able to query the identifier containing the upper case character**. An error is thrown when
querying the virtual table.

A best practice for this mode is: **never quote identifiers** (in the Trino schema as well as in the Exasol Virtual
Schema). This way everything works without having to change your queries.<br />
An alternative is to use the second mode for identifier handling (see below).

### Trino-like identifier handling

If you use quotes on the Trino side and have identifiers with uppercase characters, then it is recommended to use this
mode. The Trino-like identifier handling does no conversions but mirrors the Trino metadata as is. A small example to
make this clear:

```sql
--Trino Schema
CREATE TABLE "MyTable"
(
    "Col1" VARCHAR
);
CREATE TABLE MySecondTable
(
    Col1 VARCHAR
);
--Trino Queries
SELECT "Col1"
FROM "MyTable";
SELECT Col1
FROM MySecondTable;
```

```sql
--Create Virtual Schema on EXASOL side
CREATE
VIRTUAL SCHEMA <virtual schema name>
  USING ADAPTER.JDBC_ADAPTER
  WITH
  CATALOG_NAME = '<catalog name>'
  SCHEMA_NAME = '<schema name>'
  CONNECTION_NAME = 'TRINO_CONNECTION'
  TRINO_IDENTIFIER_MAPPING = 'PRESERVE_ORIGINAL_CASE';

-- Open Schema and see what tables are there
OPEN SCHEMA trino;
SELECT *
FROM CAT;
-- result -->
-- TABLE_NAME  TABLE_TYPE
-- ----------------------
-- MyTable       | TABLE
-- mysecondtable | TABLE
```

As you can see `MySecondTable` is displayed in lower case in the virtual schema catalog. This is exactly like it is on
the Trino side, but since unquoted identifiers are folded differently in Trino you cannot query the table like you did
in Trino:

```sql
--Querying the virtual schema
--> this works
SELECT "Col1"
FROM trino."MyTable";

--> this does not work
SELECT Col1
FROM trino.MySecondTable;
--> Error:
--  [Code: 0, SQL State: 42000]  object "TRINO"."MYSECONDTABLE" not found [line 1, column 18]

--> this works
SELECT "col1"
FROM trino."mysecondtable";
```

Unquoted identifiers are converted to lowercase on the Trino side, and since there is no catalog conversion these
identifiers are also lowercase in Exasol. To query a lowercase identifier you must use quotes in Exasol, because
everything that is unquoted gets folded to uppercase.

A best practice for this mode is: **always quote identifiers** (in the Trino schema as well as in the Exasol Virtual
Schema). This way everything works without having to change your queries.

## Data Types Conversion

Most scalar Trino types are mapped through the common JDBC type-handling in `virtual-schema-common-jdbc`. The adapter
only overrides the types that cannot be mapped directly to an Exasol data type: those are exposed as `VARCHAR`.

See the [Trino data types documentation](https://trino.io/docs/current/language/types.html) for the authoritative
definition of each source type.

| Trino Data Type           | Supported | Converted Exasol Data Type | Known limitations                                                                                                    |
|---------------------------|-----------|----------------------------|----------------------------------------------------------------------------------------------------------------------|
| BOOLEAN                   | ✓         | BOOLEAN                    |                                                                                                                      |
| TINYINT                   | ✓         | DECIMAL(3,0)               |                                                                                                                      |
| SMALLINT                  | ✓         | DECIMAL(5,0)               |                                                                                                                      |
| INTEGER                   | ✓         | DECIMAL(10,0)              |                                                                                                                      |
| BIGINT                    | ✓         | DECIMAL(19,0)              |                                                                                                                      |
| REAL                      | ✓         | DOUBLE                     |                                                                                                                      |
| DOUBLE                    | ✓         | DOUBLE                     |                                                                                                                      |
| DECIMAL                   | ✓         | DECIMAL                    | Trino `DECIMAL` values outside the Exasol `DECIMAL(36, 36)` range cannot be represented.                             |
| CHAR                      | ✓         | CHAR                       |                                                                                                                      |
| VARCHAR                   | ✓         | VARCHAR                    |                                                                                                                      |
| DATE                      | ✓         | DATE                       |                                                                                                                      |
| TIMESTAMP                 | ✓         | TIMESTAMP                  |                                                                                                                      |
| TIMESTAMP WITH TIME ZONE  | ✓         | TIMESTAMP                  | Time-zone information is lost when the value is converted to Exasol `TIMESTAMP`.                                     |
| TIME                      | ✓         | VARCHAR(2000000)           | Exposed as string because Exasol has no native `TIME` type.                                                          |
| TIME WITH TIME ZONE       | ✓         | VARCHAR(2000000)           | Exposed as string because Exasol has no native `TIME WITH TIME ZONE` type.                                           |
| VARBINARY                 | ✓         | VARCHAR(2000000)           | Binary content is serialised as a string.                                                                            |
| JSON                      | ✓         | VARCHAR(2000000)           |                                                                                                                      |
| UUID                      | ✓         | VARCHAR(2000000)           |                                                                                                                      |
| IPADDRESS                 | ✓         | VARCHAR(2000000)           |                                                                                                                      |
| INTERVAL YEAR TO MONTH    | ✓         | VARCHAR(2000000)           | Pushdown of interval literals is disabled, see [capabilities report](../generated/capabilities.md).                  |
| INTERVAL DAY TO SECOND    | ✓         | VARCHAR(2000000)           | Pushdown of interval literals is disabled, see [capabilities report](../generated/capabilities.md).                  |
| ARRAY                     | ✓         | VARCHAR(2000000)           |                                                                                                                      |
| MAP                       | ✓         | VARCHAR(2000000)           |                                                                                                                      |
| ROW                       | ✓         | VARCHAR(2000000)           |                                                                                                                      |
| Geospatial types (`ST_*`) | ✓         | VARCHAR(2000000)           | Pushdown of geospatial functions is intentionally disabled, see [capabilities report](../generated/capabilities.md). |

## Limitations

This adapter only advertises the subset of capabilities that it can safely translate from Exasol SQL to Trino SQL today.

- Several Exasol capabilities have a Trino equivalent, but the adapter does not currently rewrite or expose them.
  Examples include `ROUND`, `CONCAT`, `EDIT_DISTANCE`, `FROM_POSIX_TIME`, `CURRENT_USER`, `LISTAGG`,
  `APPROXIMATE_COUNT_DISTINCT`, and multiple bitwise/hash aliases.
- Some Exasol capabilities do not have a 1:1 Trino match with compatible semantics. This affects capabilities such as
  generic `IS JSON`, `COUNT_TUPLE`, fractional `*_BETWEEN` date-difference functions, and Exasol-specific session,
  phonetic, and hash helpers.
- Geospatial pushdown is intentionally disabled even though Trino has broad `ST_*` support, because the adapter exposes
  geospatial values as `VARCHAR` instead of Exasol geospatial types.
- Interval literals, interval constructors, and several type-checking/casting capabilities remain disabled because
  Exasol and Trino use different interval and type-system conventions.

See the generated [capabilities report](../generated/capabilities.md) for the per-capability details and the reason each
unsupported capability remains disabled.
