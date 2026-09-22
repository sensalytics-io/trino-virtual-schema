> [!NOTE]
> This project is an independent community fork and is **not** endorsed by, affiliated with, or supported by Exasol AG in any way.

# Trino Virtual Schema

[![Build Status](https://github.com/sensalytics-io/trino-virtual-schema/actions/workflows/ci-build.yml/badge.svg)](https://github.com/sensalytics-io/trino-virtual-schema/actions/workflows/ci-build.yml)

# Overview

The **Trino Virtual Schema** provides an abstraction layer that makes an external [Trino](https://trino.io/) cluster accessible from an Exasol database through regular SQL commands. The contents of the Trino catalog and schema are mapped to virtual tables which look like and can be queried as any regular Exasol table.

This adapter was created by porting the [PostgreSQL Virtual Schema](https://github.com/exasol/postgresql-virtual-schema) to the Trino SQL dialect. If you want to set up a Virtual Schema for a different database system, please head over to the [Virtual Schemas Repository][virtual-schemas].

## Features

* Access a Trino cluster using a Virtual Schema.
* Reach any data source exposed through a Trino connector (for example Hive, Iceberg, Delta Lake, Kafka, or JDBC-based catalogs) via a single Exasol Virtual Schema.

## Table of Contents

### Information for Users

* [Virtual Schema User Guide](https://docs.exasol.com/database_concepts/virtual_schemas.htm)
* [Trino Dialect User Guide](doc/user_guide/trino_user_guide.md)
* [List of supported capabilities](doc/generated/capabilities.md)
* [Changelog](doc/changes/changelog.md)
* [Dependencies](dependencies.md)

Find all the documentation in the [Virtual Schemas project][vs-doc].

## Information for Developers

* [Developers Guide](doc/developers_guide/developers_guide.md)
* [Design Notes](doc/design.md)
* [Virtual Schema API Documentation](https://github.com/exasol/virtual-schema-common-java/blob/main/doc/development/api/virtual_schema_api.md)
* [Remote logging](https://docs.exasol.com/db/latest/database_concepts/virtual_schema/logging.htm)

## Additional Resources

* [Dependencies](dependencies.md)
* [Changelog](doc/changes/changelog.md)

[virtual-schemas]: https://github.com/exasol/virtual-schemas
[vs-doc]: https://github.com/exasol/virtual-schemas/tree/main/doc
