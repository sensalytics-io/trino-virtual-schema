# Developers Guide

This guide contains a few Trino-specific notes for adapter development and local experiments.

## Password for writing to BucketFS of your Exasol database

If you run Exasol in Docker, the following script helps you read the BucketFS write password:

```shell
CONTAINER=<container id>
export BUCKETFS_PASSWORD=$(
  docker exec -it "$CONTAINER" \
  grep WritePass /exa/etc/EXAConf \
  | sed -e 's/.* = //' \
  | tr -d '\r' \
  | base64 -d)
```

## Remote logging

When creating a Virtual Schema you can enable remote logging for debugging. See the Exasol documentation on [remote logging](https://docs.exasol.com/db/latest/database_concepts/virtual_schema/logging.htm).

Use remote logging only for development and debugging because it can expose sensitive information and affect performance.

## Finding the Trino port

Trino listens on port `8080` by default.

If your installation uses a different port, check the coordinator configuration, typically in `config.properties`, for the `http-server.http.port` or `http-server.https.port` settings.

## Allowing external connections

For Exasol to reach Trino, the Trino coordinator must listen on an address reachable from the Exasol side.

Typical checks:

- verify the configured HTTP or HTTPS port in `config.properties`
- verify host and firewall settings between Exasol and the Trino coordinator
- verify any TLS or authentication settings required by the Trino deployment

Consult the official Trino documentation for deployment-specific network and security settings.

## First steps with Trino

Useful options for local development:

- run a local coordinator with Docker, for example `trinodb/trino`
- use the [Trino CLI](https://trino.io/docs/current/client/cli.html)
- use the JDBC driver with a SQL client or IDE

Example CLI invocation:

```shell
trino --server localhost:8080 --catalog memory --schema default
```

Useful SQL commands:

| Command | Description |
|---------|-------------|
| `SELECT version();` | Display the Trino version |
| `SHOW CATALOGS;` | List catalogs |
| `SHOW SCHEMAS FROM <catalog>;` | List schemas in a catalog |
| `SHOW TABLES FROM <catalog>.<schema>;` | List tables |
| `CREATE TABLE <catalog>.<schema>.mytable (name VARCHAR, age INTEGER);` | Create a table |
