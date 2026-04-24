package com.exasol.adapter.dialects.trino;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;
import com.exasol.adapter.jdbc.ConnectionFactory;
import com.exasol.logging.VersionCollector;

/**
 * Factory for the Trino SQL dialect.
 */
public class TrinoSqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return TrinoSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final ConnectionFactory connectionFactory,
                                       final AdapterProperties properties,
                                       final ExaMetadata exaMetadata) {
        return new TrinoSqlDialect(connectionFactory, properties, exaMetadata);
    }

    @Override
    public String getSqlDialectVersion() {
        final VersionCollector versionCollector = new VersionCollector("META-INF/maven/com.exasol/trino-virtual-schema/pom.properties");
        return versionCollector.getVersionNumber();
    }
}
