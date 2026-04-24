package com.exasol.adapter.dialects.trino;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.dialects.trino.TrinoIdentifierMapping.CaseFolding;
import com.exasol.adapter.jdbc.BaseTableMetadataReader;
import com.exasol.adapter.jdbc.ColumnMetadataReader;
import com.exasol.adapter.jdbc.RemoteMetadataReaderException;
import com.exasol.errorreporting.ExaError;

import java.sql.Connection;
import java.util.logging.Logger;

import static com.exasol.adapter.AdapterProperties.IGNORE_ERRORS_PROPERTY;

/**
 * This class handles the specifics of mapping Trino table metadata to Exasol.
 */
public class TrinoTableMetadataReader extends BaseTableMetadataReader {
    static final Logger LOGGER = Logger.getLogger(TrinoTableMetadataReader.class.getName());

    /**
     * Create a new {@link TrinoTableMetadataReader} instance.
     *
     * @param connection           JDBC connection to the remote data source
     * @param columnMetadataReader reader to be used to map the metadata of the tables columns
     * @param properties           user-defined adapter properties
     * @param identifierConverter  converter between source and Exasol identifiers
     */
    public TrinoTableMetadataReader(final Connection connection,
                                    final ColumnMetadataReader columnMetadataReader,
                                    final AdapterProperties properties,
                                    final ExaMetadata exaMetadata,
                                    final IdentifierConverter identifierConverter) {
        super(connection, columnMetadataReader, properties, exaMetadata, identifierConverter);
    }

    /**
     * Get the identifier mapping that the metadata reader uses when mapping Trino tables to Exasol.
     *
     * @return identifier mapping
     */
    public CaseFolding getIdentifierMapping() {
        return TrinoIdentifierMapping.from(this.properties);
    }

    /**
     * Check if the metadata reader should ignore tables where the name contains upper-case characters.
     *
     * @return <code>true</code> if the reader should ignore upper-case tables
     */
    public boolean ignoresUpperCaseTables() {
        return this.properties.getIgnoredErrors().contains(TrinoIdentifierMapping.UPPERCASE_TABLES_SWITCH);
    }

    @Override
    public boolean isTableIncludedByMapping(final String tableName) {
        if (containsUppercaseCharacter(tableName) && !isUnquotedIdentifier(tableName)) {
            return isUppercaseTableIncludedByMapping(tableName);
        } else {
            return true;
        }
    }

    protected boolean isUppercaseTableIncludedByMapping(final String tableName) {
        if (getIdentifierMapping() == CaseFolding.CONVERT_TO_UPPER) {
            if (ignoresUpperCaseTables()) {
                LOGGER.fine(() -> "Ignoring Trino table " + tableName
                        + "because it contains an uppercase character and " + IGNORE_ERRORS_PROPERTY + " is set to "
                        + TrinoIdentifierMapping.UPPERCASE_TABLES_SWITCH + ".");
                return false;
            } else {
                throw new RemoteMetadataReaderException(
                        ExaError.messageBuilder("E-VSTR-6")
                                .message("Table {{tableName}} cannot be used in virtual schema.", tableName)
                                .mitigation("Set property {{propertyName}} to {{propertyValue}} to enforce schema creation.", IGNORE_ERRORS_PROPERTY, TrinoIdentifierMapping.UPPERCASE_TABLES_SWITCH)
                                .toString()
                );
            }
        } else {
            return true;
        }
    }

    private boolean containsUppercaseCharacter(final String tableName) {
        for (int i = 0; i < tableName.length(); i++) {
            if (Character.isUpperCase(tableName.charAt(i))) {
                return true;
            }
        }

        return false;
    }
}
