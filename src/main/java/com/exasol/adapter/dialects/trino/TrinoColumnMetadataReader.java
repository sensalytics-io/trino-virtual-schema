package com.exasol.adapter.dialects.trino;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.BaseColumnMetadataReader;
import com.exasol.adapter.jdbc.JDBCTypeDescription;
import com.exasol.adapter.metadata.DataType;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.logging.Logger;

/**
 * This class implements Trino-specific reading of column metadata.
 */
public class TrinoColumnMetadataReader extends BaseColumnMetadataReader {
    private static final Logger LOGGER = Logger.getLogger(TrinoColumnMetadataReader.class.getName());

    /**
     * Create a new instance of the {@link TrinoColumnMetadataReader}.
     *
     * @param connection          JDBC connection to the remote data source
     * @param properties          user-defined adapter properties
     * @param exaMetadata         metadata of the Exasol database
     * @param identifierConverter converter between source and Exasol identifiers
     */
    public TrinoColumnMetadataReader(final Connection connection,
                                     final AdapterProperties properties,
                                     final ExaMetadata exaMetadata,
                                     final IdentifierConverter identifierConverter) {
        super(connection, properties, exaMetadata, identifierConverter);
    }

    @Override
    public DataType mapJdbcType(final JDBCTypeDescription jdbcTypeDescription) {
        switch (jdbcTypeDescription.getJdbcType()) {
            case Types.OTHER:               // NUMBER
            case Types.JAVA_OBJECT:         // MAP, ROW, JSON, JSON2016, IPADDRESS, UUID
            case Types.ARRAY:               // ARRAY
                LOGGER.finer(() -> "Mapping Trino datatype \"" + jdbcTypeDescription.getTypeName()
                        + "\" to maximum VARCHAR()");
                return DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8);
            case Types.TIME_WITH_TIMEZONE:  // TIME WITH TIME ZONE
                return DataType.createVarChar(100, DataType.ExaCharset.UTF8);
            default:
                return super.mapJdbcType(jdbcTypeDescription);
        }
    }

    @Override
    public String readColumnName(final ResultSet columns) throws SQLException {
        if (getIdentifierMapping().equals(TrinoIdentifierMapping.CaseFolding.CONVERT_TO_UPPER)) {
            return super.readColumnName(columns).toUpperCase();
        } else {
            return super.readColumnName(columns);
        }
    }

    TrinoIdentifierMapping.CaseFolding getIdentifierMapping() {
        return TrinoIdentifierMapping.from(this.properties);
    }
}
