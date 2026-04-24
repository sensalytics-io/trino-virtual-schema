package com.exasol.adapter.dialects.trino;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.AbstractRemoteMetadataReader;
import com.exasol.adapter.jdbc.BaseTableMetadataReader;
import com.exasol.adapter.jdbc.ColumnMetadataReader;

import java.sql.Connection;

/**
 * This class implements a reader for Trino-specific metadata.
 */
public class TrinoMetadataReader extends AbstractRemoteMetadataReader {
    public TrinoMetadataReader(final Connection connection,
                               final AdapterProperties properties,
                               final ExaMetadata exaMetadata) {
        super(connection, properties, exaMetadata);
    }

    @Override
    public BaseTableMetadataReader createTableMetadataReader() {
        return new TrinoTableMetadataReader(
                this.connection,
                getColumnMetadataReader(),
                this.properties,
                this.exaMetadata, getIdentifierConverter()
        );
    }

    @Override
    public ColumnMetadataReader createColumnMetadataReader() {
        return new TrinoColumnMetadataReader(
                this.connection,
                this.properties,
                this.exaMetadata,
                getIdentifierConverter()
        );
    }

    @Override
    public IdentifierConverter createIdentifierConverter() {
        return new TrinoIdentifierConverter(this.properties);
    }
}
