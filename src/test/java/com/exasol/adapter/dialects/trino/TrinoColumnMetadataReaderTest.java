package com.exasol.adapter.dialects.trino;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.dialects.trino.TrinoIdentifierMapping.CaseFolding;
import com.exasol.adapter.jdbc.JDBCTypeDescription;
import com.exasol.adapter.metadata.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TrinoColumnMetadataReaderTest {
    private TrinoColumnMetadataReader columnMetadataReader;
    private Map<String, String> rawProperties;
    @Mock
    ExaMetadata exaMetadataMock;

    @BeforeEach
    void beforeEach() {
        when(exaMetadataMock.getDatabaseVersion()).thenReturn("8.34.0");
        this.columnMetadataReader = new TrinoColumnMetadataReader(
                null,
                AdapterProperties.emptyProperties(),
                this.exaMetadataMock,
                BaseIdentifierConverter.createDefault()
        );
        this.rawProperties = new HashMap<>();
    }

    @ValueSource(ints = {Types.OTHER, Types.ARRAY, Types.JAVA_OBJECT})
    @ParameterizedTest
    void testMapJdbcTypeFallbackToMaxVarChar(final int type) {
        assertThat(mapJdbcType(type), equalTo(DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeFallbackToParent() {
        assertThat(mapJdbcType(Types.BOOLEAN), equalTo(DataType.createBool()));
    }

    @Test
    void testGetDefaultTrinoIdentifierMapping() {
        assertThat(this.columnMetadataReader.getIdentifierMapping(), equalTo(CaseFolding.CONVERT_TO_UPPER));
    }

    @Test
    void testGetPreserveCaseTrinoIdentifierMapping() {
        this.rawProperties.put("TRINO_IDENTIFIER_MAPPING", "PRESERVE_ORIGINAL_CASE");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final TrinoColumnMetadataReader testee = new TrinoColumnMetadataReader(null,
                adapterProperties, exaMetadataMock, BaseIdentifierConverter.createDefault());
        assertThat(testee.getIdentifierMapping(), equalTo(CaseFolding.PRESERVE_ORIGINAL_CASE));
    }

    @Test
    void testGetConvertToUpperTrinoIdentifierMapping() {
        this.rawProperties.put("TRINO_IDENTIFIER_MAPPING", "CONVERT_TO_UPPER");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final TrinoColumnMetadataReader testee = new TrinoColumnMetadataReader(null,
                adapterProperties, exaMetadataMock, BaseIdentifierConverter.createDefault());
        assertThat(testee.getIdentifierMapping(), equalTo(CaseFolding.CONVERT_TO_UPPER));
    }

    protected DataType mapJdbcType(final int type) {
        final JDBCTypeDescription jdbcTypeDescription = new JDBCTypeDescription(type, 0, 0, 0, "");
        return this.columnMetadataReader.mapJdbcType(jdbcTypeDescription);
    }
}
