package com.exasol.adapter.dialects.trino;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.trino.TrinoIdentifierMapping.CaseFolding;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class TrinoIdentifierConverterTest {
    private Map<String, String> rawProperties;

    @BeforeEach
    void beforeEach() {
        this.rawProperties = new HashMap<>();
    }

    @CsvSource({ //
            "foobar    , CONVERT_TO_UPPER      , FOOBAR", //
            "foobar    , PRESERVE_ORIGINAL_CASE, foobar", //
            "FooBar    , PRESERVE_ORIGINAL_CASE, FooBar", //
            "\"FooBar\", CONVERT_TO_UPPER      , \"FooBar\"", //
            "\"FooBar\", PRESERVE_ORIGINAL_CASE, \"FooBar\"" //
    })
    @ParameterizedTest
    void testAdjustIdentifierCase(final String original, final CaseFolding identifierMapping, final String adjusted) {
        selectIdentifierMapping(identifierMapping);
        assertThat(new TrinoIdentifierConverter(new AdapterProperties(this.rawProperties)).convert(original),
                equalTo(adjusted));
    }

    protected void selectIdentifierMapping(final CaseFolding identifierMapping) {
        this.rawProperties.put(TrinoIdentifierMapping.PROPERTY, identifierMapping.toString());
    }
}