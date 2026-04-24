package com.exasol.adapter.dialects.trino;

import com.exasol.adapter.dialects.trino.TrinoIdentifierMapping.CaseFolding;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TrinoIdentifierMappingTest {
    @Test
    void testParseConvertToUpperCase() {
        assertThat(TrinoIdentifierMapping.parse("CONVERT_TO_UPPER"), equalTo(CaseFolding.CONVERT_TO_UPPER));
    }

    @Test
    void testParseConvertToPreserverOriginalCase() {
        assertThat(TrinoIdentifierMapping.parse("PRESERVE_ORIGINAL_CASE"),
                equalTo(CaseFolding.PRESERVE_ORIGINAL_CASE));
    }

    @Test
    void testParseNullMappingThrowsException() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> TrinoIdentifierMapping.parse(null));
        assertThat(exception.getMessage(), containsString("E-VSTR-1"));
    }

    @Test
    void testParseUnknownMappingThrowsException() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> TrinoIdentifierMapping.parse("UNKNOWN"));
        assertThat(exception.getMessage(), containsString("E-VSTR-2"));
    }
}