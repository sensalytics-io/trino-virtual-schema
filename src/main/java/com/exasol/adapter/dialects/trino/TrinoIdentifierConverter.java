package com.exasol.adapter.dialects.trino;

import java.util.regex.Pattern;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierCaseHandling;
import com.exasol.adapter.dialects.IdentifierConverter;

/**
 * This class implements database identifier conversion for {@link TrinoSqlDialect}.
 */
public class TrinoIdentifierConverter implements IdentifierConverter {
    private static final Pattern UNQUOTED_IDENTIFIER_PATTERN = Pattern.compile("^[a-z][0-9a-z_]*");

    private final AdapterProperties properties;

    public TrinoIdentifierConverter(final AdapterProperties properties) {
        this.properties = properties;
    }

    @Override
    public String convert(final String identifier) {
        if (getIdentifierMapping() == TrinoIdentifierMapping.CaseFolding.PRESERVE_ORIGINAL_CASE) {
            return identifier;
        }

        return isUnquotedIdentifier(identifier) ? identifier.toUpperCase() : identifier;
    }

    public TrinoIdentifierMapping.CaseFolding getIdentifierMapping() {
        return TrinoIdentifierMapping.from(this.properties);
    }

    private boolean isUnquotedIdentifier(final String identifier) {
        return UNQUOTED_IDENTIFIER_PATTERN.matcher(identifier).matches();
    }

    @Override
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_LOWER;
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
    }
}
