package com.exasol.adapter.dialects.trino;

import com.exasol.adapter.AdapterException;
import com.exasol.errorreporting.ExaError;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Translates Exasol date/time format models into the format strings understood by Trino's {@code date_parse}.
 * <p>
 * Only a subset of the Exasol format model is covered. Elements without an equivalent {@code date_parse} counterpart
 * -- ISO year and week numbers, quarters, Julian days, timezone offsets, quoted literal text, and two-digit years
 * ({@code YY}, which Exasol completes with the current century while {@code %y} uses a fixed 1970 to 2069 window) --
 * are rejected instead of being approximated, because an approximation would silently produce wrong dates.
 * <p>
 * Note that all {@code FF} precisions map to {@code %f}, which parses one to nine fractional digits and truncates the
 * result to millisecond precision regardless of the precision requested.
 */
final class TrinoDateFormat {
    private static final Map<String, String> ELEMENTS = createElements();
    private static final String SEPARATORS = "-/.,:; ";
    private static final String SUPPORTED_ELEMENTS = "YYYY, MONTH, MON, MM, DDD, DD, DAY, DY, HH24, HH12, HH, MI, SS, FF1 to FF9, AM, PM";

    private TrinoDateFormat() {
        // static utility class
    }

    private static Map<String, String> createElements() {
        // Insertion order is match order, so longer elements must come before the shorter ones they start with.
        final Map<String, String> elements = new LinkedHashMap<>();
        elements.put("MONTH", "%M");
        elements.put("HH24", "%H");
        elements.put("HH12", "%h");
        elements.put("YYYY", "%Y");
        elements.put("MON", "%b");
        elements.put("DDD", "%j");
        elements.put("DAY", "%W");
        for (int precision = 1; precision <= 9; ++precision) {
            elements.put("FF" + precision, "%f");
        }
        elements.put("MM", "%m");
        elements.put("MI", "%i");
        elements.put("DD", "%d");
        elements.put("DY", "%a");
        elements.put("HH", "%H");
        elements.put("SS", "%s");
        elements.put("FF", "%f");
        elements.put("AM", "%p");
        elements.put("PM", "%p");
        return elements;
    }

    /**
     * Translate an Exasol format model to a Trino {@code date_parse} format string.
     *
     * @param exasolFormat Exasol format model, for example {@code DD.MM.YYYY}
     * @return equivalent Trino format string, for example {@code %d.%m.%Y}
     * @throws AdapterException if the format model contains an element Trino cannot express
     */
    static String toTrinoFormat(final String exasolFormat) throws AdapterException {
        final StringBuilder result = new StringBuilder();
        int position = 0;
        while (position < exasolFormat.length()) {
            final String element = findElementAt(exasolFormat, position);
            if (element == null) {
                result.append(translateLiteral(exasolFormat, position));
                ++position;
            } else {
                result.append(ELEMENTS.get(element));
                position += element.length();
            }
        }
        return result.toString();
    }

    private static String findElementAt(final String format, final int position) {
        for (final String element : ELEMENTS.keySet()) {
            if (format.regionMatches(true, position, element, 0, element.length())) {
                return element;
            }
        }
        return null;
    }

    private static String translateLiteral(final String format, final int position) throws AdapterException {
        final char character = format.charAt(position);
        if (character == '%') {
            return "%%";
        }
        if (SEPARATORS.indexOf(character) >= 0) {
            return String.valueOf(character);
        }
        throw new AdapterException(ExaError.messageBuilder("E-VSTR-7")
                .message("Unable to push down the Exasol format model {{format}} to Trino."
                                + " It contains an unsupported element at position {{position|uq}}: {{rest}}.",
                        format, position + 1, format.substring(position))
                .mitigation("Use a format model built from the supported elements {{elements|uq}}"
                        + " and the separators {{separators}}.", SUPPORTED_ELEMENTS, SEPARATORS)
                .toString());
    }
}
