package com.exasol.adapter.dialects.trino;

import com.exasol.adapter.AdapterException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TrinoDateFormatTest {
    @CsvSource(delimiter = '|', value = {
            "YYYY-MM-DD                | %Y-%m-%d",
            "DD.MM.YYYY                | %d.%m.%Y",
            "YYYY/MM/DD                | %Y/%m/%d",
            "YYYY-MM-DD HH24:MI:SS     | %Y-%m-%d %H:%i:%s",
            "YYYY-MM-DD HH:MI:SS       | %Y-%m-%d %H:%i:%s",
            "HH12:MI:SS AM             | %h:%i:%s %p",
            "HH12:MI PM                | %h:%i %p",
            "YYYY-MM-DD HH24:MI:SS.FF3 | %Y-%m-%d %H:%i:%s.%f",
            "SS.FF                     | %s.%f",
            "DD MON YYYY               | %d %b %Y",
            "DD MONTH YYYY             | %d %M %Y",
            "DAY, DD MON YYYY          | %W, %d %b %Y",
            "DY DDD                    | %a %j",
            "yyyy-mm-dd hh24:mi:ss     | %Y-%m-%d %H:%i:%s",
            "YYYYMMDD                  | %Y%m%d",
            "YYYY%                     | %Y%%"
    })
    @ParameterizedTest
    void testToTrinoFormat(final String exasolFormat, final String expected) throws AdapterException {
        assertThat(TrinoDateFormat.toTrinoFormat(exasolFormat), equalTo(expected));
    }

    @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9})
    @ParameterizedTest
    void testToTrinoFormatFractionalSecondPrecisions(final int precision) throws AdapterException {
        assertThat(TrinoDateFormat.toTrinoFormat("SS.FF" + precision), equalTo("%s.%f"));
    }

    @ValueSource(strings = {"YY", "YYY", "YYYY-MM-DD YY", "IYYY-IW", "Q", "J", "YYYY-MM-DDTHH24:MI:SS", "TZH:TZM",
            "\"year \"YYYY", "YYYY_MM_DD"})
    @ParameterizedTest
    void testToTrinoFormatRejectsUnsupportedElements(final String exasolFormat) {
        final AdapterException exception = assertThrows(AdapterException.class,
                () -> TrinoDateFormat.toTrinoFormat(exasolFormat));
        assertThat(exception.getMessage(), containsString("E-VSTR-7"));
    }
}
