package com.exasol.adapter.dialects.trino;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.DialectTestData;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.rewriting.SqlGenerationContext;
import com.exasol.adapter.jdbc.ConnectionFactory;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static com.exasol.adapter.dialects.VisitorAssertions.assertSqlNodeConvertedToOne;
import static com.exasol.adapter.sql.ScalarFunction.POSIX_TIME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@ExtendWith(MockitoExtension.class)
class TrinoSqlGenerationVisitorTest {
    private TrinoSqlGenerationVisitor visitor;

    @BeforeEach
    void beforeEach(@Mock final ConnectionFactory connectionFactoryMock) {
        final SqlDialect dialect = new TrinoSqlDialect(connectionFactoryMock, AdapterProperties.emptyProperties(), null);
        final SqlGenerationContext context = new SqlGenerationContext("test_catalog", "test_schema", false);
        this.visitor = new TrinoSqlGenerationVisitor(dialect, context);
    }

    @CsvSource({"ADD_DAYS, day", "ADD_HOURS, hour", "ADD_MINUTES, minute", "ADD_SECONDS, second",
            "ADD_YEARS, year", "ADD_WEEKS, week", "ADD_MONTHS, month"})
    @ParameterizedTest
    void testVisitSqlFunctionScalarAddDate(final ScalarFunction scalarFunction, final String expected)
            throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarForDateTest(scalarFunction, 10);
        assertThat(this.visitor.visit(sqlFunctionScalar),
                equalTo("DATE_ADD('" + expected + "', 10, \"test_column\")"));
    }

    private SqlFunctionScalar createSqlFunctionScalarForDateTest(final ScalarFunction scalarFunction,
                                                                 final int numericValue) {
        final List<com.exasol.adapter.sql.SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlColumn(1,
                ColumnMetadata.builder().name("test_column")
                        .adapterNotes("{\"jdbcDataType\":93, \"typeName\":\"TIMESTAMP\"}")
                        .type(DataType.createChar(20, DataType.ExaCharset.UTF8)).build()));
        arguments.add(new SqlLiteralExactnumeric(new BigDecimal(numericValue)));
        return new SqlFunctionScalar(scalarFunction, arguments);
    }

    @CsvSource({"SECOND, 2", "MINUTE, 2", "DAY, 2", "HOUR, 2", "WEEK, 2", "MONTH, 2", "YEAR, 4"})
    @ParameterizedTest
    void testVisitSqlFunctionScalarDatetime(final ScalarFunction scalarFunction, final String decimalSize)
            throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarForDateTest(scalarFunction, 0);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo(
                "CAST(EXTRACT(" + scalarFunction.name() + " FROM \"test_column\") AS DECIMAL(" + decimalSize + ",0))"));
    }

    @Test
    void testVisitSqlFunctionScalarPosixTime() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarForDateTest(POSIX_TIME, 0);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("TO_UNIXTIME(\"test_column\")"));
    }

    @Test
    void testVisitSqlSelectListAnyValue() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createAnyValueSelectList();
        assertSqlNodeConvertedToOne(sqlSelectList, this.visitor);
    }

    @Test
    void testVisitSqlFunctionAggregateGroupConcat() throws AdapterException {
        final SqlFunctionAggregateGroupConcat groupConcat = SqlFunctionAggregateGroupConcat
                .builder(new SqlLiteralString("test"))
                .separator(new SqlLiteralString("'"))
                .build();
        assertThat(this.visitor.visit(groupConcat), equalTo("LISTAGG('test', '''')"));
    }

    @Test
    void testVisitSqlFunctionAggregateGroupConcatWithOrderBy() throws AdapterException {
        final ColumnMetadata col1 = ColumnMetadata.builder().name("test_column").type(DataType.createBool()).build();
        final ColumnMetadata col2 = ColumnMetadata.builder().name("test_column2").type(DataType.createDouble()).build();
        final SqlOrderBy orderBy = new SqlOrderBy(
                List.of(new SqlColumn(1, col1), new SqlColumn(2, col2)),
                List.of(false, true),
                List.of(false, true));
        final SqlFunctionAggregateGroupConcat groupConcat = SqlFunctionAggregateGroupConcat
                .builder(new SqlLiteralString("test"))
                .separator(new SqlLiteralString("'"))
                .orderBy(orderBy)
                .build();
        assertThat(this.visitor.visit(groupConcat), equalTo(
                "LISTAGG('test', '''') WITHIN GROUP (ORDER BY \"test_column\" DESC NULLS FIRST, \"test_column2\" ASC NULLS LAST)"));
    }

    @Test
    void testVisitSqlFunctionScalarFloatDiv() throws AdapterException {
        final SqlFunctionScalar function = new SqlFunctionScalar(ScalarFunction.FLOAT_DIV,
                List.of(new SqlLiteralExactnumeric(new BigDecimal(5)),
                        new SqlLiteralExactnumeric(new BigDecimal(2))));
        assertThat(this.visitor.visit(function), equalTo("(CAST(5 AS DOUBLE) / CAST(2 AS DOUBLE))"));
    }

    @Test
    void testVisitSqlStatementSelect() throws AdapterException {
        final SqlStatementSelect select = (SqlStatementSelect) DialectTestData.getTestSqlNode();
        assertThat(this.visitor.visit(select), equalTo(
                "SELECT \"user_id\", COUNT(\"url\") FROM \"test_catalog\".\"test_schema\".\"clicks\" "
                        + "WHERE 1 < \"user_id\" "
                        + "GROUP BY \"user_id\" "
                        + "HAVING 1 < COUNT(\"url\") "
                        + "ORDER BY \"user_id\" LIMIT 10"));
    }

    @CsvSource(delimiter = '|', value = {
            "array(integer)       | JSON_FORMAT(CAST(\"col\" AS JSON))",
            "map(varchar,integer) | JSON_FORMAT(CAST(\"col\" AS JSON))",
            "row(a integer)       | JSON_FORMAT(CAST(\"col\" AS JSON))",
            "Geometry             | ST_AsText(\"col\")",
            "SphericalGeography   | ST_AsText(to_geometry(\"col\"))",
            "json                 | CAST(\"col\" AS VARCHAR)",
            "uuid                 | CAST(\"col\" AS VARCHAR)",
            "ipaddress            | CAST(\"col\" AS VARCHAR)",
            "varchar              | \"col\""
    })
    @ParameterizedTest
    void testColumnProjectionConversion(final String typeName, final String expected) throws AdapterException {
        final SqlColumn column = new SqlColumn(1, ColumnMetadata.builder()
                .name("col")
                .adapterNotes("{\"jdbcDataType\":12, \"typeName\":\"" + typeName.trim() + "\"}")
                .type(DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8))
                .build());
        final SqlSelectList selectList = SqlSelectList.createRegularSelectList(List.of(column));
        assertThat(this.visitor.visit(selectList), equalTo(expected.trim()));
    }
}
