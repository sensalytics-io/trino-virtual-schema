package com.exasol.adapter.dialects.trino;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.rewriting.SqlGenerationContext;
import com.exasol.adapter.dialects.rewriting.SqlGenerationVisitor;
import com.exasol.adapter.sql.*;

import java.util.*;

/**
 * This class generates SQL queries for the {@link TrinoSqlDialect}.
 */
public class TrinoSqlGenerationVisitor extends SqlGenerationVisitor {
    private static final Set<String> TYPE_NAMES_VARCHAR_CAST = Set.of("json", "uuid", "ipaddress");
    private static final Set<String> TYPE_NAMES_JSON_FORMATTED = Set.of("array", "map", "row");
    private static final Set<String> TYPE_NAMES_NOT_SUPPORTED = Collections.emptySet();
    private static final String TYPE_NAME_GEOMETRY = "geometry";
    private static final String TYPE_NAME_SPHERICAL_GEOGRAPHY = "sphericalgeography";

    public TrinoSqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        super(dialect, context);
    }

    @Override
    protected String representAnyColumnInSelectList() {
        return SqlConstants.ONE;
    }

    @Override
    public String visit(final SqlColumn column) throws AdapterException {
        final String projectionString = super.visit(column);
        if (isDirectlyInSelectList(column)) {
            return buildColumnProjectionString(getTypeNameFromColumn(column), projectionString);
        }

        return projectionString;
    }

    @Override
    public String visit(final SqlFunctionScalar function) throws AdapterException {
        final List<String> argumentsSql = new ArrayList<>(function.getArguments().size());
        for (final SqlNode node : function.getArguments()) {
            argumentsSql.add(node.accept(this));
        }
        switch (function.getFunction()) {
            case ADD_DAYS:
                return getAddDateTime(argumentsSql, "day");
            case ADD_HOURS:
                return getAddDateTime(argumentsSql, "hour");
            case ADD_MINUTES:
                return getAddDateTime(argumentsSql, "minute");
            case ADD_SECONDS:
                return getAddDateTime(argumentsSql, "second");
            case ADD_WEEKS:
                return getAddDateTime(argumentsSql, "week");
            case ADD_YEARS:
                return getAddDateTime(argumentsSql, "year");
            case ADD_MONTHS:
                return getAddDateTime(argumentsSql, "month");
            case SECOND:
            case MINUTE:
            case DAY:
            case HOUR:
            case WEEK:
            case MONTH:
            case YEAR:
                return getDateTime(argumentsSql, function.getFunction());
            case POSIX_TIME:
                return "TO_UNIXTIME(" + argumentsSql.get(0) + ")";
            case FLOAT_DIV:
                return "(CAST(" + argumentsSql.get(0) + " AS DOUBLE) / CAST(" + argumentsSql.get(1) + " AS DOUBLE))";
            default:
                return super.visit(function);
        }
    }

    private String getAddDateTime(final List<String> argumentsSql, final String unit) {
        return "DATE_ADD('" + unit + "', " + argumentsSql.get(1) + ", " + argumentsSql.get(0) + ")";
    }

    private String getDateTime(final List<String> argumentsSql, final ScalarFunction scalarFunction) {
        return "CAST(EXTRACT(" + scalarFunction.name() + " FROM " + argumentsSql.get(0) + ") AS DECIMAL("
                + getDecimalSize(scalarFunction) + ",0))";
    }

    private int getDecimalSize(final ScalarFunction scalarFunction) {
        switch (scalarFunction) {
            case YEAR:
                return 4;
            default:
                return 2;
        }
    }

    private String buildColumnProjectionString(final String typeName, final String projectionString) {
        final String normalizedTypeName = typeName.toLowerCase(Locale.ENGLISH);

        if (TYPE_NAMES_NOT_SUPPORTED.contains(normalizedTypeName)) {
            return "CAST('" + normalizedTypeName + " NOT SUPPORTED' AS VARCHAR) AS NOT_SUPPORTED";
        }

        if (startsWithAny(normalizedTypeName, TYPE_NAMES_JSON_FORMATTED)) {
            return "JSON_FORMAT(CAST(" + projectionString + " AS JSON))";
        }

        if (normalizedTypeName.startsWith(TYPE_NAME_GEOMETRY)) {
            return "ST_AsText(" + projectionString + ")";
        }

        if (normalizedTypeName.startsWith(TYPE_NAME_SPHERICAL_GEOGRAPHY)) {
            return "ST_AsText(to_geometry(" + projectionString + "))";
        }

        if (startsWithAny(normalizedTypeName, TYPE_NAMES_VARCHAR_CAST)) {
            return "CAST(" + projectionString + " AS VARCHAR)";
        }

        return projectionString;
    }

    private static boolean startsWithAny(final String typeName, final Set<String> prefixes) {
        return prefixes.stream().anyMatch(typeName::startsWith);
    }

    @Override
    public String visit(final SqlFunctionAggregateGroupConcat function) throws AdapterException {
        final String expression = function.getArgument().accept(this);
        final String separator = function.hasSeparator() ? function.getSeparator().accept(this) : "','";
        final StringBuilder builder = new StringBuilder("LISTAGG(");
        if (function.hasDistinct()) {
            builder.append("DISTINCT ");
        }
        builder.append(expression).append(", ").append(separator).append(")");
        if (function.hasOrderBy()) {
            builder.append(" WITHIN GROUP (ORDER BY ").append(renderOrderBy(function.getOrderBy())).append(")");
        }
        return builder.toString();
    }

    private String renderOrderBy(final SqlOrderBy orderBy) throws AdapterException {
        final List<SqlNode> expressions = orderBy.getExpressions();
        final List<Boolean> ascending = orderBy.isAscending();
        final List<Boolean> nullsLast = orderBy.nullsLast();
        final List<String> parts = new ArrayList<>(expressions.size());

        for (int index = 0; index < expressions.size(); index++) {
            final String expression = expressions.get(index).accept(this);
            final boolean isAscending = index < ascending.size() ? ascending.get(index) : true;
            final boolean useNullsLast = index < nullsLast.size() ? nullsLast.get(index) : true;
            parts.add(expression + (isAscending ? " ASC" : " DESC")
                    + (useNullsLast ? " NULLS LAST" : " NULLS FIRST"));
        }

        return String.join(", ", parts);
    }
}
