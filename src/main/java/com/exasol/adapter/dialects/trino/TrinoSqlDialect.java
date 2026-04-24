package com.exasol.adapter.dialects.trino;

import static com.exasol.adapter.AdapterProperties.CATALOG_NAME_PROPERTY;
import static com.exasol.adapter.AdapterProperties.IGNORE_ERRORS_PROPERTY;
import static com.exasol.adapter.AdapterProperties.SCHEMA_NAME_PROPERTY;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.AVG;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.AVG_DISTINCT;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.COUNT;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.COUNT_DISTINCT;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.COUNT_STAR;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.FIRST_VALUE;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.GROUP_CONCAT;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.LAST_VALUE;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.MAX;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.MEDIAN;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.MIN;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.STDDEV;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.STDDEV_DISTINCT;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.STDDEV_POP;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.STDDEV_POP_DISTINCT;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.STDDEV_SAMP;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.STDDEV_SAMP_DISTINCT;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.SUM;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.SUM_DISTINCT;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.VAR_POP;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.VAR_POP_DISTINCT;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.VAR_SAMP;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.VAR_SAMP_DISTINCT;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.VARIANCE;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.VARIANCE_DISTINCT;
import static com.exasol.adapter.capabilities.LiteralCapability.BOOL;
import static com.exasol.adapter.capabilities.LiteralCapability.DATE;
import static com.exasol.adapter.capabilities.LiteralCapability.DOUBLE;
import static com.exasol.adapter.capabilities.LiteralCapability.EXACTNUMERIC;
import static com.exasol.adapter.capabilities.LiteralCapability.NULL;
import static com.exasol.adapter.capabilities.LiteralCapability.STRING;
import static com.exasol.adapter.capabilities.LiteralCapability.TIMESTAMP;
import static com.exasol.adapter.capabilities.LiteralCapability.TIMESTAMP_UTC;
import static com.exasol.adapter.capabilities.MainCapability.AGGREGATE_GROUP_BY_COLUMN;
import static com.exasol.adapter.capabilities.MainCapability.AGGREGATE_GROUP_BY_EXPRESSION;
import static com.exasol.adapter.capabilities.MainCapability.AGGREGATE_GROUP_BY_TUPLE;
import static com.exasol.adapter.capabilities.MainCapability.AGGREGATE_HAVING;
import static com.exasol.adapter.capabilities.MainCapability.AGGREGATE_SINGLE_GROUP;
import static com.exasol.adapter.capabilities.MainCapability.FILTER_EXPRESSIONS;
import static com.exasol.adapter.capabilities.MainCapability.JOIN;
import static com.exasol.adapter.capabilities.MainCapability.JOIN_CONDITION_EQUI;
import static com.exasol.adapter.capabilities.MainCapability.JOIN_TYPE_FULL_OUTER;
import static com.exasol.adapter.capabilities.MainCapability.JOIN_TYPE_INNER;
import static com.exasol.adapter.capabilities.MainCapability.JOIN_TYPE_LEFT_OUTER;
import static com.exasol.adapter.capabilities.MainCapability.JOIN_TYPE_RIGHT_OUTER;
import static com.exasol.adapter.capabilities.MainCapability.LIMIT;
import static com.exasol.adapter.capabilities.MainCapability.LIMIT_WITH_OFFSET;
import static com.exasol.adapter.capabilities.MainCapability.ORDER_BY_COLUMN;
import static com.exasol.adapter.capabilities.MainCapability.ORDER_BY_EXPRESSION;
import static com.exasol.adapter.capabilities.MainCapability.SELECTLIST_EXPRESSIONS;
import static com.exasol.adapter.capabilities.MainCapability.SELECTLIST_PROJECTION;
import static com.exasol.adapter.capabilities.PredicateCapability.AND;
import static com.exasol.adapter.capabilities.PredicateCapability.BETWEEN;
import static com.exasol.adapter.capabilities.PredicateCapability.EQUAL;
import static com.exasol.adapter.capabilities.PredicateCapability.IN_CONSTLIST;
import static com.exasol.adapter.capabilities.PredicateCapability.IS_NOT_NULL;
import static com.exasol.adapter.capabilities.PredicateCapability.IS_NULL;
import static com.exasol.adapter.capabilities.PredicateCapability.LESS;
import static com.exasol.adapter.capabilities.PredicateCapability.LESSEQUAL;
import static com.exasol.adapter.capabilities.PredicateCapability.LIKE;
import static com.exasol.adapter.capabilities.PredicateCapability.LIKE_ESCAPE;
import static com.exasol.adapter.capabilities.PredicateCapability.NOT;
import static com.exasol.adapter.capabilities.PredicateCapability.NOTEQUAL;
import static com.exasol.adapter.capabilities.PredicateCapability.OR;
import static com.exasol.adapter.capabilities.PredicateCapability.REGEXP_LIKE;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.capabilities.ScalarFunctionCapability;
import com.exasol.adapter.dialects.AbstractSqlDialect;
import com.exasol.adapter.dialects.QueryRewriter;
import com.exasol.adapter.dialects.SqlDialect.NullSorting;
import com.exasol.adapter.dialects.SqlDialect.StructureElementSupport;
import com.exasol.adapter.dialects.SqlGenerator;
import com.exasol.adapter.dialects.rewriting.ImportIntoTemporaryTableQueryRewriter;
import com.exasol.adapter.dialects.rewriting.SqlGenerationContext;
import com.exasol.adapter.jdbc.ConnectionFactory;
import com.exasol.adapter.jdbc.RemoteMetadataReader;
import com.exasol.adapter.jdbc.RemoteMetadataReaderException;
import com.exasol.adapter.sql.ScalarFunction;
import com.exasol.errorreporting.ExaError;

/**
 * This class implements the Trino dialect.
 */
public class TrinoSqlDialect extends AbstractSqlDialect {
    static final String NAME = "TRINO";

    private static final Set<ScalarFunctionCapability> DISABLED_SCALAR_FUNCTION = Set.of(
            // Implementation for `BETWEEN` time functions is not supported.
            SECONDS_BETWEEN, MINUTES_BETWEEN, HOURS_BETWEEN, DAYS_BETWEEN, MONTHS_BETWEEN, YEARS_BETWEEN,
            ROUND, // Trino rounds 0.5 to nearest even (banker's rounding) while Exasol rounds it up
            COLOGNE_PHONETIC, // No Trino equivalent
            CONCAT, // Fails for boolean data types
            INSTR, // Trino has STRPOS but it lacks the position and occurrence parameters
            // Currently not implemented:
            DUMP, EDIT_DISTANCE, INSERT, LOCATE, REGEXP_INSTR, REGEXP_SUBSTR, SOUNDEX, SPACE, UNICODE, UNICODECHR,
            DBTIMEZONE, FROM_POSIX_TIME, SESSIONTIMEZONE, IS_NUMBER, IS_BOOLEAN, IS_DATE, IS_DSINTERVAL,
            IS_YMINTERVAL, IS_TIMESTAMP, TO_CHAR, TO_DATE, TO_NUMBER, TO_TIMESTAMP, BIT_AND, BIT_CHECK, BIT_LROTATE,
            BIT_LSHIFT, BIT_NOT, BIT_OR, BIT_RROTATE, BIT_RSHIFT, BIT_SET, BIT_TO_NUM, BIT_XOR, HASHTYPE_MD5,
            HASH_SHA1, HASHTYPE_SHA1, HASH_SHA256, HASHTYPE_SHA256, HASH_SHA512, HASHTYPE_SHA512, HASH_TIGER,
            HASHTYPE_TIGER, NULLIFZERO, ZEROIFNULL, MIN_SCALE, NUMTOYMINTERVAL, JSON_VALUE, TO_DSINTERVAL,
            CONVERT_TZ, NUMTODSINTERVAL, TO_YMINTERVAL, CAST, SYS_GUID, SYSTIMESTAMP, CURRENT_STATEMENT,
            CURRENT_USER, SYSDATE, CURRENT_SESSION,
            // Geospatial are currently not supported:
            ST_X, ST_Y, ST_ENDPOINT, ST_ISCLOSED, ST_ISRING, ST_LENGTH, ST_NUMPOINTS, ST_POINTN,
            ST_STARTPOINT, ST_AREA, ST_EXTERIORRING, ST_INTERIORRINGN, ST_NUMINTERIORRINGS, ST_GEOMETRYN,
            ST_NUMGEOMETRIES, ST_BOUNDARY, ST_BUFFER, ST_CENTROID, ST_CONTAINS, ST_CONVEXHULL, ST_CROSSES,
            ST_DIFFERENCE, ST_DIMENSION, ST_DISJOINT, ST_DISTANCE, ST_ENVELOPE, ST_EQUALS, ST_FORCE2D,
            ST_GEOMETRYTYPE, ST_INTERSECTION, ST_INTERSECTS, ST_ISEMPTY, ST_ISSIMPLE, ST_OVERLAPS, ST_SETSRID,
            ST_SYMDIFFERENCE, ST_TOUCHES, ST_TRANSFORM, ST_UNION, ST_WITHIN);

    private static final Capabilities CAPABILITIES = createCapabilityList();

    /*
     * IMPORTANT! Before adding new capabilities, check the `doc/design.md` file if there is a note on why the
     * capability is not supported.
     */
    private static Capabilities createCapabilityList() {
        return Capabilities.builder()
                .addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                        AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE,
                        AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT, LIMIT_WITH_OFFSET, JOIN,
                        JOIN_TYPE_INNER, JOIN_TYPE_LEFT_OUTER, JOIN_TYPE_RIGHT_OUTER, JOIN_TYPE_FULL_OUTER,
                        JOIN_CONDITION_EQUI)
                .addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, BETWEEN,
                        REGEXP_LIKE, IN_CONSTLIST, IS_NULL, IS_NOT_NULL)
                .addLiteral(BOOL, NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING)
                .addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG,
                        AVG_DISTINCT, MEDIAN, FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP,
                        STDDEV_POP_DISTINCT, STDDEV_SAMP, STDDEV_SAMP_DISTINCT, VARIANCE, VARIANCE_DISTINCT,
                        VAR_POP, VAR_POP_DISTINCT, VAR_SAMP, VAR_SAMP_DISTINCT, GROUP_CONCAT)
                .addScalarFunction(getEnabledScalarFunctionCapabilities()).build();
    }

    private static ScalarFunctionCapability[] getEnabledScalarFunctionCapabilities() {
        return Arrays.stream(ScalarFunctionCapability.values())
                .filter(Predicate.not(DISABLED_SCALAR_FUNCTION::contains))
                .toArray(ScalarFunctionCapability[]::new);
    }

    public TrinoSqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties,
            final ExaMetadata exaMetadata) {
        super(connectionFactory, properties, exaMetadata,
                Set.of(CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY, IGNORE_ERRORS_PROPERTY,
                        TrinoIdentifierMapping.PROPERTY),
                List.of(TrinoIdentifierMapping.validator()));
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        try {
            return new TrinoMetadataReader(this.connectionFactory.getConnection(), this.properties, this.exaMetadata);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException(ExaError.messageBuilder("E-VSTR-3")
                    .message("Unable to create Trino remote metadata reader. Caused by: {{cause}}",
                            exception.getMessage())
                    .toString(), exception);
        }
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new ImportIntoTemporaryTableQueryRewriter(this, createRemoteMetadataReader(), this.connectionFactory);
    }

    @Override
    public boolean omitParentheses(final ScalarFunction function) {
        return function.name().equals("CURRENT_DATE") || function.name().equals("CURRENT_TIMESTAMP")
                || function.name().equals("LOCALTIMESTAMP");
    }

    @Override
    public Capabilities getCapabilities() {
        return CAPABILITIES;
    }

    private TrinoIdentifierMapping.CaseFolding getIdentifierMapping() {
        return TrinoIdentifierMapping.from(this.properties);
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
        final Map<ScalarFunction, String> scalarAliases = new EnumMap<>(ScalarFunction.class);
        scalarAliases.put(ScalarFunction.SUBSTR, "SUBSTRING");
        scalarAliases.put(ScalarFunction.HASH_MD5, "MD5");
        scalarAliases.put(ScalarFunction.RAND, "RANDOM");
        return scalarAliases;
    }

    @Override
    public StructureElementSupport supportsJdbcCatalogs() {
        return StructureElementSupport.MULTIPLE;
    }

    @Override
    public StructureElementSupport supportsJdbcSchemas() {
        return StructureElementSupport.MULTIPLE;
    }

    @Override
    public String applyQuote(final String identifier) {
        String trinoIdentifier = identifier;
        if (getIdentifierMapping() != TrinoIdentifierMapping.CaseFolding.PRESERVE_ORIGINAL_CASE) {
            trinoIdentifier = convertIdentifierToLowerCase(trinoIdentifier);
        }
        return super.quoteIdentifierWithDoubleQuotes(trinoIdentifier);
    }

    private String convertIdentifierToLowerCase(final String identifier) {
        return identifier.toLowerCase();
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(final SqlGenerationContext context) {
        return true;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(final SqlGenerationContext context) {
        return true;
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_AT_END;
    }

    @Override
    public SqlGenerator getSqlGenerator(final SqlGenerationContext context) {
        return new TrinoSqlGenerationVisitor(this, context);
    }

    @Override
    public String getStringLiteral(final String value) {
        if (value == null) {
            return "NULL";
        }
        return "'" + value.replace("'", "''") + "'";
    }
}
