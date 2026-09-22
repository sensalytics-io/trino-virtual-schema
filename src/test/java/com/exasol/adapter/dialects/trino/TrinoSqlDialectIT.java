package com.exasol.adapter.dialects.trino;

import com.exasol.containers.ExasolContainer;
import com.exasol.containers.ExasolService;
import com.exasol.drivers.JdbcDriver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.trino.TrinoContainer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.exasol.matcher.ResultSetStructureMatcher.table;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test against a real Exasol and a real Trino: deploys the adapter from {@code target/}, creates a virtual
 * schema on Trino's {@code memory} connector and verifies that {@code TO_DATE} / {@code TO_TIMESTAMP} predicates are
 * pushed down and return the same rows as local evaluation would.
 */
class TrinoSqlDialectIT {
    private static final Path VS_JAR_PATH = findAdapterJar();
    private static final String VS_JAR = VS_JAR_PATH.getFileName().toString();
    private static final Path DRIVER_PATH = Path.of("target", "trino-driver", "trino-jdbc.jar");

    // The Exasol UDF sandbox does not use Docker's embedded DNS, so a shared network with an alias does not work.
    // The adapter instead reaches Trino through the Docker host's IP and the mapped port.
    private static final ExasolContainer<? extends ExasolContainer<?>> EXASOL = new ExasolContainer<>()
            .withRequiredServices(ExasolService.BUCKETFS, ExasolService.UDF)
            .withReuse(true);
    private static final TrinoContainer TRINO = new TrinoContainer("trinodb/trino:480");

    private static Connection exasolConnection;
    private static Statement exasol;

    @BeforeAll
    static void beforeAll() throws Exception {
        assertTrue(Files.exists(DRIVER_PATH),
                "Driver jar " + DRIVER_PATH + " is missing. Run `mvn package` before running this test from the IDE.");
        Startables.deepStart(EXASOL, TRINO).join();
        createTrinoTable();
        installAdapter();
        createVirtualSchema();
    }

    private static Path findAdapterJar() {
        try (final Stream<Path> files = Files.list(Path.of("target"))) {
            final List<Path> jars = files
                    .filter(path -> path.getFileName().toString().matches("virtual-schema-dist-.*\\.jar"))
                    .collect(Collectors.toList());
            if (jars.size() != 1) {
                throw new IllegalStateException("Expected exactly one virtual-schema-dist-*.jar in target/ but found "
                        + jars + ". Run `mvn clean package` before running this test.");
            }
            return jars.get(0);
        } catch (final IOException exception) {
            throw new IllegalStateException(
                    "Unable to list target/. Run `mvn clean package` before running this test.", exception);
        }
    }

    private static void createTrinoTable() throws SQLException {
        try (final Connection connection = TRINO.createConnection("");
                final Statement trino = connection.createStatement()) {
            trino.execute("CREATE TABLE memory.default.t (ts_col timestamp(6), varchar_col varchar(20))");
            trino.execute("INSERT INTO memory.default.t VALUES"
                    + " (TIMESTAMP '2024-05-01 10:30:00.123000', '01.05.2024'),"
                    + " (TIMESTAMP '2024-05-02 23:59:59.999000', '02.05.2024'),"
                    + " (TIMESTAMP '2024-06-15 00:00:00.000000', '15.06.2024')");
        }
    }

    private static void installAdapter() throws Exception {
        EXASOL.getDefaultBucket().uploadFile(VS_JAR_PATH, VS_JAR);
        EXASOL.getDriverManager().install(JdbcDriver.builder("TRINO")
                .enableSecurityManager(false)
                .mainClass("io.trino.jdbc.TrinoDriver")
                .prefix("jdbc:trino:")
                .sourceFile(DRIVER_PATH)
                .build());
    }

    private static void createVirtualSchema() throws SQLException {
        exasolConnection = EXASOL.createConnection("");
        exasol = exasolConnection.createStatement();
        exasol.execute("CREATE SCHEMA IF NOT EXISTS ADAPTER");
        exasol.execute("CREATE OR REPLACE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER AS\n"
                + "%scriptclass com.exasol.adapter.RequestDispatcher;\n"
                + "%jar /buckets/bfsdefault/default/" + VS_JAR + ";\n");
        // An empty password is required: trino-jdbc refuses password authentication without TLS.
        exasol.execute("CREATE OR REPLACE CONNECTION TRINO_CONNECTION TO 'jdbc:trino://" + EXASOL.getHostIp() + ":"
                + TRINO.getMappedPort(8080) + "' USER 'test' IDENTIFIED BY ''");
        exasol.execute("DROP VIRTUAL SCHEMA IF EXISTS TRINO_VS CASCADE");
        exasol.execute("CREATE VIRTUAL SCHEMA TRINO_VS USING ADAPTER.JDBC_ADAPTER WITH\n"
                + "CONNECTION_NAME = 'TRINO_CONNECTION'\n"
                + "CATALOG_NAME = 'memory'\n"
                + "SCHEMA_NAME = 'default'");
    }

    @AfterAll
    static void afterAll() throws SQLException {
        if (exasol != null) {
            exasol.close();
        }
        if (exasolConnection != null) {
            exasolConnection.close();
        }
    }

    private String explainPushdownSql(final String query) throws SQLException {
        try (final ResultSet result = exasol.executeQuery("EXPLAIN VIRTUAL " + query)) {
            assertTrue(result.next(), "EXPLAIN VIRTUAL returned no rows");
            return result.getString("PUSHDOWN_SQL");
        }
    }

    @Test
    void testToDateOnTimestampColumnIsPushedDownAsCast() throws SQLException {
        final String pushdownSql = explainPushdownSql(
                "SELECT * FROM TRINO_VS.T WHERE TO_DATE(TS_COL) = DATE '2024-05-01'");
        assertThat(pushdownSql, containsString("CAST(\"t\".\"ts_col\" AS DATE)"));
    }

    @Test
    void testToDateOnTimestampColumnReturnsMatchingRows() throws SQLException {
        try (final ResultSet result = exasol.executeQuery(
                "SELECT VARCHAR_COL FROM TRINO_VS.T WHERE TO_DATE(TS_COL) = DATE '2024-05-01'")) {
            assertThat(result, table().row("01.05.2024").matches());
        }
    }

    @Test
    void testToDateWithFormatIsPushedDownAsDateParse() throws SQLException {
        final String pushdownSql = explainPushdownSql(
                "SELECT * FROM TRINO_VS.T WHERE TO_DATE(VARCHAR_COL, 'DD.MM.YYYY') = DATE '2024-05-02'");
        // The format literal's quotes are doubled because the pushdown SQL is embedded in an IMPORT statement.
        assertThat(pushdownSql, containsString("CAST(DATE_PARSE(\"t\".\"varchar_col\", ''%d.%m.%Y'') AS DATE)"));
    }

    @Test
    void testToDateWithFormatReturnsMatchingRows() throws SQLException {
        try (final ResultSet result = exasol.executeQuery(
                "SELECT VARCHAR_COL FROM TRINO_VS.T WHERE TO_DATE(VARCHAR_COL, 'DD.MM.YYYY') = DATE '2024-05-02'")) {
            assertThat(result, table().row("02.05.2024").matches());
        }
    }

    @Test
    void testToTimestampWithFormatReturnsMatchingRows() throws SQLException {
        final String query = "SELECT VARCHAR_COL FROM TRINO_VS.T"
                + " WHERE TO_TIMESTAMP(VARCHAR_COL, 'DD.MM.YYYY') = TIMESTAMP '2024-06-15 00:00:00'";
        assertThat(explainPushdownSql(query),
                containsString("CAST(DATE_PARSE(\"t\".\"varchar_col\", ''%d.%m.%Y'') AS TIMESTAMP(9))"));
        try (final ResultSet result = exasol.executeQuery(query)) {
            assertThat(result, table().row("15.06.2024").matches());
        }
    }

    @Test
    void testUnsupportedFormatModelFailsWithError() {
        final SQLException exception = assertThrows(SQLException.class, () -> exasol.executeQuery(
                "SELECT * FROM TRINO_VS.T WHERE TO_DATE(VARCHAR_COL, 'IYYY-IW') = DATE '2024-01-01'"));
        assertThat(exception.getMessage(), containsString("E-VSTR-7"));
    }
}
