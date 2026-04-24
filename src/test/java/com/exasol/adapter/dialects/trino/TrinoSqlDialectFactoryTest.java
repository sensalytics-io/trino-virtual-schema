package com.exasol.adapter.dialects.trino;

import com.exasol.adapter.AdapterProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

class TrinoSqlDialectFactoryTest {
    private TrinoSqlDialectFactory factory;

    @BeforeEach
    void beforeEach() {
        this.factory = new TrinoSqlDialectFactory();
    }

    @Test
    void testGetName() {
        assertThat(this.factory.getSqlDialectName(), equalTo("TRINO"));
    }

    @Test
    void testCreateDialect() {
        assertThat(this.factory.createSqlDialect(null, AdapterProperties.emptyProperties(), null),
                instanceOf(TrinoSqlDialect.class));
    }
}
