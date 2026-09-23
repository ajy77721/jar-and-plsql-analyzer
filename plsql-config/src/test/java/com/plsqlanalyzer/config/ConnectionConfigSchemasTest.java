package com.plsqlanalyzer.config;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Backward-compat contract for the `schemas` field added to ConnectionConfig/DbUserConfig:
 * when absent/null/empty it must fall back to [username] so existing YAML configs (no
 * `schemas` key) keep working unchanged.
 */
class ConnectionConfigSchemasTest {

    @Test
    void connectionConfig_fallsBackToUsername_whenSchemasAbsent() {
        ConnectionConfig c = new ConnectionConfig();
        c.setUsername("analyzer");
        assertNull(c.getSchemasRaw(), "raw value should be null until explicitly set");
        assertEquals(List.of("ANALYZER"), c.getSchemas());
    }

    @Test
    void connectionConfig_usesDeclaredSchemas_whenPresent() {
        ConnectionConfig c = new ConnectionConfig();
        c.setUsername("analyzer");
        c.setSchemas(List.of("ANALYZER", "ANALYZER_REF"));
        assertEquals(List.of("ANALYZER", "ANALYZER_REF"), c.getSchemas());
    }

    @Test
    void connectionConfig_emptySchemasList_fallsBackToUsername() {
        ConnectionConfig c = new ConnectionConfig();
        c.setUsername("analyzer");
        c.setSchemas(List.of());
        assertEquals(List.of("ANALYZER"), c.getSchemas());
    }

    @Test
    void dbUserConfig_fallsBackToUsername_whenSchemasAbsent() {
        DbUserConfig u = new DbUserConfig("analyzer", "pw", "demo");
        assertEquals(List.of("ANALYZER"), u.getSchemas());
    }

    @Test
    void dbUserConfig_usesDeclaredSchemas_whenPresent() {
        DbUserConfig u = new DbUserConfig("analyzer", "pw", "demo", List.of("ANALYZER", "ANALYZER_REF"));
        assertEquals(List.of("ANALYZER", "ANALYZER_REF"), u.getSchemas());
    }

    @Test
    void toDbUserConfig_carriesSchemasThrough() {
        ConnectionConfig c = new ConnectionConfig();
        c.setUsername("analyzer");
        c.setSchemas(List.of("ANALYZER", "ANALYZER_REF"));
        DbUserConfig u = c.toDbUserConfig();
        assertEquals(List.of("ANALYZER", "ANALYZER_REF"), u.getSchemas());
    }
}
