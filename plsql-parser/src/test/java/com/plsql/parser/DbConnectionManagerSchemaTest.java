package com.plsql.parser;

import com.plsql.parser.flow.DbConnectionManager;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * getConnection(schema) must resolve against each DbUser's declared `schemas` list (falling
 * back to [username] when unset), not just an exact username match — this is what lets a
 * login user with cross-schema grants (e.g. "analyzer" reading "ANALYZER_REF") be used to
 * query a schema other than its own login name.
 *
 * These are lookup/rejection tests only — no real JDBC connection is attempted, since a
 * matching user always exists (getConnection() throws before ever dialing out when no user
 * declares the requested schema).
 */
class DbConnectionManagerSchemaTest {

    @Test
    void rejectsSchemaNotInAnyUsersSchemasList() {
        DbConnectionManager.DbUser analyzer =
                new DbConnectionManager.DbUser("analyzer", "pw", "demo", null); // falls back to [ANALYZER]
        try (DbConnectionManager mgr = new DbConnectionManager("jdbc:oracle:thin:@//localhost:1521/FREEPDB1",
                List.of(analyzer))) {
            SQLException ex = assertThrows(SQLException.class, () -> mgr.getConnection("SOME_OTHER_SCHEMA"));
            assertTrue(ex.getMessage().contains("No credentials configured for schema"));
        }
    }

    @Test
    void availableSchemas_includesDeclaredSchemasBeyondUsername() {
        DbConnectionManager.DbUser analyzer = new DbConnectionManager.DbUser(
                "analyzer", "pw", "demo", List.of("ANALYZER", "ANALYZER_REF"));
        try (DbConnectionManager mgr = new DbConnectionManager("jdbc:oracle:thin:@//localhost:1521/FREEPDB1",
                List.of(analyzer))) {
            List<String> schemas = mgr.getAvailableSchemas();
            assertTrue(schemas.contains("ANALYZER"));
            assertTrue(schemas.contains("ANALYZER_REF"));
        }
    }

    @Test
    void dbUser_getSchemas_fallsBackToUsername_whenNotDeclared() {
        DbConnectionManager.DbUser u = new DbConnectionManager.DbUser("analyzer", "pw", "demo");
        assertEquals(List.of("ANALYZER"), u.getSchemas());
    }

    @Test
    void dbUser_getSchemas_usesDeclaredList_whenPresent() {
        DbConnectionManager.DbUser u = new DbConnectionManager.DbUser(
                "analyzer", "pw", "demo", List.of("analyzer", "analyzer_ref"));
        assertEquals(List.of("ANALYZER", "ANALYZER_REF"), u.getSchemas());
    }
}
