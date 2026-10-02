package com.plsql.parser.multidb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * W1 per-overload fetch for PostgreSQL (by {@code pg_proc.oid} + identity arguments) and DB2 (by
 * {@code SPECIFICNAME}). Verifies the catalog rows carry the per-overload identity and that the
 * fetch methods address the exact overload rather than a name-based LIMIT 1.
 */
@ExtendWith(MockitoExtension.class)
class PostgresDb2OverloadFetchTest {

    @Mock Connection conn;
    @Mock PreparedStatement ps;
    @Mock ResultSet rs;

    // ── PostgreSQL ───────────────────────────────────────────────────────────────────────────

    @Test
    void postgres_listObjects_carriesOidAndIdentityArgs() throws Exception {
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(rs.getString("proname")).thenReturn("f", "f");
        when(rs.getString("prokind")).thenReturn("f", "f");
        when(rs.getString("nspname")).thenReturn("public", "public");
        when(rs.getLong("oid")).thenReturn(11L, 22L);
        // same name, same arity, DIFFERENT types → distinct identity arguments
        when(rs.getString("idargs")).thenReturn("integer", "text");

        PostgresSourceFetchStrategy strategy = new PostgresSourceFetchStrategy();
        List<SourceFetchStrategy.ObjectRef> refs = strategy.listObjects(conn, "public");

        assertEquals(2, refs.size());
        assertEquals("11", refs.get(0).oid);
        assertEquals("integer", refs.get(0).identityArgs);
        assertEquals("22", refs.get(1).oid);
        assertEquals("text", refs.get(1).identityArgs);
        assertNotEquals(refs.get(0).identityArgs, refs.get(1).identityArgs);
    }

    @Test
    void postgres_fetchSourceByOid_addressesOneOverload() throws Exception {
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getString("src")).thenReturn("CREATE FUNCTION f(integer) ... $$ ... $$");

        PostgresSourceFetchStrategy strategy = new PostgresSourceFetchStrategy();
        String src = strategy.fetchSourceByOid(conn, "42");

        assertNotNull(src);
        assertTrue(src.contains("CREATE FUNCTION"));
        verify(ps).setLong(1, 42L);
    }

    @Test
    void postgres_fetchSourceByOid_blankOrNonNumeric_returnsNull() throws Exception {
        PostgresSourceFetchStrategy strategy = new PostgresSourceFetchStrategy();
        assertNull(strategy.fetchSourceByOid(conn, "  "));
        assertNull(strategy.fetchSourceByOid(conn, "not-a-number"));
        verifyNoInteractions(conn);
    }

    // ── DB2 ─────────────────────────────────────────────────────────────────────────────────

    @Test
    void db2_listObjects_carriesSpecificName() throws Exception {
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(rs.getString("ROUTINENAME")).thenReturn("F", "F");
        when(rs.getString("ROUTINETYPE")).thenReturn("P", "F");
        when(rs.getString("ROUTINESCHEMA")).thenReturn("DB2INST1", "DB2INST1");
        when(rs.getString("SPECIFICNAME")).thenReturn("SQL123", "SQL456");

        Db2SourceFetchStrategy strategy = new Db2SourceFetchStrategy();
        List<SourceFetchStrategy.ObjectRef> refs = strategy.listObjects(conn, "DB2INST1");

        assertEquals(2, refs.size());
        assertEquals("SQL123", refs.get(0).specificName);
        assertEquals("SQL456", refs.get(1).specificName);
        assertNotEquals(refs.get(0).specificName, refs.get(1).specificName);
    }

    @Test
    void db2_fetchSourceBySpecificName_addressesOneOverload() throws Exception {
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getString("TEXT")).thenReturn("CREATE PROCEDURE F(...) BEGIN END");

        Db2SourceFetchStrategy strategy = new Db2SourceFetchStrategy();
        String src = strategy.fetchSourceBySpecificName(conn, "SQL123", "DB2INST1");

        assertNotNull(src);
        assertTrue(src.contains("CREATE PROCEDURE"));
        verify(ps).setString(1, "SQL123");
        verify(ps).setString(2, "DB2INST1");
    }

    @Test
    void db2_fetchSourceBySpecificName_blank_returnsNull() throws Exception {
        Db2SourceFetchStrategy strategy = new Db2SourceFetchStrategy();
        assertNull(strategy.fetchSourceBySpecificName(conn, "  ", "DB2INST1"));
        verifyNoInteractions(conn);
    }

    // ── DB2 modules (task #28) ────────────────────────────────────────────────────────────────

    @Test
    void db2_listObjects_carriesModuleName() throws Exception {
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getString("ROUTINENAME")).thenReturn("NET_PRICE");
        when(rs.getString("ROUTINETYPE")).thenReturn("F");
        when(rs.getString("ROUTINESCHEMA")).thenReturn("DB2INST1");
        when(rs.getString("SPECIFICNAME")).thenReturn("SQL123");
        when(rs.getString("ROUTINEMODULENAME")).thenReturn("ERP_MOD_PRICING");

        List<SourceFetchStrategy.ObjectRef> refs =
                new Db2SourceFetchStrategy().listObjects(conn, "DB2INST1");

        assertEquals(1, refs.size());
        assertEquals("ERP_MOD_PRICING", refs.get(0).moduleName,
                "module member's owning module (ROUTINEMODULENAME) is exposed for MODULE.SUB keying");
    }

    @Test
    void db2_listObjects_nonModuleRoutine_hasNullModuleName() throws Exception {
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getString("ROUTINENAME")).thenReturn("GET_ORDERS");
        when(rs.getString("ROUTINETYPE")).thenReturn("P");
        when(rs.getString("ROUTINESCHEMA")).thenReturn("DB2INST1");
        when(rs.getString("SPECIFICNAME")).thenReturn("SQL999");
        when(rs.getString("ROUTINEMODULENAME")).thenReturn(null);

        List<SourceFetchStrategy.ObjectRef> refs =
                new Db2SourceFetchStrategy().listObjects(conn, "DB2INST1");

        assertEquals(1, refs.size());
        assertNull(refs.get(0).moduleName, "non-module routine keeps a null module slot");
    }
}
