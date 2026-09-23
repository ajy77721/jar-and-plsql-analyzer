package com.plsql.parser.multidb;

import com.plsqlanalyzer.config.DbType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Verifies SourceFetchStrategy implementations against mocked JDBC objects.
 * No real database connection needed.
 */
@ExtendWith(MockitoExtension.class)
class SourceFetchStrategyMockTest {

    @Mock Connection conn;
    @Mock PreparedStatement ps;
    @Mock Statement st;
    @Mock ResultSet rs;

    // ── MySqlSourceFetchStrategy ─────────────────────────────────────────────

    @Test
    void mysql_fetchSource_returnsFullDDL() throws Exception {
        // fetchSource now uses SHOW CREATE PROCEDURE (Statement, not PreparedStatement)
        when(conn.createStatement()).thenReturn(st);
        when(st.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(false);
        String fullDdl = "CREATE DEFINER=`root`@`localhost` PROCEDURE `get_orders`(IN p_id INT)\nBEGIN\n  SELECT * FROM orders WHERE user_id = p_id;\nEND";
        when(rs.getString(3)).thenReturn(fullDdl);

        MySqlSourceFetchStrategy strategy = new MySqlSourceFetchStrategy();
        String src = strategy.fetchSource(conn, "get_orders", "PROCEDURE", "mydb");

        assertNotNull(src);
        assertTrue(src.contains("get_orders"), "Should contain proc name");
        assertTrue(src.contains("SELECT"), "Should contain body SQL");
    }

    @Test
    void mysql_fetchSource_noRows_returnsNull() throws Exception {
        when(conn.createStatement()).thenReturn(st);
        when(st.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(false); // no rows from SHOW CREATE
        // fetchSource then falls back to an information_schema.ROUTINES lookup to locate the
        // routine in another database (cross-DB calls). Stub that path too, also returning no
        // rows, so the method exhausts both strategies and returns null.
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);

        MySqlSourceFetchStrategy strategy = new MySqlSourceFetchStrategy();
        String src = strategy.fetchSource(conn, "nonexistent", "PROCEDURE", "mydb");

        assertNull(src);
    }

    @Test
    void mysql_listObjects_returnsObjectRefs() throws Exception {
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(rs.getString("ROUTINE_NAME")).thenReturn("proc1", "func1");
        when(rs.getString("ROUTINE_TYPE")).thenReturn("PROCEDURE", "FUNCTION");
        when(rs.getString("ROUTINE_SCHEMA")).thenReturn("mydb", "mydb");

        MySqlSourceFetchStrategy strategy = new MySqlSourceFetchStrategy();
        List<SourceFetchStrategy.ObjectRef> refs = strategy.listObjects(conn, "mydb");

        assertNotNull(refs);
        assertEquals(2, refs.size());
        assertTrue(refs.stream().anyMatch(r -> "proc1".equals(r.name) && "PROCEDURE".equals(r.type)));
        assertTrue(refs.stream().anyMatch(r -> "func1".equals(r.name) && "FUNCTION".equals(r.type)));
    }

    @Test
    void mysql_listObjects_noSchema_returnsAll() throws Exception {
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        MySqlSourceFetchStrategy strategy = new MySqlSourceFetchStrategy();
        List<SourceFetchStrategy.ObjectRef> refs = strategy.listObjects(conn, null);

        assertNotNull(refs);
        assertTrue(refs.isEmpty());
    }

    // ── SqlServerSourceFetchStrategy ─────────────────────────────────────────

    @Test
    void sqlserver_fetchSource_returnsDefinition() throws Exception {
        String definition = "CREATE PROCEDURE dbo.usp_test AS BEGIN SELECT 1; END";
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getString("definition")).thenReturn(definition);

        SqlServerSourceFetchStrategy strategy = new SqlServerSourceFetchStrategy();
        String src = strategy.fetchSource(conn, "usp_test", "PROCEDURE", "dbo");

        assertNotNull(src);
        assertTrue(src.contains("usp_test") || src.contains("SELECT 1"),
                "Should contain proc content, got: " + src);
    }

    @Test
    void sqlserver_fetchSource_notFound_returnsNull() throws Exception {
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        SqlServerSourceFetchStrategy strategy = new SqlServerSourceFetchStrategy();
        String src = strategy.fetchSource(conn, "nonexistent", "PROCEDURE", "dbo");

        assertNull(src);
    }

    @Test
    void sqlserver_listObjects_returnsObjectRefs() throws Exception {
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(rs.getString("name")).thenReturn("usp_orders", "fn_calc");
        when(rs.getString("type_desc")).thenReturn("SQL_STORED_PROCEDURE", "SQL_SCALAR_FUNCTION");
        when(rs.getString("schema_name")).thenReturn("dbo", "dbo");

        SqlServerSourceFetchStrategy strategy = new SqlServerSourceFetchStrategy();
        List<SourceFetchStrategy.ObjectRef> refs = strategy.listObjects(conn, "dbo");

        assertNotNull(refs);
        assertEquals(2, refs.size());
        assertTrue(refs.stream().anyMatch(r -> "usp_orders".equals(r.name)));
        assertTrue(refs.stream().anyMatch(r -> "fn_calc".equals(r.name)));
    }

    @Test
    void sqlserver_listObjects_typeMapping_P_toPROCEDURE() throws Exception {
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getString("name")).thenReturn("usp_test");
        when(rs.getString("type_desc")).thenReturn("SQL_STORED_PROCEDURE");
        when(rs.getString("schema_name")).thenReturn("dbo");

        SqlServerSourceFetchStrategy strategy = new SqlServerSourceFetchStrategy();
        List<SourceFetchStrategy.ObjectRef> refs = strategy.listObjects(conn, null);

        assertNotNull(refs);
        assertEquals(1, refs.size());
        assertEquals("PROCEDURE", refs.get(0).type);
    }

    @Test
    void sqlserver_listObjects_typeMapping_FN_toFUNCTION() throws Exception {
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getString("name")).thenReturn("fn_calc");
        when(rs.getString("type_desc")).thenReturn("SQL_SCALAR_FUNCTION");
        when(rs.getString("schema_name")).thenReturn("dbo");

        SqlServerSourceFetchStrategy strategy = new SqlServerSourceFetchStrategy();
        List<SourceFetchStrategy.ObjectRef> refs = strategy.listObjects(conn, null);

        assertEquals("FUNCTION", refs.get(0).type);
    }

    // ── SourceFetchStrategyFactory ───────────────────────────────────────────

    @Test
    void factory_mysql_returnsMysqlStrategy() {
        SourceFetchStrategy s = SourceFetchStrategyFactory.get(DbType.MYSQL);
        assertInstanceOf(MySqlSourceFetchStrategy.class, s);
    }

    @Test
    void factory_sqlserver_returnsSqlServerStrategy() {
        SourceFetchStrategy s = SourceFetchStrategyFactory.get(DbType.SQLSERVER);
        assertInstanceOf(SqlServerSourceFetchStrategy.class, s);
    }

    @Test
    void factory_oracle_returnsOracleStrategy() {
        SourceFetchStrategy s = SourceFetchStrategyFactory.get(DbType.ORACLE);
        assertInstanceOf(OracleSourceFetchStrategy.class, s);
    }

    @Test
    void factory_null_returnsOracleStrategy() {
        SourceFetchStrategy s = SourceFetchStrategyFactory.get((DbType) null);
        assertInstanceOf(OracleSourceFetchStrategy.class, s);
    }

    @Test
    void factory_stringLookup_mysql() {
        SourceFetchStrategy s = SourceFetchStrategyFactory.get("MYSQL");
        assertInstanceOf(MySqlSourceFetchStrategy.class, s);
    }

    @Test
    void factory_stringLookup_mssql() {
        SourceFetchStrategy s = SourceFetchStrategyFactory.get("MSSQL");
        assertInstanceOf(SqlServerSourceFetchStrategy.class, s);
    }

    // ── StoredProcedureParserFactory ─────────────────────────────────────────

    @Test
    void parserFactory_mysql_returnsMysqlParser() {
        StoredProcedureParser p = StoredProcedureParserFactory.get(DbType.MYSQL);
        assertInstanceOf(MySqlProcedureParser.class, p);
    }

    @Test
    void parserFactory_sqlserver_returnsSqlServerParser() {
        StoredProcedureParser p = StoredProcedureParserFactory.get(DbType.SQLSERVER);
        assertInstanceOf(SqlServerProcedureParser.class, p);
    }

    @Test
    void parserFactory_oracle_returnsNull() {
        // Oracle uses PlSqlParserEngine directly
        assertNull(StoredProcedureParserFactory.get(DbType.ORACLE));
    }

    @Test
    void parserFactory_null_returnsNull() {
        assertNull(StoredProcedureParserFactory.get((DbType) null));
    }

    @Test
    void parserFactory_supportsDbType_mysql() {
        assertTrue(StoredProcedureParserFactory.supportsDbType(DbType.MYSQL));
    }

    @Test
    void parserFactory_supportsDbType_sqlserver() {
        assertTrue(StoredProcedureParserFactory.supportsDbType(DbType.SQLSERVER));
    }

    @Test
    void parserFactory_supportsDbType_oracle_false() {
        assertFalse(StoredProcedureParserFactory.supportsDbType(DbType.ORACLE));
    }
}
