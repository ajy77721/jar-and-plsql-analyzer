package com.plsql.parser.multidb;

import com.plsqlanalyzer.config.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that each DbDialect builds the correct JDBC URL
 * and returns the expected driver class / default port.
 */
class DbDialectTest {

    // ── Oracle ───────────────────────────────────────────────────────────────

    @Test
    void oracle_serviceNameUrl() {
        ConnectionConfig c = conn("ORACLE", "db.company.com", 1521, "ORCL", null, "SERVICE_NAME");
        assertEquals("jdbc:oracle:thin:@//db.company.com:1521/ORCL", c.getJdbcUrl());
    }

    @Test
    void oracle_sidUrl() {
        ConnectionConfig c = conn("ORACLE", "db.company.com", 1521, null, "MYSID", "SID");
        assertEquals("jdbc:oracle:thin:@db.company.com:1521:MYSID", c.getJdbcUrl());
    }

    @Test
    void oracle_tnsUrl() {
        ConnectionConfig c = new ConnectionConfig();
        c.setDbType("ORACLE");
        c.setConnectionType("TNS");
        c.setTnsAlias("MY_TNS_ALIAS");
        assertEquals("jdbc:oracle:thin:@MY_TNS_ALIAS", c.getJdbcUrl());
    }

    @Test
    void oracle_customUrl() {
        ConnectionConfig c = new ConnectionConfig();
        c.setDbType("ORACLE");
        c.setConnectionType("CUSTOM");
        c.setCustomUrl("jdbc:oracle:thin:@ldap://ldap.company.com/ORCL");
        assertEquals("jdbc:oracle:thin:@ldap://ldap.company.com/ORCL", c.getJdbcUrl());
    }

    @Test
    void oracle_nullHostReturnsNull() {
        ConnectionConfig c = new ConnectionConfig();
        c.setDbType("ORACLE");
        c.setConnectionType("SERVICE_NAME");
        c.setServiceName("ORCL");
        // hostname not set → should return null
        assertNull(c.getJdbcUrl());
    }

    @Test
    void oracle_defaultPort() {
        assertEquals(1521, new OracleDialect().getDefaultPort());
    }

    @Test
    void oracle_driverClass() {
        assertEquals("oracle.jdbc.OracleDriver", new OracleDialect().getDriverClass());
    }

    // ── MySQL ────────────────────────────────────────────────────────────────

    @Test
    void mysql_standardUrl() {
        ConnectionConfig c = conn("MYSQL", "mysql.company.com", 3306, "myapp_db", null, "SERVICE_NAME");
        String url = c.getJdbcUrl();
        assertNotNull(url);
        assertTrue(url.startsWith("jdbc:mysql://mysql.company.com:3306/myapp_db"),
                "Expected jdbc:mysql:// prefix, got: " + url);
        assertTrue(url.contains("useSSL=false"), "Should contain useSSL=false");
    }

    @Test
    void mysql_customUrl_takesPreference() {
        ConnectionConfig c = new ConnectionConfig();
        c.setDbType("MYSQL");
        c.setCustomUrl("jdbc:mysql://host:3306/db?useSSL=true&rewriteBatchedStatements=true");
        assertEquals("jdbc:mysql://host:3306/db?useSSL=true&rewriteBatchedStatements=true", c.getJdbcUrl());
    }

    @Test
    void mysql_nullHostReturnsNull() {
        ConnectionConfig c = new ConnectionConfig();
        c.setDbType("MYSQL");
        c.setServiceName("mydb");
        assertNull(c.getJdbcUrl());
    }

    @Test
    void mysql_defaultPort() {
        assertEquals(3306, new MySqlDialect().getDefaultPort());
    }

    @Test
    void mysql_driverClass() {
        assertEquals("com.mysql.cj.jdbc.Driver", new MySqlDialect().getDriverClass());
    }

    // ── SQL Server ───────────────────────────────────────────────────────────

    @Test
    void sqlserver_standardUrl() {
        ConnectionConfig c = conn("SQLSERVER", "sql.company.com", 1433, "SalesDB", null, "SERVICE_NAME");
        String url = c.getJdbcUrl();
        assertNotNull(url);
        assertTrue(url.startsWith("jdbc:sqlserver://sql.company.com:1433"),
                "Expected jdbc:sqlserver:// prefix, got: " + url);
        assertTrue(url.contains("databaseName=SalesDB"), "Should contain databaseName");
        assertTrue(url.contains("encrypt=false"), "Should contain encrypt=false");
    }

    @Test
    void sqlserver_namedInstance() {
        ConnectionConfig c = conn("SQLSERVER", "sql.company.com", 1433, "SalesDB", "SQLEXPRESS", "SERVICE_NAME");
        String url = c.getJdbcUrl();
        assertTrue(url.contains("instanceName=SQLEXPRESS"), "Should contain instanceName");
    }

    @Test
    void sqlserver_windowsAuth() {
        ConnectionConfig c = new ConnectionConfig();
        c.setDbType("SQLSERVER");
        c.setHostname("sql.company.com");
        c.setPort(1433);
        c.setServiceName("SalesDB");
        c.setRole("WINDOWS");
        String url = c.getJdbcUrl();
        assertTrue(url.contains("integratedSecurity=true"), "Windows auth should add integratedSecurity=true");
    }

    @Test
    void sqlserver_customUrl_takesPreference() {
        ConnectionConfig c = new ConnectionConfig();
        c.setDbType("SQLSERVER");
        c.setCustomUrl("jdbc:sqlserver://host:1433;databaseName=DB;encrypt=true");
        assertEquals("jdbc:sqlserver://host:1433;databaseName=DB;encrypt=true", c.getJdbcUrl());
    }

    @Test
    void sqlserver_nullHostReturnsNull() {
        ConnectionConfig c = new ConnectionConfig();
        c.setDbType("SQLSERVER");
        c.setServiceName("DB");
        assertNull(c.getJdbcUrl());
    }

    @Test
    void sqlserver_defaultPort() {
        assertEquals(1433, new SqlServerDialect().getDefaultPort());
    }

    @Test
    void sqlserver_driverClass() {
        assertEquals("com.microsoft.sqlserver.jdbc.SQLServerDriver", new SqlServerDialect().getDriverClass());
    }

    // ── DbType.fromString ────────────────────────────────────────────────────

    @Test
    void dbType_fromString_oracle() {
        assertEquals(DbType.ORACLE,    DbType.fromString("ORACLE"));
        assertEquals(DbType.ORACLE,    DbType.fromString(null));
        assertEquals(DbType.ORACLE,    DbType.fromString(""));
        assertEquals(DbType.ORACLE,    DbType.fromString("oracle"));
    }

    @Test
    void dbType_fromString_mysql() {
        assertEquals(DbType.MYSQL,     DbType.fromString("MYSQL"));
        assertEquals(DbType.MYSQL,     DbType.fromString("mysql"));
    }

    @Test
    void dbType_fromString_sqlserver() {
        assertEquals(DbType.SQLSERVER, DbType.fromString("SQLSERVER"));
        assertEquals(DbType.SQLSERVER, DbType.fromString("MSSQL"));
        assertEquals(DbType.SQLSERVER, DbType.fromString("SQL_SERVER"));
        assertEquals(DbType.SQLSERVER, DbType.fromString("SQL SERVER"));
    }

    // ── DbDialectRegistry ────────────────────────────────────────────────────

    @Test
    void registry_returnsCorrectDialects() {
        assertInstanceOf(OracleDialect.class,    DbDialectRegistry.get(DbType.ORACLE));
        assertInstanceOf(MySqlDialect.class,     DbDialectRegistry.get(DbType.MYSQL));
        assertInstanceOf(SqlServerDialect.class, DbDialectRegistry.get(DbType.SQLSERVER));
        assertInstanceOf(OracleDialect.class,    DbDialectRegistry.get((DbType) null));
    }

    // ── ConnectionConfig backward-compat (no dbType = Oracle) ────────────────

    @Test
    void connectionConfig_defaultDbTypeIsOracle() {
        ConnectionConfig c = new ConnectionConfig();
        assertEquals("ORACLE", c.getDbType());
        assertInstanceOf(OracleDialect.class, c.dialect());
    }

    // ── Helper ───────────────────────────────────────────────────────────────

    private ConnectionConfig conn(String dbType, String host, int port, String service, String sid, String connType) {
        ConnectionConfig c = new ConnectionConfig();
        c.setDbType(dbType);
        c.setHostname(host);
        c.setPort(port);
        c.setServiceName(service);
        c.setSid(sid);
        c.setConnectionType(connType);
        return c;
    }
}
