package com.plsqlanalyzer.config;

public class SqlServerDialect implements DbDialect {

    @Override
    public String buildJdbcUrl(ConnectionConfig conn) {
        if (conn.getCustomUrl() != null && !conn.getCustomUrl().isBlank()) {
            return conn.getCustomUrl();
        }
        if (conn.getHostname() == null) return null;
        int port = conn.getPort() > 0 ? conn.getPort() : 1433;
        StringBuilder url = new StringBuilder("jdbc:sqlserver://");
        url.append(conn.getHostname()).append(":").append(port);
        url.append(";encrypt=false;trustServerCertificate=true;loginTimeout=10");
        if (conn.getServiceName() != null && !conn.getServiceName().isBlank()) {
            url.append(";databaseName=").append(conn.getServiceName());
        }
        // sid field reused for named instance (e.g. SQLEXPRESS)
        if (conn.getSid() != null && !conn.getSid().isBlank()) {
            url.append(";instanceName=").append(conn.getSid());
        }
        // role=WINDOWS triggers integrated security
        if ("WINDOWS".equalsIgnoreCase(conn.getRole())) {
            url.append(";integratedSecurity=true");
        }
        return url.toString();
    }

    @Override public int    getDefaultPort()  { return 1433; }
    @Override public String getDriverClass()  { return "com.microsoft.sqlserver.jdbc.SQLServerDriver"; }
    @Override public String getDisplayName()  { return "SQL Server"; }
}
