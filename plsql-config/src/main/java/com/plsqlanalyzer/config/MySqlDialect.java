package com.plsqlanalyzer.config;

public class MySqlDialect implements DbDialect {

    @Override
    public String buildJdbcUrl(ConnectionConfig conn) {
        if (conn.getCustomUrl() != null && !conn.getCustomUrl().isBlank()) {
            return conn.getCustomUrl();
        }
        if (conn.getHostname() == null) return null;
        int port = conn.getPort() > 0 ? conn.getPort() : 3306;
        StringBuilder url = new StringBuilder("jdbc:mysql://");
        url.append(conn.getHostname()).append(":").append(port);
        if (conn.getServiceName() != null && !conn.getServiceName().isBlank()) {
            url.append("/").append(conn.getServiceName());
        }
        url.append("?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&connectTimeout=10000");
        return url.toString();
    }

    @Override public int    getDefaultPort()  { return 3306; }
    @Override public String getDriverClass()  { return "com.mysql.cj.jdbc.Driver"; }
    @Override public String getDisplayName()  { return "MySQL"; }
}
