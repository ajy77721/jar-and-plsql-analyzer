package com.plsqlanalyzer.config;

import java.nio.file.Path;

public class OracleDialect implements DbDialect {

    @Override
    public String buildJdbcUrl(ConnectionConfig conn) {
        return buildJdbcUrl(conn, null);
    }

    /**
     * Oracle TLS is expressed by switching the thin URL to the TCPS protocol, which requires
     * the full {@code (DESCRIPTION=...)} form. CUSTOM / LDAP URLs (and TNS aliases, which carry
     * no host/port to rewrite) are returned untouched. Wallet and truststore material is not a
     * URL concern and is supplied separately via
     * {@link ConnectionConfig#buildSslProperties(java.nio.file.Path)}.
     */
    @Override
    public String buildJdbcUrl(ConnectionConfig conn, Path configDir) {
        String type = conn.getConnectionType() != null
                ? conn.getConnectionType().toUpperCase() : "SERVICE_NAME";
        switch (type) {
            case "SID":
                if (conn.getHostname() == null || conn.getSid() == null) return null;
                if (configDir != null && conn.isTlsEnabled()) {
                    StringBuilder tcps = new StringBuilder("jdbc:oracle:thin:@");
                    appendTls(conn, tcps, configDir);
                    return tcps.toString();
                }
                return "jdbc:oracle:thin:@" + conn.getHostname()
                        + ":" + conn.getPort() + ":" + conn.getSid();
            case "TNS":
                if (conn.getTnsAlias() == null) return null;
                return "jdbc:oracle:thin:@" + conn.getTnsAlias();
            case "CUSTOM":
            case "LDAP":
                return conn.getCustomUrl();
            default: // SERVICE_NAME
                if (conn.getHostname() == null || conn.getServiceName() == null) return null;
                if (configDir != null && conn.isTlsEnabled()) {
                    StringBuilder tcps = new StringBuilder("jdbc:oracle:thin:@");
                    appendTls(conn, tcps, configDir);
                    return tcps.toString();
                }
                return "jdbc:oracle:thin:@//" + conn.getHostname()
                        + ":" + conn.getPort() + "/" + conn.getServiceName();
        }
    }

    /**
     * Appends the TCPS {@code (DESCRIPTION=...)} descriptor for SERVICE_NAME and SID
     * connections. TNS / CUSTOM / LDAP connections have no host information to encrypt here
     * and are left untouched. The caller is responsible for having emitted the
     * {@code jdbc:oracle:thin:@} prefix.
     */
    @Override
    public void appendTls(ConnectionConfig c, StringBuilder url, Path configDir) {
        String type = c.getConnectionType() != null
                ? c.getConnectionType().toUpperCase() : "SERVICE_NAME";
        if ("SID".equals(type)) {
            if (c.getHostname() == null || c.getSid() == null) return;
            url.append("(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=").append(c.getHostname())
               .append(")(PORT=").append(c.getEffectivePort())
               .append("))(CONNECT_DATA=(SID=").append(c.getSid()).append(")))");
        } else if ("SERVICE_NAME".equals(type)) {
            if (c.getHostname() == null || c.getServiceName() == null) return;
            url.append("(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=").append(c.getHostname())
               .append(")(PORT=").append(c.getEffectivePort())
               .append("))(CONNECT_DATA=(SERVICE_NAME=").append(c.getServiceName()).append(")))");
        }
        // TNS / CUSTOM / LDAP: no host descriptor to rewrite — TLS is not applied in the URL.
    }

    @Override public int    getDefaultPort()  { return 1521; }
    @Override public String getDriverClass()  { return "oracle.jdbc.OracleDriver"; }
    @Override public String getDisplayName()  { return "Oracle"; }
}
