package com.plsqlanalyzer.config;

public class OracleDialect implements DbDialect {

    @Override
    public String buildJdbcUrl(ConnectionConfig conn) {
        String type = conn.getConnectionType() != null
                ? conn.getConnectionType().toUpperCase() : "SERVICE_NAME";
        switch (type) {
            case "SID":
                if (conn.getHostname() == null || conn.getSid() == null) return null;
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
                return "jdbc:oracle:thin:@//" + conn.getHostname()
                        + ":" + conn.getPort() + "/" + conn.getServiceName();
        }
    }

    @Override public int    getDefaultPort()  { return 1521; }
    @Override public String getDriverClass()  { return "oracle.jdbc.OracleDriver"; }
    @Override public String getDisplayName()  { return "Oracle"; }
}
