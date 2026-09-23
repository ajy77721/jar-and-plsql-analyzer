package com.plsqlanalyzer.config;

import java.nio.file.Path;

/**
 * Per-database SQL/URL dialect. Each dialect knows how to render a JDBC URL for a
 * {@link ConnectionConfig}, plus its default port / driver class / display name.
 *
 * <p>Two overloads exist for URL building. The single-argument
 * {@link #buildJdbcUrl(ConnectionConfig)} is the long-standing, TLS-free entry point and
 * MUST keep producing byte-identical URLs to the pre-TLS implementation (the parser
 * module's dialect tests assert exact strings). The two-argument overload carries the
 * configuration directory that relative TLS material paths are anchored at; when it is
 * {@code null} the call is explicitly "no TLS context available" and behaves exactly like
 * the single-argument form.
 */
public interface DbDialect {
    String buildJdbcUrl(ConnectionConfig conn);
    int getDefaultPort();
    String getDriverClass();
    String getDisplayName();

    /**
     * TLS-aware URL builder. The default delegates to the legacy single-argument form, i.e.
     * produces a URL with no TLS parameters; dialects that can express TLS in the URL
     * (MySQL, SQL Server, PostgreSQL, Oracle) override this and call
     * {@link #appendTls(ConnectionConfig, StringBuilder, Path)} when TLS is enabled.
     *
     * @param configDir directory that relative trust/client cert paths resolve against, or
     *                  {@code null} to build a plain, TLS-free URL
     */
    default String buildJdbcUrl(ConnectionConfig conn, Path configDir) {
        return buildJdbcUrl(conn);
    }

    /**
     * Appends/enables the TLS portion of a JDBC URL. Called by a dialect's two-argument
     * {@code buildJdbcUrl} only when {@link ConnectionConfig#isTlsEnabled()} is true and a
     * non-null {@code configDir} was supplied. Dialects whose TLS knobs live entirely in
     * driver {@link java.util.Properties} (DB2) leave the URL untouched.
     *
     * @param c         the connection being rendered
     * @param url       the URL under construction; implementations append to it
     * @param configDir directory relative certificate paths resolve against
     */
    default void appendTls(ConnectionConfig c, StringBuilder url, Path configDir) {
        // No TLS URL parameters for this dialect by default.
    }

    /**
     * Appends a {@code key=value} pair to a query-string style URL, choosing {@code &} when a
     * {@code ?} is already present and {@code ?} otherwise. Null values are ignored so callers
     * can append optional parameters unconditionally.
     */
    static void appendQueryParam(StringBuilder url, String key, String value) {
        if (key == null || value == null) {
            return;
        }
        url.append(url.indexOf("?") >= 0 ? '&' : '?').append(key).append('=').append(value);
    }

    /**
     * Appends a {@code ;key=value} pair to a semicolon-separated URL (the SQL Server style).
     * Null values are ignored.
     */
    static void appendSemiParam(StringBuilder url, String key, String value) {
        if (key == null || value == null) {
            return;
        }
        url.append(';').append(key).append('=').append(value);
    }
}
