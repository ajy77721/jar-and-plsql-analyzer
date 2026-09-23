package com.plsqlanalyzer.web.browse;

import com.plsqlanalyzer.web.service.DbSourceFetcher;
import com.plsqlanalyzer.web.util.DbDialectSupport;

/**
 * Resolves the {@link DbBrowseProvider} for a given dialect string
 * (as produced by {@link DbDialectSupport#dialectFromJdbcUrl}).
 *
 * <p>All five supported dialects (Oracle, SQL Server, MySQL, DB2, Postgres) have a provider;
 * returns {@code null} only for an unrecognized/{@code UNKNOWN} dialect, so the controller
 * falls back to the Phase-1 "not yet supported" response.</p>
 */
public final class DbBrowseProviderFactory {

    private DbBrowseProviderFactory() {
    }

    /**
     * @param dialect ORACLE / SQLSERVER / MYSQL / DB2 / POSTGRES / UNKNOWN
     * @param fetcher existing Oracle fetcher (only used by the Oracle provider)
     * @return a provider, or {@code null} when the dialect is unrecognized
     */
    public static DbBrowseProvider get(String dialect, DbSourceFetcher fetcher) {
        if (dialect == null) return null;
        switch (dialect) {
            case DbDialectSupport.ORACLE:
                return new OracleBrowseProvider(fetcher);
            case DbDialectSupport.SQLSERVER:
                return new SqlServerBrowseProvider();
            case DbDialectSupport.MYSQL:
                return new MySqlBrowseProvider();
            case DbDialectSupport.DB2:
                return new Db2BrowseProvider();
            case DbDialectSupport.POSTGRES:
                return new PostgresBrowseProvider();
            default:
                return null; // UNKNOWN -> Phase-1 unsupported fallback
        }
    }
}
