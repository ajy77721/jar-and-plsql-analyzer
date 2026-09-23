package com.plsqlanalyzer.web.browse;

import com.plsqlanalyzer.web.service.DbSourceFetcher;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Oracle provider — a thin, pluggable adapter that DELEGATES to the existing
 * {@link DbSourceFetcher} (ALL_OBJECTS / ALL_SOURCE) so Oracle behaviour is
 * byte-identical to the historical code path. No Oracle SQL is rewritten here.
 *
 * <p>Note: {@code DatabaseController} continues to serve Oracle requests through
 * its original inline code path to guarantee zero behavioural change; this class
 * is the factory-registered Oracle implementation and reuses the very same
 * fetcher SQL for the object/source operations. Table metadata for Oracle is
 * served by the controller's existing dictionary queries and is therefore not
 * duplicated here.</p>
 */
public class OracleBrowseProvider implements DbBrowseProvider {

    /** Types tried, in priority order, when fetching a source object by name. */
    private static final List<String> SOURCE_TYPES =
            List.of("PACKAGE BODY", "PACKAGE", "PROCEDURE", "FUNCTION", "TRIGGER");

    private final DbSourceFetcher fetcher;

    public OracleBrowseProvider(DbSourceFetcher fetcher) {
        this.fetcher = fetcher;
    }

    @Override
    public List<Map<String, Object>> listObjects(Connection c, String schema) throws Exception {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, String> row : fetcher.listObjects(c, schema)) {
            out.add(new LinkedHashMap<>(row));
        }
        return out;
    }

    @Override
    public String fetchSource(Connection c, String schema, String objectName) throws Exception {
        for (String type : SOURCE_TYPES) {
            String src = fetcher.fetchSource(c, schema, objectName, type);
            if (src != null && !src.isBlank()) return src;
        }
        return null;
    }

    @Override
    public Map<String, Object> tableInfo(Connection c, String schema, String tableName) {
        // Oracle table metadata is served by DatabaseController's existing inline
        // dictionary queries (ALL_TAB_COLUMNS etc.) to keep behaviour identical.
        throw new UnsupportedOperationException(
                "Oracle tableInfo is served by the controller's inline dictionary path");
    }
}
