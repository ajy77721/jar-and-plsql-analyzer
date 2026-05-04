package com.plsql.parser.flow;

import java.io.IOException;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Downloads PL/SQL source code from the database using ALL_SOURCE.
 * Checks local file cache first, then falls back to DB.
 */
public class SourceDownloader {

    private final DbConnectionManager connManager;

    // Cache: "TYPE:NAME" -> source text
    private final Map<String, String> sourceCache = new ConcurrentHashMap<>();

    // Cache: "NAME" -> owner schema
    private final Map<String, String> ownerCache = new ConcurrentHashMap<>();

    // Local file cache directory (sources/ folder from build output)
    private Path localCacheDir;

    public SourceDownloader(DbConnectionManager connManager) {
        this.connManager = connManager;
    }

    public DbConnectionManager getConnManager() {
        return connManager;
    }

    public void setLocalCacheDir(Path dir) {
        this.localCacheDir = dir;
    }

    /**
     * Download source for a given object name and type.
     * Searches across all configured schemas.
     * Returns the source text with CREATE OR REPLACE prefix, or null if not found.
     */
    public String downloadSource(String objectName, String objectType) {
        String key = objectType.toUpperCase() + ":" + objectName.toUpperCase();
        String cached = sourceCache.get(key);
        if (cached != null) {
            return cached;
        }

        // Check local file cache first
        String localSource = checkLocalCache(objectName.toUpperCase(), objectType.toUpperCase());
        if (localSource != null) {
            sourceCache.put(key, localSource);
            System.err.println("[SourceDownloader] Loaded from local cache: "
                    + objectType + " " + objectName);
            return localSource;
        }

        if (connManager == null) {
            return null;
        }

        try {
            Connection conn = connManager.getAnyConnection();
            String source = querySource(conn, objectName.toUpperCase(), objectType.toUpperCase());
            if (source != null) {
                sourceCache.put(key, source);
            }
            return source;
        } catch (SQLException e) {
            System.err.println("[SourceDownloader] Error downloading " + objectType + " "
                    + objectName + ": " + e.getMessage());
            return null;
        }
    }

    private String checkLocalCache(String objectName, String objectType) {
        if (localCacheDir == null || !Files.isDirectory(localCacheDir)) return null;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(localCacheDir)) {
            String ext = typeToExtension(objectType);
            for (Path file : stream) {
                String fn = file.getFileName().toString().toUpperCase();
                // Match patterns: SCHEMA.NAME.ext or NAME.ext (exact segment match)
                String exactSuffix = objectName + "." + ext.toUpperCase();
                if (fn.equals(exactSuffix) || fn.endsWith("." + exactSuffix)) {
                    String content = Files.readString(file);
                    if (content != null && !content.isBlank()) {
                        // Extract owner schema from SCHEMA.NAME.ext filename
                        if (fn.endsWith("." + exactSuffix) && fn.length() > exactSuffix.length() + 1) {
                            String schemaPrefix = fn.substring(0, fn.length() - exactSuffix.length() - 1);
                            if (!schemaPrefix.isEmpty()) {
                                ownerCache.put(objectName, schemaPrefix);
                            }
                        }
                        return content;
                    }
                }
            }
        } catch (IOException e) {
            // Fall through to DB
        }
        return null;
    }

    private String typeToExtension(String objectType) {
        switch (objectType) {
            case "PACKAGE BODY": return "pkb";
            case "PACKAGE": return "pks";
            case "PROCEDURE": return "prc";
            case "FUNCTION": return "fnc";
            default: return "sql";
        }
    }

    /**
     * Download PACKAGE BODY source.
     */
    public String downloadPackageBody(String packageName) {
        return downloadSource(packageName, "PACKAGE BODY");
    }

    /**
     * Download PACKAGE (spec) source.
     */
    public String downloadPackageSpec(String packageName) {
        return downloadSource(packageName, "PACKAGE");
    }

    /**
     * Download a standalone PROCEDURE.
     */
    public String downloadProcedure(String procName) {
        return downloadSource(procName, "PROCEDURE");
    }

    /**
     * Download a standalone FUNCTION.
     */
    public String downloadFunction(String funcName) {
        return downloadSource(funcName, "FUNCTION");
    }

    /**
     * Try to download source, trying multiple object types.
     * Tries PACKAGE BODY first, then PROCEDURE, then FUNCTION.
     * Returns the source or null.
     */
    public String downloadAny(String objectName) {
        // Try PACKAGE BODY first (most common for package.subprogram calls)
        String source = downloadPackageBody(objectName);
        if (source != null) return source;

        // Try standalone PROCEDURE
        source = downloadProcedure(objectName);
        if (source != null) return source;

        // Try standalone FUNCTION
        source = downloadFunction(objectName);
        if (source != null) return source;

        // Try PACKAGE spec (less useful but might be needed)
        source = downloadPackageSpec(objectName);
        return source;
    }

    /**
     * Get the owner schema for a previously downloaded source.
     */
    public String getOwner(String objectName) {
        return ownerCache.get(objectName.toUpperCase());
    }

    /**
     * Query ALL_SOURCE for the given object. Searches across all schemas.
     */
    private String querySource(Connection conn, String objectName, String objectType)
            throws SQLException {
        String sql = "SELECT OWNER, LINE, TEXT FROM ALL_SOURCE "
                + "WHERE NAME = ? AND TYPE = ? "
                + "AND OWNER IN (" + buildOwnerInClause() + ") "
                + "ORDER BY OWNER, LINE";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, objectName);
            ps.setString(2, objectType);

            connManager.incrementDbCalls();
            try (ResultSet rs = ps.executeQuery()) {
                StringBuilder sb = new StringBuilder();
                String owner = null;
                boolean hasRows = false;

                while (rs.next()) {
                    String rowOwner = rs.getString("OWNER");
                    if (!hasRows) {
                        owner = rowOwner;
                        hasRows = true;
                        sb.append("CREATE OR REPLACE ");
                    } else if (!owner.equals(rowOwner)) {
                        break;
                    }
                    sb.append(rs.getString("TEXT"));
                }

                if (!hasRows) {
                    return null;
                }

                // Cache the owner
                ownerCache.put(objectName, owner);

                String source = sb.toString();
                // Detect wrapped/encrypted source
                if (source.contains(" wrapped") || source.contains("0000000")) {
                    String firstLines = source.length() > 200 ? source.substring(0, 200) : source;
                    if (firstLines.toUpperCase().contains("WRAPPED") ||
                            firstLines.matches("(?s).*\\b[0-9a-f]{8,}\\b.*")) {
                        wrappedObjects.add(objectName);
                        System.err.println("[SourceDownloader] WRAPPED/ENCRYPTED " + objectType + " "
                                + objectName + " from schema " + owner);
                        return source;
                    }
                }

                System.err.println("[SourceDownloader] Downloaded " + objectType + " "
                        + objectName + " from schema " + owner
                        + " (" + sb.length() + " chars)");
                return source;
            }
        }
    }

    /**
     * Build the IN clause for the configured schemas.
     */
    private String buildOwnerInClause() {
        List<String> schemas = connManager.getAvailableSchemas();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < schemas.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("'").append(schemas.get(i).replace("'", "''")).append("'");
        }
        return sb.toString();
    }

    /**
     * Check if a source has already been downloaded and cached.
     */
    public boolean isCached(String objectName, String objectType) {
        String key = objectType.toUpperCase() + ":" + objectName.toUpperCase();
        return sourceCache.containsKey(key);
    }

    /**
     * Get cached source for a given object, trying multiple types.
     */
    public String getCachedSource(String objectName) {
        String upper = objectName.toUpperCase();
        for (String type : new String[]{"PACKAGE BODY", "PROCEDURE", "FUNCTION", "PACKAGE"}) {
            String key = type + ":" + upper;
            String src = sourceCache.get(key);
            if (src != null) return src;
        }
        return null;
    }

    /**
     * Get the cached type for a given object.
     */
    public String getCachedType(String objectName) {
        String upper = objectName.toUpperCase();
        for (String type : new String[]{"PACKAGE BODY", "PROCEDURE", "FUNCTION", "PACKAGE"}) {
            String key = type + ":" + upper;
            if (sourceCache.containsKey(key)) return type;
        }
        return null;
    }

    private final Set<String> wrappedObjects = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public boolean isWrapped(String objectName) {
        return wrappedObjects.contains(objectName.toUpperCase());
    }

    /**
     * Clear all caches.
     */
    public void clearCache() {
        sourceCache.clear();
        ownerCache.clear();
        wrappedObjects.clear();
    }
}
