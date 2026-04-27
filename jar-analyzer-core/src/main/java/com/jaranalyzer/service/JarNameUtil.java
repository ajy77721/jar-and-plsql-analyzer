package com.jaranalyzer.service;

/**
 * Centralized JAR name normalization.
 * <p>
 * Throughout the system, JARs are referenced by two forms:
 * <ul>
 *   <li>Original filename: {@code core-accounts-0.0.1-SNAPSHOT.jar}</li>
 *   <li>URL / filesystem id: {@code core-accounts-0.0.1-SNAPSHOT} (no .jar)</li>
 * </ul>
 * Services that store data keyed by JAR name (tracker, corrections, fragments,
 * sessions) must use a canonical key so lookups work regardless of which form
 * the caller passes.
 * <p>
 * {@link #normalizeKey(String)} strips the {@code .jar} suffix and removes
 * unsafe filesystem characters, producing the same key from either form.
 */
public final class JarNameUtil {

    private JarNameUtil() {}

    /**
     * Produce a canonical key from any JAR name variant.
     * <ol>
     *   <li>Strip trailing {@code .jar} (case-insensitive)</li>
     *   <li>Replace non-safe characters with underscore</li>
     * </ol>
     * Safe characters: {@code a-zA-Z0-9._-}
     *
     * @param name original jarName or sanitized URL id
     * @return canonical key, never null
     */
    public static String normalizeKey(String name) {
        if (name == null || name.isBlank()) return "";
        String key = name.strip();
        if (key.toLowerCase().endsWith(".jar")) {
            key = key.substring(0, key.length() - 4);
        }
        return key.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    /**
     * Sanitize a string for safe filesystem use (does NOT strip .jar).
     * Use this only for filenames where the .jar extension is meaningful
     * (e.g. stored JAR copies).  For keying maps / directories, use
     * {@link #normalizeKey(String)} instead.
     */
    public static String sanitize(String name) {
        if (name == null) return "";
        return name.replaceAll("[^a-zA-Z0-9._-]", "_");
    }
}
