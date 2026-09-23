package com.jaranalyzer.crossref;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Infers Oracle package and procedure names from Java class/method names.
 *
 * Convention observed in this codebase:
 *   Java class:  ExtensionPolicy  Pg Uwge CI  InsertCover  ServiceImpl
 *   Oracle proc: PG_UWGE_CI       .INSERT_COVER
 *
 * The "Pg" marker in the Java class name starts an Oracle "PG_" package segment.
 * Everything between "Pg" markers (or to the end) is decoded camelCase → UPPER_SNAKE.
 *
 * This class is PURE UTILITY — no Spring beans, no I/O.
 */
public class OracleProcNameInferencer {

    // Known two-letter Oracle abbreviations that should stay together (not split on second letter)
    private static final java.util.Set<String> KNOWN_ABBREVS = java.util.Set.of(
            "CI", "CN", "MT", "WIP", "OCP", "OCQ", "PME", "API", "UW", "RI", "TP", "NB",
            "FN", "PC", "SP", "PR", "ID", "DT", "TMP", "CHK", "INS", "UPD", "DEL"
    );

    /**
     * Infer the Oracle package name from a Java class simple name.
     *
     * Examples:
     *   ExtensionPolicyPgUwgeCnInterfaceServiceImpl → PG_UWGE_CN
     *   ExtensionPolicyPgUwgeCIInsertCoverServiceImpl → PG_UWGE_CI
     *   CorePolicyInvokeNewBusinessImpl → (no Pg marker) → null
     *   ExtensionPolicyPgUwgeCIPcGetMotorCnoteToConvertServiceImpl → PG_UWGE_CI
     */
    public String inferPackageName(String simpleClassName) {
        if (simpleClassName == null || simpleClassName.isBlank()) return null;

        String stripped = stripPrefixesAndSuffixes(simpleClassName);
        if (stripped == null || !stripped.startsWith("Pg")) return null;

        // Find all "Pg" segments: split on "Pg" (capital P, lowercase g)
        // Take just the package portion = everything up to but not including the proc segment
        // Strategy: find first Pg, then find second Pg or end-of-class abbreviation boundary
        // Package = PG_ + decode(segment after first Pg, up to proc verb)
        String afterPg = stripped.substring(2); // strip leading "Pg"

        // Find the package abbreviation: next uppercase-start segment before a proc verb
        // e.g. "UwgeCnInterfaceServiceImpl" → package segment = "UwgeCn"
        // e.g. "UwgeCIInsertCoverServiceImpl" → package = "UwgeCI"
        // e.g. "UwgeCIPcGetMotorCnoteToConvert" → package = "UwgeCI"
        // extractPackageSegment already returns UPPER_SNAKE
        String packageSegment = extractPackageSegment(afterPg);
        if (packageSegment == null || packageSegment.isBlank()) return null;

        return "PG_" + packageSegment;
    }

    /**
     * Infer the Oracle procedure name from a Java method name.
     * Examples:
     *   insertRisk → INSERT_RISK
     *   getMotorCoverNoteForConversion → GET_MOTOR_COVER_NOTE_FOR_CONVERSION
     *   checkMotorJpjStatus → CHECK_MOTOR_JPJ_STATUS
     *   wipAbort → WIP_ABORT
     *   checkValidData → CHECK_VALID_DATA
     */
    public String inferProcedureName(String methodName) {
        if (methodName == null || methodName.isBlank()) return null;
        // Strip lambda suffixes: lambda$prefetchJpjStatus$0 → prefetchJpjStatus
        String m = methodName.replaceAll("^lambda\\$([^$]+)\\$.*$", "$1");
        return camelToUpperSnake(m);
    }

    /**
     * Infer the fully qualified Oracle proc reference from a Java class + method name.
     * Returns "PG_PACKAGE.PROC_NAME" when inferrable, or just "PROC_NAME" when no package found.
     */
    public String inferFullProcRef(String simpleClassName, String methodName) {
        String pkg = inferPackageName(simpleClassName);
        String proc = inferProcedureName(methodName);
        if (proc == null) return null;
        return (pkg != null) ? pkg + "." + proc : proc;
    }

    /**
     * Infer package from class name when the class name itself encodes the proc
     * (e.g. ExtensionPolicyPgUwgeCIPcCreateMotorNewBusinessServiceImpl → whole class = one proc).
     * Returns the proc-level name extracted from the class name after the package segment.
     */
    public String inferProcNameFromClass(String simpleClassName) {
        if (simpleClassName == null || simpleClassName.isBlank()) return null;
        String stripped = stripPrefixesAndSuffixes(simpleClassName);
        if (stripped == null || !stripped.startsWith("Pg")) return null;
        String afterPg = stripped.substring(2);
        // extractPackageSegment works on UPPER_SNAKE internally and returns UPPER_SNAKE
        String fullSnake = camelToUpperSnake(afterPg);          // e.g. "UWGE_CI_INSERT_COVER"
        String packageSegment = extractPackageSegment(afterPg); // returns UPPER_SNAKE pkg portion
        if (packageSegment == null || packageSegment.equals(fullSnake)) return null; // no proc part
        // Proc portion = everything after "PKG_" + the separator underscore
        String remainder = fullSnake.substring(packageSegment.length()).replaceFirst("^_", "");
        if (remainder.isBlank()) return null;
        return remainder; // already UPPER_SNAKE
    }

    /**
     * Returns confidence level of the inference.
     * HIGH  — class has "Pg" marker (Oracle package naming convention present)
     * MEDIUM — class name contains common Oracle proc verbs but no Pg marker
     * LOW   — generic inference, low reliability
     */
    public MatchConfidence inferenceConfidence(String simpleClassName) {
        if (simpleClassName == null) return MatchConfidence.NONE;
        String stripped = stripPrefixesAndSuffixes(simpleClassName);
        if (stripped == null) return MatchConfidence.LOW;
        if (stripped.startsWith("Pg")) return MatchConfidence.HIGH;
        if (stripped.matches(".*(?i)(Insert|Update|Delete|Select|Fetch|Load|Process|Invoke|Check|Validate).*")) {
            return MatchConfidence.MEDIUM;
        }
        return MatchConfidence.LOW;
    }

    public enum MatchConfidence { HIGH, MEDIUM, LOW, NONE }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /** Strip known Java service/impl prefixes and suffixes. */
    private String stripPrefixesAndSuffixes(String name) {
        // Strip leading domain prefixes
        String s = name;
        for (String prefix : List.of("ExtensionPolicy", "CorePolicy", "Extension", "Core")) {
            if (s.startsWith(prefix)) {
                s = s.substring(prefix.length());
                break;
            }
        }
        // Strip trailing impl/service suffixes
        for (String suffix : List.of("ServiceCustomerExtensionPolicy", "ServiceCustomer",
                "ServiceCustomerUtils", "ServiceImpl", "RepositoryImpl", "DaoImpl",
                "Service", "Repository", "Impl")) {
            if (s.endsWith(suffix)) {
                s = s.substring(0, s.length() - suffix.length());
                break;
            }
        }
        return s.isBlank() ? null : s;
    }

    /**
     * Extract the Oracle package segment from the string after the initial "Pg" marker.
     * The package segment ends where a proc verb begins (Insert, Get, Check, Validate, Fn, Pc, etc.)
     * or where "Interface" / end of class name occurs.
     *
     * Examples:
     *   "UwgeCnInterface"          → "UwgeCn"      (stop before "Interface")
     *   "UwgeCIInsertCover"        → "UwgeCI"      (stop before "Insert")
     *   "UwgeCIPcGetMotorCnote..." → "UwgeCI"      (stop before "Pc")
     *   "UwgeCIFnCheckValidData"   → "UwgeCI"      (stop before "Fn")
     */
    // Words that begin an Oracle proc name — never part of the package identifier.
    // Deliberately conservative: ambiguous words like "Process", "Load", "Run" are excluded
    // because they can also appear at the end of a package name (e.g. PG_UWGE_BATCH_PROCESS).
    // A word is only treated as a proc starter when it has further content after it.
    private static final java.util.Set<String> PROC_VERB_STARTERS = java.util.Set.of(
        "INSERT", "UPDATE", "DELETE", "GET", "SET", "CHECK", "VALIDATE",
        "CREATE", "BUILD", "INIT", "FIND", "LIST", "COUNT", "SAVE", "REMOVE",
        "FETCH", "INVOKE", "EXECUTE", "START", "STOP", "RUN", "PROCESS", "LOAD",
        "PC", "FN", "SP", "PR"
    );

    /**
     * Determine the Oracle package segment from the class name after the leading "Pg".
     *
     * Strategy: convert to UPPER_SNAKE, find the first proc-verb word, and only split
     * there when the verb is NOT the last word (a trailing verb is part of the package name,
     * e.g. PgUwgeBatchProcess → PG_UWGE_BATCH_PROCESS, not PG_UWGE_BATCH).
     */
    private String extractPackageSegment(String afterPg) {
        String snake = camelToUpperSnake(afterPg);          // e.g. "UWGE_CI_INSERT_COVER"
        String[] parts = snake.split("_");

        int verbIdx = -1;
        for (int i = 0; i < parts.length; i++) {
            if (PROC_VERB_STARTERS.contains(parts[i])) {
                verbIdx = i;
                break;
            }
        }

        // No verb found, or verb is the very last word → whole string is the package
        if (verbIdx <= 0 || verbIdx >= parts.length - 1) return snake;

        // Verb found with content after it → everything before the verb is the package
        return String.join("_", java.util.Arrays.copyOfRange(parts, 0, verbIdx));
    }

    /**
     * Convert camelCase / PascalCase to UPPER_SNAKE_CASE.
     * Handles consecutive caps as single tokens: "UwgeCI" → "UWGE_CI"
     */
    static String camelToUpperSnake(String name) {
        if (name == null || name.isBlank()) return name;
        // Insert underscore before transitions:
        //   lowercase→uppercase: "insertRisk" → "insert_Risk"
        //   uppercase sequence→uppercase+lowercase: "CIInsert" → "CI_Insert"
        String result = name
            .replaceAll("([a-z0-9])([A-Z])", "$1_$2")          // lower→upper
            .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")       // sequence of caps → last cap starts new word
            .toUpperCase()
            .replaceAll("_+", "_")                              // collapse multiple underscores
            .replaceAll("^_|_$", "");                           // trim leading/trailing
        return result;
    }

    /**
     * Given a full Oracle proc reference string (e.g. "PROC:PG_UWGE_CI.INSERT_RISK"),
     * extract just the proc portion: "PG_UWGE_CI.INSERT_RISK".
     */
    public static String stripProcPrefix(String oracleProcRef) {
        if (oracleProcRef == null) return null;
        if (oracleProcRef.startsWith("PROC:") || oracleProcRef.startsWith("FUNC:")) {
            return oracleProcRef.substring(5);
        }
        return oracleProcRef;
    }

    /**
     * Normalise an Oracle proc name for comparison (strip schema prefix, uppercase, trim).
     * "CUSTOMER.PG_UWGE_CI.INSERT_RISK" → "PG_UWGE_CI.INSERT_RISK"
     * "PG_UWGE_CI.INSERT_RISK" → "PG_UWGE_CI.INSERT_RISK"
     */
    public static String normalise(String procName) {
        if (procName == null) return null;
        String s = stripProcPrefix(procName).toUpperCase().trim();
        // Strip leading schema (more than 2 dot-segments means schema.pkg.proc)
        String[] parts = s.split("\\.");
        if (parts.length > 2) {
            // Drop the first segment (schema)
            s = String.join(".", java.util.Arrays.copyOfRange(parts, 1, parts.length));
        }
        return s;
    }
}
