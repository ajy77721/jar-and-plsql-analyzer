package com.plsql.parser.multidb;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Best-effort extraction of the routine a scheduled job invokes, from a free-form action text.
 *
 * <p>A scheduler catalog rarely states the target structurally: Oracle stores a PL/SQL
 * {@code job_action} block or a {@code program_name}; SQL Agent stores a T-SQL {@code command};
 * pg_cron stores a SQL {@code command}; MySQL stores the event's {@code EVENT_DEFINITION}. In every
 * case the target is one of a handful of spellings — {@code CALL pkg.proc(...)},
 * {@code EXEC pkg.proc}, a bare {@code pkg.proc(...)}, or an {@code EXECUTE pkg.proc}. This helper
 * recognizes those spellings and returns the bare (or {@code PACKAGE.MEMBER}) name. It is a
 * fallback used ONLY to seed the job→target SCHEDULES edge and the "target proc" facet column;
 * the action itself is still parsed like a procedure by the dialect's grammar, so the authoritative
 * call list is the grammar's, not this regex's.
 */
public final class JobActionParser {

    private JobActionParser() {}

    private static final Pattern CALL = Pattern.compile(
            "(?is)\\b(?:CALL|EXEC(?:UTE)?(?:\\s+AS\\s+\\w+)?)\\s+([A-Za-z_$][\\w$]*(?:\\s*\\.\\s*[A-Za-z_$][\\w$]*)?)");

    /** A bare qualified/unqualified identifier immediately followed by '(' at a statement start. */
    private static final Pattern BARE_CALL = Pattern.compile(
            "(?im)(?:^|[;\\n]|\\bBEGIN\\b|\\bTHEN\\b|\\bELSE\\b)\\s*([A-Za-z_$][\\w$]*(?:\\s*\\.\\s*[A-Za-z_$][\\w$]*)?)\\s*\\(");

    /**
     * A whole action that is just a bare routine statement, optionally qualified and optionally
     * terminated — the DBMS_JOB {@code WHAT} shape ({@code erp_usp_purge;}) and the SQL Agent
     * {@code command} shape ({@code dbo.usp_load_orders;}) when no {@code EXEC}/{@code CALL} is used.
     */
    private static final Pattern BARE_NAME = Pattern.compile(
            "^([A-Za-z_$][\\w$]*(?:\\s*\\.\\s*[A-Za-z_$][\\w$]*)?)\\s*;?\\s*$");

    /**
     * Extract the invoked routine from {@code action}, or null when nothing recognizable is found.
     * Returns the name with any whitespace around the dot removed, preserving case.
     */
    public static String targetFromAction(String action) {
        if (action == null || action.isBlank()) return null;
        Matcher m = CALL.matcher(action);
        if (m.find()) return normalize(m.group(1));
        Matcher b = BARE_CALL.matcher(action);
        if (b.find()) {
            String name = normalize(b.group(1));
            // Reject obvious SQL keywords that precede a '(' in an action but are not a routine.
            if (!isKeyword(name)) return name;
        }
        Matcher bare = BARE_NAME.matcher(action.trim());
        if (bare.matches()) {
            String name = normalize(bare.group(1));
            if (!isKeyword(name)) return name;
        }
        return null;
    }

    /**
     * Extract a routine name from a scheduler {@code program_name} / {@code program_action} value.
     * A program is often a bare name ({@code MY_PROC}) or {@code SCHEMA.PROC}; when it is an
     * anonymous block, fall back to {@link #targetFromAction}.
     */
    public static String targetFromName(String name) {
        if (name == null || name.isBlank()) return null;
        String trimmed = name.trim();
        if (trimmed.indexOf('(') >= 0 || trimmed.indexOf(';') >= 0
                || trimmed.toUpperCase().startsWith("BEGIN")) {
            return targetFromAction(trimmed);
        }
        // Strip a quoted/qualified wrapper: SCHEMA.PROC, "SCHEMA"."PROC".
        String cleaned = trimmed.replace("\"", "").replace("`", "");
        if (cleaned.contains(".")) {
            String[] parts = cleaned.split("\\.");
            if (parts.length >= 2) return parts[parts.length - 2] + "." + parts[parts.length - 1];
        }
        return cleaned;
    }

    private static String normalize(String s) {
        return s.replaceAll("\\s*\\.\\s*", ".").trim();
    }

    private static boolean isKeyword(String name) {
        String u = name.toUpperCase();
        return u.equals("BEGIN") || u.equals("DECLARE") || u.equals("IF") || u.equals("WHILE")
                || u.equals("CASE") || u.equals("SELECT") || u.equals("INSERT")
                || u.equals("UPDATE") || u.equals("DELETE") || u.equals("MERGE")
                || u.equals("RETURN") || u.equals("EXCEPTION") || u.equals("FOR")
                || u.equals("LOOP") || u.equals("END");
    }
}
