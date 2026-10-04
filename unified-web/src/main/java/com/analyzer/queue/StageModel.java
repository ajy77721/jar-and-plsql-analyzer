package com.analyzer.queue;

import java.util.List;
import java.util.Map;

/**
 * The first-class stage models: which stages a job of each type runs through, in order, with the
 * <em>default</em> calibrated share of the total work each one is expected to take and the size
 * unit its work scales with.
 *
 * <p>The default shares are only a starting point — {@link ProgressCalibrationStore} learns the real
 * shares from the {@code [timing]} data of past runs and {@link ProgressEstimator} applies them. The
 * models exist so every stage is <b>visible</b> from the moment a job starts, including the stages
 * that used to hide in the background (sidecar warm-up, estate facets, entity ingest, decompile):
 * the job cannot reach "Analysis complete" until all of them are done.
 *
 * <p>Both the PL/SQL model (8 stages) and the JAR/WAR model (9 stages) are declared here so the
 * frontend and the estimator cannot drift: the UI renders the same list the engine builds.
 */
public final class StageModel {

    /** A stage's definition: stable id, human label, default share and size unit (may be null). */
    public record Spec(String id, String label, double share, String unit) {
        public boolean sized() {
            return unit != null;
        }
    }

    /** How many units a size-less default estimate assumes for each unit, when nothing is known. */
    public static final Map<String, Long> DEFAULT_UNIT_COUNTS =
            Map.of("proc", 40L, "class", 300L, "endpoint", 80L, "facet", 14L);

    private static final List<Spec> PLSQL = List.of(
            new Spec("connect-discover", "Connect & discover", 0.10, "proc"),
            new Spec("source-fetch",     "Fetch sources",      0.16, "proc"),
            new Spec("parse",            "Parse procedures",   0.30, "proc"),
            new Spec("call-graph",       "Build call graph",   0.08, null),
            new Spec("save",             "Save artifacts",     0.12, null),
            new Spec("sidecar-warmup",   "Warm up facets",     0.16, "facet"),
            new Spec("estate-facets",    "Estate facets",      0.04, null),
            new Spec("entity-ingest",    "Ingest entities",    0.04, null));

    private static final List<Spec> JAR = List.of(
            new Spec("parse-classes",    "Parse classes",         0.16, "class"),
            new Spec("config-extract",   "Extract config",        0.04, null),
            new Spec("call-graph",       "Call graph & endpoints",0.16, "class"),
            new Spec("save-document",    "Save document",         0.14, null),
            new Spec("endpoint-outputs", "Write endpoint outputs",0.16, "endpoint"),
            new Spec("decompile",        "Decompile sources",     0.24, "endpoint"),
            new Spec("summary-tree",     "Summary & class tree",  0.05, null),
            new Spec("db-calls",         "DB-call facet",         0.02, null),
            new Spec("ingest",           "Ingest entities",       0.03, null));

    private StageModel() {}

    /**
     * The stages for a job type, or an empty list for a type the engine does not model (Claude
     * enrichment, validation, …), which keeps its legacy single-step progress.
     */
    public static List<Spec> forType(QueueJob.Type type) {
        if (type == null) return List.of();
        return switch (type) {
            case PARSER_ANALYSIS -> PLSQL;
            case JAR_UPLOAD, JAR_TARGETED, WAR_UPLOAD -> JAR;
            default -> List.of();
        };
    }

    /** The calibration dialect for a job type ({@code "plsql"} / {@code "jar"}), or null. */
    public static String dialectOf(QueueJob.Type type) {
        if (type == null) return null;
        return switch (type) {
            case PARSER_ANALYSIS -> "plsql";
            case JAR_UPLOAD, JAR_TARGETED, WAR_UPLOAD -> "jar";
            default -> null;
        };
    }

    /** The default (uncalibrated) stage list for a dialect — used by the calibration store. */
    public static List<Spec> defaults(String dialect) {
        return "plsql".equals(dialect) ? PLSQL : JAR;
    }

    public static List<Spec> plsql() { return PLSQL; }

    public static List<Spec> jar() { return JAR; }
}
