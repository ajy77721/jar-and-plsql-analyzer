package com.analyzer.queue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The small, self-updating calibration store behind the size-aware estimates.
 *
 * <p>One JSON document per dialect ({@code progress-calibration-plsql.json},
 * {@code progress-calibration-jar.json}) under the app's config directory, holding:
 *
 * <ul>
 *   <li>{@code msPerUnit} — the observed milliseconds per size unit ({@code proc}, {@code class},
 *       {@code endpoint}, {@code facet}). This is what turns a discovered count into an estimate.</li>
 *   <li>{@code shares} — the observed share of total wall time each stage took. This is what weights
 *       the overall percentage and stops the "five equal steps" lie.</li>
 *   <li>{@code runs} — how many runs have fed the file, so the averaging can warm up.</li>
 * </ul>
 *
 * <p><b>Self-updating and bounded.</b> Every completed analysis job calls {@link #observe}: over the
 * first {@link #WARMUP_RUNS} runs the value is a simple average, and thereafter a decayed average
 * ({@code next = prev x (1 - DECAY) + observed x DECAY}) so the store tracks a changing workload
 * without ever growing. Per-unit costs are clamped to a sane range and shares are renormalised to
 * sum to one. The file is written atomically and never holds a secret — only durations and counts.
 *
 * <p><b>Never throws.</b> A missing, unreadable or corrupt file yields the defaults, and a failed
 * write is logged at debug and ignored: calibration is an optimisation, not a correctness
 * dependency of any analysis.
 */
public final class ProgressCalibrationStore {

    private static final Logger log = LoggerFactory.getLogger(ProgressCalibrationStore.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Steady-state weight of a fresh observation once past warm-up. */
    static final double DECAY = 0.2;
    /** Observations averaged with equal weight before the decayed average takes over. */
    static final int WARMUP_RUNS = 5;
    static final double MIN_MS = 1.0;
    static final double MAX_MS = 600_000.0;

    /** Sane initial per-unit costs, overridden by observation. */
    public static final Map<String, Double> DEFAULT_MS_PER_UNIT =
            Map.of("proc", 800.0, "class", 15.0, "endpoint", 120.0, "facet", 3000.0);

    /** One dialect's calibration. Immutable. */
    public record Snapshot(int runs, Map<String, Double> msPerUnit, Map<String, Double> shares) {
        public double msPerUnit(String unit) {
            Double v = msPerUnit.get(unit);
            return v != null ? v : DEFAULT_MS_PER_UNIT.getOrDefault(unit, 100.0);
        }

        public double share(String stageId, double fallback) {
            Double v = shares.get(stageId);
            return v != null && v > 0 ? v : fallback;
        }
    }

    private final Path dir;
    private final Map<String, Snapshot> cache = new ConcurrentHashMap<>();

    public ProgressCalibrationStore(Path dir) {
        this.dir = dir;
    }

    /** The current calibration for a dialect, loading or defaulting on first use. Never null. */
    public Snapshot get(String dialect) {
        return cache.computeIfAbsent(dialect, this::load);
    }

    /**
     * Folds one completed run's observations into the dialect's calibration.
     *
     * @param observedMsPerUnit per-unit costs measured this run (ms/unit), may be empty
     * @param observedShares    each stage's fraction of the run's wall time, may be empty
     */
    public synchronized void observe(String dialect,
                                     Map<String, Double> observedMsPerUnit,
                                     Map<String, Double> observedShares) {
        Snapshot old = get(dialect);
        int runs = old.runs();
        double alpha = runs < WARMUP_RUNS ? 1.0 / (runs + 1) : DECAY;

        Map<String, Double> ms = new LinkedHashMap<>(old.msPerUnit());
        if (observedMsPerUnit != null) {
            for (Map.Entry<String, Double> e : observedMsPerUnit.entrySet()) {
                Double observed = e.getValue();
                if (observed == null || !Double.isFinite(observed) || observed <= 0) continue;
                double prev = ms.getOrDefault(e.getKey(),
                        DEFAULT_MS_PER_UNIT.getOrDefault(e.getKey(), 100.0));
                ms.put(e.getKey(), clamp(prev * (1.0 - alpha) + observed * alpha, MIN_MS, MAX_MS));
            }
        }

        Map<String, Double> shares = new LinkedHashMap<>(old.shares());
        if (observedShares != null && !observedShares.isEmpty()) {
            double totalObserved = 0.0;
            for (Double v : observedShares.values()) {
                if (v != null && Double.isFinite(v) && v > 0) totalObserved += v;
            }
            if (totalObserved > 0) {
                for (Map.Entry<String, Double> e : observedShares.entrySet()) {
                    Double raw = e.getValue();
                    if (raw == null || !Double.isFinite(raw) || raw <= 0) continue;
                    double observed = raw / totalObserved;
                    double prev = shares.getOrDefault(e.getKey(), 0.0);
                    // A stage never observed before starts at this observation; one that has is
                    // blended, so a new stage does not drag the existing shares toward zero.
                    double next = prev <= 0 ? observed : prev * (1.0 - alpha) + observed * alpha;
                    shares.put(e.getKey(), clamp(next, 0.0, 1.0));
                }
                normalise(shares);
            }
        }

        Snapshot next = new Snapshot(runs + 1, ms, shares);
        cache.put(dialect, next);
        persist(dialect, next);
    }

    /** The default (unobserved) snapshot for a dialect: model shares, default per-unit costs. */
    public static Snapshot defaults(String dialect) {
        Map<String, Double> shares = new LinkedHashMap<>();
        for (StageModel.Spec spec : StageModel.defaults(dialect)) shares.put(spec.id(), spec.share());
        return new Snapshot(0, new LinkedHashMap<>(DEFAULT_MS_PER_UNIT), shares);
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    private Snapshot load(String dialect) {
        Snapshot fallback = defaults(dialect);
        Path file = fileFor(dialect);
        if (file == null || !Files.isRegularFile(file)) return fallback;
        try {
            JsonNode root = MAPPER.readTree(Files.readAllBytes(file));
            int runs = root.path("runs").asInt(0);
            Map<String, Double> ms = new LinkedHashMap<>(fallback.msPerUnit());
            JsonNode msNode = root.path("msPerUnit");
            msNode.fieldNames().forEachRemaining(k -> {
                double v = msNode.path(k).asDouble(0);
                if (v > 0) ms.put(k, clamp(v, MIN_MS, MAX_MS));
            });
            Map<String, Double> shares = new LinkedHashMap<>(fallback.shares());
            JsonNode sharesNode = root.path("shares");
            sharesNode.fieldNames().forEachRemaining(k -> {
                double v = sharesNode.path(k).asDouble(0);
                if (v > 0) shares.put(k, clamp(v, 0.0, 1.0));
            });
            normalise(shares);
            return new Snapshot(Math.max(0, runs), ms, shares);
        } catch (Exception e) {
            log.debug("progress calibration for {} unreadable ({}); using defaults", dialect, e.toString());
            return fallback;
        }
    }

    private void persist(String dialect, Snapshot snapshot) {
        Path file = fileFor(dialect);
        if (file == null) return;
        try {
            if (file.getParent() != null) Files.createDirectories(file.getParent());
            ObjectNode root = MAPPER.createObjectNode();
            root.put("runs", snapshot.runs());
            root.put("updatedAt", java.time.Instant.now().toString());
            ObjectNode ms = root.putObject("msPerUnit");
            snapshot.msPerUnit().forEach(ms::put);
            ObjectNode shares = root.putObject("shares");
            snapshot.shares().forEach(shares::put);
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            Files.write(tmp, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(root));
            try {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (Exception atomicUnsupported) {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (Exception e) {
            // Calibration is best-effort: a read-only config dir must not affect any analysis.
            log.debug("could not persist progress calibration for {}: {}", dialect, e.toString());
        }
    }

    private Path fileFor(String dialect) {
        if (dir == null || dialect == null || dialect.isBlank()) return null;
        return dir.resolve("progress-calibration-" + dialect + ".json");
    }

    private static void normalise(Map<String, Double> shares) {
        double sum = 0.0;
        for (Double v : shares.values()) if (v != null && v > 0) sum += v;
        if (sum <= 0) return;
        for (Map.Entry<String, Double> e : shares.entrySet()) {
            if (e.getValue() != null && e.getValue() > 0) e.setValue(e.getValue() / sum);
        }
    }

    private static double clamp(double v, double lo, double hi) {
        return Math.max(lo, Math.min(hi, v));
    }
}
