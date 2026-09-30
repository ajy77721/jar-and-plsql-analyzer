package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.repo.AnalysisQuery;
import com.analyzer.storage.repo.AnalysisRef;
import com.analyzer.storage.sso.CapturedCredentials;
import com.analyzer.storage.sso.CapturedStores;
import com.analyzer.storage.sso.SsoJobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Eagerly precomputes every analysis's precomputed view summaries, so the <em>first</em> reader of
 * a detail screen does not pay the lazy backfill: the legacy computation re-scans every
 * {@code api/nodes/*.json} (~116 store reads for a real analysis, measured at 35–41s cold), and
 * with four scope variants per view a cold analysis is paid four times, once per first visitor.
 *
 * <p><b>One sweep at boot and every {@code app.sidecar.warmup.interval-hours}.</b> The sweep walks
 * the same enumeration {@code AnalysisService.listAnalyses} uses
 * ({@link AnalysisRepository#analyses} with {@code PLSQL_ANALYSES}), and for each analysis whose
 * sidecar set is not already complete ({@link AnalysisViewStore#hasCompleteViewSet}) computes the
 * header and every facet through <b>the Phase-1 writer's own code path</b>
 * ({@link AnalysisViewWriter#writeAll}) — nothing is reimplemented, so a warmed sidecar is
 * byte-identical to the one a first read would have produced.
 *
 * <h2>Why it is safe at boot, where there is no user session</h2>
 *
 * <p>Sweep threads have no request-bound session, so the per-user routing store falls back to the
 * startup store — which on a pod signs with the default AWS chain and has no credentials. The
 * compute therefore fails. That is expected and contained:
 *
 * <ul>
 *   <li><b>It never throws.</b> The writer catches every facet independently, this class catches
 *       everything else, and a failure only advances a counter.</li>
 *   <li><b>It is quiet.</b> Failures are tallied and reported as a single INFO line per sweep, not
 *       one WARN per facet per analysis; the writer is asked to stay quiet for the same reason.</li>
 *   <li><b>It retries.</b> An analysis is skipped only when its sidecar set is genuinely complete,
 *       so whatever failed is attempted again next sweep — and a deployment whose default chain does
 *       have credentials (the local workflow) warms fully on the first pass.</li>
 * </ul>
 *
 * <h2>Why the completion hook still matters</h2>
 *
 * <p>A new analysis is beyond the sweep's reach on a pod for the same credential reason, but the
 * queue job that produced it runs under its submitter's captured credentials
 * ({@code SsoJobContext}), so {@link #warmOne} called from
 * {@code ParserAnalysisExecutor} at the completion point warms it within seconds, on-thread, with
 * the credentials that created it. The sweep is then the backfill for everything older.
 *
 * <h2>Warming with the first logged-in user</h2>
 *
 * <p>The boot/periodic sweep has no principal of its own on a pod with no ambient credentials, so the
 * first login is what finally gives the sweep a way to sign. {@link #triggerLoginSweep} takes the
 * just-logged-in session's captured credentials (and stores, when connected) and installs them via
 * {@code SsoJobContext} around one sweep — the same mechanism a queue job uses — so the whole corpus
 * is precomputed in the background while nobody is waiting. It runs at most once per pod per sweep
 * interval and never blocks the login response.
 *
 * <p>The same sweep also warms the JAR decompile cache through {@link DecompileWarmUpService} (each
 * class otherwise costs CFR 8–21s on a cold first click) — best-effort and unable to fail the sidecar
 * half.
 *
 * <h2>Cost</h2>
 *
 * <p>Two worker threads at most, and a single coordinator. It is background warmth, not a request
 * path, so it must never be able to starve the request or queue threads; a sweep already in flight
 * causes the next tick to be skipped rather than overlapped.
 */
@Component
public class SidecarWarmUpService {

    private static final Logger log = LoggerFactory.getLogger(SidecarWarmUpService.class);

    private final AnalysisRepository repository;
    private final AnalysisViewWriter writer;
    private final AnalysisViewStore views;
    private final AnalysisService analysisService;

    /** Warms the JAR decompile cache too; {@code null} in fixtures that only exercise sidecars. */
    private final DecompileWarmUpService decompileWarmUp;

    /** Whether the boot + periodic sweep runs. The completion hook is not gated by this. */
    private final boolean enabled;

    /** Whether a completed login may trigger the sweep with the user's own credentials. */
    private final boolean onLogin;

    private final int intervalHours;

    /** Runs one sweep off the request path, so a slow sweep never blocks anything else. */
    private final ExecutorService coordinator =
            Executors.newSingleThreadExecutor(r -> daemon(r, "sidecar-warmup-coordinator"));

    /** Drives the periodic sweep without occupying Spring's own shared scheduler thread. */
    private final java.util.concurrent.ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "sidecar-warmup-scheduler"));

    /** The per-analysis work, bounded to two threads: background warmth, never a request thread. */
    private final ExecutorService workers =
            Executors.newFixedThreadPool(2, r -> daemon(r, "sidecar-warmup-worker"));

    /** One sweep at a time; a tick that arrives mid-sweep is skipped, not queued. */
    private final AtomicBoolean running = new AtomicBoolean();

    /** When the last login-triggered sweep STARTED, or 0 when none ever has. */
    private final AtomicLong lastLoginSweepAt = new AtomicLong();

    /** Counts from the most recent completed sweep; the on-demand endpoint reports them. */
    private volatile SweepStats lastSweep = new SweepStats(0, 0, 0, 0, 0, 0, 0);

    public SidecarWarmUpService(AnalysisRepository repository,
                                AnalysisViewWriter writer,
                                AnalysisViewStore views,
                                @Qualifier("parserAnalysisService") AnalysisService analysisService,
                                DecompileWarmUpService decompileWarmUp,
                                @Value("${app.sidecar.warmup.enabled:true}") boolean enabled,
                                @Value("${app.sidecar.warmup.interval-hours:6}") int intervalHours,
                                @Value("${app.sidecar.warmup.on-login:true}") boolean onLogin) {
        this.repository = repository;
        this.writer = writer;
        this.views = views;
        this.analysisService = analysisService;
        this.decompileWarmUp = decompileWarmUp;
        this.enabled = enabled;
        this.intervalHours = intervalHours;
        this.onLogin = onLogin;
    }

    private static Thread daemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    /** The sweep interval in millis, from {@code app.sidecar.warmup.interval-hours}. */
    public long getIntervalMillis() {
        return Math.max(1, intervalHours) * 3_600_000L;
    }

    /** A short, non-reversible session suffix for a log line, or {@code null} for no session. */
    private static String mask(String sessionId) {
        if (sessionId == null) return "none";
        return sessionId.length() <= 8 ? "…" : "…" + sessionId.substring(sessionId.length() - 8);
    }

    /**
     * The eager boot pass and the periodic schedule.
     *
     * <p>The first sweep runs now — as soon as the context is ready — so a pod's first visitor after
     * a restart is not the one who pays for it; the next runs one {@code interval-hours} later, and
     * so on. The schedule is driven by this bean's own daemon thread rather than {@code @Scheduled}
     * so that a sweep (which can take minutes over a large corpus) can never occupy Spring's single
     * shared scheduler thread, the one {@code LogArchiveService} and {@code MongoHousekeepingService}
     * also poll on.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (!enabled) return;
        long interval = getIntervalMillis();
        scheduler.scheduleWithFixedDelay(() -> kick(), interval, interval, TimeUnit.MILLISECONDS);
        kick();
    }

    /**
     * Starts a sweep in the background if one is not already running.
     *
     * @return true when this call started a sweep, false when warm-up is disabled or one was
     *         already in flight
     */
    public boolean triggerSweep() {
        return kick();
    }

    /** Counts from the most recent sweep, for the on-demand endpoint to report. Never null. */
    public SweepStats lastSweep() {
        return lastSweep;
    }

    private boolean kick() {
        if (!enabled) return false;
        if (!running.compareAndSet(false, true)) {
            log.debug("Sidecar warm-up sweep still in progress; skipping this tick");
            return false;
        }
        try {
            coordinator.submit(() -> {
                try {
                    sweep();
                } catch (Throwable t) {
                    // Sweep-level containment: nothing here may escape onto a pool thread.
                    log.info("Sidecar warm-up sweep aborted: {}", t.toString());
                } finally {
                    running.set(false);
                }
            });
            return true;
        } catch (RejectedExecutionException e) {
            running.set(false);
            return false;
        }
    }

    /**
     * Starts a sweep in the background bound to a just-logged-in user's <em>captured</em>
     * credentials, so a pod with no ambient AWS credentials finally has a principal that can reach
     * the store.
     *
     * <p>The capture is taken on the login thread by the caller ({@code LoginWarmUpTrigger}); this
     * method installs it via {@link SsoJobContext} around the sweep, the same mechanism a queue job
     * uses. It runs at most once per pod per sweep interval: the first completed login triggers it,
     * and a later login within the interval is a no-op (the {@code running} guard also means it can
     * never overlap an in-flight boot/periodic sweep). Returns immediately — the login response is
     * never made to wait on a sweep.
     *
     * @return true when this call started a sweep
     */
    public boolean triggerLoginSweep(CapturedStores stores, CapturedCredentials credentials,
                                     String sessionId) {
        if (!enabled || !onLogin) return false;
        long now = System.currentTimeMillis();
        long last = lastLoginSweepAt.get();
        if (last != 0 && now - last < getIntervalMillis()) {
            log.debug("Sidecar warm-up: login sweep skipped; last ran {} ms ago", now - last);
            return false;
        }
        if (!running.compareAndSet(false, true)) {
            log.debug("Sidecar warm-up: login sweep skipped; a sweep is already running");
            return false;
        }
        lastLoginSweepAt.set(now);
        // One INFO at sweep start; the session is masked to a non-reversible suffix, never the id.
        log.info("Sidecar warm-up: login-triggered sweep starting (session {})", mask(sessionId));
        try {
            coordinator.submit(() -> {
                try {
                    sweep(stores, credentials);
                } catch (Throwable t) {
                    log.info("Sidecar warm-up (login) aborted: {}", t.toString());
                } finally {
                    running.set(false);
                }
            });
            return true;
        } catch (RejectedExecutionException e) {
            running.set(false);
            return false;
        }
    }

    /**
     * One sweep: enumerate every analysis, skip the complete ones, warm the rest with bounded
     * parallelism, and emit exactly one INFO line — analyses warmed, sidecars written, duration.
     * Never throws.
     *
     * <p><b>Scope coverage.</b> The four node-scan facet documents and the two table maps are
     * scope-independent: an endpoint applies {@code direct}/{@code subtree}/{@code subflow}/{@code
     * full} as an in-memory predicate over the whole document <em>after</em> reading it
     * ({@code ParserAnalysisController.facet}/{@code facetSummary}), so the one sidecar per view
     * this sweep writes serves every scope variant. There is deliberately no per-scope sidecar: the
     * scoped shape is not a separate artifact, and a per-(scope, procId) cache would be unbounded in
     * the number of nodes.
     *
     * @return how many analyses were warmed (for tests)
     */
    int sweep() {
        return sweep(null, null);
    }

    /**
     * The sweep proper. When {@code stores}/{@code credentials} are non-null they are installed as
     * the {@link SsoJobContext} on this coordinator thread <em>and</em> on every worker task, so the
     * listing and each analysis's reads/writes sign with the captured user's role instead of the
     * pod's (absent) default chain. {@link com.analyzer.storage.sso.SessionRoutingArtifactStore}
     * additionally carries the snapshot's session onto {@code ShallowTree}'s own listing pool, so
     * the parallel walk routes as the same user. Never throws.
     */
    int sweep(CapturedStores stores, CapturedCredentials credentials) {
        long startNanos = System.nanoTime();
        List<String> names;
        // Installed on the coordinator so the analyses listing (which fans out on ShallowTree) and
        // the decompile warm-up below run under the capture. Null-safe: a legacy sweep with no
        // capture installs nothing.
        try (SsoJobContext ignored = SsoJobContext.install(stores, credentials)) {
            try {
                names = repository.analyses(AnalysisQuery.Dataset.PLSQL_ANALYSES)
                        .stream().map(AnalysisRef::name).toList();
            } catch (Exception e) {
                // An unreachable store: one line, retried next sweep. Never per analysis.
                log.info("Sidecar warm-up: could not list analyses ({}); will retry next sweep", e.toString());
                return 0;
            }
        }

        AtomicInteger warmed = new AtomicInteger();
        AtomicInteger skipped = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        AtomicInteger sidecars = new AtomicInteger();

        List<Future<?>> inFlight = new ArrayList<>(names.size());
        for (String name : names) {
            inFlight.add(workers.submit(() -> {
                // Each worker is a pool thread: the coordinator's ThreadLocal does not reach it, so
                // the snapshot is re-installed here for the duration of this one analysis.
                try (SsoJobContext ignored = SsoJobContext.install(stores, credentials)) {
                    if (views.hasCompleteViewSet(name)) {
                        skipped.incrementAndGet();
                    } else {
                        int written = warmByName(name);
                        if (written >= 0) {
                            warmed.incrementAndGet();
                            sidecars.addAndGet(written);
                        } else {
                            failed.incrementAndGet();
                        }
                    }
                }
            }));
        }
        for (Future<?> f : inFlight) {
            try {
                f.get();
            } catch (Exception ignored) {
                // Each task contains its own failures; a future only fails on interruption.
            }
        }

        // The JAR decompile cache, warmed the same sweep: best-effort and never able to fail the
        // sidecar half.
        DecompileWarmUpService.WarmerResult decompiled = DecompileWarmUpService.WarmerResult.EMPTY;
        if (decompileWarmUp != null) {
            try (SsoJobContext ignored = SsoJobContext.install(stores, credentials)) {
                decompiled = decompileWarmUp.warm();
            } catch (Throwable t) {
                log.debug("Decompile warm-up aborted: {}", t.toString());
            }
        }

        long durationMs = (System.nanoTime() - startNanos) / 1_000_000L;
        lastSweep = new SweepStats(names.size(), warmed.get(), skipped.get(), failed.get(),
                sidecars.get(), durationMs, System.currentTimeMillis());
        // One INFO at sweep end: warmed counts only, no credential or token material.
        log.info("Sidecar warm-up: {} analyses — {} warmed, {} already complete, {} failed; "
                        + "{} sidecars written; {} JAR classes decompiled ({} already cached, {} failed) "
                        + "in {} ms",
                names.size(), warmed.get(), skipped.get(), failed.get(), sidecars.get(),
                decompiled.decompiled(), decompiled.alreadyCached(), decompiled.failed(), durationMs);
        return warmed.get();
    }

    /**
     * Computes and stores one analysis's full sidecar set — the header plus every facet — reusing
     * the Phase-1 writer's exact code path. Never throws.
     *
     * <p>Called by the sweep and, at the analysis-completion point, by {@code ParserAnalysisExecutor}
     * while the completing job's captured credentials are installed on the thread, so a brand-new
     * analysis is warmed within seconds even where the sweep itself cannot reach the store.
     *
     * @return true when the analysis already had, or now has, a complete sidecar set
     */
    public boolean warmOne(String name) {
        if (name == null || name.isBlank()) return false;
        if (views.hasCompleteViewSet(name)) return true;
        return warmByName(name) >= 0;
    }

    /**
     * The compute half of {@link #warmOne}, returning the number of sidecars written so the sweep
     * can report a total. Never throws; {@code -1} means the set could not be completed (and is
     * therefore retried next sweep).
     */
    private int warmByName(String name) {
        try {
            // The header is the completeness marker; write it only when missing (a fresh analysis
            // already has one from save time, so this is a no-op there).
            boolean headerNew = false;
            boolean header = views.readHeader(name) != null;
            if (!header) {
                // An older-format header means the facet sidecars beside it were written by that
                // older format too. Drop the whole set before rebuilding it: read() already refuses
                // them, but leaving them in place would let a facet whose read path does not
                // recompute (the reader-served node-scan facets) be served stale once the fresh
                // header lands. A genuinely absent header (a never-warmed analysis) has nothing to
                // drop, so this is a no-op there.
                if (views.isHeaderFormatStale(name)) {
                    views.invalidate(name);
                }
                header = analysisService.writeHeaderSidecar(name, false);
                headerNew = header;
            }
            int written = writer.writeAll(name, false);
            if (!header || written != AnalysisViewWriter.VIEW_COUNT) return -1;
            return written + (headerNew ? 1 : 0);
        } catch (Throwable t) {
            // Any error is one failed analysis, not a failed sweep, and is retried next time.
            log.debug("Sidecar warm-up failed for {}: {}", name, t.toString());
            return -1;
        }
    }

    /** The counts from one sweep, logged once and returned by the on-demand admin endpoint. */
    public record SweepStats(int analyses, int warmed, int alreadyComplete, int failed,
                             int sidecarsWritten, long durationMs, long finishedAt) {
    }
}
