package com.analyzer.queue;

import com.analyzer.auth.ConnectionGuard;
import com.analyzer.storage.sso.CapturedCredentials;
import com.analyzer.storage.sso.CapturedStores;
import com.analyzer.storage.sso.SsoCredentialsResolver;
import com.analyzer.storage.sso.SsoJobContext;
import com.analyzer.storage.sso.SsoStoreSwapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

@Service
public class AnalysisQueueService {

    private static final Logger log = LoggerFactory.getLogger(AnalysisQueueService.class);
    private static final int MAX_HISTORY = 50;

    private final LinkedList<QueueJob> pendingQueue = new LinkedList<>();
    private final LinkedList<QueueJob> history = new LinkedList<>();
    private volatile QueueJob currentJob;
    private final Object lock = new Object();

    private final ExecutorService worker = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "analysis-queue-worker");
        t.setDaemon(true);
        return t;
    });

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    private final JarAnalysisExecutor jarExecutor;
    private final JarTargetedAnalysisExecutor jarTargetedExecutor;
    private final WarAnalysisExecutor warExecutor;
    private final PlsqlAnalysisExecutor plsqlExecutor;
    private final FastAnalysisExecutor fastExecutor;
    private final ParserAnalysisExecutor parserExecutor;
    private final ParserValidationExecutor parserValidationExecutor;
    private final ClaudeJobExecutor claudeExecutor;
    private final JarExportBundleExecutor jarExportBundleExecutor;
    private final PurposeSummaryExecutor purposeSummaryExecutor;

    /** Snapshots the submitter's per-user stores at submit time, on the request thread. */
    private final SsoStoreSwapper ssoStoreSwapper;
    /** Snapshots the submitter's role credentials for jobs whose calls sign with them directly. */
    private final SsoCredentialsResolver ssoCredentialsResolver;

    /** Fails a submit fast when the caller is logged in but has not connected a bucket. */
    private final ConnectionGuard connectionGuard;

    public AnalysisQueueService(JarAnalysisExecutor jarExecutor,
                                 JarTargetedAnalysisExecutor jarTargetedExecutor,
                                 WarAnalysisExecutor warExecutor,
                                 PlsqlAnalysisExecutor plsqlExecutor,
                                 FastAnalysisExecutor fastExecutor,
                                 ParserAnalysisExecutor parserExecutor,
                                 ParserValidationExecutor parserValidationExecutor,
                                 ClaudeJobExecutor claudeExecutor,
                                 JarExportBundleExecutor jarExportBundleExecutor,
                                 PurposeSummaryExecutor purposeSummaryExecutor,
                                 SsoStoreSwapper ssoStoreSwapper,
                                 SsoCredentialsResolver ssoCredentialsResolver,
                                 ConnectionGuard connectionGuard) {
        this.jarExecutor = jarExecutor;
        this.jarTargetedExecutor = jarTargetedExecutor;
        this.warExecutor = warExecutor;
        this.plsqlExecutor = plsqlExecutor;
        this.fastExecutor = fastExecutor;
        this.parserExecutor = parserExecutor;
        this.parserValidationExecutor = parserValidationExecutor;
        this.claudeExecutor = claudeExecutor;
        this.jarExportBundleExecutor = jarExportBundleExecutor;
        this.purposeSummaryExecutor = purposeSummaryExecutor;
        this.ssoStoreSwapper = ssoStoreSwapper;
        this.ssoCredentialsResolver = ssoCredentialsResolver;
        this.connectionGuard = connectionGuard;
    }

    /**
     * Submits a job, capturing the calling user's per-user SSO state <em>now</em>, while the request
     * thread still exists, and attaching it to the in-memory job for the queue thread to install.
     *
     * <p>This is the single submit point every controller reaches, so capturing here rather than in
     * each of them cannot be forgotten by a new endpoint. {@link SsoStoreSwapper#captureCurrent()}
     * and {@link SsoCredentialsResolver#captureCurrent()} both answer {@code null} when the thread
     * has no SSO-bound session — the documented legacy path (see {@code QueueJob}).
     */
    public QueueJob submit(QueueJob.Type type, String displayName, Map<String, Object> metadata) {
        // Capture the submitter's stores first, then fail fast if they are logged in but have not
        // connected a bucket and this job's storage is S3-backed. This single submit point covers
        // every controller that reaches the queue, so a new endpoint cannot forget the guard.
        CapturedStores capturedStores = ssoStoreSwapper.captureCurrent();
        connectionGuard.requireConnected(featureOf(type));
        return submit(type, displayName, metadata,
                capturedStores,
                needsCredentials(type) ? ssoCredentialsResolver.captureCurrent() : null);
    }

    /**
     * Which SSO feature's store this job writes through — the PL/SQL parser's or the JAR
     * analyzer's. Used to apply the connect guard against the right storage mode, since the two
     * features can run on different backends ({@code app.storage.parser.mode} vs
     * {@code app.storage.jar.mode}).
     */
    private static String featureOf(QueueJob.Type type) {
        return switch (type) {
            case PLSQL_ANALYSIS, PLSQL_FAST_ANALYSIS, PARSER_ANALYSIS, PARSER_VALIDATION,
                 PLSQL_CLAUDE_VERIFY, PLSQL_PURPOSE_SUMMARY -> ConnectionGuard.PARSER;
            default -> ConnectionGuard.JAR;
        };
    }

    /**
     * Captures both snapshots explicitly. Used for a follow-up job, which is submitted from the queue
     * worker — a thread with no request context — and therefore inherits its parent's snapshot rather
     * than trying (and failing) to capture its own.
     */
    private QueueJob submit(QueueJob.Type type, String displayName, Map<String, Object> metadata,
                            CapturedStores capturedStores, CapturedCredentials capturedCredentials) {
        QueueJob job = new QueueJob(type, displayName, metadata);
        job.capturedStores = capturedStores;
        job.capturedCredentials = capturedCredentials;
        synchronized (lock) {
            pendingQueue.add(job);
        }
        log.info("[QUEUE] Job submitted: {} — {}", job.id, displayName);
        broadcastUpdate(job, "job-queued");
        processNext();
        return job;
    }

    /**
     * True for the job kinds whose code signs AWS calls with a credential provider directly (the
     * Claude/Bedrock path), so the submitter's role is captured alongside their stores. The
     * artifact-only jobs get their credentials from the captured stores' own clients and need no
     * separate snapshot.
     */
    private static boolean needsCredentials(QueueJob.Type type) {
        return switch (type) {
            case CLAUDE_ENRICH, CLAUDE_ENRICH_SINGLE, CLAUDE_RESCAN, CLAUDE_FULL_SCAN,
                 CLAUDE_CORRECT, CLAUDE_CORRECT_SINGLE, PLSQL_CLAUDE_VERIFY,
                 PLSQL_PURPOSE_SUMMARY -> true;
            default -> false;
        };
    }

    public boolean cancel(String jobId) {
        synchronized (lock) {
            Iterator<QueueJob> it = pendingQueue.iterator();
            while (it.hasNext()) {
                QueueJob job = it.next();
                if (job.id.equals(jobId)) {
                    it.remove();
                    job.status = QueueJob.Status.CANCELLED;
                    job.completedAt = Instant.now();
                    history.addFirst(job);
                    trimHistory();
                    log.info("[QUEUE] Cancelled queued job: {}", jobId);
                    broadcastUpdate(job, "job-cancelled");
                    return true;
                }
            }
        }
        QueueJob running = currentJob;
        if (running != null && running.id.equals(jobId)) {
            running.status = QueueJob.Status.CANCELLED;
            running.completedAt = Instant.now();
            Thread t = running.runThread;
            if (t != null) t.interrupt();
            log.info("[QUEUE] Cancelling running job: {}", jobId);
            broadcastUpdate(running, "job-cancelled");
            return true;
        }
        return false;
    }

    public boolean reorder(List<String> orderedIds) {
        synchronized (lock) {
            Map<String, QueueJob> jobMap = new LinkedHashMap<>();
            for (QueueJob j : pendingQueue) jobMap.put(j.id, j);

            LinkedList<QueueJob> reordered = new LinkedList<>();
            for (String id : orderedIds) {
                QueueJob j = jobMap.remove(id);
                if (j != null) reordered.add(j);
            }
            reordered.addAll(jobMap.values());

            pendingQueue.clear();
            pendingQueue.addAll(reordered);
        }
        broadcastFullState();
        return true;
    }

    public Map<String, Object> getState() {
        synchronized (lock) {
            Map<String, Object> state = new LinkedHashMap<>();
            state.put("current", currentJob != null ? currentJob.toMap() : null);
            state.put("queued", pendingQueue.stream().map(QueueJob::toMap).toList());
            state.put("history", history.stream().map(QueueJob::toMap).toList());
            return state;
        }
    }

    /**
     * The queue, filtered and bounded on the server.
     *
     * <p>Q7 in PLAN-08 §1.1. Both queue screens fetched the whole global queue — current, every
     * pending job and 50 history entries, each with a 50-line log tail — and then filtered it in
     * the browser to the job kinds they care about. {@code plsql/js/claude.js} shipped all of that
     * to render at most three history rows.
     *
     * <p>Every parameter is optional and absence means "no filter", so {@link #getState()} stays the
     * exact response it always was and the two callers convert independently.
     *
     * @param kinds        job types to keep, or null/empty for all
     * @param queuedLimit  maximum pending jobs, or a negative number for all
     * @param historyLimit maximum finished jobs, most recent first, or a negative number for all
     */
    public Map<String, Object> getState(Set<QueueJob.Type> kinds, int queuedLimit,
                                        int historyLimit) {
        synchronized (lock) {
            Map<String, Object> state = new LinkedHashMap<>();
            QueueJob cur = currentJob;
            state.put("current", cur != null && matches(cur, kinds) ? cur.toSummaryMap() : null);
            state.put("queued", project(pendingQueue, kinds, queuedLimit));
            state.put("history", project(history, kinds, historyLimit));
            return state;
        }
    }

    private static boolean matches(QueueJob job, Set<QueueJob.Type> kinds) {
        return kinds == null || kinds.isEmpty() || kinds.contains(job.type);
    }

    private static List<Map<String, Object>> project(List<QueueJob> jobs, Set<QueueJob.Type> kinds,
                                                     int limit) {
        // Filter, then limit: limiting first would return fewer matches than asked for whenever
        // the head of the list happens to be a kind the caller does not want.
        Stream<QueueJob> s = jobs.stream().filter(j -> matches(j, kinds));
        if (limit >= 0) s = s.limit(limit);
        return s.map(QueueJob::toSummaryMap).toList();
    }

    public QueueJob getJob(String jobId) {
        synchronized (lock) {
            if (currentJob != null && currentJob.id.equals(jobId)) return currentJob;
            for (QueueJob j : pendingQueue) if (j.id.equals(jobId)) return j;
            for (QueueJob j : history) if (j.id.equals(jobId)) return j;
            return null;
        }
    }

    public SseEmitter createEmitter() {
        SseEmitter emitter = new SseEmitter(1_800_000L);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));

        try {
            // Wrapped as {eventType, state}, the same shape every other event carries, so a
            // listener reads d.state uniformly instead of branching on which event arrived.
            emitter.send(SseEmitter.event().name("state")
                    .data(Map.of("eventType", "state", "state", getState())));
        } catch (IOException ignored) {}
        return emitter;
    }

    public void broadcastUpdate(QueueJob job, String eventType) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("eventType", eventType);
        event.put("job", job.toMap());
        // An event carries the state; it never means "go fetch". Without this, every listener
        // answered a job-progress event with a full GET /api/queue, so a burst of jobs became a
        // burst of requests multiplied by the number of open tabs — the amplification PLAN-08
        // §5.1 lists as queue defect 4. Assembling it here is one in-process map build, shared by
        // every subscriber.
        event.put("state", getState());

        List<SseEmitter> dead = new ArrayList<>();
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name(eventType).data(event));
            } catch (Exception e) {
                dead.add(emitter);
            }
        }
        emitters.removeAll(dead);
    }

    private void broadcastFullState() {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("eventType", "state");
        event.put("state", getState());

        List<SseEmitter> dead = new ArrayList<>();
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name("state").data(event));
            } catch (Exception e) {
                dead.add(emitter);
            }
        }
        emitters.removeAll(dead);
    }

    private void processNext() {
        QueueJob job;
        synchronized (lock) {
            if (currentJob != null) return;
            if (pendingQueue.isEmpty()) return;

            job = pendingQueue.poll();
            currentJob = job;
            job.status = QueueJob.Status.RUNNING;
            job.startedAt = Instant.now();
            job.currentStep = "Starting...";
        }

        log.info("[QUEUE] Starting job: {} — {}", job.id, job.displayName);
        broadcastUpdate(job, "job-started");

        final QueueJob fJob = job;
        BiConsumer<QueueJob, String> broadcast = this::broadcastUpdate;

        worker.submit(() -> {
            fJob.runThread = Thread.currentThread();
            try {
                // Run under the submitter's snapshot, when one was captured. install() returns null
                // (and installs nothing) for a job with no SSO session, leaving the legacy shared-bean
                // / default-credentials path untouched; try-with-resources then restores whatever was
                // in force. This covers every executor uniformly — and, crucially, the store writes
                // the executors make through PersistenceService/ClaudeAnalysisService, which each hold
                // the same routing bean rather than the job's own field.
                try (SsoJobContext sso = SsoJobContext.install(fJob.capturedStores, fJob.capturedCredentials)) {
                    switch (fJob.type) {
                        case JAR_UPLOAD -> jarExecutor.execute(fJob, broadcast);
                        case JAR_TARGETED -> jarTargetedExecutor.execute(fJob, broadcast);
                        case WAR_UPLOAD -> warExecutor.execute(fJob, broadcast);
                        case PLSQL_ANALYSIS -> plsqlExecutor.execute(fJob, broadcast);
                        case PLSQL_FAST_ANALYSIS -> fastExecutor.execute(fJob, broadcast);
                        case PARSER_ANALYSIS -> parserExecutor.execute(fJob, broadcast);
                        case PARSER_VALIDATION -> parserValidationExecutor.execute(fJob, broadcast);
                        case CLAUDE_ENRICH, CLAUDE_ENRICH_SINGLE, CLAUDE_RESCAN,
                             CLAUDE_FULL_SCAN, CLAUDE_CORRECT, CLAUDE_CORRECT_SINGLE,
                             PLSQL_CLAUDE_VERIFY -> claudeExecutor.execute(fJob, broadcast);
                        case JAR_EXPORT_BUNDLE -> jarExportBundleExecutor.execute(fJob, broadcast);
                        case PLSQL_PURPOSE_SUMMARY -> purposeSummaryExecutor.execute(fJob, broadcast);
                    }
                }
                if (fJob.status == QueueJob.Status.RUNNING) {
                    fJob.status = QueueJob.Status.COMPLETE;
                    fJob.completedAt = Instant.now();
                    broadcastUpdate(fJob, "job-complete");
                }
            } catch (Throwable e) {
                // Throwable, not Exception: a large analysis can exhaust the heap, and an
                // OutOfMemoryError escaping here left the job stuck as RUNNING forever with no
                // job-failed broadcast — the UI showed a spinner that never resolved. Catching
                // it lets the job be reported FAILED like any other error. The worker thread
                // survives and processNext() in the finally block keeps the queue draining.
                if (fJob.status == QueueJob.Status.CANCELLED) {
                    // completedAt and broadcast already handled by cancel()
                } else {
                    fJob.status = QueueJob.Status.FAILED;
                    // OOM and friends usually carry a null message; fall back to the type name
                    // so the UI shows "OutOfMemoryError" rather than an empty error field.
                    fJob.error = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                    fJob.completedAt = Instant.now();
                    log.error("[QUEUE] Job failed: {} — {}", fJob.id, fJob.error, e);
                    broadcastUpdate(fJob, "job-failed");
                }
            } finally {
                fJob.runThread = null;
                Map<String, Object> followUp = fJob.followUpJob;
                synchronized (lock) {
                    history.addFirst(fJob);
                    trimHistory();
                    currentJob = null;
                }
                if (followUp != null && fJob.status == QueueJob.Status.COMPLETE) {
                    try {
                        QueueJob.Type fType = QueueJob.Type.valueOf((String) followUp.get("type"));
                        String fDisplay = (String) followUp.get("displayName");
                        followUp.remove("type");
                        followUp.remove("displayName");
                        // Inherit the parent's SSO snapshot: this runs on the queue worker, so a
                        // fresh captureCurrent() would see no request context and hand the follow-up
                        // the default credentials — the exact fallback the snapshot exists to avoid.
                        submit(fType, fDisplay, followUp, fJob.capturedStores, fJob.capturedCredentials);
                    } catch (Exception e) {
                        log.warn("[QUEUE] Failed to submit follow-up job: {}", e.getMessage());
                    }
                }
                processNext();
            }
        });
    }
    private void trimHistory() {
        while (history.size() > MAX_HISTORY) history.removeLast();
    }

    @PreDestroy
    public void shutdown() {
        worker.shutdownNow();
    }
}
