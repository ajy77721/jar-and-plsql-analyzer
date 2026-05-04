package com.analyzer.queue;

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
    private final WarAnalysisExecutor warExecutor;
    private final PlsqlAnalysisExecutor plsqlExecutor;
    private final FastAnalysisExecutor fastExecutor;
    private final ParserAnalysisExecutor parserExecutor;
    private final ClaudeJobExecutor claudeExecutor;

    public AnalysisQueueService(JarAnalysisExecutor jarExecutor,
                                 WarAnalysisExecutor warExecutor,
                                 PlsqlAnalysisExecutor plsqlExecutor,
                                 FastAnalysisExecutor fastExecutor,
                                 ParserAnalysisExecutor parserExecutor,
                                 ClaudeJobExecutor claudeExecutor) {
        this.jarExecutor = jarExecutor;
        this.warExecutor = warExecutor;
        this.plsqlExecutor = plsqlExecutor;
        this.fastExecutor = fastExecutor;
        this.parserExecutor = parserExecutor;
        this.claudeExecutor = claudeExecutor;
    }

    public QueueJob submit(QueueJob.Type type, String displayName, Map<String, Object> metadata) {
        QueueJob job = new QueueJob(type, displayName, metadata);
        synchronized (lock) {
            pendingQueue.add(job);
        }
        log.info("[QUEUE] Job submitted: {} — {}", job.id, displayName);
        broadcastUpdate(job, "job-queued");
        processNext();
        return job;
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
            emitter.send(SseEmitter.event().name("state").data(getState()));
        } catch (IOException ignored) {}
        return emitter;
    }

    public void broadcastUpdate(QueueJob job, String eventType) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("eventType", eventType);
        event.put("job", job.toMap());

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
                switch (fJob.type) {
                    case JAR_UPLOAD -> jarExecutor.execute(fJob, broadcast);
                    case WAR_UPLOAD -> warExecutor.execute(fJob, broadcast);
                    case PLSQL_ANALYSIS -> plsqlExecutor.execute(fJob, broadcast);
                    case PLSQL_FAST_ANALYSIS -> fastExecutor.execute(fJob, broadcast);
                    case PARSER_ANALYSIS -> parserExecutor.execute(fJob, broadcast);
                    case CLAUDE_ENRICH, CLAUDE_ENRICH_SINGLE, CLAUDE_RESCAN,
                         CLAUDE_FULL_SCAN, CLAUDE_CORRECT, CLAUDE_CORRECT_SINGLE,
                         PLSQL_CLAUDE_VERIFY -> claudeExecutor.execute(fJob, broadcast);
                }
                if (fJob.status == QueueJob.Status.RUNNING) {
                    fJob.status = QueueJob.Status.COMPLETE;
                    fJob.completedAt = Instant.now();
                    broadcastUpdate(fJob, "job-complete");
                }
            } catch (Exception e) {
                if (fJob.status == QueueJob.Status.CANCELLED) {
                    // completedAt and broadcast already handled by cancel()
                } else {
                    fJob.status = QueueJob.Status.FAILED;
                    fJob.error = e.getMessage();
                    fJob.completedAt = Instant.now();
                    log.error("[QUEUE] Job failed: {} — {}", fJob.id, e.getMessage(), e);
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
                        submit(fType, fDisplay, followUp);
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
