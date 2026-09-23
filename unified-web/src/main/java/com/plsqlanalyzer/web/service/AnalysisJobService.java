package com.plsqlanalyzer.web.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/**
 * Tracks analysis jobs — submitted, running, complete, failed, cancelled.
 * Each analysis run becomes a job with a unique ID, live progress, and cancel support.
 */
public class AnalysisJobService {

    private static final Logger log = LoggerFactory.getLogger(AnalysisJobService.class);

    // Keep last 20 jobs in memory
    private static final int MAX_JOBS = 20;
    private final LinkedHashMap<String, Job> jobs = new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Job> eldest) {
            return size() > MAX_JOBS;
        }
    };

    public enum JobStatus {
        QUEUED, RUNNING, COMPLETE, FAILED, CANCELLED
    }

    public static class Job {
        public final String id;
        public final String schema;
        public final String objectName;
        public final String objectType;
        public final String procedureName;
        public final Instant submittedAt;
        public volatile JobStatus status;
        public volatile String currentStep;
        public volatile String lastMessage;
        public volatile Instant startedAt;
        public volatile Instant completedAt;
        public volatile String resultName;  // analysis name after save
        public volatile String error;
        public volatile int stepNumber;
        public volatile int totalSteps = 6;
        public final List<String> log = Collections.synchronizedList(new ArrayList<>());
        public volatile Future<?> future;  // for cancel support
        public volatile Thread runThread;  // for interrupt support
        public volatile String queueJobId; // link to unified queue job

        public Job(String id, String schema, String objectName, String objectType, String procedureName) {
            this.id = id;
            this.schema = schema;
            this.objectName = objectName;
            this.objectType = objectType;
            this.procedureName = procedureName;
            this.submittedAt = Instant.now();
            this.status = JobStatus.QUEUED;
            this.currentStep = "Queued";
        }

        public long elapsedMs() {
            Instant start = startedAt != null ? startedAt : submittedAt;
            Instant end = completedAt != null ? completedAt : Instant.now();
            return end.toEpochMilli() - start.toEpochMilli();
        }

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", id);
            m.put("schema", schema);
            m.put("objectName", objectName);
            m.put("objectType", objectType);
            m.put("procedureName", procedureName);
            m.put("status", status.name());
            m.put("currentStep", currentStep);
            m.put("lastMessage", lastMessage);
            m.put("stepNumber", stepNumber);
            m.put("totalSteps", totalSteps);
            m.put("submittedAt", submittedAt.toString());
            m.put("startedAt", startedAt != null ? startedAt.toString() : null);
            m.put("completedAt", completedAt != null ? completedAt.toString() : null);
            m.put("elapsedMs", elapsedMs());
            m.put("resultName", resultName);
            m.put("error", error);
            List<String> recent;
            synchronized (log) {
                recent = new ArrayList<>(log.subList(Math.max(0, log.size() - 50), log.size()));
            }
            m.put("log", recent);
            return m;
        }
    }

    /**
     * Create and register a new job.
     */
    public Job createJob(String schema, String objectName, String objectType, String procedureName) {
        String id = UUID.randomUUID().toString().substring(0, 8);
        Job job = new Job(id, schema, objectName, objectType, procedureName);
        synchronized (jobs) {
            jobs.put(id, job);
        }
        log.info("[JOB {}] Created: {}.{} type={} proc={}", id, schema, objectName, objectType, procedureName);
        return job;
    }

    /**
     * Mark job as running.
     */
    public void markRunning(Job job) {
        job.status = JobStatus.RUNNING;
        job.startedAt = Instant.now();
        job.currentStep = "Starting...";
        log.info("[JOB {}] Running", job.id);
    }

    /**
     * Update job progress (called by analysis callbacks).
     */
    public void updateProgress(Job job, String message) {
        job.lastMessage = message;
        job.log.add(message);
        // Parse step number from "[N/6]" pattern
        if (message.startsWith("[")) {
            int slash = message.indexOf('/');
            if (slash > 1) {
                try {
                    job.stepNumber = Integer.parseInt(message.substring(1, slash));
                } catch (NumberFormatException ignored) {}
            }
        }
        job.currentStep = message;
    }

    /**
     * Mark job as complete.
     */
    public void markComplete(Job job, String resultName) {
        job.status = JobStatus.COMPLETE;
        job.completedAt = Instant.now();
        job.resultName = resultName;
        job.currentStep = "Complete";
        log.info("[JOB {}] Complete in {}ms, result={}", job.id, job.elapsedMs(), resultName);
    }

    /**
     * Mark job as failed.
     */
    public void markFailed(Job job, String error) {
        job.status = JobStatus.FAILED;
        job.completedAt = Instant.now();
        job.error = error;
        job.currentStep = "Failed";
        log.info("[JOB {}] Failed: {}", job.id, error);
    }

    /**
     * Cancel a running job.
     */
    public boolean cancel(String jobId) {
        Job job;
        synchronized (jobs) {
            job = jobs.get(jobId);
        }
        if (job == null) return false;
        if (job.status != JobStatus.RUNNING && job.status != JobStatus.QUEUED) return false;

        job.status = JobStatus.CANCELLED;
        job.completedAt = Instant.now();
        job.currentStep = "Cancelled";
        log.info("[JOB {}] Cancelled", jobId);

        // Interrupt the thread
        if (job.future != null) {
            job.future.cancel(true);
        }
        if (job.runThread != null) {
            job.runThread.interrupt();
        }
        return true;
    }

    /**
     * Check if a job has been cancelled (for cooperative cancellation).
     */
    public boolean isCancelled(Job job) {
        return job.status == JobStatus.CANCELLED;
    }

    /**
     * Get a specific job.
     */
    public Job getJob(String jobId) {
        synchronized (jobs) {
            return jobs.get(jobId);
        }
    }

    /**
     * Find a legacy job by its linked queue job ID.
     */
    public Job getJobByQueueId(String queueJobId) {
        if (queueJobId == null) return null;
        synchronized (jobs) {
            for (Job job : jobs.values()) {
                if (queueJobId.equals(job.queueJobId)) {
                    return job;
                }
            }
        }
        return null;
    }

    /**
     * Get the currently running job (if any).
     */
    public Job getRunningJob() {
        synchronized (jobs) {
            for (Job job : jobs.values()) {
                if (job.status == JobStatus.RUNNING || job.status == JobStatus.QUEUED) {
                    return job;
                }
            }
        }
        return null;
    }

    /**
     * List all jobs (newest first).
     */
    public List<Map<String, Object>> listJobs() {
        List<Map<String, Object>> result = new ArrayList<>();
        synchronized (jobs) {
            List<Job> jobList = new ArrayList<>(jobs.values());
            Collections.reverse(jobList); // newest first
            for (Job job : jobList) {
                result.add(job.toMap());
            }
        }
        return result;
    }
}
