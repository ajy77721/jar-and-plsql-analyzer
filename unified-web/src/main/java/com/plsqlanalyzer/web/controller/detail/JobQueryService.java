package com.plsqlanalyzer.web.controller.detail;

import com.plsqlanalyzer.web.service.AnalysisJobService;
import com.plsqlanalyzer.web.service.AnalysisJobService.Job;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Job listing/status endpoints (extracted from AnalyzerController). */
@Service
public class JobQueryService {

    private final AnalysisJobService jobService;

    public JobQueryService(AnalysisJobService jobService) {
        this.jobService = jobService;
    }

    public ResponseEntity<List<Map<String, Object>>> listJobs() {
        return ResponseEntity.ok(jobService.listJobs());
    }

    public ResponseEntity<Map<String, Object>> getJob(String jobId) {
        Job job = jobService.getJob(jobId);
        if (job == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(job.toMap());
    }

    public ResponseEntity<Map<String, Object>> cancelJob(String jobId) {
        boolean cancelled = jobService.cancel(jobId);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("jobId", jobId);
        response.put("cancelled", cancelled);
        return ResponseEntity.ok(response);
    }
}
