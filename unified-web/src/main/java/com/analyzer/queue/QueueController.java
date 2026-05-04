package com.analyzer.queue;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/queue")
public class QueueController {

    private final AnalysisQueueService queueService;

    public QueueController(AnalysisQueueService queueService) {
        this.queueService = queueService;
    }

    @GetMapping
    public Map<String, Object> getQueue() {
        return queueService.getState();
    }

    @GetMapping("/{id}")
    public Map<String, Object> getJob(@PathVariable String id) {
        QueueJob job = queueService.getJob(id);
        if (job == null) return Map.of("error", "Job not found: " + id);
        return job.toMap();
    }

    @PostMapping("/{id}/cancel")
    public Map<String, Object> cancelJob(@PathVariable String id) {
        boolean cancelled = queueService.cancel(id);
        return Map.of("jobId", id, "cancelled", cancelled);
    }

    @PostMapping("/reorder")
    public Map<String, Object> reorder(@RequestBody Map<String, List<String>> body) {
        List<String> order = body.get("order");
        if (order == null) return Map.of("error", "Missing 'order' array");
        queueService.reorder(order);
        return Map.of("status", "reordered");
    }

    @GetMapping(value = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter events() {
        return queueService.createEmitter();
    }
}
