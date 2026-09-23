package com.analyzer.queue;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api/queue")
public class QueueController {

    private final AnalysisQueueService queueService;

    public QueueController(AnalysisQueueService queueService) {
        this.queueService = queueService;
    }

    /**
     * GET /api/queue[?kind=&queuedLimit=&historyLimit=]
     *
     * <p>With no parameters this is byte-for-byte the response it has always been, so an
     * unconverted caller sees no change. With any of them, the filter and the bounds are applied
     * here instead of in the browser: the parser Queue tab wanted four PL/SQL job kinds out of a
     * global queue, and {@code plsql/js/claude.js} wanted two kinds and three history rows — and
     * both were polling every 2–3 seconds for the whole thing.
     *
     * @param kind comma-separated {@link QueueJob.Type} names. An unknown name is a 400 rather
     *             than a silently empty result, which would look exactly like an idle queue.
     */
    @GetMapping
    public Map<String, Object> getQueue(@RequestParam(required = false) String kind,
                                        @RequestParam(required = false) Integer queuedLimit,
                                        @RequestParam(required = false) Integer historyLimit) {
        if (kind == null && queuedLimit == null && historyLimit == null) {
            return queueService.getState();
        }
        return queueService.getState(parseKinds(kind),
                queuedLimit == null ? -1 : queuedLimit,
                historyLimit == null ? -1 : historyLimit);
    }

    private static Set<QueueJob.Type> parseKinds(String kind) {
        if (kind == null || kind.isBlank()) return Set.of();
        Set<QueueJob.Type> kinds = EnumSet.noneOf(QueueJob.Type.class);
        for (String raw : kind.split(",")) {
            String name = raw.trim().toUpperCase(Locale.ROOT);
            if (name.isEmpty()) continue;
            try {
                kinds.add(QueueJob.Type.valueOf(name));
            } catch (IllegalArgumentException e) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "Unknown job kind '" + raw.trim() + "'");
            }
        }
        return kinds;
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
