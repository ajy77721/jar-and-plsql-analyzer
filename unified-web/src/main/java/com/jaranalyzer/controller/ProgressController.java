package com.jaranalyzer.controller;

import com.jaranalyzer.service.ProgressService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * SSE endpoint for real-time analysis progress streaming.
 */
@RestController("jarProgressController")
public class ProgressController {

    private final ProgressService progressService;

    public ProgressController(ProgressService progressService) {
        this.progressService = progressService;
    }

    @GetMapping(path = "/api/jar/progress", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamProgress() {
        return progressService.register();
    }

    @GetMapping(path = "/api/war/progress", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamWarProgress() {
        return progressService.register();
    }
}
