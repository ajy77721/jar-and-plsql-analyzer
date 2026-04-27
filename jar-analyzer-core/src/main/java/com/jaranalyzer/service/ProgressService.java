package com.jaranalyzer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Tracks analysis progress and streams updates to SSE clients.
 * Simple design: one analysis at a time; each update is pushed to all connected emitters.
 */
@Service("jarProgressService")
public class ProgressService {

    private static final Logger log = LoggerFactory.getLogger(ProgressService.class);

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    /**
     * Register a new SSE emitter. Returns it configured with cleanup callbacks.
     */
    public SseEmitter register() {
        SseEmitter emitter = new SseEmitter(-1L); // no timeout — alive as long as progress flows
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        return emitter;
    }

    /**
     * Signal the start of a new analysis.
     */
    public void start(String jarName) {
        send("start", "Analyzing " + jarName + "...");
    }

    /**
     * Send a step-level progress update (e.g. "[1/5] Saving uploaded file...").
     */
    public void step(String message) {
        send("step", message);
    }

    /**
     * Send a detail-level progress update (sub-step info).
     */
    public void detail(String message) {
        send("detail", message);
    }

    /**
     * Signal that analysis is complete.
     */
    public void complete(String message) {
        send("complete", message);
        // Complete all emitters so the client EventSource gets a clean close
        for (SseEmitter emitter : emitters) {
            try {
                emitter.complete();
            } catch (Exception ignored) {}
        }
        emitters.clear();
    }

    /**
     * Signal that analysis failed.
     */
    public void error(String message) {
        send("error", message);
        for (SseEmitter emitter : emitters) {
            try {
                emitter.complete();
            } catch (Exception ignored) {}
        }
        emitters.clear();
    }

    private void send(String type, String message) {
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event()
                        .name("progress")
                        .data(type + "|" + message));
            } catch (IOException e) {
                emitters.remove(emitter);
            }
        }
    }
}
