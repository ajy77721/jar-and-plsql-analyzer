package com.plsqlanalyzer.web.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Manages SSE (Server-Sent Events) connections for progress streaming.
 * Analysis steps push progress updates, and all connected SSE clients receive them.
 */
@Service("plsqlProgressService")
public class ProgressService {

    private static final Logger log = LoggerFactory.getLogger(ProgressService.class);
    private static final long SSE_TIMEOUT = 600_000L; // 10 minutes

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public SseEmitter createEmitter() {
        SseEmitter emitter = new SseEmitter(SSE_TIMEOUT);
        emitters.add(emitter);
        log.info("SSE emitter created, active connections: {}", emitters.size());
        emitter.onCompletion(() -> {
            emitters.remove(emitter);
            log.info("SSE emitter completed, active connections: {}", emitters.size());
        });
        emitter.onTimeout(() -> {
            emitters.remove(emitter);
            log.info("SSE emitter timed out, active connections: {}", emitters.size());
        });
        emitter.onError(e -> {
            emitters.remove(emitter);
            log.info("SSE emitter error: {}, active connections: {}", e.getMessage(), emitters.size());
        });
        return emitter;
    }

    /**
     * Send a progress event to all connected clients.
     */
    public void sendProgress(String message) {
        log.info("Sending progress event to {} clients: {}", emitters.size(), message);
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event()
                        .name("progress")
                        .data(message));
            } catch (IOException e) {
                emitters.remove(emitter);
            }
        }
    }

    /**
     * Send a completion event and close all emitters.
     */
    public void sendComplete(String message) {
        log.info("Sending complete event to {} clients: {}", emitters.size(), message);
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event()
                        .name("complete")
                        .data(message));
                emitter.complete();
            } catch (IOException e) {
                // ignore
            }
        }
        emitters.clear();
    }

    /**
     * Send an error event.
     */
    public void sendError(String message) {
        log.info("Sending error event to {} clients: {}", emitters.size(), message);
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event()
                        .name("error")
                        .data(message));
                emitter.complete();
            } catch (IOException e) {
                // ignore
            }
        }
        emitters.clear();
    }

    public int getActiveConnections() {
        return emitters.size();
    }
}
