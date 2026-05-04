package com.analyzer.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/config")
public class PollingConfigController {

    @Value("${app.poll.session-ms:30000}")
    private int sessionPollMs;

    @Value("${app.poll.claude-progress-ms:30000}")
    private int claudeProgressMs;

    @Value("${app.poll.job-status-ms:10000}")
    private int jobStatusMs;

    @Value("${app.poll.log-refresh-ms:10000}")
    private int logRefreshMs;

    @Value("${app.poll.correction-ms:30000}")
    private int correctionMs;

    @GetMapping("/polling")
    public Map<String, Object> getPollingConfig() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("sessionPollMs", sessionPollMs);
        config.put("claudeProgressMs", claudeProgressMs);
        config.put("jobStatusMs", jobStatusMs);
        config.put("logRefreshMs", logRefreshMs);
        config.put("correctionMs", correctionMs);
        return config;
    }
}
