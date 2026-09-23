package com.analyzer.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

@Component
public class AnthropicKeyValidator {

    private static final Logger log = LoggerFactory.getLogger(AnthropicKeyValidator.class);
    private static final String MODELS_URL = "https://api.anthropic.com/v1/models";

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(8))
            .build();

    public boolean validate(String apiKey) {
        if (apiKey == null || apiKey.isBlank()) return false;
        if (!apiKey.startsWith("sk-ant-")) return false;
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(MODELS_URL))
                    .header("x-api-key", apiKey)
                    .header("anthropic-version", "2023-06-01")
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();
            HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
            boolean ok = res.statusCode() == 200;
            if (!ok) log.debug("Anthropic key validation failed — HTTP {}", res.statusCode());
            return ok;
        } catch (Exception e) {
            log.warn("Anthropic key validation error: {}", e.getMessage());
            return false;
        }
    }
}
