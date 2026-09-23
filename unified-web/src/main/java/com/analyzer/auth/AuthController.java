package com.analyzer.auth;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

    private final AnthropicKeyValidator validator;

    public AuthController(AnthropicKeyValidator validator) {
        this.validator = validator;
    }

    @PostMapping("/login")
    public Map<String, Object> login(@RequestBody Map<String, String> body, HttpServletRequest req) {
        String apiKey = body.get("apiKey");
        if (apiKey == null || apiKey.isBlank()) {
            return Map.of("ok", false, "error", "API key is required");
        }
        if (!validator.validate(apiKey)) {
            return Map.of("ok", false, "error", "Invalid Anthropic API key — check console.anthropic.com");
        }
        HttpSession session = req.getSession(true);
        session.setAttribute(ApiKeyAuthInterceptor.SESSION_KEY, apiKey);
        return Map.of("ok", true);
    }

    @PostMapping("/logout")
    public Map<String, Object> logout(HttpServletRequest req) {
        HttpSession session = req.getSession(false);
        if (session != null) session.invalidate();
        return Map.of("ok", true);
    }

    @GetMapping("/status")
    public Map<String, Object> status(HttpServletRequest req) {
        HttpSession session = req.getSession(false);
        boolean auth = session != null && session.getAttribute(ApiKeyAuthInterceptor.SESSION_KEY) != null;
        return Map.of("authenticated", auth);
    }
}
