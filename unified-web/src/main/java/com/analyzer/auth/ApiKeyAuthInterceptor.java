package com.analyzer.auth;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.io.IOException;

@Component
public class ApiKeyAuthInterceptor implements HandlerInterceptor {

    public static final String SESSION_KEY = "auth.api.key";

    private static boolean isPublic(String path) {
        return path.equals("/login")
                || path.equals("/login.html")
                || path.startsWith("/api/auth/")
                || path.equals("/favicon.ico")
                || path.equals("/error");
    }

    @Override
    public boolean preHandle(HttpServletRequest req, HttpServletResponse res, Object handler) throws IOException {
        String path = req.getRequestURI();
        if (isPublic(path)) return true;

        HttpSession session = req.getSession(false);
        if (session != null && session.getAttribute(SESSION_KEY) != null) return true;

        if (path.startsWith("/api/")) {
            res.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            res.setContentType("application/json;charset=UTF-8");
            res.getWriter().write("{\"error\":\"Not authenticated\",\"code\":401}");
            return false;
        }
        res.sendRedirect(req.getContextPath() + "/login");
        return false;
    }
}
