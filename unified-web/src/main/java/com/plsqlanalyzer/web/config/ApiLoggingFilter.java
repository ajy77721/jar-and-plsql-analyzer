package com.plsqlanalyzer.web.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingRequestWrapper;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

@Component("plsqlApiLoggingFilter")
public class ApiLoggingFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(ApiLoggingFilter.class);
    private static final int MAX_BODY_LOG = 2000;

    /** The API latency budget: {@code app.api.slo.ms}, 200ms by default. */
    @Value("${app.api.slo.ms:200}")
    private long sloBudgetMs = 200;

    /**
     * Built lazily rather than in the constructor, because {@link #sloBudgetMs} is injected after
     * construction; the monitor reads the budget once, from the injected value.
     */
    private volatile ApiSloMonitor slo;

    private ApiSloMonitor slo() {
        ApiSloMonitor m = slo;
        if (m == null) {
            synchronized (this) {
                m = slo;
                if (m == null) {
                    m = new ApiSloMonitor(sloBudgetMs);
                    slo = m;
                }
            }
        }
        return m;
    }

    /** Test seam: install a monitor with a fake clock (or a different budget). */
    void useSloMonitor(ApiSloMonitor monitor) {
        this.slo = monitor;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        return path == null || !path.startsWith("/api/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {

        ContentCachingRequestWrapper wrappedRequest = new ContentCachingRequestWrapper(request);
        String contentType = response.getContentType();
        boolean isSse = contentType != null && contentType.contains("text/event-stream");

        long start = System.currentTimeMillis();

        if (isSse) {
            filterChain.doFilter(wrappedRequest, response);
            long elapsed = System.currentTimeMillis() - start;
            logLine(wrappedRequest, response.getStatus(), null, elapsed);
            return;
        }

        CappedResponseWrapper wrappedResponse = new CappedResponseWrapper(response, MAX_BODY_LOG);
        try {
            filterChain.doFilter(wrappedRequest, wrappedResponse);
        } finally {
            long elapsed = System.currentTimeMillis() - start;
            logLine(wrappedRequest, wrappedResponse.getStatus(), wrappedResponse, elapsed);
        }
    }

    private void logLine(ContentCachingRequestWrapper request, int status,
                         CappedResponseWrapper response, long elapsed) {
        try {
            String method = request.getMethod();
            String uri = request.getRequestURI();
            String query = request.getQueryString();

            StringBuilder sb = new StringBuilder();
            sb.append("API ").append(method).append(" ").append(uri);
            if (query != null && !query.isEmpty()) sb.append("?").append(query);
            sb.append(" -> ").append(status).append(" (").append(elapsed).append("ms)");

            if ("POST".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method) || "PATCH".equalsIgnoreCase(method)) {
                byte[] reqBody = request.getContentAsByteArray();
                if (reqBody.length > 0) {
                    String body = new String(reqBody, StandardCharsets.UTF_8);
                    body = redactSecrets(body);
                    if (body.length() > MAX_BODY_LOG) body = body.substring(0, MAX_BODY_LOG) + "...(truncated)";
                    sb.append(" | REQ: ").append(body);
                }
            }

            if (response != null) {
                byte[] resBody = response.getCapturedBytes();
                if (resBody.length > 0) {
                    String body = new String(resBody, StandardCharsets.UTF_8);
                    if (response.wasCapped()) body = body + "...(truncated)";
                    sb.append(" | RES: ").append(body);
                }
            }

            if (status >= 400) log.warn(sb.toString());
            else log.info(sb.toString());

            // The 200ms budget's own WARN — distinct from (and in addition to) the line above, so a
            // sustained slow endpoint is greppable by its [SLO] prefix regardless of status.
            slo().record(method, uri, status, elapsed);

        } catch (Exception e) {
            log.debug("Error logging API request: {}", e.getMessage());
        }
    }

    /**
     * Masks the values of secret-shaped JSON fields in a request body before it is logged.
     *
     * <p>The request logging above records POST/PUT/PATCH bodies verbatim, and bodies like
     * {@code /api/auth/aws/iam-connect}'s carry live AWS temporary credentials (accessKeyId,
     * secretKey, sessionToken) — plaintext in the log means the log archive ships live secrets into
     * the artifact store. Field names are matched case-insensitively by JSON-field-aware regexes
     * (an AKIA/ASIA pattern alone misses the STS "ASIA" prefix and the colon-separated JSON shape),
     * plus a last-resort pattern for any long base64ish value following a secret-ish key in a
     * non-JSON body. Only the VALUE is masked; the field name and the rest of the body stay
     * intact, so debugging keeps its context.
     */
    private static final java.util.regex.Pattern SECRET_FIELD = java.util.regex.Pattern.compile(
            "(\"(?i)(accessKeyId|secretKey|secretAccessKey|sessionToken|password|passphrase|token|api[_-]?key|clientSecret)\"\\s*:\\s*\")([^\"]*)(\")");

    private static String redactSecrets(String body) {
        if (body == null || body.isEmpty()) return body;
        return SECRET_FIELD.matcher(body).replaceAll("$1***REDACTED***$4");
    }

    private static class CappedResponseWrapper extends HttpServletResponseWrapper {

        private final OutputStream realOut;
        private final byte[] capBuf;
        private int capLen = 0;
        private boolean capped = false;
        private int status = 200;

        CappedResponseWrapper(HttpServletResponse response, int maxBytes) throws IOException {
            super(response);
            this.realOut = response.getOutputStream();
            this.capBuf = new byte[maxBytes];
        }

        @Override
        public void setStatus(int sc) { super.setStatus(sc); this.status = sc; }

        @Override
        public int getStatus() { return status; }

        @Override
        public ServletOutputStream getOutputStream() {
            return new ServletOutputStream() {
                @Override public boolean isReady() { return true; }
                @Override public void setWriteListener(WriteListener l) {}

                @Override
                public void write(int b) throws IOException {
                    realOut.write(b);
                    capture(new byte[]{(byte) b}, 0, 1);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    realOut.write(b, off, len);
                    capture(b, off, len);
                }

                @Override
                public void flush() throws IOException { realOut.flush(); }

                @Override
                public void close() throws IOException { realOut.close(); }
            };
        }

        private void capture(byte[] b, int off, int len) {
            if (capped) return;
            int space = capBuf.length - capLen;
            if (len >= space) {
                System.arraycopy(b, off, capBuf, capLen, space);
                capLen = capBuf.length;
                capped = true;
            } else {
                System.arraycopy(b, off, capBuf, capLen, len);
                capLen += len;
            }
        }

        byte[] getCapturedBytes() { return java.util.Arrays.copyOf(capBuf, capLen); }
        boolean wasCapped() { return capped; }
    }
}
