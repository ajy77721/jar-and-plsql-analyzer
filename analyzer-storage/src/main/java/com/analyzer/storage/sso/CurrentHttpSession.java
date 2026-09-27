package com.analyzer.storage.sso;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

/**
 * Answers "which {@code HttpSession} is the current thread serving?", which is how the SSO layer
 * finds the calling user's state without any controller having to pass it down into the S3 stack.
 *
 * <h2>Why this is reflection and not a direct call</h2>
 *
 * <p>The design wants the current session id read from Spring's request context, i.e.
 * {@code RequestContextHolder.currentRequestAttributes().getRequest().getSession().getId()}. The
 * honest way to write that is a direct import of {@code org.springframework.web.context.request.RequestContextHolder}
 * and {@code jakarta.servlet.http.HttpSession} — but this is {@code analyzer-storage}, whose POM
 * deliberately carries {@code spring-boot-starter} and nothing web-shaped ("Spring for
 * {@code @Configuration}/{@code @Value} only; no web, no analyzer modules"), and that POM is out of
 * scope here. A direct import would not compile.
 *
 * <p>So the same call is made reflectively. At runtime in {@code unified-web} — where Spring MVC and
 * the servlet API are on the classpath — this resolves the real
 * {@code ServletRequestAttributes} and returns its session id. In any context without a web request
 * (a background job thread, a CLI, a unit test) the reflective lookup finds no request attributes
 * and this returns {@code null}, which is the meaningful "no request-bound session" answer the
 * routing and credential layers key their default-path fallback on. The reflective lookup is
 * cached after the first hit, so the steady-state cost on the S3 request path is one volatile read.
 *
 * <h2>Never throws</h2>
 *
 * <p>Every failure mode — Spring web absent, no request bound to the thread, an already-invalidated
 * session — collapses to {@code null}. A missing session must never turn into an exception from a
 * component consulted on every S3 operation; it is an expected state (background work), not an
 * error.
 */
@Component
public class CurrentHttpSession {

    private static final Logger log = LoggerFactory.getLogger(CurrentHttpSession.class);

    private static final String REQUEST_CONTEXT_HOLDER = "org.springframework.web.context.request.RequestContextHolder";
    private static final String CURRENT_REQUEST_ATTRIBUTES = "currentRequestAttributes";
    private static final String GET_REQUEST = "getRequest";
    private static final String GET_SESSION = "getSession";
    private static final String GET_ID = "getId";

    /** Cached reflective handles, resolved once. A sentinel {@code Boolean.FALSE} means "not present". */
    private volatile Object holderClassOrSentinel;
    private volatile Method currentRequestAttributes;

    /**
     * A session id propagated onto a pooled worker thread by a fan-out (see
     * {@code RequestScopedArtifactStore}). A request thread never sets it, so on the request path
     * this is empty and the reflective lookup below answers as before.
     */
    private static final ThreadLocal<String> PROPAGATED = new ThreadLocal<>();

    /**
     * The current thread's {@code HttpSession} id, or {@code null} when the thread is not serving a
     * request with a session.
     *
     * <p>Deliberately uses {@code getSession(false)}: creating a session as a side effect of an S3
     * read would defeat the "a request with no session-bound state routes to the default store"
     * rule, so a request that has not opted into an SSO flow must not have one conjured for it here.
     *
     * <p>A session id propagated onto this thread by a fan-out (see {@link #propagate}) wins: the
     * worker is serving the request that propagated it, so it must answer as that request's session
     * rather than as the context-free thread its pool slot happens to be.
     */
    public String currentSessionId() {
        String propagated = PROPAGATED.get();
        if (propagated != null) return propagated;
        return requestBoundSessionId();
    }

    /**
     * Binds {@code sessionId} to the current thread so that {@link #currentSessionId()} answers with
     * it, and returns a handle whose {@code close()} unbinds it. A nested install restores whatever
     * was bound before, so one fan-out cannot leak its session into the next on a shared worker.
     *
     * <p>Intended to be called by a pool worker for the duration of one task, with a session id the
     * request thread already resolved. A {@code null}/blank id installs nothing (the no-session
     * fallback must stay exactly as it was), and the returned handle is always safe to close.
     */
    public AutoCloseable propagate(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) return () -> { };
        String previous = PROPAGATED.get();
        PROPAGATED.set(sessionId);
        return () -> {
            if (previous == null) PROPAGATED.remove();
            else PROPAGATED.set(previous);
        };
    }

    /** The thread's real request-bound session id, ignoring any propagated override. */
    private String requestBoundSessionId() {
        try {
            Object attributes = currentRequestAttributes();
            if (attributes == null) return null;
            Object request = attributes.getClass().getMethod(GET_REQUEST).invoke(attributes);
            if (request == null) return null;
            Object session = request.getClass().getMethod(GET_SESSION, boolean.class).invoke(request, false);
            if (session == null) return null;
            Object id = session.getClass().getMethod(GET_ID).invoke(session);
            return id == null ? null : id.toString();
        } catch (ReflectiveOperationException | RuntimeException e) {
            // Expected whenever there is no servlet request bound (jobs, CLIs, tests). Trace only.
            log.trace("[aws-sso] no current HttpSession ({})", e.getClass().getSimpleName());
            return null;
        }
    }

    /**
     * {@code RequestContextHolder.currentRequestAttributes()}, reflectively, or {@code null} when
     * Spring web is absent or nothing is bound to the thread.
     */
    private Object currentRequestAttributes() throws ReflectiveOperationException {
        Method method = currentRequestAttributes;
        if (method == null) {
            Object holder = holderClassOrSentinel;
            if (holder == null) {
                try {
                    holder = Class.forName(REQUEST_CONTEXT_HOLDER);
                } catch (ClassNotFoundException e) {
                    // Not a web application; remember that and stop trying.
                    holder = Boolean.FALSE;
                }
                holderClassOrSentinel = holder;
            }
            if (holder == Boolean.FALSE) return null;
            method = ((Class<?>) holder).getMethod(CURRENT_REQUEST_ATTRIBUTES);
            currentRequestAttributes = method;
        }
        return method.invoke(null);
    }
}
