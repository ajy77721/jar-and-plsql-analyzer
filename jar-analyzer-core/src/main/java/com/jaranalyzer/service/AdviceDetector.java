package com.jaranalyzer.service;

import com.jaranalyzer.model.AdviceInfo;
import com.jaranalyzer.model.AnnotationInfo;
import com.jaranalyzer.model.ClassInfo;
import com.jaranalyzer.model.FieldAccessInfo;
import com.jaranalyzer.model.MethodCallInfo;
import com.jaranalyzer.model.MethodInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Finds cross-cutting behaviour that the call tree structurally cannot show.
 *
 * <p>Interception runs through a proxy or a framework chain, so there is no static call edge from an
 * intercepted method to its advice: an aspect around every {@code @Service} method appears in no
 * endpoint's call tree at all. This is a real gap in every call-tree answer, and it is invisible —
 * the tree looks complete. Rather than fabricate synthetic edges (which would need a full AspectJ
 * pointcut matcher to place correctly, and would silently mis-attribute when it got them wrong),
 * this reports the advice as its own collection and lets the reader join it.
 *
 * <p>It is a <b>post-pass over already-parsed {@link ClassInfo}</b>, not a bytecode visitor. Every
 * input it needs — annotations by simple name, {@code interfaces}, and constant string arguments in
 * {@code invocations.recentStringArgs} — is already captured, so this adds no parsing cost and
 * cannot change any existing field.
 *
 * <h2>Registration is the point</h2>
 * Implementing {@code HandlerInterceptor} has no effect until something registers it. Detecting the
 * implementation is easy and nearly useless on its own; detecting that the <i>registration is
 * missing</i> is the finding. See {@link AdviceInfo#isRegistered()}.
 */
public final class AdviceDetector {

    private static final Logger log = LoggerFactory.getLogger(AdviceDetector.class);

    /** Interface simple name → advice kind. Matched on the simple name because the analysis stores
     *  interfaces as FQNs but the framework packages vary across Spring/Jakarta/javax generations. */
    private static final Map<String, String> ADVICE_INTERFACES = new LinkedHashMap<>();
    static {
        ADVICE_INTERFACES.put("HandlerInterceptor", "HANDLER_INTERCEPTOR");
        ADVICE_INTERFACES.put("AsyncHandlerInterceptor", "HANDLER_INTERCEPTOR");
        ADVICE_INTERFACES.put("MethodInterceptor", "METHOD_INTERCEPTOR");
        ADVICE_INTERFACES.put("Filter", "SERVLET_FILTER");
        ADVICE_INTERFACES.put("WebFilter", "REACTIVE_FILTER");
        ADVICE_INTERFACES.put("ClientHttpRequestInterceptor", "HTTP_CLIENT_INTERCEPTOR");
        ADVICE_INTERFACES.put("RequestInterceptor", "FEIGN_INTERCEPTOR");
        ADVICE_INTERFACES.put("CommandListener", "MONGO_COMMAND_LISTENER");
        ADVICE_INTERFACES.put("ConnectionPoolListener", "MONGO_POOL_LISTENER");
        ADVICE_INTERFACES.put("ResponseBodyAdvice", "RESPONSE_BODY_ADVICE");
        ADVICE_INTERFACES.put("RequestBodyAdvice", "REQUEST_BODY_ADVICE");
        ADVICE_INTERFACES.put("HandlerMethodArgumentResolver", "ARGUMENT_RESOLVER");
        ADVICE_INTERFACES.put("ChannelInterceptor", "MESSAGING_INTERCEPTOR");
        ADVICE_INTERFACES.put("ProducerInterceptor", "KAFKA_INTERCEPTOR");
        ADVICE_INTERFACES.put("ConsumerInterceptor", "KAFKA_INTERCEPTOR");
    }

    /**
     * Base classes that make a subclass advice, by simple name.
     *
     * <p>Interfaces are not enough. The most common way to write a servlet filter in Spring is to
     * {@code extend OncePerRequestFilter} — which implements {@code Filter} for you — so a detector
     * that only looks at {@code interfaces} misses it. This repository's own
     * {@code ApiLoggingFilter} is exactly that shape and was missed by the first version of this
     * class.
     */
    private static final Map<String, String> ADVICE_SUPERCLASSES = new LinkedHashMap<>();
    static {
        ADVICE_SUPERCLASSES.put("OncePerRequestFilter", "SERVLET_FILTER");
        ADVICE_SUPERCLASSES.put("GenericFilterBean", "SERVLET_FILTER");
        ADVICE_SUPERCLASSES.put("HttpFilter", "SERVLET_FILTER");
        ADVICE_SUPERCLASSES.put("HandlerInterceptorAdapter", "HANDLER_INTERCEPTOR");
        ADVICE_SUPERCLASSES.put("WebRequestHandlerInterceptorAdapter", "HANDLER_INTERCEPTOR");
        ADVICE_SUPERCLASSES.put("RequestBodyAdviceAdapter", "REQUEST_BODY_ADVICE");
    }

    /** AspectJ advice annotations, by the simple name the parser records. */
    private static final Set<String> ADVICE_ANNOTATIONS =
            Set.of("Around", "Before", "After", "AfterReturning", "AfterThrowing");

    /** Interface methods that constitute advice, so they can be listed without a pointcut. */
    private static final Set<String> ADVICE_METHOD_NAMES =
            Set.of("preHandle", "postHandle", "afterCompletion", "invoke", "doFilter", "filter",
                    "intercept", "apply", "commandStarted", "commandSucceeded", "commandFailed",
                    "beforeBodyWrite", "beforeBodyRead", "presend", "preSend", "postSend",
                    "onSend", "onAcknowledgement");

    /**
     * Kinds that being a Spring bean is <b>sufficient</b> to activate.
     *
     * <p>This distinction is the one thing in this class that must not be "simplified" into
     * "any bean is registered". Spring Boot auto-registers a servlet {@code Filter} bean into the
     * chain, and auto-proxies an {@code @Aspect} bean. It does <b>not</b> do this for a
     * {@code HandlerInterceptor}: that requires an explicit
     * {@code WebMvcConfigurer.addInterceptors} call, so a {@code @Component HandlerInterceptor}
     * with no such call is inert. Treating bean-scanning as registration for every kind would
     * report exactly those inert interceptors as active — which is the single finding this
     * detector exists to surface.
     */
    private static final Set<String> AUTO_REGISTERED_KINDS =
            Set.of("ASPECT", "SERVLET_FILTER", "REACTIVE_FILTER");

    /** Registration calls whose string arguments narrow or widen an interceptor's scope. */
    private static final Set<String> PATH_INCLUDE_CALLS = Set.of("addPathPatterns", "addUrlPatterns");
    private static final Set<String> PATH_EXCLUDE_CALLS = Set.of("excludePathPatterns");

    private AdviceDetector() {}

    /**
     * Streaming entry point: reads the classes JSONL and keeps only what advice detection needs.
     *
     * <p>The analysis pipeline never holds every {@link ClassInfo} in memory — classes are streamed
     * from this file straight into the output generator, which is what keeps peak memory flat on a
     * 1,600-class JAR. So this makes one extra pass and <b>retains only the few classes that are
     * either advice candidates or plausible registrars</b>, then hands that small list to
     * {@link #detect(List)}. Memory is proportional to the amount of advice, not to class count.
     *
     * <p>A malformed line is skipped rather than failing the analysis: advice is supplementary
     * information, and losing it must never lose the whole document.
     *
     * <p><b>The reader is supplied, not opened here.</b> This class does no file I/O of its own —
     * the caller already owns and budgets that read (see the storage gate,
     * {@code DirectFileIoGateTest}), and opening a second handle here would both duplicate that
     * responsibility and put a raw filesystem path into a module that is otherwise storage-agnostic.
     * The caller closes it.
     *
     * @param lines one serialised {@link ClassInfo} per line
     */
    public static List<AdviceInfo> detect(BufferedReader lines,
                                          com.fasterxml.jackson.databind.ObjectMapper mapper) {
        if (lines == null) return List.of();
        List<ClassInfo> relevant = new ArrayList<>();
        int scanned = 0;
        try {
            String line;
            while ((line = lines.readLine()) != null) {
                if (line.isBlank()) continue;
                scanned++;
                ClassInfo cls;
                try {
                    cls = mapper.readValue(line, ClassInfo.class);
                } catch (Exception skip) {
                    continue;
                }
                if (isCandidate(cls) || hasRegistrarMethod(cls)) relevant.add(cls);
            }
        } catch (IOException e) {
            log.warn("AdviceDetector: could not scan classes for advice ({}); "
                    + "the analysis is unaffected", e.toString());
            return List.of();
        }
        log.debug("AdviceDetector: {} of {} classes retained for advice detection",
                relevant.size(), scanned);
        return detect(relevant);
    }

    /** True when a class could itself be advice — an {@code @Aspect}, or an advice type. */
    private static boolean isCandidate(ClassInfo cls) {
        if (cls == null || cls.getFullyQualifiedName() == null) return false;
        return hasAnnotation(cls.getAnnotations(), "Aspect") || kindOf(cls) != null;
    }

    /**
     * The advice kind implied by a class's interfaces or superclass, else null.
     *
     * <p>Interfaces are checked first: a class that both implements {@code Filter} and extends a
     * filter base gets the same answer either way, and an explicit interface is the stronger signal.
     */
    private static String kindOf(ClassInfo cls) {
        for (String iface : orEmpty(cls.getInterfaces())) {
            String k = ADVICE_INTERFACES.get(simpleName(iface));
            if (k != null) return k;
        }
        return ADVICE_SUPERCLASSES.get(simpleName(cls.getSuperClass()));
    }

    /** True when a class holds a method that might register advice, so it must be kept. */
    private static boolean hasRegistrarMethod(ClassInfo cls) {
        if (cls == null) return false;
        for (String iface : orEmpty(cls.getInterfaces())) {
            // WebMvcConfigurer is the canonical registrar and is not itself advice.
            if ("WebMvcConfigurer".equals(simpleName(iface))) return true;
        }
        for (MethodInfo m : orEmpty(cls.getMethods())) {
            if (m == null || m.getName() == null) continue;
            if (isRegistrarName(m.getName()) || looksLikeRegistrationBody(m)) return true;
        }
        return false;
    }

    /**
     * @param classes every parsed class in the analysis
     * @return one entry per advice class, in encounter order; empty when the JAR has no advice
     */
    public static List<AdviceInfo> detect(List<ClassInfo> classes) {
        if (classes == null || classes.isEmpty()) return List.of();

        List<AdviceInfo> found = new ArrayList<>();
        // Registrations are discovered across the whole JAR, so collect them before deciding
        // whether any given advice class is registered.
        Registrations regs = collectRegistrations(classes);

        for (ClassInfo cls : classes) {
            if (cls == null || cls.getFullyQualifiedName() == null) continue;
            AdviceInfo advice = classify(cls, regs);
            if (advice != null) found.add(advice);
        }

        if (!found.isEmpty()) {
            long inert = found.stream().filter(a -> !a.isRegistered()).count();
            log.info("AdviceDetector: {} advice class(es) found, {} with no registration site",
                    found.size(), inert);
        }
        return found;
    }

    /** Decides whether one class is advice, and builds its entry. Null when it is not. */
    private static AdviceInfo classify(ClassInfo cls, Registrations regs) {
        String kind = null;

        // An @Aspect is advice regardless of what it implements.
        if (hasAnnotation(cls.getAnnotations(), "Aspect")) {
            kind = "ASPECT";
        } else {
            kind = kindOf(cls);
        }
        if (kind == null) return null;

        AdviceInfo advice = new AdviceInfo();
        advice.setKind(kind);
        advice.setClassName(cls.getFullyQualifiedName());
        advice.setStereotype(cls.getStereotype());

        for (MethodInfo m : orEmpty(cls.getMethods())) {
            if (m == null || m.getName() == null) continue;
            String adviceKind = adviceKindOf(m);
            if (adviceKind == null) continue;
            advice.getMethods().add(new AdviceInfo.AdviceMethod(
                    m.getName(), m.getDescriptor(), adviceKind, pointcutOf(m)));
        }

        String site = regs.siteFor(cls.getFullyQualifiedName());
        if (site == null && AUTO_REGISTERED_KINDS.contains(kind)) {
            String bean = beanAnnotation(cls);
            if (bean != null) site = "@" + bean;
        }
        advice.setRegistered(site != null);
        advice.setRegistrationSite(site);
        advice.getPathPatterns().addAll(regs.includesFor(cls.getFullyQualifiedName()));
        advice.getExcludedPathPatterns().addAll(regs.excludesFor(cls.getFullyQualifiedName()));
        return advice;
    }

    /** {@code Around}/{@code Before}/… from an annotation, else the interface method name. */
    private static String adviceKindOf(MethodInfo m) {
        for (AnnotationInfo a : orEmpty(m.getAnnotations())) {
            if (a != null && a.getName() != null && ADVICE_ANNOTATIONS.contains(a.getName())) {
                return a.getName();
            }
        }
        return ADVICE_METHOD_NAMES.contains(m.getName()) ? m.getName() : null;
    }

    /** The pointcut expression verbatim — {@code value} on the advice annotation. */
    private static String pointcutOf(MethodInfo m) {
        for (AnnotationInfo a : orEmpty(m.getAnnotations())) {
            if (a == null || a.getName() == null) continue;
            if (!ADVICE_ANNOTATIONS.contains(a.getName()) && !"Pointcut".equals(a.getName())) continue;
            Object v = a.getAttributes().get("value");
            if (v == null) v = a.getAttributes().get("pointcut");
            if (v instanceof String s && !s.isBlank()) return s;
        }
        return null;
    }

    // Deliberately NOT implemented: inferring path scope from constants in the advice body.
    //
    // A path-guarding interceptor compares the request path against string constants, and it is
    // tempting to report them as its scope. But `path.startsWith("/api/")` in ApiKeyAuthInterceptor
    // is the path it PROTECTS, while `"/login"` in its isPublic helper is one it EXEMPTS — and both
    // are just a String.startsWith call with a constant. Telling them apart needs to know whether
    // the branch returns allow or deny, which is dataflow this analysis does not do. An earlier
    // version of this class collected them all as excludedPathPatterns and duly reported "/api/"
    // — every authenticated route — as exempt from authentication. Only registration-derived
    // patterns (addPathPatterns/excludePathPatterns) are recorded, because there the framework
    // itself defines which list is which.

    private static String beanAnnotation(ClassInfo cls) {
        for (AnnotationInfo a : orEmpty(cls.getAnnotations())) {
            if (a == null || a.getName() == null) continue;
            switch (a.getName()) {
                case "Component": case "Service": case "Configuration": case "Bean":
                    return a.getName();
                default: break;
            }
        }
        return null;
    }

    /* ---- registration discovery ---- */

    /**
     * Where each advice class is registered, and with what path scope.
     *
     * <p>Keyed by the advice's FQN. The registration call takes the interceptor as an <i>argument</i>
     * and bytecode does not record argument types in {@code invocations}, so the link is made by
     * looking at which advice types the registering method references at all — via its own
     * invocations' owner classes and its field accesses. That is why a commented-out registration
     * is detected as absent: the method body then references nothing.
     */
    private record Registrations(Map<String, String> sites,
                                 Map<String, List<String>> includes,
                                 Map<String, List<String>> excludes) {
        String siteFor(String fqn) { return sites.get(fqn); }
        List<String> includesFor(String fqn) { return includes.getOrDefault(fqn, List.of()); }
        List<String> excludesFor(String fqn) { return excludes.getOrDefault(fqn, List.of()); }
    }

    private static Registrations collectRegistrations(List<ClassInfo> classes) {
        Map<String, String> sites = new LinkedHashMap<>();
        Map<String, List<String>> includes = new LinkedHashMap<>();
        Map<String, List<String>> excludes = new LinkedHashMap<>();

        // Every class that could be advice, so a registration can be attributed to it.
        Set<String> candidates = new LinkedHashSet<>();
        for (ClassInfo c : classes) {
            if (c == null || c.getFullyQualifiedName() == null) continue;
            if (isCandidate(c)) candidates.add(c.getFullyQualifiedName());
        }
        if (candidates.isEmpty()) return new Registrations(sites, includes, excludes);

        for (ClassInfo cls : classes) {
            if (cls == null) continue;
            for (MethodInfo m : orEmpty(cls.getMethods())) {
                if (m == null || m.getName() == null) continue;
                if (!isRegistrarName(m.getName()) && !looksLikeRegistrationBody(m)) continue;

                // Which candidate advice types does this body actually mention?
                Set<String> mentioned = new LinkedHashSet<>();
                for (MethodCallInfo inv : orEmpty(m.getInvocations())) {
                    if (inv != null && inv.getOwnerClass() != null) {
                        String owner = inv.getOwnerClass().replace('/', '.');
                        if (candidates.contains(owner)) mentioned.add(owner);
                    }
                }
                for (FieldAccessInfo fa : orEmpty(m.getFieldAccesses())) {
                    if (fa == null) continue;
                    String t = fieldType(fa);
                    if (t != null && candidates.contains(t)) mentioned.add(t);
                }
                if (mentioned.isEmpty()) continue;

                String site = cls.getFullyQualifiedName() + "#" + m.getName();
                List<String> inc = stringArgsOf(m, PATH_INCLUDE_CALLS);
                List<String> exc = stringArgsOf(m, PATH_EXCLUDE_CALLS);
                for (String fqn : mentioned) {
                    sites.putIfAbsent(fqn, site);
                    if (!inc.isEmpty()) includes.computeIfAbsent(fqn, k -> new ArrayList<>()).addAll(inc);
                    if (!exc.isEmpty()) excludes.computeIfAbsent(fqn, k -> new ArrayList<>()).addAll(exc);
                }
            }
        }
        return new Registrations(sites, includes, excludes);
    }

    /** Method names that conventionally register advice. */
    private static boolean isRegistrarName(String name) {
        return "addInterceptors".equals(name)
                || "addFilters".equals(name)
                || "configureMessageConverters".equals(name)
                || name.startsWith("filterRegistration")
                || name.startsWith("interceptor");
    }

    /** A body that calls a registry's add* method is registering something, whatever it is named. */
    private static boolean looksLikeRegistrationBody(MethodInfo m) {
        for (MethodCallInfo inv : orEmpty(m.getInvocations())) {
            if (inv == null || inv.getMethodName() == null || inv.getOwnerClass() == null) continue;
            if ("addInterceptor".equals(inv.getMethodName())
                    || "setInterceptors".equals(inv.getMethodName())
                    || "addCommandListener".equals(inv.getMethodName())
                    || (inv.getMethodName().startsWith("add")
                        && inv.getOwnerClass().contains("Registr"))) {
                return true;
            }
        }
        return false;
    }

    private static List<String> stringArgsOf(MethodInfo m, Set<String> callNames) {
        List<String> out = new ArrayList<>();
        for (MethodCallInfo inv : orEmpty(m.getInvocations())) {
            if (inv == null || inv.getMethodName() == null) continue;
            if (!callNames.contains(inv.getMethodName())) continue;
            for (String s : orEmpty(inv.getRecentStringArgs())) {
                if (s != null && !out.contains(s)) out.add(s);
            }
        }
        return out;
    }

    /** Normalises a field descriptor ({@code Lcom/x/Y;}) or type name to an FQN. */
    private static String fieldType(FieldAccessInfo fieldAccess) {
        String t = fieldAccess.getFieldType();
        if (t == null || t.isBlank()) return null;
        if (t.startsWith("L") && t.endsWith(";")) t = t.substring(1, t.length() - 1);
        return t.replace('/', '.');
    }

    private static boolean hasAnnotation(List<AnnotationInfo> anns, String name) {
        for (AnnotationInfo a : orEmpty(anns)) {
            if (a != null && name.equals(a.getName())) return true;
        }
        return false;
    }

    private static String simpleName(String type) {
        if (type == null) return "";
        String t = type.replace('/', '.');
        int i = t.lastIndexOf('.');
        return i < 0 ? t : t.substring(i + 1);
    }

    private static <T> List<T> orEmpty(List<T> l) { return l == null ? List.of() : l; }
}
