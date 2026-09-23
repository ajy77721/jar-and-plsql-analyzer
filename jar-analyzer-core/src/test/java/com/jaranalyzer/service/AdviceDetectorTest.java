package com.jaranalyzer.service;

import com.jaranalyzer.model.AdviceInfo;
import com.jaranalyzer.model.AnnotationInfo;
import com.jaranalyzer.model.ClassInfo;
import com.jaranalyzer.model.MethodCallInfo;
import com.jaranalyzer.model.MethodInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Advice detection, and specifically the one distinction that carries the value.
 *
 * <p>Interception runs through a proxy, so an aspect or interceptor appears in <b>no</b> endpoint's
 * call tree however much of the request it handles. Detecting the class is easy; the finding worth
 * having is whether it is actually <i>registered</i>, because an unregistered
 * {@code HandlerInterceptor} is inert and looks identical to a working one in the class list.
 *
 * <p>The case these tests are modelled on is real. In this repository
 * {@code ApiKeyAuthInterceptor} implements {@code HandlerInterceptor}, is a {@code @Component}, and
 * has a complete {@code preHandle} that 401s every {@code /api/} path — but
 * {@code WebConfig.addInterceptors} has its {@code registry.addInterceptor(...)} line commented
 * out. Every {@code /api/} route is therefore unauthenticated while the codebase appears to have an
 * auth gate.
 */
class AdviceDetectorTest {

    /* ---- fixtures ---- */

    private ClassInfo cls(String fqn, List<String> ifaces, String... annotations) {
        ClassInfo c = new ClassInfo();
        c.setFullyQualifiedName(fqn);
        c.setInterfaces(new java.util.ArrayList<>(ifaces));
        for (String a : annotations) {
            AnnotationInfo ai = new AnnotationInfo();
            ai.setName(a);
            c.getAnnotations().add(ai);
        }
        return c;
    }

    private MethodInfo method(String name, String descriptor) {
        MethodInfo m = new MethodInfo();
        m.setName(name);
        m.setDescriptor(descriptor);
        return m;
    }

    private MethodCallInfo call(String owner, String name, String... stringArgs) {
        MethodCallInfo i = new MethodCallInfo();
        i.setOwnerClass(owner);
        i.setMethodName(name);
        if (stringArgs.length > 0) i.setRecentStringArgs(List.of(stringArgs));
        return i;
    }

    /** The real shape: @Component HandlerInterceptor whose registration is commented out. */
    private ClassInfo apiKeyInterceptor() {
        ClassInfo c = cls("com.analyzer.auth.ApiKeyAuthInterceptor",
                List.of("org.springframework.web.servlet.HandlerInterceptor"), "Component");
        MethodInfo pre = method("preHandle",
                "(Ljakarta/servlet/http/HttpServletRequest;Ljakarta/servlet/http/HttpServletResponse;"
                        + "Ljava/lang/Object;)Z");
        pre.getInvocations().add(call("java.lang.String", "startsWith", "/api/"));
        c.getMethods().add(pre);
        MethodInfo isPublic = method("isPublic", "(Ljava/lang/String;)Z");
        isPublic.getInvocations().add(call("java.lang.String", "equals", "/login"));
        isPublic.getInvocations().add(call("java.lang.String", "startsWith", "/api/auth/"));
        c.getMethods().add(isPublic);
        return c;
    }

    /** WebConfig with an EMPTY addInterceptors — the commented-out registration. */
    private ClassInfo webConfigNotRegistering() {
        ClassInfo c = cls("com.analyzer.config.WebConfig",
                List.of("org.springframework.web.servlet.config.annotation.WebMvcConfigurer"),
                "Configuration");
        c.getMethods().add(method("addInterceptors",
                "(Lorg/springframework/web/servlet/config/annotation/InterceptorRegistry;)V"));
        return c;
    }

    /* ---- the central distinction ---- */

    @Test
    @DisplayName("a @Component HandlerInterceptor with no addInterceptor call is NOT registered")
    void componentInterceptorIsNotAutoRegistered() {
        List<AdviceInfo> found =
                AdviceDetector.detect(List.of(apiKeyInterceptor(), webConfigNotRegistering()));

        assertEquals(1, found.size(), "the interceptor must be found: " + found);
        AdviceInfo a = found.get(0);
        assertEquals("HANDLER_INTERCEPTOR", a.getKind());
        assertEquals("com.analyzer.auth.ApiKeyAuthInterceptor", a.getClassName());

        // THE assertion. Spring does not register a HandlerInterceptor just because it is a bean;
        // it needs an explicit addInterceptors call. If this ever reports true, the detector is
        // reporting an inert security control as active — worse than not detecting it at all.
        assertFalse(a.isRegistered(),
                "@Component alone does not register a HandlerInterceptor — reporting it as "
                        + "registered would hide exactly the defect this detector exists to find");
        assertNull(a.getRegistrationSite());
    }

    @Test
    @DisplayName("the same interceptor IS registered once addInterceptor is called")
    void explicitRegistrationIsDetected() {
        ClassInfo webConfig = webConfigNotRegistering();
        MethodInfo add = webConfig.getMethods().get(0);
        add.getInvocations().add(call(
                "org.springframework.web.servlet.config.annotation.InterceptorRegistry",
                "addInterceptor"));
        // The registration references the interceptor type; that is the link, since bytecode does
        // not record argument types on an invocation.
        add.getInvocations().add(call("com.analyzer.auth.ApiKeyAuthInterceptor", "<init>"));

        List<AdviceInfo> found = AdviceDetector.detect(List.of(apiKeyInterceptor(), webConfig));

        assertEquals(1, found.size());
        assertTrue(found.get(0).isRegistered(), "an explicit addInterceptor call registers it");
        assertEquals("com.analyzer.config.WebConfig#addInterceptors",
                found.get(0).getRegistrationSite());
    }

    @Test
    @DisplayName("path patterns on the registration are captured")
    void pathPatternsAreCaptured() {
        ClassInfo webConfig = webConfigNotRegistering();
        MethodInfo add = webConfig.getMethods().get(0);
        add.getInvocations().add(call(
                "org.springframework.web.servlet.config.annotation.InterceptorRegistry",
                "addInterceptor"));
        add.getInvocations().add(call("com.analyzer.auth.ApiKeyAuthInterceptor", "<init>"));
        add.getInvocations().add(call("org.springframework.web.servlet.config.annotation."
                + "InterceptorRegistration", "addPathPatterns", "/api/**"));
        add.getInvocations().add(call("org.springframework.web.servlet.config.annotation."
                + "InterceptorRegistration", "excludePathPatterns", "/api/auth/**"));

        AdviceInfo a = AdviceDetector.detect(List.of(apiKeyInterceptor(), webConfig)).get(0);
        assertTrue(a.getPathPatterns().contains("/api/**"), a.getPathPatterns().toString());
        assertTrue(a.getExcludedPathPatterns().contains("/api/auth/**"),
                a.getExcludedPathPatterns().toString());
    }

    @Test
    @DisplayName("path constants in the advice BODY are not reported as scope")
    void bodyPathConstantsAreNotScope() {
        // Looks like a missing feature; it is a removed one. The interceptor body contains
        // startsWith("/api/") — the paths it PROTECTS — and equals("/login") — a path it EXEMPTS.
        // Both are a String call with a constant argument, and telling them apart needs to know
        // whether the branch allows or denies. An earlier version collected them all as
        // excludedPathPatterns and reported "/api/", every authenticated route, as exempt from
        // authentication: a confidently inverted answer about an auth control.
        AdviceInfo a = AdviceDetector.detect(
                List.of(apiKeyInterceptor(), webConfigNotRegistering())).get(0);

        assertTrue(a.getExcludedPathPatterns().isEmpty(),
                "body constants must not become exemptions, got " + a.getExcludedPathPatterns());
        assertTrue(a.getPathPatterns().isEmpty(),
                "nor scope, got " + a.getPathPatterns());
    }

    /* ---- kinds that a bean annotation DOES activate ---- */

    @Test
    @DisplayName("a @Component servlet Filter is auto-registered, unlike an interceptor")
    void componentFilterIsAutoRegistered() {
        ClassInfo f = cls("com.demo.TraceFilter", List.of("jakarta.servlet.Filter"), "Component");
        f.getMethods().add(method("doFilter", "(Ljakarta/servlet/ServletRequest;"
                + "Ljakarta/servlet/ServletResponse;Ljakarta/servlet/FilterChain;)V"));

        AdviceInfo a = AdviceDetector.detect(List.of(f)).get(0);
        assertEquals("SERVLET_FILTER", a.getKind());
        assertTrue(a.isRegistered(), "Spring Boot registers a Filter bean into the chain");
        assertEquals("@Component", a.getRegistrationSite());
    }

    @Test
    @DisplayName("a filter that EXTENDS OncePerRequestFilter is found, not just one that implements Filter")
    void filterBySuperclass() {
        // The common Spring idiom, and the first version of this detector missed it: extending
        // OncePerRequestFilter implements Filter for you, so nothing appears in `interfaces`.
        // This repository's own ApiLoggingFilter is exactly this shape.
        ClassInfo f = cls("com.demo.ApiLoggingFilter", List.of(), "Component");
        f.setSuperClass("org.springframework.web.filter.OncePerRequestFilter");
        f.getMethods().add(method("doFilterInternal", "(Ljakarta/servlet/http/HttpServletRequest;"
                + "Ljakarta/servlet/http/HttpServletResponse;Ljakarta/servlet/FilterChain;)V"));

        List<AdviceInfo> found = AdviceDetector.detect(List.of(f));
        assertEquals(1, found.size(), "a filter subclass must be detected: " + found);
        assertEquals("SERVLET_FILTER", found.get(0).getKind());
        assertTrue(found.get(0).isRegistered(), "a @Component filter is auto-registered");
    }

    @Test
    @DisplayName("an @Aspect is advice, keeps its pointcut, and is active as a bean")
    void aspectWithPointcut() {
        ClassInfo asp = cls("com.demo.AuditAspect", List.of(), "Aspect", "Component");
        MethodInfo around = method("audit", "(Lorg/aspectj/lang/ProceedingJoinPoint;)Ljava/lang/Object;");
        AnnotationInfo ann = new AnnotationInfo();
        ann.setName("Around");
        ann.getAttributes().put("value", "execution(* com.demo.service.*.*(..))");
        around.getAnnotations().add(ann);
        asp.getMethods().add(around);

        AdviceInfo a = AdviceDetector.detect(List.of(asp)).get(0);
        assertEquals("ASPECT", a.getKind());
        assertTrue(a.isRegistered());
        assertEquals(1, a.getMethods().size());
        assertEquals("Around", a.getMethods().get(0).getAdvice());
        assertEquals("execution(* com.demo.service.*.*(..))", a.getMethods().get(0).getPointcut(),
                "the pointcut must be kept verbatim — it is the only record of what is intercepted");
    }

    @Test
    @DisplayName("a MethodInterceptor on a template needs explicit wiring")
    void methodInterceptorRequiresWiring() {
        ClassInfo mi = cls("com.demo.MongoTracer",
                List.of("org.aopalliance.intercept.MethodInterceptor"), "Component");
        mi.getMethods().add(method("invoke", "(Lorg/aopalliance/intercept/MethodInvocation;)Ljava/lang/Object;"));

        AdviceInfo a = AdviceDetector.detect(List.of(mi)).get(0);
        assertEquals("METHOD_INTERCEPTOR", a.getKind());
        assertFalse(a.isRegistered(),
                "a MethodInterceptor bean does nothing until a proxy or advisor uses it");
        assertEquals(1, a.getMethods().size());
        assertEquals("invoke", a.getMethods().get(0).getAdvice());
    }

    /* ---- non-advice must stay out ---- */

    @Test
    @DisplayName("ordinary classes produce nothing")
    void ordinaryClassesAreNotAdvice() {
        ClassInfo svc = cls("com.demo.OrderService", List.of(), "Service");
        svc.getMethods().add(method("save", "(Lcom/demo/Order;)V"));
        ClassInfo repo = cls("com.demo.OrderRepo",
                List.of("org.springframework.data.mongodb.repository.MongoRepository"), "Repository");

        assertTrue(AdviceDetector.detect(List.of(svc, repo)).isEmpty(),
                "a service and a repository are not cross-cutting advice");
    }

    @Test
    @DisplayName("WebMvcConfigurer itself is a registrar, not advice")
    void configurerIsNotItselfAdvice() {
        List<AdviceInfo> found = AdviceDetector.detect(List.of(webConfigNotRegistering()));
        assertTrue(found.isEmpty(), "the class doing the registering is not the advice: " + found);
    }

    @Test
    @DisplayName("empty and null input are handled")
    void degenerateInput() {
        assertTrue(AdviceDetector.detect(List.of()).isEmpty());
        assertTrue(AdviceDetector.detect((List<ClassInfo>) null).isEmpty());
    }

    /* ---- the streaming entry point ---- */

    private java.io.BufferedReader reader(String... lines) {
        return new java.io.BufferedReader(new java.io.StringReader(String.join("\n", lines)));
    }

    @Test
    @DisplayName("the JSONL entry point finds the same advice without loading every class")
    void streamingMatchesInMemory() throws Exception {
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        var lines = new java.util.ArrayList<String>();
        // Bulk it out with ordinary classes: the point of the streaming path is that these are
        // read and discarded rather than retained.
        for (int i = 0; i < 200; i++) {
            lines.add(mapper.writeValueAsString(cls("com.demo.Filler" + i, List.of(), "Service")));
        }
        lines.add(mapper.writeValueAsString(apiKeyInterceptor()));
        lines.add(mapper.writeValueAsString(webConfigNotRegistering()));

        List<AdviceInfo> found =
                AdviceDetector.detect(reader(lines.toArray(String[]::new)), mapper);
        assertEquals(1, found.size(), "one interceptor among 202 classes: " + found);
        assertEquals("com.analyzer.auth.ApiKeyAuthInterceptor", found.get(0).getClassName());
        assertFalse(found.get(0).isRegistered());
    }

    @Test
    @DisplayName("a malformed line is skipped, not fatal")
    void malformedLineIsSkipped() throws Exception {
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        // Advice is supplementary; a bad line must not cost the whole analysis document.
        List<AdviceInfo> found = AdviceDetector.detect(reader(
                "{ this is not json",
                mapper.writeValueAsString(apiKeyInterceptor()),
                ""), mapper);
        assertEquals(1, found.size(), "the good line must still be read");
    }

    @Test
    @DisplayName("a null or empty reader yields nothing rather than throwing")
    void degenerateReader() {
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        assertTrue(AdviceDetector.detect((java.io.BufferedReader) null, mapper).isEmpty());
        assertTrue(AdviceDetector.detect(reader(""), mapper).isEmpty());
    }

    @Test
    @DisplayName("the detector opens no files of its own")
    void detectorDoesNoFileIo() throws Exception {
        // It takes a reader precisely so that the module stays storage-agnostic and the storage
        // gate has one owner for this read. Re-introducing Files/Path here would put a raw
        // filesystem dependency into a module that must work against S3 and MongoDB too.
        String src = java.nio.file.Files.readString(java.nio.file.Path.of(
                "src/main/java/com/jaranalyzer/service/AdviceDetector.java"));
        assertFalse(src.contains("Files.new") || src.contains("Files.read")
                        || src.contains("new FileReader") || src.contains("new FileInputStream"),
                "AdviceDetector must not do its own file I/O — the caller supplies the reader");
    }
}
