package com.jaranalyzer.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

/**
 * One piece of cross-cutting behaviour: an AOP aspect, a Spring MVC {@code HandlerInterceptor}, a
 * servlet {@code Filter}, a {@code ClientHttpRequestInterceptor}, a Mongo {@code CommandListener},
 * and so on.
 *
 * <p><b>Why this exists as its own collection.</b> Interception happens through a runtime proxy or a
 * framework-managed chain, so there is <i>no static call edge</i> from the intercepted method to the
 * advice. Nothing in {@code classes[]} or the call tree can express it: the aspect never appears in
 * any endpoint's tree, however much of the request it actually handles. A reader of the call tree
 * therefore sees a flow that is genuinely incomplete, and cannot tell that it is.
 *
 * <p><b>{@link #registered} is the field that matters most.</b> Implementing
 * {@code HandlerInterceptor} does nothing on its own — Spring only invokes it if something registers
 * it (a {@code WebMvcConfigurer.addInterceptors} call, a {@code FilterRegistrationBean}, a
 * {@code @Component} filter picked up by boot). An interceptor that is implemented but never
 * registered looks exactly like a working one from the class list, and is inert. In the analysis
 * that motivated this class, {@code ApiKeyAuthInterceptor} was fully implemented and its
 * registration was a commented-out line, so every {@code /api/} route was unauthenticated while the
 * codebase appeared to have an auth gate.
 *
 * <p>Absent fields are omitted rather than serialised as null — this collection is small but sits
 * on the same hot document as everything else.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class AdviceInfo {

    /** What kind of cross-cutting hook this is. See {@code AdviceDetector} for the mapping. */
    private String kind;

    /** FQN of the class providing the advice. */
    private String className;

    /** The class's stereotype, so a reader need not cross-reference {@code classes[]}. */
    private String stereotype;

    /**
     * How this advice is known to be active, when that could be determined statically:
     * the FQN#method of the registration call site, or {@code "@Component"} for a bean-scanned
     * filter. Null when no registration was found.
     */
    private String registrationSite;

    /**
     * Whether a registration was found. {@code false} means the advice is <b>inert</b> — see the
     * class javadoc. This is a static determination and can be wrong in one direction: registration
     * through reflection or a conditional bean is invisible to bytecode analysis, so {@code false}
     * means "no registration found", not "provably never runs".
     */
    private boolean registered;

    /** The advice methods themselves — {@code preHandle}, an {@code @Around} method, etc. */
    private List<AdviceMethod> methods = new ArrayList<>();

    /**
     * Path patterns the advice applies to, from the registration's {@code addPathPatterns} /
     * {@code addUrlPatterns} string arguments. Read from constants, so a pattern built at runtime
     * is absent; empty means "no narrowing found", not "applies to nothing".
     *
     * <p>Only registration-derived patterns appear here. Path constants inside the advice body are
     * deliberately not used — see the note in {@code AdviceDetector}, where doing so reported a
     * protected path as an exempt one.
     */
    private List<String> pathPatterns = new ArrayList<>();

    /** Patterns explicitly exempted at registration — {@code excludePathPatterns}. */
    private List<String> excludedPathPatterns = new ArrayList<>();

    /** One advice method and its pointcut, if it declares one. */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class AdviceMethod {
        private String name;
        private String descriptor;
        /** {@code Around}, {@code Before}, {@code preHandle}, … */
        private String advice;
        /** The raw AspectJ pointcut expression, verbatim. Null for interface-based advice. */
        private String pointcut;

        public AdviceMethod() {}

        public AdviceMethod(String name, String descriptor, String advice, String pointcut) {
            this.name = name;
            this.descriptor = descriptor;
            this.advice = advice;
            this.pointcut = pointcut;
        }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getDescriptor() { return descriptor; }
        public void setDescriptor(String descriptor) { this.descriptor = descriptor; }
        public String getAdvice() { return advice; }
        public void setAdvice(String advice) { this.advice = advice; }
        public String getPointcut() { return pointcut; }
        public void setPointcut(String pointcut) { this.pointcut = pointcut; }
    }

    public String getKind() { return kind; }
    public void setKind(String kind) { this.kind = kind; }
    public String getClassName() { return className; }
    public void setClassName(String className) { this.className = className; }
    public String getStereotype() { return stereotype; }
    public void setStereotype(String stereotype) { this.stereotype = stereotype; }
    public String getRegistrationSite() { return registrationSite; }
    public void setRegistrationSite(String registrationSite) { this.registrationSite = registrationSite; }
    public boolean isRegistered() { return registered; }
    public void setRegistered(boolean registered) { this.registered = registered; }
    public List<AdviceMethod> getMethods() { return methods; }
    public void setMethods(List<AdviceMethod> methods) { this.methods = methods; }
    public List<String> getPathPatterns() { return pathPatterns; }
    public void setPathPatterns(List<String> pathPatterns) { this.pathPatterns = pathPatterns; }
    public List<String> getExcludedPathPatterns() { return excludedPathPatterns; }
    public void setExcludedPathPatterns(List<String> excludedPathPatterns) {
        this.excludedPathPatterns = excludedPathPatterns;
    }
}
