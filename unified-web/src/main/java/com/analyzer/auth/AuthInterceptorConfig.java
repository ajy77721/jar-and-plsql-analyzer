package com.analyzer.auth;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Registers {@link ApiKeyAuthInterceptor} with Spring MVC so the authentication gate actually runs.
 *
 * <p>Until this class existed the interceptor was a {@code @Component} that nothing ever added to
 * the handler chain — a registered bean with no effect, i.e. dead code guarding nothing. Spring MVC
 * only consults an interceptor that an {@code InterceptorRegistry} has been told about, and there
 * was no {@code WebMvcConfigurer} doing so. This configuration supplies exactly that one
 * registration.
 *
 * <p>Registration is intentionally the <em>only</em> job here: it decides which requests reach the
 * interceptor, but never what happens to them. The public-path decision (API key exemptions, the
 * 401-vs-redirect choice, the AWS SSO second gate) lives entirely in
 * {@link ApiKeyAuthInterceptor#preHandle}, so there is a single source of truth for the rules. The
 * exclusions below mirror the interceptor's own public list for the static/shell resources that
 * must load before a user can authenticate; {@code /api/auth/*} is deliberately <em>not</em>
 * excluded here, because it stays in the chain and the interceptor itself treats it as public.
 *
 * <p>The patterns registered are:
 * <ul>
 *   <li>included: {@code /**} — everything by default;</li>
 *   <li>excluded: {@code /login}, {@code /login.html}, {@code /favicon.ico}, {@code /error}, the
 *       static asset prefixes ({@code /shared/**}, {@code /jar/**}, {@code /war/**},
 *       {@code /parser/**}, {@code /plsql/**}, {@code /conversion/**}, {@code /docs/**}) and the
 *       root documents ({@code /index.html}, {@code /}, {@code /*.css}, {@code /*.js}).</li>
 * </ul>
 */
@Configuration
public class AuthInterceptorConfig implements WebMvcConfigurer {

    private final ApiKeyAuthInterceptor authInterceptor;

    public AuthInterceptorConfig(ApiKeyAuthInterceptor authInterceptor) {
        this.authInterceptor = authInterceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(authInterceptor)
                .addPathPatterns("/**")
                .excludePathPatterns(
                        // Shell/login pages and error handling: reachable without any credential.
                        "/login",
                        "/login.html",
                        "/favicon.ico",
                        "/error",
                        // Static assets and per-app front-end roots.
                        "/*.css",
                        "/*.js",
                        "/shared/**",
                        "/jar/**",
                        "/war/**",
                        "/parser/**",
                        "/plsql/**",
                        "/conversion/**",
                        "/docs/**",
                        "/index.html",
                        "/");
        // NOTE: "/api/auth/**" is intentionally absent. Those routes must pass through the
        // interceptor so that ApiKeyAuthInterceptor can classify them as public itself; excluding
        // them here would fork the rule in two places.
    }
}
