package com.analyzer.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Configuration;
import org.springframework.mock.web.MockServletContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * F7 — the UI resource layer. Every page, stylesheet and script the browser loads must resolve
 * through Spring's real static-resource handler, and each app's entry page must still contain the
 * markers the front end keys on. This is the layer that, when it silently 404s, makes the product
 * look "broken" with no server error at all.
 *
 * <p>Boots a minimal {@code @EnableWebMvc} context with the same {@code classpath:/static/} resource
 * location Spring Boot registers, rather than the whole application (which would pull in Mongo and
 * the per-user storage beans) — so the handler under test is the real one.
 */
@DisplayName("Integration F7: every page/JS/CSS resolves and the key markers survive")
class UiResourcesEndToEndTest {

    private static final Path STATIC = Path.of("src/main/resources/static");

    /** The app entry pages and a marker each must contain. */
    private static final Map<String, String> PAGES = new LinkedHashMap<>();
    static {
        PAGES.put("index.html", "<title>Code Analyzer</title>");
        PAGES.put("login.html", "<title>Signing in");
        PAGES.put("jar/index.html", "<title>JAR Analyzer</title>");
        PAGES.put("war/index.html", "<title>WAR Analyzer</title>");
        PAGES.put("parser/index.html", "<title>PL/SQL Parse Analyzer</title>");
        PAGES.put("plsql/index.html", "<title>PL/SQL Analyzer</title>");
        PAGES.put("conversion/index.html", "<title>PL/SQL");
        PAGES.put("admin-portal/index.html", "<title>AMP Admin Portal</title>");
    }

    @Configuration
    @EnableWebMvc
    static class StaticResourceConfig implements WebMvcConfigurer {
        @Override
        public void addResourceHandlers(ResourceHandlerRegistry registry) {
            registry.addResourceHandler("/**").addResourceLocations("classpath:/static/");
        }
    }

    private MockMvc mvc() {
        AnnotationConfigWebApplicationContext ctx = new AnnotationConfigWebApplicationContext();
        ctx.setServletContext(new MockServletContext());
        ctx.register(StaticResourceConfig.class);
        ctx.refresh();
        return MockMvcBuilders.webAppContextSetup(ctx).build();
    }

    private static List<String> resources() throws Exception {
        List<String> out = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(STATIC)) {
            walk.filter(Files::isRegularFile)
                .map(STATIC::relativize)
                .map(Path::toString)
                .map(p -> p.replace('\\', '/'))
                .filter(p -> p.endsWith(".html") || p.endsWith(".js") || p.endsWith(".css"))
                // static/tests/** are Node front-end test fixtures, not UI assets the browser loads.
                .filter(p -> !p.startsWith("tests/"))
                .sorted()
                .forEach(out::add);
        }
        return out;
    }

    @Test
    @DisplayName("every page, JS and CSS under /static is served with a 200")
    void everyResourceIsServed() throws Exception {
        MockMvc mvc = mvc();
        List<String> resources = resources();
        assertTrue(resources.size() > 200, "sanity: the UI ships hundreds of assets, found "
                + resources.size());

        List<String> failures = new ArrayList<>();
        for (String resource : resources) {
            try {
                mvc.perform(get("/" + resource)).andExpect(status().isOk());
            } catch (Throwable t) {
                failures.add(resource + " -> " + t.getMessage());
            }
        }
        assertTrue(failures.isEmpty(), "resources that did not return 200:\n" + String.join("\n", failures));
    }

    @Test
    @DisplayName("each app entry page still carries its title/marker")
    void pageMarkersSurvive() throws Exception {
        MockMvc mvc = mvc();
        for (Map.Entry<String, String> page : PAGES.entrySet()) {
            String html = mvc.perform(get("/" + page.getKey()))
                    .andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();
            assertTrue(html.contains(page.getValue()),
                    page.getKey() + " must contain marker '" + page.getValue() + "'");
        }
    }

    @Test
    @DisplayName("a missing resource is a 404, not a silent 200")
    void missingResourceIs404() throws Exception {
        mvc().perform(get("/does-not-exist.js")).andExpect(status().isNotFound());
    }
}
