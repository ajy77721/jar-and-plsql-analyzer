package com.analyzer.web;

import org.junit.jupiter.api.BeforeAll;
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

import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * End-to-end over the real static-resource handler: the served HTML carries content-versioned
 * references, the version matches the file's content hash, external/CDN tags are untouched, and the
 * Cache-Control policy distinguishes versioned assets (immutable) from everything else (revalidate /
 * not stored).
 */
class PageHtmlTransformerTest {

    private static StaticResourceVersionIndex index;
    private static MockMvc mvc;

    @Configuration
    @EnableWebMvc
    static class StaticResourceConfig implements WebMvcConfigurer {
        @Override
        public void addResourceHandlers(ResourceHandlerRegistry registry) {
            registry.addResourceHandler("/**").addResourceLocations("classpath:/static/");
        }
    }

    @BeforeAll
    static void setUp() {
        index = new StaticResourceVersionIndex();
        index.buildIndex();

        AnnotationConfigWebApplicationContext ctx = new AnnotationConfigWebApplicationContext();
        ctx.setServletContext(new MockServletContext());
        ctx.register(StaticResourceConfig.class);
        ctx.refresh();

        mvc = MockMvcBuilders.webAppContextSetup(ctx)
                .addFilters(new PageHtmlTransformer(index))
                .build();
    }

    private static String body(String path) throws Exception {
        return mvc.perform(get(path)).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
    }

    @Test
    @DisplayName("the parser page's script and link tags carry the content hash")
    void parserPageIsVersioned() throws Exception {
        String html = body("/parser/index.html");
        String escapeVersion = index.versionFor("/shared/escape.js");
        String variablesVersion = index.versionFor("/parser/css/variables.css");
        String themeVersion = index.versionFor("/shared/theme-toggle.js");

        assertNotNull(escapeVersion);
        assertTrue(html.contains("escape.js?v=" + escapeVersion),
                "the relative ../shared/escape.js must be rewritten to its content hash");
        assertTrue(html.contains("css/variables.css?v=" + variablesVersion),
                "a page-relative stylesheet must be rewritten too");
        assertTrue(html.contains("/shared/theme-toggle.js?v=" + themeVersion),
                "an absolute reference must be rewritten as well");
        assertFalse(html.contains("escape.js?v=1788675772"),
                "the stale hardcoded timestamp must be replaced, not kept");
    }

    @Test
    @DisplayName("the root page's references are versioned against the root directory")
    void rootPageIsVersioned() throws Exception {
        String html = body("/index.html");
        String version = index.versionFor("/shared/escape.js");
        assertTrue(html.contains("shared/escape.js?v=" + version));
    }

    @Test
    @DisplayName("the entry routes are recognised as HTML pages (the forward targets are rewritten)")
    void entryRoutesAreHtmlPages() {
        // /parser/ etc. are internal forwards to their index.html; the filter buffers those because
        // the route — not the forwarded file name — is what it sees on the request.
        assertTrue(PageHtmlTransformer.isHtmlPage("/"));
        assertTrue(PageHtmlTransformer.isHtmlPage("/parser/"));
        assertTrue(PageHtmlTransformer.isHtmlPage("/jar/"));
        assertTrue(PageHtmlTransformer.isHtmlPage("/parser/index.html"));
        assertTrue(PageHtmlTransformer.isHtmlPage("/login"));
        assertFalse(PageHtmlTransformer.isHtmlPage("/parser/js/app.js"));
        assertFalse(PageHtmlTransformer.isHtmlPage("/api/parser/analyses"));
    }

    @Test
    @DisplayName("external CDN scripts are left exactly as authored")
    void externalScriptsAreUntouched() throws Exception {
        String html = body("/plsql/index.html");
        assertTrue(html.contains("https://cdn.jsdelivr.net/npm/xlsx-js-style@1.2.0/dist/xlsx.bundle.js"),
                "the external bundle must survive the rewrite verbatim");
        assertFalse(html.contains("xlsx.bundle.js?v="),
                "an external URL must never be versioned");
    }

    @Test
    @DisplayName("a versioned asset is cached immutably; the same asset unversioned revalidates")
    void versionedAssetsAreImmutable() throws Exception {
        String version = index.versionFor("/shared/escape.js");

        mvc.perform(get("/shared/escape.js").param("v", version))
                .andExpect(status().isOk())
                .andExpect(header().string("Cache-Control", containsString("immutable")));

        mvc.perform(get("/shared/escape.js"))
                .andExpect(status().isOk())
                .andExpect(header().string("Cache-Control", containsString("no-cache")));
    }

    @Test
    @DisplayName("an unversioned fetch still works (backward compatibility)")
    void unversionedFetchStillServes() throws Exception {
        mvc.perform(get("/shared/escape.js")).andExpect(status().isOk());
        mvc.perform(get("/parser/js/app.js")).andExpect(status().isOk());
    }

    @Test
    @DisplayName("HTML entry pages are never stored; a missing asset is not cached immutably")
    void htmlIsNotStoredAndMissingAssetsAreNotImmutable() throws Exception {
        mvc.perform(get("/parser/index.html"))
                .andExpect(status().isOk())
                .andExpect(header().string("Cache-Control", containsString("no-store")));

        mvc.perform(get("/does-not-exist.js"))
                .andExpect(status().isNotFound())
                .andExpect(header().string("Cache-Control", containsString("no-cache")));
    }
}
