package com.jaranalyzer.service;

import com.jaranalyzer.model.ClassInfo;
import com.jaranalyzer.model.MethodInfo;
import com.jaranalyzer.service.CallGraphIndex.AnnotationData;
import com.jaranalyzer.service.CallGraphIndex.IndexedClass;
import com.jaranalyzer.service.CallGraphIndex.IndexedMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Category D — Cache Annotation Tests (15 tests).
 *
 * Tests target CallTreeBuilder.extractCacheOperations(), which is package-private.
 * We invoke it via reflection on a minimal CallTreeBuilder stub.
 *
 * The method signature is:
 *   private List&lt;Map&lt;String, Object&gt;&gt; extractCacheOperations(List&lt;AnnotationData&gt; annotations)
 *
 * For @Caching composite annotations, nested annotation instances are represented as
 * List&lt;Map&lt;String,Object&gt;&gt; where each map has the annotation attribute keys.
 * The bytecode parser stores nested annotations in AnnotationData.attributes() with
 * the nested annotation's own attributes directly (no @type wrapper needed by the
 * extractor — extractNestedCacheOps iterates whatever value is in the attribute).
 */
@DisplayName("CallTreeBuilder — Spring Cache annotation extraction (Category D)")
class CacheTaggingTest {

    // ──────────────────────────────────────────────────────────────────────────
    // Reflection setup: invoke extractCacheOperations on a stubbed CallTreeBuilder
    // ──────────────────────────────────────────────────────────────────────────

    private Method extractCacheOpsMethod;
    private CallTreeBuilder builderStub;

    @BeforeEach
    void setUp() throws Exception {
        // Build a minimal CallTreeBuilder. Its constructor needs DomainConfigLoader and
        // CollectionResolver. We supply a DomainConfigLoader (with defaults), and null
        // for CollectionResolver (extractCacheOperations doesn't use it).
        DomainConfigLoader loader = new DomainConfigLoader();
        applyDefaults(loader);

        // CollectionResolver needs multiple collaborators — use a lightweight stub approach
        // by instantiating the production CallTreeBuilder via a minimal constructor path.
        // Since CollectionResolver is package-private and cannot be easily null-instantiated,
        // we create a test CollectionResolver with null collaborators and rely on the
        // fact that extractCacheOperations never touches collectionResolver.
        // Use the constructor that accepts @Value-annotated ints directly.
        builderStub = new CallTreeBuilder(loader, null /* collectionResolver not used */,
                10, 20, 200, 100000);

        extractCacheOpsMethod = CallTreeBuilder.class.getDeclaredMethod(
                "extractCacheOperations", List.class);
        extractCacheOpsMethod.setAccessible(true);
    }

    private static void applyDefaults(DomainConfigLoader loader) throws Exception {
        Method m = DomainConfigLoader.class.getDeclaredMethod("applyDefaults");
        m.setAccessible(true);
        m.invoke(loader);
    }

    /** Invoke extractCacheOperations via reflection and cast the result. */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> extractCacheOps(List<AnnotationData> annotations) throws Exception {
        return (List<Map<String, Object>>) extractCacheOpsMethod.invoke(builderStub, annotations);
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Builders for AnnotationData stubs
    // ──────────────────────────────────────────────────────────────────────────

    /** Build an AnnotationData for @Cacheable, @CacheEvict, or @CachePut. */
    private static AnnotationData cacheAnn(String name, Map<String, Object> attrs) {
        return new AnnotationData(name, attrs);
    }

    /** Shorthand: single-string cacheNames in "value" attribute. */
    private static Map<String, Object> attrs(String... keyValues) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            String key = keyValues[i];
            String val = keyValues[i + 1];
            m.put(key, val);
        }
        return m;
    }

    /** Put a typed (non-String) value into attrs. */
    private static Map<String, Object> attrsTyped(Object... keyValues) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            m.put((String) keyValues[i], keyValues[i + 1]);
        }
        return m;
    }

    // ──────────────────────────────────────────────────────────────────────────
    // D1-D9 — Single cache annotations
    // ──────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("D1-D9 — Direct @Cacheable / @CacheEvict / @CachePut")
    class DirectCacheAnnotations {

        @Test
        @DisplayName("D1 — @Cacheable(\"users\") → type=CACHEABLE, cacheNames=[users]")
        void d1_cacheableSimple() throws Exception {
            AnnotationData ann = cacheAnn("Cacheable", attrs("value", "users"));
            List<Map<String, Object>> ops = extractCacheOps(List.of(ann));

            assertEquals(1, ops.size(), "Exactly one cache op expected");
            assertEquals("CACHEABLE", ops.get(0).get("type"));
            @SuppressWarnings("unchecked")
            List<String> names = (List<String>) ops.get(0).get("cacheNames");
            assertNotNull(names, "cacheNames must not be null");
            assertTrue(names.contains("users"), "cacheNames must contain 'users'. Actual: " + names);
        }

        @Test
        @DisplayName("D2 — @Cacheable(cacheNames={\"users\",\"recent\"}) → both names in cacheNames")
        void d2_cacheableMultipleNames() throws Exception {
            Map<String, Object> attrMap = new LinkedHashMap<>();
            attrMap.put("cacheNames", List.of("users", "recent"));
            AnnotationData ann = cacheAnn("Cacheable", attrMap);
            List<Map<String, Object>> ops = extractCacheOps(List.of(ann));

            assertEquals(1, ops.size());
            assertEquals("CACHEABLE", ops.get(0).get("type"));
            @SuppressWarnings("unchecked")
            List<String> names = (List<String>) ops.get(0).get("cacheNames");
            assertNotNull(names);
            assertTrue(names.contains("users"), "Must contain 'users'");
            assertTrue(names.contains("recent"), "Must contain 'recent'");
        }

        @Test
        @DisplayName("D3 — @CacheEvict(\"users\") → type=CACHE_EVICT")
        void d3_cacheEvictSimple() throws Exception {
            AnnotationData ann = cacheAnn("CacheEvict", attrs("value", "users"));
            List<Map<String, Object>> ops = extractCacheOps(List.of(ann));

            assertEquals(1, ops.size());
            assertEquals("CACHE_EVICT", ops.get(0).get("type"));
            @SuppressWarnings("unchecked")
            List<String> names = (List<String>) ops.get(0).get("cacheNames");
            assertTrue(names.contains("users"), "cacheNames must contain 'users'");
        }

        @Test
        @DisplayName("D4 — @CacheEvict(value=\"users\", allEntries=true) → allEntries=true")
        void d4_cacheEvictAllEntries() throws Exception {
            Map<String, Object> attrMap = new LinkedHashMap<>();
            attrMap.put("value", "users");
            attrMap.put("allEntries", Boolean.TRUE);
            AnnotationData ann = cacheAnn("CacheEvict", attrMap);
            List<Map<String, Object>> ops = extractCacheOps(List.of(ann));

            assertEquals(1, ops.size());
            assertEquals("CACHE_EVICT", ops.get(0).get("type"));
            assertEquals(Boolean.TRUE, ops.get(0).get("allEntries"),
                    "allEntries must be true. Actual op: " + ops.get(0));
        }

        @Test
        @DisplayName("D5 — @CacheEvict(beforeInvocation=true) → beforeInvocation=true")
        void d5_cacheEvictBeforeInvocation() throws Exception {
            Map<String, Object> attrMap = new LinkedHashMap<>();
            attrMap.put("value", "users");
            attrMap.put("beforeInvocation", Boolean.TRUE);
            AnnotationData ann = cacheAnn("CacheEvict", attrMap);
            List<Map<String, Object>> ops = extractCacheOps(List.of(ann));

            assertEquals(1, ops.size());
            assertEquals("CACHE_EVICT", ops.get(0).get("type"));
            assertEquals(Boolean.TRUE, ops.get(0).get("beforeInvocation"),
                    "beforeInvocation must be true. Actual op: " + ops.get(0));
        }

        @Test
        @DisplayName("D6 — @CachePut(\"users\") → type=CACHE_PUT")
        void d6_cachePutSimple() throws Exception {
            AnnotationData ann = cacheAnn("CachePut", attrs("value", "users"));
            List<Map<String, Object>> ops = extractCacheOps(List.of(ann));

            assertEquals(1, ops.size());
            assertEquals("CACHE_PUT", ops.get(0).get("type"));
        }

        @Test
        @DisplayName("D7 — @Cacheable(key=\"#id\") → key=\"#id\" in op map")
        void d7_cacheableWithKey() throws Exception {
            AnnotationData ann = cacheAnn("Cacheable", attrs("value", "users", "key", "#id"));
            List<Map<String, Object>> ops = extractCacheOps(List.of(ann));

            assertEquals(1, ops.size());
            assertEquals("#id", ops.get(0).get("key"),
                    "key SpEL expression must be captured. Actual: " + ops.get(0));
        }

        @Test
        @DisplayName("D8 — @Cacheable(condition=\"#result != null\") → condition captured")
        void d8_cacheableWithCondition() throws Exception {
            AnnotationData ann = cacheAnn("Cacheable",
                    attrs("value", "users", "condition", "#result != null"));
            List<Map<String, Object>> ops = extractCacheOps(List.of(ann));

            assertEquals(1, ops.size());
            assertEquals("#result != null", ops.get(0).get("condition"),
                    "condition SpEL must be captured. Actual: " + ops.get(0));
        }

        @Test
        @DisplayName("D9 — @Cacheable(unless=\"#result == null\") → unless captured")
        void d9_cacheableWithUnless() throws Exception {
            AnnotationData ann = cacheAnn("Cacheable",
                    attrs("value", "users", "unless", "#result == null"));
            List<Map<String, Object>> ops = extractCacheOps(List.of(ann));

            assertEquals(1, ops.size());
            assertEquals("#result == null", ops.get(0).get("unless"),
                    "unless SpEL must be captured. Actual: " + ops.get(0));
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // D10-D11 — @Caching composite
    // ──────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("D10-D11 — Composite @Caching annotation")
    class CompositeCaching {

        @Test
        @DisplayName("D10 — @Caching(cacheable={@Cacheable(\"a\"),@Cacheable(\"b\")}) → two CACHEABLE entries")
        void d10_cachingTwoCacheable() throws Exception {
            // Nested annotations represented as List<Map<String,Object>>
            Map<String, Object> nested1 = new LinkedHashMap<>();
            nested1.put("value", "a");
            Map<String, Object> nested2 = new LinkedHashMap<>();
            nested2.put("value", "b");

            Map<String, Object> cachingAttrs = new LinkedHashMap<>();
            cachingAttrs.put("cacheable", List.of(nested1, nested2));

            AnnotationData ann = cacheAnn("Caching", cachingAttrs);
            List<Map<String, Object>> ops = extractCacheOps(List.of(ann));

            long cacheableCount = ops.stream()
                    .filter(op -> "CACHEABLE".equals(op.get("type")))
                    .count();
            assertEquals(2, cacheableCount,
                    "Must have two CACHEABLE entries. Actual: " + ops);
        }

        @Test
        @DisplayName("D11 — @Caching(evict={@CacheEvict(\"a\")}, put={@CachePut(\"b\")}) → one CACHE_EVICT + one CACHE_PUT")
        void d11_cachingEvictAndPut() throws Exception {
            Map<String, Object> evictNested = new LinkedHashMap<>();
            evictNested.put("value", "a");
            Map<String, Object> putNested = new LinkedHashMap<>();
            putNested.put("value", "b");

            Map<String, Object> cachingAttrs = new LinkedHashMap<>();
            cachingAttrs.put("evict", List.of(evictNested));
            cachingAttrs.put("put", List.of(putNested));

            AnnotationData ann = cacheAnn("Caching", cachingAttrs);
            List<Map<String, Object>> ops = extractCacheOps(List.of(ann));

            long evictCount = ops.stream().filter(op -> "CACHE_EVICT".equals(op.get("type"))).count();
            long putCount   = ops.stream().filter(op -> "CACHE_PUT".equals(op.get("type"))).count();
            assertEquals(1, evictCount, "Must have one CACHE_EVICT entry. Actual: " + ops);
            assertEquals(1, putCount,   "Must have one CACHE_PUT entry. Actual: " + ops);
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // D12 — No annotation
    // ──────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("D12 — No cache annotation")
    class NoCacheAnnotation {

        @Test
        @DisplayName("D12 — method with no cache annotation → empty list")
        void d12_noCacheAnnotation() throws Exception {
            // Method has a non-cache annotation
            AnnotationData ann = cacheAnn("Transactional", Map.of());
            List<Map<String, Object>> ops = extractCacheOps(List.of(ann));

            assertNotNull(ops, "Result must never be null");
            assertTrue(ops.isEmpty(),
                    "Non-cache annotations must produce empty cacheOperations. Actual: " + ops);
        }

        @Test
        @DisplayName("D12b — empty annotation list → empty list")
        void d12b_emptyAnnotationList() throws Exception {
            List<Map<String, Object>> ops = extractCacheOps(List.of());

            assertNotNull(ops);
            assertTrue(ops.isEmpty(), "Empty annotation list must produce empty result");
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // D13 — Class-level @Cacheable
    // ──────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("D13 — Class-level @Cacheable")
    class ClassLevelCacheable {

        /**
         * The extractCacheOperations method operates on method-level annotationDetailList.
         * Class-level @Cacheable is not passed into it — that's a gap in the implementation.
         * We document the current behavior: if no annotation is in the list, result is empty.
         */
        @Test
        @Disabled("GAP: class-level @Cacheable is not extracted by extractCacheOperations — " +
                  "it only processes method-level annotationDetailList; class annotations are not passed in")
        @DisplayName("D13 — class-level @Cacheable would require class-level annotation processing")
        void d13_classLevelCacheable() throws Exception {
            // Class-level annotations would need to be included in the method's annotationDetailList
            // or processed separately. Current implementation does not handle this.
            // If class-level @Cacheable("users") were passed, we'd expect:
            AnnotationData ann = cacheAnn("Cacheable", attrs("value", "users"));
            List<Map<String, Object>> ops = extractCacheOps(List.of(ann));
            // When passed directly, it does produce a result (the method doesn't distinguish class vs method)
            // The real gap is that ClassInfo annotations don't flow into this method.
            assertEquals("CACHEABLE", ops.get(0).get("type"),
                    "If annotation is passed in, it is processed regardless of level");
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // D14-D15 — Key and unless extraction
    // ──────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("D14-D15 — Key and unless SpEL extraction")
    class KeyAndUnlessExtraction {

        @Test
        @DisplayName("D14 — @CacheEvict(cacheNames=\"users\", key=\"#entity.id\") → key extracted")
        void d14_cacheEvictWithKey() throws Exception {
            AnnotationData ann = cacheAnn("CacheEvict",
                    attrs("cacheNames", "users", "key", "#entity.id"));
            List<Map<String, Object>> ops = extractCacheOps(List.of(ann));

            assertEquals(1, ops.size());
            assertEquals("CACHE_EVICT", ops.get(0).get("type"));
            assertEquals("#entity.id", ops.get(0).get("key"),
                    "key must be '#entity.id'. Actual: " + ops.get(0));
        }

        @Test
        @DisplayName("D15 — @CachePut(cacheNames=\"users\", unless=\"#result.error != null\") → unless extracted")
        void d15_cachePutWithUnless() throws Exception {
            AnnotationData ann = cacheAnn("CachePut",
                    attrs("cacheNames", "users", "unless", "#result.error != null"));
            List<Map<String, Object>> ops = extractCacheOps(List.of(ann));

            assertEquals(1, ops.size());
            assertEquals("CACHE_PUT", ops.get(0).get("type"));
            assertEquals("#result.error != null", ops.get(0).get("unless"),
                    "unless SpEL must be captured. Actual: " + ops.get(0));
        }
    }
}
