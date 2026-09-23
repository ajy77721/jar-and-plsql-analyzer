package com.jaranalyzer.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * PLAN-08 §6.1 #7 — the ten Summary headline aggregates, computed on the server.
 *
 * <p><b>What this replaces.</b> {@code jar/js/summary.js:_render} walked every endpoint in the
 * browser behind a {@code requestAnimationFrame} progress bar, built an {@code epReport} for each,
 * and then {@code _finishRender} folded ten aggregates out of the result — six of them set
 * cardinalities. PLAN-08 names the progress bar as "the smell that proves the work does not belong
 * in the browser". The ten numbers are a pure function of the analysis document, so they are
 * computed once here instead of once per JAR click per browser.
 *
 * <p><b>Fidelity is the whole point.</b> A changed headline number is a regression, not an
 * improvement, so every rule below is a deliberate transcription of the JavaScript that produced
 * the number, including the parts that look accidental:
 *
 * <ul>
 *   <li>The input is the <b>summary document</b> ({@code _summary_static.json}), not
 *       {@code analysis.json}. That is the document {@code /summary/headers} serves and therefore
 *       the exact bytes the browser aggregated. {@code scheduledCount} exists only there, and the
 *       summary writer strips {@code callTree}, which selects the branch of
 *       {@code _analyzeEndpoint} transcribed here.</li>
 *   <li>A collection's domain is read from the endpoint's {@code aggregatedCollectionDomains} map
 *       first, exactly as {@code _analyzeEndpoint} does, and only falls back to prefix detection
 *       when the map has no entry. The fallback is {@link #detectDomain}, a transcription of
 *       {@code summary-helpers.js:_detectDomain} — <b>not</b>
 *       {@code DomainConfigLoader#detectCollectionDomain}, which upper-cases the name, strips a
 *       {@code WIP_} prefix and has a {@code "Generic"} bucket the browser has never had. Calling
 *       the server's richer detector here would classify collections the browser calls
 *       {@code "Other"} and silently move the Domains count.</li>
 *   <li>{@code modules} counts distinct <em>project labels</em>, not distinct JAR names, because
 *       {@code _analyzeEndpoint} stores {@code modules.map(_jarToProject)}. Two JARs that munge to
 *       the same label are one module in the chip, and that has to stay true.</li>
 *   <li>Collections are never dropped. {@code _addColl} discards a low-confidence collection whose
 *       domain is {@code "Other"} <em>only when no domain was forced</em>, and this branch always
 *       forces one ({@code aggregatedCollectionDomains[c] || _detectDomain(c)}, and
 *       {@code _detectDomain} returns the truthy string {@code "Other"}). So every entry of
 *       {@code aggregatedCollections} counts.</li>
 * </ul>
 *
 * <p><b>Memory.</b> One endpoint is read as a tree at a time; the document is never materialised.
 * The retained state is four string sets over collection names, domains and module labels —
 * bounded by the analysis's vocabulary, not by its endpoint count.
 *
 * @param endpoints   endpoints in the analysis ({@code epReports.length})
 * @param collections distinct collection names across every endpoint
 * @param views       of those, the ones the view-detection rules classify as views
 * @param modules     distinct project labels across every endpoint's {@code modules}
 * @param domains     distinct collection domains, excluding {@code "Other"}
 * @param crossModule endpoints with at least one out-of-module call
 * @param loc         summed {@code totalLoc}
 * @param txn         endpoints writing to collections in more than one domain
 * @param batch       endpoints whose path or handler name matches a batch keyword
 * @param scheduled   {@code @Scheduled} methods, straight from the summary document
 */
public record SummaryStats(long endpoints, long collections, long views, long modules,
                           long domains, long crossModule, long loc, long txn,
                           long batch, long scheduled) {

    /** The operations {@code _finishRender} treats as writes when deciding "needs a transaction". */
    private static final Set<String> WRITE_OPS = Set.of("WRITE", "UPDATE", "DELETE");

    /** {@code /-\d[\d.]*(?:-SNAPSHOT)?\.jar$/i} — the version suffix {@code _jarToProject} strips. */
    private static final Pattern VERSIONED_JAR =
            Pattern.compile("(?i)-\\d[\\d.]*(?:-SNAPSHOT)?\\.jar$");
    private static final Pattern PLAIN_JAR = Pattern.compile("(?i)\\.jar$");

    /**
     * The slice of {@code domain-config.json} the ten aggregates depend on.
     *
     * <p>Deliberately a value object rather than a reference to {@code DomainConfigLoader}: the
     * numbers must be reproducible in a test from a config file alone, without a Spring context,
     * because that reproduction is how "unchanged" is proved.
     *
     * @param collPrefixList {@code [prefix, domain]} pairs, <b>pre-sorted by prefix length
     *                       descending</b> — the browser sorts them that way at load, and a
     *                       shorter prefix winning first would reassign domains
     * @param sbDomainMap    {@code SB_*} overrides, iterated in file order as the browser does
     * @param batchKeywords  matched case-sensitively against a lower-cased haystack, exactly as
     *                       {@code _isBatchEndpoint} does — a config keyword with a capital letter
     *                       matches nothing there and must match nothing here
     */
    public record Config(List<String[]> collPrefixList, Map<String, String> sbDomainMap,
                         List<String> viewContains, List<String> viewStartsWith,
                         List<String> batchKeywords, Set<String> jarIgnoreSegments) {

        public Config(List<String[]> collPrefixList, Map<String, String> sbDomainMap,
                      List<String> viewContains, List<String> viewStartsWith,
                      List<String> batchKeywords) {
            this(collPrefixList, sbDomainMap, viewContains, viewStartsWith, batchKeywords, Set.of());
        }

        public Config(List<String[]> collPrefixList, Map<String, String> sbDomainMap,
                      List<String> viewContains, List<String> viewStartsWith,
                      List<String> batchKeywords, Set<String> jarIgnoreSegments) {
            this.collPrefixList = collPrefixList == null ? List.of() : List.copyOf(collPrefixList);
            this.sbDomainMap = sbDomainMap == null ? Map.of() : new LinkedHashMap<>(sbDomainMap);
            this.viewContains = viewContains == null ? List.of() : List.copyOf(viewContains);
            this.viewStartsWith = viewStartsWith == null ? List.of() : List.copyOf(viewStartsWith);
            this.batchKeywords = batchKeywords == null ? List.of() : List.copyOf(batchKeywords);
            this.jarIgnoreSegments = jarIgnoreSegments == null ? Set.of() : Set.copyOf(jarIgnoreSegments);
        }

        /**
         * Builds the config the way {@code summary.js:_loadConfig} builds it, from the same file.
         *
         * <p>The prefix sort and the flattening of {@code endpoint_classification} into one
         * keyword list are transcribed rather than reimplemented; both are load-bearing.
         */
        public static Config fromDomainConfig(Map<String, Object> config) {
            Map<String, Object> cfg = config == null ? Map.of() : config;

            List<String[]> pairs = new ArrayList<>();
            if (cfg.get("module_types") instanceof Map<?, ?> modTypes) {
                for (Map.Entry<?, ?> e : modTypes.entrySet()) {
                    if (!(e.getValue() instanceof Map<?, ?> info)) continue;
                    if (!(info.get("prefixes") instanceof List<?> prefixes)) continue;
                    for (Object p : prefixes) {
                        if (p != null) {
                            pairs.add(new String[]{String.valueOf(p), String.valueOf(e.getKey())});
                        }
                    }
                }
            }
            // Stable, longest-prefix-first — identical to
            // `pairs.sort((a, b) => b[0].length - a[0].length)` over an insertion-ordered object,
            // because both sorts are stable and both inputs are in file order.
            pairs.sort((a, b) -> b[0].length() - a[0].length());

            Map<String, String> sb = new LinkedHashMap<>();
            if (cfg.get("snapshot_buffer_domain_map") instanceof Map<?, ?> m) {
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    sb.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
                }
            }

            List<String> contains = new ArrayList<>();
            List<String> startsWith = new ArrayList<>();
            if (cfg.get("view_detection") instanceof Map<?, ?> vd) {
                addStrings(contains, vd.get("contains"));
                addStrings(startsWith, vd.get("starts_with"));
            }

            List<String> keywords = new ArrayList<>();
            if (cfg.get("endpoint_classification") instanceof Map<?, ?> ec) {
                for (Object v : ec.values()) {
                    if (v instanceof Map<?, ?> info) addStrings(keywords, info.get("keywords"));
                }
            }
            Set<String> ignoreSegments = new java.util.LinkedHashSet<>();
            if (cfg.get("jar_name_ignore_segments") instanceof List<?> segs) {
                for (Object o : segs) if (o != null) ignoreSegments.add(String.valueOf(o));
            }
            return new Config(pairs, sb, contains, startsWith, keywords, ignoreSegments);
        }

        private static void addStrings(List<String> into, Object maybeList) {
            if (maybeList instanceof List<?> list) {
                for (Object o : list) if (o != null) into.add(String.valueOf(o));
            }
        }
    }

    /**
     * Streams the summary document once and returns the ten aggregates.
     *
     * <p>The uploaded JAR's name is taken from the document's own {@code jarName} field rather than
     * from the provider, because that is where the browser gets it
     * ({@code JA.app.currentAnalysis.jarName}) and it is what {@code _jarToProject} munges into the
     * label for the {@code "main"} module. Raw JAR names are collected during the pass and mapped
     * to labels after it, so the answer does not depend on {@code jarName} preceding
     * {@code endpoints} in the document.
     *
     * @param parser positioned before the document's opening brace
     */
    public static SummaryStats compute(JsonParser parser, Config cfg, ObjectMapper mapper)
            throws IOException {
        Set<String> allColls = new HashSet<>();
        Set<String> allViews = new HashSet<>();
        Set<String> rawModules = new HashSet<>();
        Set<String> allDomains = new HashSet<>();
        long endpoints = 0, crossModule = 0, loc = 0, txn = 0, batch = 0;
        long scheduledCount = 0, scheduledJobs = 0;
        String jarName = null;

        parser.nextToken();     // START_OBJECT
        while (parser.nextToken() != null) {
            JsonToken token = parser.currentToken();
            if (token == JsonToken.END_OBJECT) break;
            if (token != JsonToken.FIELD_NAME) continue;
            String field = parser.currentName();

            if (InvocationPool.POOL_FIELD.equals(field)) {
                // Skipped as a unit, and it must be: the loop breaks on any END_OBJECT, so
                // descending into the pool array would end the pass at the first pooled
                // invocation's closing brace and silently report stats for a fraction of the
                // document.
                parser.nextToken();
                parser.skipChildren();
                continue;
            }
            if ("jarName".equals(field)) {
                parser.nextToken();
                jarName = parser.getValueAsString("");
                continue;
            }
            if ("scheduledCount".equals(field)) {
                parser.nextToken();
                scheduledCount = parser.getValueAsLong(0);
                continue;
            }
            if ("scheduledJobs".equals(field)) {
                // Counted rather than read: `scheduledCount || scheduledJobs.length` is the
                // browser's expression, and the fallback arm has to survive an old analysis that
                // has the array but not the scalar.
                if (parser.nextToken() == JsonToken.START_ARRAY) {
                    int depth = 0;
                    while (parser.nextToken() != null) {
                        JsonToken t = parser.currentToken();
                        if (t == JsonToken.END_ARRAY && depth == 0) break;
                        if (t == JsonToken.START_OBJECT || t == JsonToken.START_ARRAY) {
                            if (depth == 0) scheduledJobs++;
                            parser.skipChildren();
                        } else if (depth == 0) {
                            scheduledJobs++;
                        }
                    }
                }
                continue;
            }
            if (!"endpoints".equals(field)) {
                parser.nextToken();
                parser.skipChildren();      // classIndex, the collection maps, scalars
                continue;
            }

            parser.nextToken();             // START_ARRAY
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                if (parser.currentToken() != JsonToken.START_OBJECT) continue;
                // One endpoint, not the document — the same bound `streamSummaryEndpointPage` uses.
                JsonNode ep = mapper.readTree(parser);
                endpoints++;
                loc += ep.path("totalLoc").asLong(0);
                if (ep.path("externalScopeCalls").asLong(0) > 0) crossModule++;

                // `(ep.modules && ep.modules.length) ? ep.modules : ['main']`
                JsonNode modules = ep.get("modules");
                if (modules != null && modules.isArray() && !modules.isEmpty()) {
                    for (JsonNode m : modules) rawModules.add(m.asText(""));
                } else {
                    rawModules.add("main");
                }

                JsonNode domainMap = ep.get("aggregatedCollectionDomains");
                JsonNode opMap = ep.get("aggregatedOperations");
                String derivedOp = text(ep.get("derivedOperationType"));
                Set<String> writeDomains = new HashSet<>();

                JsonNode colls = ep.get("aggregatedCollections");
                if (colls != null && colls.isArray()) {
                    for (JsonNode cn : colls) {
                        String name = cn.asText("");
                        if (name.isEmpty()) continue;       // `_addColl` returns on a falsy name
                        allColls.add(name);
                        if (isView(name, cfg)) allViews.add(name);

                        String domain = domainMap == null ? "" : text(domainMap.get(name));
                        if (domain.isEmpty()) domain = detectDomain(name, cfg);
                        if (!domain.isEmpty() && !"Other".equals(domain)) allDomains.add(domain);

                        String op = opMap == null ? "" : text(opMap.get(name));
                        if (op.isEmpty()) op = derivedOp;
                        if (op.isEmpty()) op = "READ";
                        if (WRITE_OPS.contains(op)) writeDomains.add(domain);
                    }
                }
                if (writeDomains.size() > 1) txn++;
                if (isBatch(ep, cfg)) batch++;
            }
        }

        long scheduled = scheduledCount != 0 ? scheduledCount : scheduledJobs;
        Set<String> moduleLabels = new HashSet<>();
        for (String raw : rawModules) moduleLabels.add(jarToProject(raw, jarName));
        return new SummaryStats(endpoints, allColls.size(), allViews.size(), moduleLabels.size(),
                allDomains.size(), crossModule, loc, txn, batch, scheduled);
    }

    /** The chip keys the topbar reads, so the client can render straight from this object. */
    public Map<String, Long> asChipMap() {
        Map<String, Long> m = new LinkedHashMap<>();
        m.put("endpoints", endpoints);
        m.put("collections", collections);
        m.put("views", views);
        m.put("modules", modules);
        m.put("domains", domains);
        m.put("crossmodule", crossModule);
        m.put("loc", loc);
        m.put("txn", txn);
        m.put("batch", batch);
        m.put("scheduled", scheduled);
        return m;
    }

    /** {@code summary-helpers.js:_classifyColl} — a name test, so it is global to the analysis. */
    static boolean isView(String name, Config cfg) {
        for (String s : cfg.viewContains()) if (name.contains(s)) return true;
        for (String s : cfg.viewStartsWith()) if (name.startsWith(s)) return true;
        return false;
    }

    /**
     * {@code summary-helpers.js:_detectDomain}, transcribed.
     *
     * <p>Only reached for a collection the analysis did not already carry a domain for. See the
     * class comment for why this is not {@code DomainConfigLoader#detectCollectionDomain}.
     */
    static String detectDomain(String name, Config cfg) {
        if (name == null || name.isEmpty()) return "Other";
        if (name.startsWith("SB_")) {
            for (Map.Entry<String, String> e : cfg.sbDomainMap().entrySet()) {
                if (name.startsWith(e.getKey())) return e.getValue();
            }
        }
        String n = name.startsWith("SB_") ? name.substring(3) : name;
        for (String[] pair : cfg.collPrefixList()) {
            if (n.startsWith(pair[0])) return pair[1];
        }
        return "Other";
    }

    /**
     * {@code summary-analyzer.js:_isBatchEndpoint}, transcribed.
     *
     * <p>The haystack is {@code fullPath + ' ' + endpointName} where {@code endpointName} is
     * {@code controllerSimpleName + '.' + methodName} — including the dot, and including it even
     * when both halves are absent, because a keyword containing a dot would otherwise match
     * differently here than in the browser.
     */
    static boolean isBatch(JsonNode ep, Config cfg) {
        String haystack = (text(ep.get("fullPath")) + ' '
                + text(ep.get("controllerSimpleName")) + '.' + text(ep.get("methodName")))
                .toLowerCase(Locale.ROOT);
        for (String kw : cfg.batchKeywords()) {
            if (!kw.isEmpty() && haystack.contains(kw)) return true;
        }
        return false;
    }

    /**
     * {@code summary-helpers.js:_jarToProject}, transcribed for the state the Summary tab is in
     * when it computes the chips.
     *
     * <p>The browser consults {@code _jarModuleMap} (POM artifactIds) first, but that map is
     * populated from each endpoint's {@code externalCalls} — a field {@code /summary/headers}
     * strips. So at the moment the chips are computed the map is empty and every label comes from
     * the file-name munge below. Transcribing the munge rather than calling
     * {@code DomainConfigLoader#jarToProject} keeps two differences from mattering: that method
     * returns {@code "Main App"} for a null name and would split away a leading empty segment.
     */
    static String jarToProject(String jarName, String uploadedJarName) {
        if (jarName == null || jarName.isEmpty() || "main".equals(jarName)) {
            if (uploadedJarName != null && !uploadedJarName.isEmpty()) {
                return munge(uploadedJarName);
            }
            return "Primary";
        }
        return munge(jarName);
    }

    /**
     * {@code summary-helpers.js:_jarToProject}, transcribed with the live {@code _jarModuleMap}
     * consulted (unlike {@link #jarToProject(String, String)} above, which is only valid where
     * that map is known to be empty). Used by {@link EndpointReportBuilder}, which reads the
     * document {@code /summary/headers} does <b>not</b> strip {@code externalCalls} from, so the
     * map is not reliably empty there.
     *
     * <p>A map hit is returned as-is (it already holds a POM artifactId, not a raw JAR name) —
     * unlike the no-map fallback, which munges the raw name into a label.
     */
    static String jarToProject(String jarName, String uploadedJarName, Map<String, String> jarModuleMap) {
        if (jarName == null || jarName.isEmpty() || "main".equals(jarName)) {
            if (jarModuleMap != null && jarModuleMap.get("main") != null && !jarModuleMap.get("main").isEmpty()) {
                return jarModuleMap.get("main");
            }
            return jarToProject(jarName, uploadedJarName);
        }
        if (jarModuleMap != null && jarModuleMap.get(jarName) != null && !jarModuleMap.get(jarName).isEmpty()) {
            return jarModuleMap.get(jarName);
        }
        return jarToProject(jarName, uploadedJarName);
    }

    /**
     * {@code summary-helpers.js:_jarToDomain}, transcribed. Like {@link #jarToProject(String,
     * String, Map)}, this consults the live {@code _jarModuleMap} rather than assuming it empty.
     */
    static String jarToDomain(String jarName, Map<String, String> jarModuleMap, Config cfg) {
        Set<String> ignore = cfg.jarIgnoreSegments();
        if (jarName == null || jarName.isEmpty() || "main".equals(jarName)) {
            String mapped = jarModuleMap == null ? null : jarModuleMap.get("main");
            if (mapped != null && !mapped.isEmpty()) {
                List<String> parts = filterSegments(mapped.toLowerCase(Locale.ROOT), ignore);
                if (!parts.isEmpty()) return titleJoin(parts);
            }
            return "Core";
        }
        String mapped = jarModuleMap == null ? null : jarModuleMap.get(jarName);
        String name = (mapped != null && !mapped.isEmpty())
                ? mapped.toLowerCase(Locale.ROOT)
                : PLAIN_JAR.matcher(VERSIONED_JAR.matcher(jarName).replaceAll("")).replaceAll("")
                          .toLowerCase(Locale.ROOT);
        List<String> parts = filterSegments(name, ignore);
        if (parts.isEmpty()) return titleJoin(List.of(name.split("-", -1)));
        return titleJoin(parts);
    }

    private static List<String> filterSegments(String name, Set<String> ignore) {
        List<String> out = new ArrayList<>();
        for (String p : name.split("-", -1)) if (!ignore.contains(p)) out.add(p);
        return out;
    }

    /** {@code parts.map(w => upper(w[0]) + w.slice(1)).join(' ')} — an empty {@code w} contributes
     *  an empty string at its position, not a skipped position, exactly as JS join does. */
    private static String titleJoin(List<String> parts) {
        List<String> titled = new ArrayList<>(parts.size());
        for (String w : parts) {
            titled.add(w.isEmpty() ? "" : Character.toUpperCase(w.charAt(0)) + w.substring(1));
        }
        return String.join(" ", titled);
    }

    /** {@code n.split('-').map(w => upper(w[0]) + lower(w.slice(1))).join(' ')}. */
    private static String munge(String name) {
        String n = PLAIN_JAR.matcher(VERSIONED_JAR.matcher(name).replaceAll("")).replaceAll("");
        // -1 keeps trailing empty segments, as JavaScript's split does.
        String[] parts = n.split("-", -1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) sb.append(' ');
            String w = parts[i];
            if (w.isEmpty()) continue;
            sb.append(Character.toUpperCase(w.charAt(0)))
              .append(w.substring(1).toLowerCase(Locale.ROOT));
        }
        return sb.toString();
    }

    private static String text(JsonNode node) {
        return node == null || node.isNull() ? "" : node.asText("");
    }
}
