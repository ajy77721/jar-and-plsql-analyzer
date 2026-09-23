package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.ArtifactKey;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Single source of truth for mapping between an analysis <b>identity name</b>
 * (used in REST URLs and reader lookups) and its <b>on-disk directory</b>.
 *
 * <p>Storage is organised <i>datasource / project / environment / object</i> wise
 * (not a flat hash), i.e. the physical folder is:
 * <pre>{data}/plsql-parse/&lt;DBTYPE&gt;/&lt;PROJECT&gt;/&lt;ENV&gt;/&lt;OBJECT&gt;/</pre>
 * and the schema-resolver cache mirrors it:
 * <pre>{data}/cache/schema-resolver/&lt;DBTYPE&gt;/&lt;PROJECT&gt;/&lt;ENV&gt;/</pre>
 *
 * <p>The identity {@code name} is the same path with {@code '/'} replaced by the
 * URL-safe delimiter {@code '~'} — e.g. {@code DB2~Db2Demo~local~ERP_USP_RUN_CYCLE}.
 * {@code '~'} is an RFC-3986 unreserved character (safe in a Spring {@code @PathVariable}
 * and in {@code encodeURIComponent}) and is never produced by the object-name
 * sanitiser, so the mapping is unambiguous and reversible.
 *
 * <p>Scopeless runs (no datasource/project/env) fall back to a bare {@code OBJECT}
 * name and a flat folder, preserving backwards compatibility with older data.
 */
public final class AnalysisPaths {

    /** URL-safe delimiter separating the path segments inside an identity name. */
    public static final char SEP = '~';

    private AnalysisPaths() {}

    /** Uppercase + strip anything that can't be a single safe path segment. */
    public static String seg(String raw) {
        if (raw == null) return "";
        String s = raw.trim().toUpperCase()
                .replace('.', '_').replace('/', '_').replace('\\', '_')
                .replace(SEP, '_');
        // collapse whitespace and any other unsafe chars
        s = s.replaceAll("[^A-Za-z0-9_\\-]", "_");
        return s;
    }

    /**
     * Build the identity name from context + object. Empty context parts are dropped,
     * so a fully scopeless call returns just the object segment.
     */
    public static String name(String dbType, String project, String environment, String object) {
        List<String> parts = new ArrayList<>();
        String d = seg(dbType), p = seg(project), e = seg(environment), o = seg(object);
        if (!d.isEmpty()) parts.add(d);
        if (!p.isEmpty()) parts.add(p);
        if (!e.isEmpty()) parts.add(e);
        parts.add(o.isEmpty() ? "UNNAMED" : o);
        return String.join(String.valueOf(SEP), parts);
    }

    /** The scope token (context only, no object) used for the schema-resolver cache. */
    public static String scope(String dbType, String project, String environment) {
        List<String> parts = new ArrayList<>();
        String d = seg(dbType), p = seg(project), e = seg(environment);
        if (!d.isEmpty()) parts.add(d);
        if (!p.isEmpty()) parts.add(p);
        if (!e.isEmpty()) parts.add(e);
        return String.join(String.valueOf(SEP), parts);
    }

    /** The object (leaf) segment of an identity name. */
    public static String leaf(String name) {
        if (name == null || name.isEmpty()) return name;
        int i = name.lastIndexOf(SEP);
        return i < 0 ? name : name.substring(i + 1);
    }

    // ── Path accessors ──────────────────────────────────────────────────────
    //
    // Path-based ONLY: the base must be supplied as a Path the caller already holds. The
    // String-base twins that used to sit here (dir/parentDir/resolve taking a String) rebuilt the
    // root with Path.of(...), which ALWAYS resolves on the default local filesystem — so when the
    // data root is not local the write succeeds, nothing errors, and the data lands in the wrong
    // place. They had no callers left and are gone; use the ArtifactKey accessors below unless you
    // genuinely hold a local path (AnalysisService's env-config file is the only such case).

    /** Resolve an identity name to its nested directory under {@code base}. */
    public static Path dir(Path base, String name) {
        Path p = base;
        for (String seg : name.split(String.valueOf(SEP))) {
            if (!seg.isEmpty()) p = p.resolve(seg);
        }
        return p;
    }

    /** The parent directory (DBTYPE/PROJECT/ENV) under a base, for a given name. */
    public static Path parentDir(Path base, String name) {
        Path full = dir(base, name);
        Path parent = full.getParent();
        return parent != null ? parent : base;
    }

    /** Resolve a file inside an analysis directory. */
    public static Path resolve(Path base, String name, String relative) {
        Path p = dir(base, name);
        for (String seg : relative.split("/")) {
            if (!seg.isEmpty()) p = p.resolve(seg);
        }
        return p;
    }

    /**
     * Convert a directory (that lives somewhere under {@code base}) back to its
     * identity name, i.e. the base-relative path with {@code '/'} → {@code '~'}.
     */
    public static String nameOf(Path base, Path dir) {
        Path rel = base.relativize(dir);
        return rel.toString().replace('/', SEP).replace('\\', SEP);
    }

    // ── Storage keys ────────────────────────────────────────────────────────
    //
    // Parallel to the Path accessors above. A key is storage-neutral: the same call resolves to a
    // local file or an S3 object depending only on which ArtifactStore is wired.
    //
    // NOTE ON ROOT: keys are relative to the DATA ROOT (app.data-dir.base — see StorageConfig),
    // while the Path forms above are resolved against a base that already includes the subtree
    // segment ({root}/plsql-parse for parser output, {root}/plsql for the Claude sidecar). Keys
    // must therefore name that subtree EXPLICITLY, or every key would land one level too high.
    // AnalysisPathsKeyParityTest guards exactly this.

    /** Parser output subtree: {@code {root}/plsql-parse}. */
    public static final String PARSE_ROOT = "plsql-parse";

    /** Claude sidecar subtree: {@code {root}/plsql} (see DirPropertyResolver's plsql.data-dir). */
    public static final String CLAUDE_ROOT = "plsql";

    /** {@code plsql-parse/<DBTYPE>/<PROJECT>/<ENV>/<OBJECT>} for an identity name. */
    public static ArtifactKey dirKey(String name) {
        ArtifactKey k = ArtifactKey.of(PARSE_ROOT);
        for (String seg : name.split(String.valueOf(SEP))) {
            if (!seg.isEmpty()) k = k.resolve(seg);
        }
        return k;
    }

    /** The nested parent (DBTYPE/PROJECT/ENV) of an identity name, under {@code plsql-parse/}. */
    public static ArtifactKey parentDirKey(String name) {
        ArtifactKey full = dirKey(name);
        ArtifactKey parent = full.parent();
        return parent != null ? parent : ArtifactKey.of(PARSE_ROOT);
    }

    /** A file inside an analysis directory, e.g. {@code "api/index.json"}. */
    public static ArtifactKey resolveKey(String name, String relative) {
        ArtifactKey k = dirKey(name);
        for (String seg : relative.split("/")) {
            if (!seg.isEmpty()) k = k.resolve(seg);
        }
        return k;
    }

    /** {@code plsql/<name>} — the flat Claude sidecar folder, keyed by the identity name. */
    public static ArtifactKey claudeDirKey(String name) {
        return ArtifactKey.of(CLAUDE_ROOT, name);
    }

    /** A file inside the Claude sidecar folder, e.g. {@code "analysis_claude.json"}. */
    public static ArtifactKey claudeFileKey(String name, String relative) {
        ArtifactKey k = claudeDirKey(name);
        for (String seg : relative.split("/")) {
            if (!seg.isEmpty()) k = k.resolve(seg);
        }
        return k;
    }

    /**
     * Maps a key back to the identity name of the analysis it belongs to — the inverse of
     * {@link #dirKey(String)} for the {@code plsql-parse/} subtree.
     *
     * @param dirKey a key naming an analysis directory (not a file inside it)
     */
    public static String nameOfKey(ArtifactKey dirKey) {
        List<String> segs = dirKey.segments();
        if (segs.isEmpty() || !PARSE_ROOT.equals(segs.get(0))) return dirKey.toString();
        return String.join(String.valueOf(SEP), segs.subList(1, segs.size()));
    }

    // The directory walk that used to live here (listAnalysisDirs) is gone: it walked the local
    // filesystem and stat-ed api/index.json per candidate directory, so it found nothing once the
    // parse tree stopped being local — and, on object storage, cost one HEAD per directory. Its
    // replacement is AnalysisService.indexKeysByName(), a single statPrefix over plsql-parse/.
    // It had no production callers when it was removed.
}
