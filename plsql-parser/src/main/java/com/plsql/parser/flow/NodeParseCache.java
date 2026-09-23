package com.plsql.parser.flow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.plsql.parser.model.FlowNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Disk cache for fully-resolved FlowNode results.
 *
 * Cache key = visitKey (e.g. "PG_UWGE_UTILS.FN_CHK_PREMIUM_VALIDATE") + MD5 of raw source.
 * If the source hasn't changed the node is returned from cache — skipping ANTLR entirely.
 *
 * Cache lives at: <flowOutputKey>/.node-cache/<VISITKEY_HASH>.json, written through the
 * {@link ArtifactStore} so it follows the configured backend.
 */
public class NodeParseCache {

    private static final Logger log = LoggerFactory.getLogger(NodeParseCache.class);

    private final ArtifactStore store;
    private final ArtifactKey cacheDir;
    private final ObjectMapper mapper;

    public NodeParseCache(ArtifactStore store, ArtifactKey flowOutputKey) throws IOException {
        this.store = store;
        this.cacheDir = flowOutputKey.resolve(".node-cache");
        this.mapper = new ObjectMapper();
        // Indentation is 27.6% of the bytes on this corpus (310.3 MB pretty, 224.7 MB
        // compact across 2,908 files) and nothing reads it. -Dplsql.artifacts.pretty=true
        // restores it for a debugging run.
        if (ArtifactFormatting.pretty()) mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    /** Return a cached FlowNode, or null if not in cache / cache corrupt. */
    public FlowNode get(String visitKey, String sourceText) {
        ArtifactKey file = cacheFile(visitKey, sourceText);
        if (!store.exists(file)) return null;
        try (InputStream in = store.read(file)) {
            FlowNode node = mapper.readValue(in, FlowNode.class);
            log.info("[NodeCache] HIT  {} ({})", visitKey, file.leaf());
            return node;
        } catch (IOException e) {
            log.warn("[NodeCache] Corrupt cache entry for {}, will reparse: {}", visitKey, e.getMessage());
            store.delete(file);
            return null;
        }
    }

    /** Persist a FlowNode to cache. */
    public void put(String visitKey, String sourceText, FlowNode node) {
        ArtifactKey file = cacheFile(visitKey, sourceText);
        try (OutputStream out = store.write(file)) {
            mapper.writeValue(out, node);
            log.info("[NodeCache] STORED {} ({})", visitKey, file.leaf());
        } catch (IOException e) {
            log.warn("[NodeCache] Failed to cache {}: {}", visitKey, e.getMessage());
        }
    }

    /**
     * Bumped whenever a change alters what a parsed node CONTAINS.
     *
     * <p>The cache key is a hash of the visit key and the source text. Identical source therefore
     * always hit the cache, however much the analyser had changed underneath — so an improvement to
     * what we extract never reached any node that was already cached.
     *
     * <p>That is not hypothetical. After column-level detail was added and then improved, a re-run
     * produced FEWER tables with column detail than before (Oracle 30 -> 19, and row counts fell
     * for MySQL and DB2), because 48 Oracle nodes were served from caches written by an older
     * build and those cached operations carry no {@code columnUsages}. Nothing failed; the analysis
     * was simply, silently, less complete than the code could produce — the worst shape of bug this
     * project keeps finding.
     *
     * <p>Including the version in the hash means an old cache is not invalidated so much as never
     * consulted: its key can no longer be produced. Old files are inert and get cleaned by whatever
     * retention already applies.
     *
     * <p><b>Bump this</b> when changing what any visitor, {@code TableOperationEnricher}, or
     * {@code ChunkedFlowWriter} records on a node. Adding a field nobody reads does not need it;
     * changing what is extracted does.
     */
    /**
     * Bumped whenever a grammar or visitor change alters what a parse produces.
     *
     * <p>v19: two extraction fixes changed what a node carries without any grammar change, so the
     * cache would otherwise hide both exactly the way v3's own note describes. (1) ING-019: dynamic
     * SQL DDL (an {@code EXECUTE IMMEDIATE 'CREATE TABLE ...'}, etc. — the only way Oracle writes
     * DDL inside a procedure body) is now classified as {@code DDL} with a real {@code sqlText}
     * instead of the generic {@code DYNAMIC} bucket it was silently filed under before. (2) MID-021
     * (§138): {@code TableOperationEnricher}/{@code ChunkedFlowWriter} now also enrich and carry
     * through {@code SubprogramInfo}-level table operations (procedures declared inside a
     * {@code PACKAGE BODY}), which previously got zero column/predicate/join/DDL enrichment at all —
     * and the predicate/{@code WHERE}-filter data on every table access, always extracted but
     * previously dropped in {@code writeTableIndex}'s own grouping step, now survives into
     * {@code usedBy[].predicates}. A cached node from before this version carries none of this.
     *
     * <p>v3: the MySQL catch-all rule was swallowing ELSE/ELSEIF branches and the DML inside them.
     * Fixing the grammar changed nothing on its own, because every node was served from this cache —
     * the analysis re-ran in 20 seconds and reported the same 160 lost statements. A grammar fix
     * that the cache hides is not a fix, and this constant is what makes it land.
     *
    
    
    
    
    
    
     * <p>v10 (Sep05-02/03): joins reach the operation and are emitted, and each access carries a
     * derived accessKind.
     *
     * <p>v9: captured statement text is cut at the first unquoted semicolon, so a context spanning
     * two statements no longer yields text that parses as neither.
     *
     * <p>v8 (Sep05-09): DDL column definitions are emitted, so a CREATE TABLE shows the columns it
     * declares instead of a blank cell.
     *
     * <p>v7 (Sep05-01): predicates, INTO targets and column qualifiers now reach the operation.
     * Cached nodes carry none of them.
     *
     * <p>v6: the captured SELECT text now stops at the SELECT rather than the control-flow
     * statement wrapping it, so a trailing LOOP/THEN no longer makes it unparseable.
     *
     * <p>v5: DB2 and PostgreSQL now capture the enclosing statement text for SELECT table ops, so
     * column detail can be derived from reads as well as writes.
     *
     * <p>v4: TableOperationInfo now records columnsParsed per statement, so a blank column cell can
     * be told apart from one we failed to parse. Cached nodes carry neither.
     *
     * <p>v19: PostgreSQL EXECUTE now captures the dynamic-SQL expression's real source text into
     * {@code sqlSource}/{@code sqlExpression} instead of leaving it blank (every EXECUTE row in the
     * real corpus previously carried {@code sqlExpression: ""}), and cursors now record OPEN/FETCH/
     * CLOSE as additional operations rather than only ever showing {@code ["DECLARE"]}. Both are
     * PostgreSQL-visitor-only changes with no grammar-shape difference the cache key would otherwise
     * see, so a stale cached node would keep serving the old, incomplete shape forever.
     *
     * <p>v20: the SQL Server SELECT/FROM/JOIN grammar migration plus three full cross-dialect audit
     * rounds (~50 real bugs fixed across all five dialects' grammars/visitors). Same class of
     * problem as v19 at far larger scale — none of it changes the outer statement-skeleton grammar
     * shape the cache key would otherwise see.
     *
     * <p>v21: DB2's {@code visitLeaveStatement}/{@code visitIterateStatement} no longer fold the
     * loop label into the statement TYPE string ({@code "LEAVE:" + label} produced one distinct
     * statement type per uniquely-labeled loop — 554 of 567 keys in one real routine — which tripped
     * the entity-ingest schema-safety guard). Type is now the bare {@code LEAVE}/{@code ITERATE};
     * the label lives in {@code Db2StatementInfo.label}. Visitor-only change, no grammar-shape
     * difference, so — same as v19/v20 — a cached node would keep emitting the old exploded type
     * strings forever without this bump.
     *
     * <p>v22: Postgres's {@code visitFunctionCall} no longer records SQL operator keywords as
     * calls. PostgresParser.g4's permissive {@code id} rule admits {@code IN_KW}/{@code EXISTS}/
     * {@code CAST}/… as identifiers, so {@code WHERE source_type IN ('a','b')} structurally matched
     * {@code functionCall} and was emitted as a call edge to a routine named {@code IN} — a phantom
     * {@code ERP$IN} node in the call graph. Only {@code VALUES} had been special-cased; the guard
     * is now the {@code SQL_NON_CALL_KEYWORDS} set. Visitor-only change, no grammar-shape
     * difference, so — same as v19/v20/v21 — a cached node would keep emitting the phantom edge
     * forever without this bump.
     */
    static final String PARSE_SCHEMA_VERSION = "v22-postgres-sql-keyword-not-a-call";

    private ArtifactKey cacheFile(String visitKey, String sourceText) {
        String hash = md5(PARSE_SCHEMA_VERSION + "||" + visitKey.toUpperCase()
                + "||" + (sourceText != null ? sourceText : ""));
        // safe filename: visitKey prefix for readability + full hash for uniqueness
        String safeName = visitKey.toUpperCase()
                .replaceAll("[^A-Z0-9_.]", "_")
                .replaceAll("_{2,}", "_");
        if (safeName.length() > 80) safeName = safeName.substring(0, 80);
        return cacheDir.resolve(safeName + "_" + hash + ".json");
    }

    private static String md5(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(32);
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            return Integer.toHexString(input.hashCode());
        }
    }
}
