package com.plsql.parser.flow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsql.parser.model.ParseResult;
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
 * Disk cache for ParseResult objects keyed on MD5 of source text.
 * One entry per unique source text — shared across all subprograms that come from
 * the same package body. Eliminates repeated ANTLR parses of the same source across runs.
 * Cache lives at: <outputKey>/.parse-cache/<source_md5>.json, via the ArtifactStore.
 */
public class ParseResultCache {

    private static final Logger log = LoggerFactory.getLogger(ParseResultCache.class);

    private final ArtifactStore store;
    private final ArtifactKey cacheDir;
    private final ObjectMapper mapper;

    public ParseResultCache(ArtifactStore store, ArtifactKey outputKey) throws IOException {
        this.store = store;
        this.cacheDir = outputKey.resolve(".parse-cache");
        this.mapper = new ObjectMapper();
    }

    public ParseResult get(String sourceText) {
        if (sourceText == null) return null;
        ArtifactKey file = cacheFile(sourceText);
        if (!store.exists(file)) return null;
        try (InputStream in = store.read(file)) {
            ParseResult result = mapper.readValue(in, ParseResult.class);
            log.info("[ParseCache] HIT {}", file.leaf());
            return result;
        } catch (IOException e) {
            log.warn("[ParseCache] Corrupt entry, will reparse: {}", e.getMessage());
            store.delete(file);
            return null;
        }
    }

    public void put(String sourceText, ParseResult result) {
        if (sourceText == null || result == null) return;
        ArtifactKey file = cacheFile(sourceText);
        if (store.exists(file)) return; // already stored — don't overwrite
        try (OutputStream out = store.write(file)) {
            mapper.writeValue(out, result);
        } catch (IOException e) {
            log.warn("[ParseCache] Failed to store: {}", e.getMessage());
        }
    }

    private ArtifactKey cacheFile(String sourceText) {
        return cacheDir.resolve(md5(sourceText) + ".json");
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
