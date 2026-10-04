package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Splits the class-tree sidecar into fixed-size chunk files plus a small index (plan 2d).
 *
 * <h2>Why chunk at all</h2>
 *
 * <p>{@code _class-tree.json} is a single projected array. Splitting it into {@code ~500}-class files
 * under {@code _class-tree/} lets the serving path read a chunk at a time instead of opening one
 * large array, and gives a future index-aware pager the {@code (firstFqn, lastFqn, rowCount)} bounds
 * it needs to seek straight to the chunk covering a cursor. The chunk files carry the <b>same rows in
 * the same order</b> as the monolithic array, so concatenating them reproduces the array exactly —
 * which is what keeps every paged response byte-identical.
 *
 * <h2>How the chunks are built</h2>
 *
 * <p>From the already-written {@code _class-tree.json}, in one streaming pass with a buffer of at
 * most one row. This is a pass over the class-tree sidecar (KB-MB), never over {@code analysis.json}.
 * The monolithic file is still written for backward compatibility; the serving path prefers chunks
 * when the index is present.
 *
 * <h2>Index format</h2>
 *
 * <pre>{"chunkSize":500,"chunkCount":K,"rowCount":N,
 *  "chunks":[{"index":0,"rowCount":500,"firstFqn":"…","lastFqn":"…","length":12345}, …]}</pre>
 */
public final class ClassTreeChunks {

    /** Rows per chunk. Small enough to open cheaply, large enough to bound per-file overhead. */
    public static final int CHUNK_SIZE = 500;

    private static final ObjectMapper COMPACT = new ObjectMapper();

    private ClassTreeChunks() {
    }

    /** One chunk's bounds, as recorded in the index. */
    public record Chunk(int index, int rowCount, String firstFqn, String lastFqn, long length) {
    }

    /** The parsed index. */
    public record Index(int chunkSize, int chunkCount, int rowCount, List<Chunk> chunks) {
        /**
         * The first chunk whose {@code lastFqn} is at or after {@code fqn}, or 0.
         *
         * <p>Valid when the array is sorted by fully-qualified name — the bounds are only meaningful
         * under the order the rows are in. The class tree is in fact ordered by source JAR then
         * package then simple name; a caller seeking by that key must map it to the same string the
         * bounds were recorded from.
         */
        public int chunkForFqn(String fqn) {
            if (fqn == null || chunks.isEmpty()) return 0;
            for (Chunk c : chunks) {
                if (c.lastFqn() != null && c.lastFqn().compareTo(fqn) >= 0) return c.index();
            }
            return chunks.get(chunks.size() - 1).index();
        }
    }

    /**
     * Splits {@code monolithKey} into chunk files under {@code chunksDir} and writes the index.
     *
     * @return the index that was written
     */
    public static Index split(ArtifactStore store, ArtifactKey monolithKey, ArtifactKey chunksDir,
                              ArtifactKey indexKey) throws IOException {
        List<Chunk> chunks = new ArrayList<>();
        int rowIndex = 0;
        int chunkIndex = 0;
        int inChunk = 0;
        String firstFqn = null;
        String lastFqn = null;
        ChunkWriter writer = null;

        try (InputStream in = store.read(monolithKey);
             JsonParser parser = COMPACT.getFactory().createParser(in)) {
            if (parser.nextToken() != JsonToken.START_ARRAY) {
                Index empty = new Index(CHUNK_SIZE, 0, 0, List.of());
                writeIndex(store, indexKey, empty);
                return empty;
            }
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                if (parser.currentToken() != JsonToken.START_OBJECT) continue;
                JsonNode row = COMPACT.readTree(parser);
                if (writer == null) writer = new ChunkWriter(store, chunksDir, chunkIndex, "");
                writer.write(row);
                if (inChunk == 0) firstFqn = row.path("fullyQualifiedName").asText(null);
                lastFqn = row.path("fullyQualifiedName").asText(null);
                rowIndex++;
                inChunk++;
                if (inChunk == CHUNK_SIZE) {
                    chunks.add(writer.close(chunkIndex, inChunk, firstFqn, lastFqn));
                    chunkIndex++;
                    inChunk = 0;
                    firstFqn = null;
                    writer = null;
                }
            }
            if (writer != null) {
                chunks.add(writer.close(chunkIndex, inChunk, firstFqn, lastFqn));
            }
        }

        Index index = new Index(CHUNK_SIZE, chunks.size(), rowIndex, List.copyOf(chunks));
        writeIndex(store, indexKey, index);
        return index;
    }

    /** Loads a chunk's rows, or an empty list when the chunk is absent/unreadable. */
    public static List<JsonNode> readChunk(ArtifactStore store, ArtifactKey chunkKey) throws IOException {
        List<JsonNode> rows = new ArrayList<>();
        try (InputStream in = store.read(chunkKey);
             JsonParser parser = COMPACT.getFactory().createParser(in)) {
            if (parser.nextToken() != JsonToken.START_ARRAY) return rows;
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                if (parser.currentToken() != JsonToken.START_OBJECT) continue;
                rows.add(COMPACT.readTree(parser));
            }
        }
        return rows;
    }

    /** Parses the chunk index, or returns {@code null} when it is absent or malformed. */
    public static Index loadIndex(ArtifactStore store, ArtifactKey indexKey) throws IOException {
        if (store == null || indexKey == null) return null;
        JsonNode doc;
        try (InputStream in = store.read(indexKey)) {
            doc = COMPACT.readTree(in);
        } catch (IOException | RuntimeException e) {
            return null;
        }
        if (doc == null || !doc.isObject()) return null;
        JsonNode chunks = doc.get("chunks");
        if (chunks == null || !chunks.isArray()) return null;
        List<Chunk> out = new ArrayList<>();
        for (JsonNode c : chunks) {
            out.add(new Chunk(c.path("index").asInt(), c.path("rowCount").asInt(),
                    c.path("firstFqn").asText(null), c.path("lastFqn").asText(null),
                    c.path("length").asLong()));
        }
        int rowCount = doc.path("rowCount").asInt();
        int chunkSize = doc.path("chunkSize").asInt(CHUNK_SIZE);
        return new Index(chunkSize, out.size(), rowCount, List.copyOf(out));
    }

    private static void writeIndex(ArtifactStore store, ArtifactKey indexKey, Index index)
            throws IOException {
        List<Map<String, Object>> chunks = new ArrayList<>();
        for (Chunk c : index.chunks()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("index", c.index());
            m.put("rowCount", c.rowCount());
            if (c.firstFqn() != null) m.put("firstFqn", c.firstFqn());
            if (c.lastFqn() != null) m.put("lastFqn", c.lastFqn());
            m.put("length", c.length());
            chunks.add(m);
        }
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("chunkSize", index.chunkSize());
        doc.put("chunkCount", index.chunkCount());
        doc.put("rowCount", index.rowCount());
        doc.put("chunks", chunks);
        try (OutputStream out = store.write(indexKey)) {
            COMPACT.writeValue(out, doc);
        }
    }

    /**
     * Feeds rows to the chunk files as they are produced, so the chunks are built from the same
     * pass that writes the monolithic array — no second read of {@code _class-tree.json}.
     *
     * <p>Rows must be supplied in array order. {@link #finish()} closes the last chunk and writes
     * the index; the chunk files and the index are written under {@code suffix}, so a caller that
     * stages its writes can move them into place only after the document that produced them has
     * committed (see {@code PersistenceService}). Passing {@code ""} writes final keys directly.
     */
    public static final class Accumulator {
        private final ArtifactStore store;
        private final ArtifactKey chunksDir;
        private final ArtifactKey indexKey;
        private final String suffix;
        private final List<Chunk> chunks = new ArrayList<>();
        private int rowIndex;
        private int chunkIndex;
        private int inChunk;
        private String firstFqn;
        private String lastFqn;
        private ChunkWriter writer;

        public Accumulator(ArtifactStore store, ArtifactKey chunksDir, ArtifactKey indexKey,
                           String suffix) {
            this.store = store;
            this.chunksDir = chunksDir;
            this.indexKey = indexKey;
            this.suffix = suffix == null ? "" : suffix;
        }

        public void add(JsonNode row) throws IOException {
            if (writer == null) writer = new ChunkWriter(store, chunksDir, chunkIndex, suffix);
            writer.write(row);
            if (inChunk == 0) firstFqn = row.path("fullyQualifiedName").asText(null);
            lastFqn = row.path("fullyQualifiedName").asText(null);
            rowIndex++;
            inChunk++;
            if (inChunk == CHUNK_SIZE) {
                chunks.add(writer.close(chunkIndex, inChunk, firstFqn, lastFqn));
                chunkIndex++;
                inChunk = 0;
                firstFqn = null;
                writer = null;
            }
        }

        public Index finish() throws IOException {
            if (writer != null) {
                chunks.add(writer.close(chunkIndex, inChunk, firstFqn, lastFqn));
                writer = null;
            }
            Index index = new Index(CHUNK_SIZE, chunks.size(), rowIndex, List.copyOf(chunks));
            writeIndex(store, indexKey, index);
            return index;
        }
    }

    /** Streams one chunk file, tracking its byte length as it is written. */
    private static final class ChunkWriter {
        private final CountingOutputStream counting;
        private final JsonGenerator gen;
        private boolean first = true;

        ChunkWriter(ArtifactStore store, ArtifactKey chunksDir, int index, String suffix)
                throws IOException {
            counting = new CountingOutputStream(
                    new BufferedOutputStream(
                            store.write(chunksDir.resolve(index + ".json" + suffix)), 128 * 1024));
            gen = COMPACT.getFactory().createGenerator(counting);
            gen.writeStartArray();
            // The array brackets go through the generator; the rows are written raw to the counting
            // stream so separators are ours, not the generator's — the same technique the document
            // writer uses, and unambiguous about who writes the commas.
            gen.flush();
        }

        void write(JsonNode row) throws IOException {
            if (!first) counting.write(',');
            first = false;
            counting.write(COMPACT.writeValueAsBytes(row));
        }

        Chunk close(int index, int rowCount, String firstFqn, String lastFqn) throws IOException {
            gen.writeEndArray();
            gen.flush();
            long length = counting.count();
            gen.close();
            return new Chunk(index, rowCount, firstFqn, lastFqn, length);
        }
    }

    /** Counts bytes through a stream, so a chunk can record its own length without a stat. */
    private static final class CountingOutputStream extends java.io.FilterOutputStream {
        private long count;

        CountingOutputStream(OutputStream out) { super(out); }

        long count() { return count; }

        @Override public void write(int x) throws IOException { out.write(x); count++; }

        @Override public void write(byte[] buf, int off, int len) throws IOException {
            out.write(buf, off, len);
            count += len;
        }
    }
}
