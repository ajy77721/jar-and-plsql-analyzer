package com.analyzer.storage.repo;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Closeable;
import java.util.Iterator;

/**
 * A lazily-read sequence of rows that owns something needing closing — a {@code JsonParser} over a
 * store stream in the scan implementation, a database cursor in the Mongo one.
 *
 * <p>{@code Iterator} rather than {@code Stream} because the consumer is
 * {@code JsonFacetPager}, which is a {@code while (rows.hasNext())} loop with a bounded heap, and
 * because an {@code Iterator} makes the "consumed once" contract obvious. {@code Closeable} rather
 * than a callback because the caller already sits inside a try-with-resources.
 *
 * <p><b>Consumed once.</b> Re-iterating is not supported and implementations need not detect it:
 * the underlying parser has already moved.
 */
public interface RowStream extends Iterator<JsonNode>, Closeable {

    /** An exhausted stream, for "the artifact is absent" without allocating a parser. */
    static RowStream empty() {
        return new RowStream() {
            @Override public boolean hasNext() { return false; }
            @Override public JsonNode next() { throw new java.util.NoSuchElementException(); }
            @Override public void close() { }
        };
    }

    /**
     * Adapts an iterator that owns a closeable resource. Used by the scan implementation to wrap
     * {@code JsonArrayRows}, which already is both, without that class having to depend on this
     * module.
     */
    static RowStream of(Iterator<JsonNode> rows, Closeable resource) {
        return new RowStream() {
            @Override public boolean hasNext() { return rows.hasNext(); }
            @Override public JsonNode next() { return rows.next(); }
            @Override public void close() throws java.io.IOException { resource.close(); }
        };
    }
}
