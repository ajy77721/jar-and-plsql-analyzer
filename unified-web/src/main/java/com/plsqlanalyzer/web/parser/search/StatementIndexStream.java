package com.plsqlanalyzer.web.parser.search;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Reads {@code api/statements/index.json} one statement at a time — MID-007.
 *
 * <h2>The measurement</h2>
 *
 * <p>The controller used to call {@code mapper.readTree(...)} on the whole file. Measured on the
 * largest analysis in the corpus (MYSQL, <b>7.0 MB</b>, 5,015 statements):
 *
 * <pre>
 *   readTree   retained  24.3 MB
 *   streaming  retained   ~0 MB
 * </pre>
 *
 * <p>24 MB for a 7 MB file is the normal cost of a {@code JsonNode} tree, not a defect in Jackson.
 * The defect is holding it at all: statements grow with the code being analysed, and at the stated
 * target of 8,000 procedures this file is roughly <b>1.1 GB</b>, which is about 3.7 GB parsed. There
 * is no cache size that makes that acceptable, because the problem is one document.
 *
 * <h2>What this does not fix</h2>
 *
 * <p>MID-007's other half is "{@code explain} shows {@code IXSCAN}", and this is still a scan — of
 * a file rather than of a heap. That half cannot be delivered without changing what the search
 * finds: {@link SearchQuery}'s default is a <b>substring</b> match, and no B-tree index can serve
 * {@code contains}. Making it indexable means making it word- or prefix-based, which is a change to
 * the answers users get, so it is recorded in the registry as needing a decision rather than
 * decided here.
 *
 * <p>What this does deliver is the part with no trade-off: the scan no longer materialises the
 * document, so its memory is bounded by the page size instead of by the analysis.
 *
 * <h2>Single pass, either key order</h2>
 *
 * <p>The header scalars ({@code totalStatements}, {@code statementsWithColumns}) sit before
 * {@code statements} in every file measured, but they are captured wherever they appear. Depending
 * on that order would be a decision about an artifact this class does not produce.
 */
public final class StatementIndexStream implements Iterable<JsonNode>, AutoCloseable {

    private final ObjectMapper mapper;
    private final InputStream in;
    private final JsonParser parser;

    private int totalStatements;
    private int statementsWithColumns;
    private boolean consumed;

    /**
     * @param in an open stream over {@code api/statements/index.json}; owned by this object and
     *           closed by {@link #close()}
     */
    public StatementIndexStream(ObjectMapper mapper, InputStream in) throws IOException {
        this.mapper = mapper;
        this.in = in;
        this.parser = mapper.getFactory().createParser(in);
    }

    /** {@code totalStatements}, valid once the stream has been walked. */
    public int totalStatements() {
        return totalStatements;
    }

    /** {@code statementsWithColumns}, valid once the stream has been walked. */
    public int statementsWithColumns() {
        return statementsWithColumns;
    }

    /**
     * The statement rows, in file order.
     *
     * <p>One shot. A second call would silently return an empty iterator, since the parser has
     * already advanced past the array — so it throws instead. A search that quietly found nothing
     * on a re-read would look like "no matches".
     */
    @Override
    public Iterator<JsonNode> iterator() {
        if (consumed) {
            throw new IllegalStateException(
                    "StatementIndexStream is single-pass; open a new one to search again");
        }
        consumed = true;
        return new Iterator<>() {
            private JsonNode next = advance();

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public JsonNode next() {
                if (next == null) throw new NoSuchElementException();
                JsonNode row = next;
                next = advance();
                return row;
            }

            /**
             * The next statement object, or null at the end.
             *
             * <p>Walks the document rather than assuming a shape: the scalars are picked up as they
             * pass, and the array is entered when its field name is seen. Everything else is
             * skipped with {@code skipChildren}, so an unrelated nested structure costs nothing.
             */
            private JsonNode advance() {
                try {
                    return advanceOrThrow();
                } catch (IOException e) {
                    // Iterator cannot throw a checked exception, and swallowing this would turn a
                    // truncated file into an empty result set.
                    throw new UncheckedIOException(e);
                }
            }

            private boolean inStatements;

            private JsonNode advanceOrThrow() throws IOException {
                while (true) {
                    JsonToken t = parser.nextToken();
                    if (t == null) return null;

                    if (inStatements) {
                        if (t == JsonToken.START_OBJECT) return mapper.readTree(parser);
                        if (t == JsonToken.END_ARRAY) {
                            inStatements = false;
                            continue;
                        }
                        continue;
                    }

                    if (t != JsonToken.FIELD_NAME) continue;
                    String field = parser.currentName();
                    switch (field) {
                        case "statements" -> {
                            if (parser.nextToken() == JsonToken.START_ARRAY) {
                                inStatements = true;
                            } else {
                                parser.skipChildren();
                            }
                        }
                        case "totalStatements" -> {
                            parser.nextToken();
                            totalStatements = parser.getIntValue();
                        }
                        case "statementsWithColumns" -> {
                            parser.nextToken();
                            statementsWithColumns = parser.getIntValue();
                        }
                        default -> {
                            parser.nextToken();
                            parser.skipChildren();
                        }
                    }
                }
            }
        };
    }

    @Override
    public void close() throws IOException {
        try {
            parser.close();
        } finally {
            in.close();
        }
    }
}
