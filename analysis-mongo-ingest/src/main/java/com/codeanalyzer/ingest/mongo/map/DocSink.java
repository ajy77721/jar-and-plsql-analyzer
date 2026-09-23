package com.codeanalyzer.ingest.mongo.map;

import org.bson.Document;

/**
 * Where a mapper puts the documents it produces.
 *
 * <p>Separating this from the writer is what lets a mapper be unit-tested with no MongoDB present:
 * the test collects into a list and asserts on shape. Given the corpus is 82.9 MB of PL/SQL JSON
 * and a 49.3 MB {@code analysis.json}, it also keeps mapping streaming — nothing accumulates a
 * full document set in memory on the way to the database.
 */
@FunctionalInterface
public interface DocSink {

    void emit(String collection, Document doc);
}
