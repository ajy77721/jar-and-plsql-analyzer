package com.jaranalyzer.crossref.model;

/** A single MongoDB collection accessed by a Java node, with its operation and optional sequence. */
public class CrossRefCollectionEntry {

    private String collection;
    private String operation;
    private String sequence;  // SQ_* name when found in stringLiterals

    public CrossRefCollectionEntry() {}

    public CrossRefCollectionEntry(String collection, String operation, String sequence) {
        this.collection = collection;
        this.operation = operation;
        this.sequence = sequence;
    }

    public String getCollection() { return collection; }
    public void setCollection(String collection) { this.collection = collection; }
    public String getOperation() { return operation; }
    public void setOperation(String operation) { this.operation = operation; }
    public String getSequence() { return sequence; }
    public void setSequence(String sequence) { this.sequence = sequence; }
}
