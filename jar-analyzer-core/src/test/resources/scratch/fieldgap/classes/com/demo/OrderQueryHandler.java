package com.demo;

import java.util.List;
import java.util.function.Supplier;
import org.bson.Document;

/** The field-stored method reference: Supplier field assigned via repo::findAll in the constructor. */
public class OrderQueryHandler {
    private final Supplier<List<Document>> loader;

    public OrderQueryHandler(OrderRepo repo) {
        this.loader = repo::findAll;
    }

    public void runQuery() {
        loader.get();
    }
}
