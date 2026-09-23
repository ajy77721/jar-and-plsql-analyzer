package com.demo;

import com.mongodb.client.MongoDatabase;

/** NATIVE_DRIVER detection idiom: MongoDatabase.getCollection("orders").find(). */
public class OrderRepo {
    private final MongoDatabase db;
    public OrderRepo(MongoDatabase db) { this.db = db; }

    public java.util.List<org.bson.Document> findAll() {
        java.util.List<org.bson.Document> out = new java.util.ArrayList<>();
        db.getCollection("orders").find().into(out);
        return out;
    }
}
