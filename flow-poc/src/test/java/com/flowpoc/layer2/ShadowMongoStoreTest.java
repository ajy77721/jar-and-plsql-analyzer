package com.flowpoc.layer2;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ShadowMongoStoreTest {

    private ShadowMongoStore store;

    @BeforeEach
    void setUp() { store = new ShadowMongoStore(); }

    // ── Core invariant: read-after-write consistency ──────────────────────────

    @Test
    void insertThenFindReturnsInsertedDoc() {
        Document doc = new Document("userId", "u1").append("name", "Alice");
        store.applyWrite(MongoOp.INSERT, "users", null, new Object[]{doc});

        List<Map<String, Object>> results = store.query("users", null, 10);

        assertThat(results).hasSize(1);
        assertThat(results.get(0)).containsEntry("userId", "u1");
        assertThat(results.get(0)).containsEntry("name", "Alice");
    }

    @Test
    void insertManyThenFindAll() {
        Object[] args = {List.of(
                new Document("id", "1").append("type", "A"),
                new Document("id", "2").append("type", "B"),
                new Document("id", "3").append("type", "A")
        )};
        store.applyWrite(MongoOp.INSERT_MANY, "items", null, args);

        List<Map<String, Object>> results = store.query("items", null, 10);
        assertThat(results).hasSize(3);
    }

    @Test
    void filterMatchesOnEqualityField() {
        store.applyWrite(MongoOp.INSERT, "orders", null,
                new Object[]{new Document("orderId", "o1").append("status", "PENDING")});
        store.applyWrite(MongoOp.INSERT, "orders", null,
                new Object[]{new Document("orderId", "o2").append("status", "SHIPPED")});

        Document filter = new Document("status", "PENDING");
        List<Map<String, Object>> results = store.query("orders", filter, 10);

        assertThat(results).hasSize(1);
        assertThat(results.get(0)).containsEntry("orderId", "o1");
    }

    @Test
    void emptyCollectionReturnsEmpty() {
        List<Map<String, Object>> results = store.query("nonexistent", null, 10);
        assertThat(results).isEmpty();
    }

    @Test
    void updateModifiesMatchingDoc() {
        store.applyWrite(MongoOp.INSERT, "users", null,
                new Object[]{new Document("userId", "u1").append("status", "active")});

        Document filter = new Document("userId", "u1");
        Document update = new Document("$set", new Document("status", "inactive"));
        store.applyWrite(MongoOp.UPDATE, "users", filter, new Object[]{filter, update});

        List<Map<String, Object>> results = store.query("users",
                new Document("userId", "u1"), 10);
        assertThat(results).hasSize(1);
        assertThat(results.get(0)).containsEntry("status", "inactive");
    }

    @Test
    void deleteRemovesMatchingDoc() {
        store.applyWrite(MongoOp.INSERT, "sessions", null,
                new Object[]{new Document("sessionId", "s1")});
        store.applyWrite(MongoOp.INSERT, "sessions", null,
                new Object[]{new Document("sessionId", "s2")});

        Document filter = new Document("sessionId", "s1");
        store.applyWrite(MongoOp.DELETE, "sessions", filter, new Object[]{filter});

        List<Map<String, Object>> results = store.query("sessions", null, 10);
        assertThat(results).hasSize(1);
        assertThat(results.get(0)).containsEntry("sessionId", "s2");
    }

    @Test
    void replaceSwapsDocument() {
        store.applyWrite(MongoOp.INSERT, "products", null,
                new Object[]{new Document("sku", "p1").append("price", 10)});

        Document filter      = new Document("sku", "p1");
        Document replacement = new Document("sku", "p1").append("price", 20).append("sale", true);
        store.applyWrite(MongoOp.REPLACE, "products", filter,
                new Object[]{filter, replacement});

        List<Map<String, Object>> results =
                store.query("products", new Document("sku", "p1"), 10);
        assertThat(results).hasSize(1);
        assertThat(results.get(0)).containsEntry("price", 20);
        assertThat(results.get(0)).containsEntry("sale", true);
    }

    @Test
    void clearRemovesAllData() {
        store.applyWrite(MongoOp.INSERT, "col", null,
                new Object[]{new Document("x", 1)});
        store.clear();
        assertThat(store.query("col", null, 10)).isEmpty();
        assertThat(store.hasCollection("col")).isFalse();
    }

    @Test
    void hasCollectionFalseWhenEmpty() {
        assertThat(store.hasCollection("anything")).isFalse();
    }

    @Test
    void hasCollectionTrueAfterInsert() {
        store.applyWrite(MongoOp.INSERT, "audit", null,
                new Object[]{new Document("event", "LOGIN")});
        assertThat(store.hasCollection("audit")).isTrue();
    }

    // ── Cross-step flow invariant ─────────────────────────────────────────────

    @Test
    void step1InsertVisibleToStep2Find() {
        // Step 1: create user (write — blocked on real DB, buffered in shadow)
        Document newUser = new Document("userId", "u42").append("email", "x@y.com");
        store.applyWrite(MongoOp.INSERT, "users", null, new Object[]{newUser});

        // Step 2: load user by id (read — shadow-first)
        List<Map<String, Object>> found =
                store.query("users", new Document("userId", "u42"), 10);

        assertThat(found).hasSize(1);
        assertThat(found.get(0)).containsEntry("email", "x@y.com");
    }

    @Test
    void step2UpdateVisibleToStep3Find() {
        // Step 1: insert
        store.applyWrite(MongoOp.INSERT, "cart", null,
                new Object[]{new Document("cartId", "c1").append("total", 0)});
        // Step 2: update total
        Document filter = new Document("cartId", "c1");
        Document update = new Document("$set", new Document("total", 150));
        store.applyWrite(MongoOp.UPDATE, "cart", filter, new Object[]{filter, update});
        // Step 3: find
        List<Map<String, Object>> cart =
                store.query("cart", new Document("cartId", "c1"), 10);
        assertThat(cart.get(0)).containsEntry("total", 150);
    }
}
