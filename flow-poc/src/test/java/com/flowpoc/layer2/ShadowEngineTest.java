package com.flowpoc.layer2;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the complete shadow engine: filter operators, update operators, and
 * aggregation pipeline execution against in-memory documents.
 */
class ShadowEngineTest {

    private ShadowMongoStore store;

    @BeforeEach
    void setUp() {
        store = new ShadowMongoStore();
        // Seed products collection
        for (int i = 1; i <= 5; i++) {
            store.applyWrite(MongoOp.INSERT, "products", null, new Object[]{
                    new Document("sku", "p" + i)
                            .append("price", i * 10)
                            .append("stock", i * 2)
                            .append("category", i <= 3 ? "A" : "B")
                            .append("tags", List.of("t" + i, "common"))
            });
        }
    }

    // ── Filter operator tests ─────────────────────────────────────────────────

    @Test void filter_gt() {
        var r = store.query("products", new Document("price", new Document("$gt", 20)), 10);
        assertThat(r).hasSize(3);
        r.forEach(d -> assertThat((Integer) d.get("price")).isGreaterThan(20));
    }

    @Test void filter_lte() {
        var r = store.query("products", new Document("price", new Document("$lte", 20)), 10);
        assertThat(r).hasSize(2);
    }

    @Test void filter_in() {
        var r = store.query("products",
                new Document("sku", new Document("$in", List.of("p1", "p3", "p5"))), 10);
        assertThat(r).hasSize(3);
    }

    @Test void filter_nin() {
        var r = store.query("products",
                new Document("sku", new Document("$nin", List.of("p1", "p2"))), 10);
        assertThat(r).hasSize(3);
    }

    @Test void filter_ne() {
        var r = store.query("products", new Document("category", new Document("$ne", "A")), 10);
        assertThat(r).hasSize(2);
        r.forEach(d -> assertThat(d.get("category")).isEqualTo("B"));
    }

    @Test void filter_exists() {
        var r = store.query("products", new Document("stock", new Document("$exists", true)), 10);
        assertThat(r).hasSize(5);
        var r2 = store.query("products", new Document("missing", new Document("$exists", false)), 10);
        assertThat(r2).hasSize(5);
    }

    @Test void filter_and() {
        var r = store.query("products", new Document("$and", List.of(
                new Document("category", "A"),
                new Document("price", new Document("$gt", 10))
        )), 10);
        assertThat(r).hasSize(2);
    }

    @Test void filter_or() {
        var r = store.query("products", new Document("$or", List.of(
                new Document("sku", "p1"),
                new Document("sku", "p5")
        )), 10);
        assertThat(r).hasSize(2);
    }

    @Test void filter_regex() {
        var r = store.query("products",
                new Document("sku", new Document("$regex", "^p[12]$")), 10);
        assertThat(r).hasSize(2);
    }

    // ── Update operator tests ─────────────────────────────────────────────────

    @Test void update_set() {
        Document filter = new Document("sku", "p1");
        Document update = new Document("$set", new Document("price", 999));
        store.applyWrite(MongoOp.UPDATE, "products", filter, new Object[]{filter, update});
        var r = store.query("products", filter, 1);
        assertThat(r.get(0)).containsEntry("price", 999);
    }

    @Test void update_inc() {
        Document filter = new Document("sku", "p2");
        Document update = new Document("$inc", new Document("stock", 10));
        store.applyWrite(MongoOp.UPDATE, "products", filter, new Object[]{filter, update});
        var r = store.query("products", filter, 1);
        assertThat(((Number) r.get(0).get("stock")).intValue()).isEqualTo(14); // 4 + 10
    }

    @Test void update_unset() {
        Document filter = new Document("sku", "p3");
        Document update = new Document("$unset", new Document("stock", ""));
        store.applyWrite(MongoOp.UPDATE, "products", filter, new Object[]{filter, update});
        var r = store.query("products", filter, 1);
        assertThat(r.get(0)).doesNotContainKey("stock");
    }

    @Test void update_push() {
        Document filter = new Document("sku", "p1");
        Document update = new Document("$push", new Document("tags", "newTag"));
        store.applyWrite(MongoOp.UPDATE, "products", filter, new Object[]{filter, update});
        var r = store.query("products", filter, 1);
        assertThat((List<Object>) r.get(0).get("tags")).contains("newTag");
    }

    @Test void update_pull() {
        Document filter = new Document("sku", "p1");
        Document update = new Document("$pull", new Document("tags", "common"));
        store.applyWrite(MongoOp.UPDATE, "products", filter, new Object[]{filter, update});
        var r = store.query("products", filter, 1);
        assertThat((List<?>) r.get(0).get("tags")).noneMatch("common"::equals);
    }

    @Test void upsert_insertsWhenNoMatch() {
        Document filter = new Document("sku", "p99");
        Document update = new Document("$set", new Document("price", 77));
        store.applyWrite(MongoOp.UPSERT, "products", filter, new Object[]{filter, update});
        var r = store.query("products", new Document("sku", "p99"), 1);
        assertThat(r).hasSize(1);
        assertThat(r.get(0)).containsEntry("price", 77);
    }

    @Test void delete_many_removes_matching() {
        store.applyWrite(MongoOp.DELETE_MANY, "products",
                new Document("category", "A"),
                new Object[]{new Document("category", "A")});
        var r = store.query("products", null, 10);
        assertThat(r).hasSize(2);
        r.forEach(d -> assertThat(d.get("category")).isEqualTo("B"));
    }

    // ── Aggregation pipeline tests ────────────────────────────────────────────

    @Test void aggregate_match_group_sort() {
        String pipeline = """
            [
              {"$match": {"category": "A"}},
              {"$group": {"_id": "$category", "total": {"$sum": "$price"}}},
              {"$sort": {"total": 1}}
            ]
            """;
        var r = store.aggregate("products", pipeline, 10);
        assertThat(r).hasSize(1);
        // p1=10, p2=20, p3=30 → total=60
        assertThat(((Number) r.get(0).get("total")).intValue()).isEqualTo(60);
    }

    @Test void aggregate_match_project() {
        String pipeline = """
            [
              {"$match": {"price": {"$gt": 30}}},
              {"$project": {"sku": 1, "price": 1, "_id": 0}}
            ]
            """;
        var r = store.aggregate("products", pipeline, 10);
        assertThat(r).hasSize(2);
        r.forEach(d -> {
            assertThat(d).containsKey("sku");
            assertThat(d).containsKey("price");
            assertThat(d).doesNotContainKey("stock");
        });
    }

    @Test void aggregate_unwind_and_group() {
        String pipeline = """
            [
              {"$unwind": "$tags"},
              {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
              {"$sort": {"count": -1}},
              {"$limit": 1}
            ]
            """;
        var r = store.aggregate("products", pipeline, 10);
        assertThat(r).hasSize(1);
        // "common" appears in all 5 docs → count=5
        assertThat(r.get(0)).containsEntry("_id", "common");
        assertThat(((Number) r.get(0).get("count")).intValue()).isEqualTo(5);
    }

    @Test void aggregate_count_stage() {
        String pipeline = """
            [
              {"$match": {"category": "B"}},
              {"$count": "total"}
            ]
            """;
        var r = store.aggregate("products", pipeline, 10);
        assertThat(r).hasSize(1);
        assertThat(((Number) r.get(0).get("total")).intValue()).isEqualTo(2);
    }

    @Test void aggregate_lookup_joins_shadow_collections() {
        // Seed an orders collection referencing products
        store.applyWrite(MongoOp.INSERT, "orders", null, new Object[]{
                new Document("orderId", "o1").append("sku", "p1").append("qty", 3)
        });
        store.applyWrite(MongoOp.INSERT, "orders", null, new Object[]{
                new Document("orderId", "o2").append("sku", "p2").append("qty", 1)
        });

        String pipeline = """
            [
              {"$lookup": {
                "from": "products",
                "localField": "sku",
                "foreignField": "sku",
                "as": "product"
              }}
            ]
            """;
        var r = store.aggregate("orders", pipeline, 10);
        assertThat(r).hasSize(2);
        assertThat((List<?>) r.get(0).get("product")).hasSize(1);
    }

    @Test void count_op_against_shadow() {
        long c = store.count("products", new Document("category", "A"));
        assertThat(c).isEqualTo(3);
    }

    @Test void exists_op_against_shadow() {
        assertThat(store.exists("products", new Document("sku", "p3"))).isTrue();
        assertThat(store.exists("products", new Document("sku", "p99"))).isFalse();
    }
}
