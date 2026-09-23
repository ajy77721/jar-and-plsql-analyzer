package com.flowpoc.engine;

import com.flowpoc.model.Predicate;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MongoFilterParserTest {

    private final MongoFilterParser parser = new MongoFilterParser();

    @Test
    void simpleEqString() {
        List<Predicate> preds = parser.parse("{\"status\": \"active\"}");
        assertEquals(1, preds.size());
        Predicate p = preds.get(0);
        assertEquals("status", p.getColumn());
        assertEquals(Predicate.Op.EQ, p.getOp());
        assertEquals("active", p.getRawValue());
    }

    @Test
    void gtOperator() {
        List<Predicate> preds = parser.parse("{\"age\": {\"$gt\": 18}}");
        assertEquals(1, preds.size());
        Predicate p = preds.get(0);
        assertEquals("age", p.getColumn());
        assertEquals(Predicate.Op.GT, p.getOp());
        assertEquals("18", p.getRawValue());
    }

    @Test
    void nullValueProducesIsNull() {
        List<Predicate> preds = parser.parse("{\"name\": null}");
        assertEquals(1, preds.size());
        Predicate p = preds.get(0);
        assertEquals("name", p.getColumn());
        assertEquals(Predicate.Op.IS_NULL, p.getOp());
        assertNull(p.getRawValue());
    }

    @Test
    void andRecursesIntoTwoPredicates() {
        List<Predicate> preds = parser.parse(
                "{\"$and\": [{\"a\": \"x\"}, {\"b\": {\"$lt\": 5}}]}");
        assertEquals(2, preds.size());
        assertEquals("a", preds.get(0).getColumn());
        assertEquals(Predicate.Op.EQ, preds.get(0).getOp());
        assertEquals("b", preds.get(1).getColumn());
        assertEquals(Predicate.Op.LT, preds.get(1).getOp());
        assertEquals("5", preds.get(1).getRawValue());
    }

    @Test
    void bindPlaceholderIsMarkedAsBindParam() {
        List<Predicate> preds = parser.parse("{\"userId\": \"?0\"}");
        assertEquals(1, preds.size());
        Predicate p = preds.get(0);
        assertEquals("userId", p.getColumn());
        assertTrue(p.isBindParam());
    }

    @Test
    void colonNamedBindPlaceholderIsBindParam() {
        List<Predicate> preds = parser.parse("{\"userId\": \":userId\"}");
        assertEquals(1, preds.size());
        assertTrue(preds.get(0).isBindParam());
    }

    @Test
    void ltOperator() {
        List<Predicate> preds = parser.parse("{\"score\": {\"$lt\": 100}}");
        assertEquals(1, preds.size());
        assertEquals(Predicate.Op.LT, preds.get(0).getOp());
    }

    @Test
    void lteOperator() {
        List<Predicate> preds = parser.parse("{\"score\": {\"$lte\": 100}}");
        assertEquals(Predicate.Op.LTE, preds.get(0).getOp());
    }

    @Test
    void gteOperator() {
        List<Predicate> preds = parser.parse("{\"score\": {\"$gte\": 0}}");
        assertEquals(Predicate.Op.GTE, preds.get(0).getOp());
    }

    @Test
    void neqOperator() {
        List<Predicate> preds = parser.parse("{\"status\": {\"$ne\": \"inactive\"}}");
        assertEquals(Predicate.Op.NEQ, preds.get(0).getOp());
        assertEquals("inactive", preds.get(0).getRawValue());
    }

    @Test
    void regexProducesLikePredicate() {
        List<Predicate> preds = parser.parse("{\"email\": {\"$regex\": \".*@example.com\"}}");
        assertEquals(Predicate.Op.LIKE, preds.get(0).getOp());
    }

    @Test
    void inOperator() {
        List<Predicate> preds = parser.parse("{\"status\": {\"$in\": [\"active\", \"pending\"]}}");
        assertEquals(Predicate.Op.IN, preds.get(0).getOp());
        assertNotNull(preds.get(0).getRawValue());
    }

    @Test
    void invalidJsonReturnsEmptyList() {
        List<Predicate> preds = parser.parse("not valid json{{{");
        assertTrue(preds.isEmpty());
    }

    @Test
    void nullInputReturnsEmptyList() {
        List<Predicate> preds = parser.parse(null);
        assertTrue(preds.isEmpty());
    }

    @Test
    void orRecurses() {
        List<Predicate> preds = parser.parse(
                "{\"$or\": [{\"x\": \"1\"}, {\"y\": \"2\"}]}");
        assertEquals(2, preds.size());
    }
}
