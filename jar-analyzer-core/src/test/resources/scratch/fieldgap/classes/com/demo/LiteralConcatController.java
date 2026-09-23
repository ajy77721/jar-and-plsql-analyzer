package com.demo;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-003: BOTH operands are literals in the same method — javac folds the concat to one LDC. */
@RestController
public class LiteralConcatController {
    private final MongoTemplate mongo;
    public LiteralConcatController(MongoTemplate mongo) { this.mongo = mongo; }

    @GetMapping("/orders-literal-concat")
    public String listOrdersLiteralConcat() {
        String collection = "orders_" + "east";
        mongo.find(null, null, collection);
        return "ok";
    }
}
