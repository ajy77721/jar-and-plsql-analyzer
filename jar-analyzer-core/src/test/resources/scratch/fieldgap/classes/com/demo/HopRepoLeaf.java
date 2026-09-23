package com.demo;

import org.springframework.data.mongodb.core.MongoTemplate;

/** Hop 3 (leaf): the mongo call's own body only sees the unresolved parameter. */
public class HopRepoLeaf {
    private final MongoTemplate mongo;
    public HopRepoLeaf(MongoTemplate mongo) { this.mongo = mongo; }

    public void execute(String collectionName) {
        mongo.find(null, null, collectionName);
    }
}
