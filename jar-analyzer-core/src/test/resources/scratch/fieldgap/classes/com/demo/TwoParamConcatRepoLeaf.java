package com.demo;

import org.springframework.data.mongodb.core.MongoTemplate;

/** Hop 3 (leaf): the mongo call's own body only sees the unresolved parameter. */
public class TwoParamConcatRepoLeaf {
    private final MongoTemplate mongo;
    public TwoParamConcatRepoLeaf(MongoTemplate mongo) { this.mongo = mongo; }

    public void execute(String collectionName) {
        mongo.find(null, null, collectionName);
    }
}
