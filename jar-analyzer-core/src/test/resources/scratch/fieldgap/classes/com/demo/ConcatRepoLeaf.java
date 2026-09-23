package com.demo;

import org.springframework.data.mongodb.core.MongoTemplate;

/** Shared leaf for the CALL-003 concat fixtures: the mongo call's body only sees the parameter. */
public class ConcatRepoLeaf {
    private final MongoTemplate mongo;
    public ConcatRepoLeaf(MongoTemplate mongo) { this.mongo = mongo; }

    public void executeQuery(String collectionName) {
        mongo.find(null, null, collectionName);
    }
}
