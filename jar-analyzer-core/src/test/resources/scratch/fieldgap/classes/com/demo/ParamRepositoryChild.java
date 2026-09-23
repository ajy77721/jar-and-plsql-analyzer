package com.demo;

import org.springframework.data.mongodb.core.MongoTemplate;

public class ParamRepositoryChild {
    private final MongoTemplate mongo;
    public ParamRepositoryChild(MongoTemplate mongo) { this.mongo = mongo; }

    public void execute(String collectionName) {
        mongo.find(null, null, collectionName);
    }
}
