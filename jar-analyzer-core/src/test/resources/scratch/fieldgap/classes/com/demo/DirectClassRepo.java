package com.demo;

import org.springframework.data.mongodb.core.MongoTemplate;

public class DirectClassRepo {
    private final MongoTemplate mongo;
    public DirectClassRepo(MongoTemplate mongo) { this.mongo = mongo; }

    public void findByEntity(Class<?> entityClass) {
        mongo.find(null, entityClass);
    }
}
