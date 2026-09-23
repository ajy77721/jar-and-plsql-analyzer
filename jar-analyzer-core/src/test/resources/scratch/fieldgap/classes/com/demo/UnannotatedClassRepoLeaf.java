package com.demo;

import org.springframework.data.mongodb.core.MongoTemplate;

public class UnannotatedClassRepoLeaf {
    private final MongoTemplate mongo;
    public UnannotatedClassRepoLeaf(MongoTemplate mongo) { this.mongo = mongo; }

    public void execute(Class<?> entityClass) {
        mongo.find(null, entityClass);
    }
}
