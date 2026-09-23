package com.demo;

import org.springframework.data.mongodb.core.MongoTemplate;

/** Hop 2 (leaf): uses entityClass (never a literal in ITS own context) as the entity arg. */
public class ClassRepoLeaf {
    private final MongoTemplate mongo;
    public ClassRepoLeaf(MongoTemplate mongo) { this.mongo = mongo; }

    public void execute(Class<?> entityClass) {
        mongo.find(null, entityClass);
    }
}
