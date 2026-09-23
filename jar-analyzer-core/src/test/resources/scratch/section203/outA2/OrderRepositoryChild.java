package demo;

import org.springframework.data.mongodb.core.MongoTemplate;

/** Hop 3 (leaf): the mongo call's own body only sees the unresolved PARAMETER. */
public class OrderRepositoryChild {
    private final MongoTemplate mongo;
    public OrderRepositoryChild(MongoTemplate mongo) { this.mongo = mongo; }

    public void execute(String collectionName) {
        mongo.find(null, null, collectionName);
    }
}
