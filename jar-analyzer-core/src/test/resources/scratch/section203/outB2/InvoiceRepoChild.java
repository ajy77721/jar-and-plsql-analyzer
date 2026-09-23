package demo;

import org.springframework.data.mongodb.core.MongoTemplate;

/** Hop 2 (leaf): uses the Class<?> directly at the mongo call. */
public class InvoiceRepoChild {
    private final MongoTemplate mongo;
    public InvoiceRepoChild(MongoTemplate mongo) { this.mongo = mongo; }

    public void execute(Class<?> entityType) {
        mongo.find(null, entityType);
    }
}
