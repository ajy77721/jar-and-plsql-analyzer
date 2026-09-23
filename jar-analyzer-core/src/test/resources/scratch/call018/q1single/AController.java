package demo;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-018 single-hop: the collection name arrives as C's RETURN VALUE. */
@RestController
public class AController {
    private final MongoTemplate mongo;
    private final CBuilder builder;
    public AController(MongoTemplate mongo, CBuilder builder) { this.mongo = mongo; this.builder = builder; }

    @GetMapping("/q1single")
    public void handle() {
        run();
    }

    public void run() {
        String collectionName = builder.resolveCollectionName();
        mongo.find(null, null, collectionName);
    }
}
