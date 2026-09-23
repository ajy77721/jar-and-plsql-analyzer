package demo;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-019 3-hop: D -> B -> C -> E, E has the literal. */
@RestController
public class AController {
    private final MongoTemplate mongo;
    private final BPassThrough passthrough;
    public AController(MongoTemplate mongo, BPassThrough passthrough) { this.mongo = mongo; this.passthrough = passthrough; }

    @GetMapping("/q1chain3")
    public void handle() {
        run();
    }

    public void run() {
        String collectionName = passthrough.get();
        mongo.find(null, null, collectionName);
    }
}
