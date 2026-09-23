package demo;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-019 5-hop: D -> B -> C -> E -> F -> G — ONE hop beyond MAX_HOP_DEPTH (4). */
@RestController
public class AController {
    private final MongoTemplate mongo;
    private final BPassThrough passthrough;
    public AController(MongoTemplate mongo, BPassThrough passthrough) { this.mongo = mongo; this.passthrough = passthrough; }

    @GetMapping("/q1chain5")
    public void handle() {
        run();
    }

    public void run() {
        String collectionName = passthrough.get();
        mongo.find(null, null, collectionName);
    }
}
