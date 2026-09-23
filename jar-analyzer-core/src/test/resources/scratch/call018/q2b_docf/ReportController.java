package demo;

import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Q2b: same $lookup + base-collection shape, raw native-driver Document pipeline. */
@RestController
public class ReportController {
    private final MongoDatabase db;
    public ReportController(MongoDatabase db) { this.db = db; }

    @GetMapping("/q2b")
    public void handle() {
        Document lookup = new Document("$lookup",
                new Document("from", "other_collection_doc").append("localField", "local").append("foreignField", "foreign"));
        db.getCollection("base_collection_doc").aggregate(List.of(lookup));
    }
}
