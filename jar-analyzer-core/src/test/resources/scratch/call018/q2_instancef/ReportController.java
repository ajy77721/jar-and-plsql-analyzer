package demo;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** DIAGNOSTIC: instance-field mongoTemplate receiver, $lookup present, lower_snake_case naming. */
@RestController
public class ReportController {
    private final MongoTemplate mongo;
    public ReportController(MongoTemplate mongo) { this.mongo = mongo; }

    @GetMapping("/q2i")
    public void handle() {
        Aggregation agg = Aggregation.newAggregation(
                Aggregation.lookup("other_collection", "local", "foreign", "joined"),
                Aggregation.match(null));
        mongo.aggregate(agg, "base_collection", java.util.Map.class);
    }
}
