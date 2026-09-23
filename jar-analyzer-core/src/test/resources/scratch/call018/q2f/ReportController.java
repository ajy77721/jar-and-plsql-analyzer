package demo;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Q2: $lookup 'from' target captured separately from the base aggregation collection. */
@RestController
public class ReportController {
    private static final MongoTemplate mongo = new MongoTemplate();

    @GetMapping("/q2")
    public void handle() {
        Aggregation agg = Aggregation.newAggregation(
                Aggregation.lookup("other_collection", "local", "foreign", "joined"),
                Aggregation.match(null));
        mongo.aggregate(agg, "base_collection", java.util.Map.class);
    }
}
