package demo;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** DIAGNOSTIC: static-field mongoTemplate receiver, NO $lookup, plain aggregation + literal base. */
@RestController
public class ReportController {
    private static final MongoTemplate mongo = new MongoTemplate();

    @GetMapping("/q2s")
    public void handle() {
        Aggregation agg = Aggregation.newAggregation(Aggregation.match(null));
        mongo.aggregate(agg, "base_collection", java.util.Map.class);
    }
}
