package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-016 param+param chain: the endpoint's literals flow through a middle hop into the concat. */
@RestController
public class EsController {
    private final QueryPlanner planner;
    public EsController(QueryPlanner planner) { this.planner = planner; }

    @GetMapping("/paramparam")
    public void paramparam() {
        planner.query("orders_", "east");
    }
}
