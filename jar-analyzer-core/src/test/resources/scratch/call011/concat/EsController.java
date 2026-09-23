package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-011 concat-fold: the caller supplies the literal region; the concat is one level below. */
@RestController
public class EsController {
    private final OrderFeed feed;
    public EsController(OrderFeed feed) { this.feed = feed; }

    @GetMapping("/concat")
    public void concat() {
        feed.fetchOrders("east");
    }
}
