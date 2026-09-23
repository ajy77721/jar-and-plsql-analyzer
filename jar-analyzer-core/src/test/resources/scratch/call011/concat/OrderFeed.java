package demo;

/** Intermediate pass-through: threads the region down untouched (the hop the caller supplies). */
public class OrderFeed {
    private final EsService service;
    public OrderFeed(EsService service) { this.service = service; }

    public void fetchOrders(String region) {
        service.searchRegion(region);
    }
}
