package demo;

/** Hop 1: forwards both values untouched. */
public class OrderService {
    private final RegionAppender appender;
    public OrderService(RegionAppender appender) { this.appender = appender; }

    public void fetchOrders(String prefix, String region) {
        appender.append(prefix, region);
    }
}
