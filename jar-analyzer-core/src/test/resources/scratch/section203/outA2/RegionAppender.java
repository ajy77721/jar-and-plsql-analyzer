package demo;

/** Hop 2: builds the collection name — NEITHER operand is a same-method literal. */
public class RegionAppender {
    private final OrderRepositoryChild child;
    public RegionAppender(OrderRepositoryChild child) { this.child = child; }

    public void append(String prefix, String region) {
        String fullyBuiltCollectionName = prefix + region;
        child.execute(fullyBuiltCollectionName);
    }
}
