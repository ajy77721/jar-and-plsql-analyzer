package demo;

/** Pure pass-through of C's return value. */
public class BPassThrough {
    private final CBuilder builder;
    public BPassThrough(CBuilder builder) { this.builder = builder; }

    public String get() {
        return builder.resolveCollectionName();
    }
}
