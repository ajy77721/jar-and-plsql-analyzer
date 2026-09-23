package demo;

public class FPassThrough {
    private final GSource source;
    public FPassThrough(GSource source) { this.source = source; }

    public String get() {
        return source.resolveCollectionName();
    }
}
