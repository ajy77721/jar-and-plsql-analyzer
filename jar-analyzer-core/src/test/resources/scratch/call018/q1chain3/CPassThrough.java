package demo;

public class CPassThrough {
    private final ESource source;
    public CPassThrough(ESource source) { this.source = source; }

    public String get() {
        return source.resolveCollectionName();
    }
}
