package demo;

public class CPassThrough {
    private final EPassThrough next;
    public CPassThrough(EPassThrough next) { this.next = next; }

    public String get() {
        return next.get();
    }
}
