package demo;

public class BPassThrough {
    private final CPassThrough next;
    public BPassThrough(CPassThrough next) { this.next = next; }

    public String get() {
        return next.get();
    }
}
