package demo;

public class EPassThrough {
    private final FPassThrough next;
    public EPassThrough(FPassThrough next) { this.next = next; }

    public String get() {
        return next.get();
    }
}
