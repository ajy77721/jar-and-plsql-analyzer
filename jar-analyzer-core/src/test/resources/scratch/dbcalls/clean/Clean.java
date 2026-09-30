package fixture;

/** A class with no database call of any shape — the detector must report nothing. */
public class Clean {

    public int add(int a, int b) {
        return a + b;
    }

    public String greet(String name) {
        return "hello " + name;
    }
}
