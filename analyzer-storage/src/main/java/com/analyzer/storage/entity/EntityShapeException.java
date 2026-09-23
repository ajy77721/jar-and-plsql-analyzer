package com.analyzer.storage.entity;

/**
 * Thrown when a JSON field arrives in a shape the field scan never observed.
 *
 * <p>PLAN-11 §4 rule 6, made real: <b>fail loudly on an unobserved shape</b> rather than coercing.
 * The scan behind PLAN-11 covered 4,308 files on one machine; a dialect or code pattern not
 * represented there can introduce a seventh polymorphic field, and the failure mode of coercion is
 * a <em>missing row</em> — silent, undetectable later, and indistinguishable from "that node really
 * does not touch any table". An exception naming the artifact, the path and the shape turns that
 * into a bug report.
 *
 * <p>It carries {@code (artifact, path)} and not just the path because PLAN-11 §2.4's rule is that
 * a field means what it means <em>in its artifact</em>: {@code endpoints} is an array in
 * {@code analysis.json} and a count in {@code _summary_stats.json}. A shape complaint that named
 * only {@code endpoints} would be unactionable.
 */
public class EntityShapeException extends RuntimeException {

    private final String artifact;
    private final String path;
    private final String observed;

    public EntityShapeException(String artifact, String path, String observed, String expected) {
        super("unobserved JSON shape at (" + artifact + ", " + path + "): got " + observed
              + ", expected " + expected
              + ". PLAN-11 rule 6: this is new information about the data, not something to coerce."
              + " Re-run field-scan.py and extend the mapper.");
        this.artifact = artifact;
        this.path = path;
        this.observed = observed;
    }

    public String artifact() {
        return artifact;
    }

    public String path() {
        return path;
    }

    public String observed() {
        return observed;
    }
}
