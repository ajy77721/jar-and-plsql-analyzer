package jakarta.persistence;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/** Minimal same-FQN stub (persistence API not on this module's test classpath). */
@Retention(RetentionPolicy.RUNTIME)
public @interface Table { String name() default ""; }
