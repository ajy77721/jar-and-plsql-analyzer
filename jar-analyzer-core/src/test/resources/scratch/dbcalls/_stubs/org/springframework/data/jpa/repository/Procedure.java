package org.springframework.data.jpa.repository;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Minimal same-FQN stub of Spring Data's @Procedure annotation. */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Procedure {
    String name() default "";
    String procedureName() default "";
    String value() default "";
}
