package org.springframework.data.mongodb.core.mapping;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/** Minimal same-FQN stub (spring-data-mongodb not on this module's test classpath). */
@Retention(RetentionPolicy.RUNTIME)
public @interface Document { String collection() default ""; }
