package org.springframework.data.elasticsearch.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/** Minimal same-FQN stub (spring-data-elasticsearch not on this module's test classpath). */
@Retention(RetentionPolicy.RUNTIME)
public @interface Document { String indexName() default ""; }
