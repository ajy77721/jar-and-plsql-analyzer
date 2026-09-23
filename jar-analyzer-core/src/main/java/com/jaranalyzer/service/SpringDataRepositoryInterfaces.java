package com.jaranalyzer.service;

import java.util.Set;

/**
 * Single source of truth for the literal Spring Data repository marker interfaces. Referenced by
 * {@link BytecodeClassParser} (direct, one-hop stereotype/entity-type detection) and
 * {@link RepositoryStereotypeResolver} (transitive, multi-level chain detection) so both stay in
 * sync -- previously duplicated as two separately-maintained copies in BytecodeClassParser.java.
 */
final class SpringDataRepositoryInterfaces {

    static final Set<String> MARKER_INTERFACES = Set.of(
            "org.springframework.data.repository.CrudRepository",
            "org.springframework.data.repository.PagingAndSortingRepository",
            "org.springframework.data.repository.reactive.ReactiveCrudRepository",
            "org.springframework.data.repository.reactive.ReactiveSortingRepository",
            "org.springframework.data.jpa.repository.JpaRepository",
            "org.springframework.data.mongodb.repository.MongoRepository",
            "org.springframework.data.mongodb.repository.ReactiveMongoRepository",
            "org.springframework.data.repository.Repository"
    );

    /** True if {@code iface}'s simple name looks like a repository interface by naming convention
     * (the existing, pre-existing heuristic -- kept identical here so transitive resolution never
     * diverges from the one-hop behavior it's extending). */
    static boolean looksLikeRepositoryByName(String iface) {
        return iface.endsWith("Repository") || iface.endsWith("CrudRepository");
    }

    private SpringDataRepositoryInterfaces() {
    }
}
