package com.plsql.parser;

import com.plsql.parser.flow.SchemaResolver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Error handling tests for SchemaResolver.
 * Tests null/empty inputs, unknown objects, and constructor validation.
 * SchemaResolver requires DB connections for full construction, so these tests
 * focus on the constructor guards and contract boundaries.
 */
@DisplayName("SchemaResolver Error Handling")
public class SchemaResolverErrorTest {

    // ── Constructor validation ──

    @Test
    @DisplayName("Constructor without entry points (single-arg) throws IllegalArgumentException")
    void testConstructorWithoutEntryPoints() {
        assertThrows(IllegalArgumentException.class, () -> {
            new SchemaResolver(null);
        });
    }

    @Test
    @DisplayName("Constructor with null connManager and null entry points throws")
    void testConstructorNullConnManagerNullEntryPoints() {
        // With null connManager, line 50 accesses connManager.getAvailableSchemas() -> NPE
        // This is expected: connManager must not be null
        assertThrows(NullPointerException.class, () -> {
            new SchemaResolver(null, null);
        });
    }

    @Test
    @DisplayName("Constructor with null connManager and empty entry points throws")
    void testConstructorNullConnManagerEmptyEntryPoints() {
        // connManager is accessed at line 50 before entry point check at line 53
        // with null connManager, NPE is thrown
        assertThrows(NullPointerException.class, () -> {
            new SchemaResolver(null, Collections.emptyList());
        });
    }

    @Test
    @DisplayName("Constructor with null connManager and valid entry points throws NPE")
    void testConstructorNullConnManagerValidEntryPoints() {
        // connManager.getAvailableSchemas() is called at line 50
        assertThrows(NullPointerException.class, () -> {
            new SchemaResolver(null, List.of("MY_PKG.MY_PROC"));
        });
    }
}
