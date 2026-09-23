package com.plsqlanalyzer.analyzer.service;

import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A failed data-dictionary lookup must not become a finding about the user's database.
 *
 * <p>When the owner map came back empty, {@code schemaName} stayed null and
 * {@code !entrySchema.equalsIgnoreCase(null)} evaluates to <b>true</b> — so every table in the
 * analysis was reported as living in another schema. An empty type map defaulted every object to
 * {@code "TABLE"}, so views were reported as tables. Neither is a small cosmetic issue: this
 * application exists to tell people what their code touches, and a dependency report asserting
 * cross-schema access that does not exist is worse than no report.
 *
 * <p>Exercised through {@code FastAnalysisService.applyTableInfo}, which is the shared helper both
 * of that service's paths use.
 */
class MetadataUnresolvedTest {

    private TableOperationSummary table(String name, String schema) {
        TableOperationSummary s = new TableOperationSummary();
        s.setTableName(name);
        s.setSchemaName(schema);
        return s;
    }

    private void apply(Map<String, TableOperationSummary> ops,
                       Map<String, String> owners, Map<String, String> types,
                       String entrySchema) throws Exception {
        // applyTableInfo is an instance method that touches no instance state, so an
        // Unsafe-free allocation is enough — constructing the real service would drag in a
        // datasource this test has no need for.
        Method m = FastAnalysisService.class.getDeclaredMethod(
                "applyTableInfo", Map.class, Map.class, Map.class, String.class);
        m.setAccessible(true);
        Object instance = newBare(FastAnalysisService.class);
        m.invoke(instance, ops, owners, types, entrySchema);
    }

    /** Allocates without running a constructor, via the no-arg one if present. */
    @SuppressWarnings("unchecked")
    private static <T> T newBare(Class<T> c) throws Exception {
        var ctor = c.getDeclaredConstructors()[0];
        ctor.setAccessible(true);
        Object[] args = new Object[ctor.getParameterCount()];
        return (T) ctor.newInstance(args);
    }

    @Test
    @DisplayName("a failed owner lookup does not mark every table external")
    void failedOwnerLookupDoesNotClaimExternal() throws Exception {
        Map<String, TableOperationSummary> ops = new LinkedHashMap<>();
        ops.put("ORDERS", table("ORDERS", null));
        apply(ops, Map.of(), Map.of(), "APP");          // both lookups failed

        TableOperationSummary t = ops.get("ORDERS");
        assertFalse(t.isExternal(),
                "with the owning schema unknown, 'external' is not a claim we may make — the old "
                        + "code said true for every table because equalsIgnoreCase(null) is false");
        assertTrue(t.isMetadataUnresolved(), "and the row must say the lookup did not resolve");
    }

    @Test
    @DisplayName("a failed type lookup does not report every object as a TABLE")
    void failedTypeLookupDoesNotClaimTable() throws Exception {
        Map<String, TableOperationSummary> ops = new LinkedHashMap<>();
        ops.put("V_ORDERS", table("V_ORDERS", "APP"));
        apply(ops, Map.of("V_ORDERS", "APP"), Map.of(), "APP");

        TableOperationSummary t = ops.get("V_ORDERS");
        assertNull(t.getTableType(),
                "an unknown type must be null, not \"TABLE\" — a view reported as a table is a "
                        + "wrong answer, not a default");
        assertTrue(t.isMetadataUnresolved());
    }

    @Test
    @DisplayName("a successful lookup is unchanged — external and type are still reported")
    void successfulLookupStillWorks() throws Exception {
        Map<String, TableOperationSummary> ops = new LinkedHashMap<>();
        ops.put("ORDERS", table("ORDERS", null));
        ops.put("V_RPT", table("V_RPT", null));
        apply(ops, Map.of("ORDERS", "APP", "V_RPT", "OTHER"),
                   Map.of("ORDERS", "TABLE", "V_RPT", "VIEW"), "APP");

        assertFalse(ops.get("ORDERS").isExternal(), "same schema as the entry point");
        assertEquals("TABLE", ops.get("ORDERS").getTableType());
        assertFalse(ops.get("ORDERS").isMetadataUnresolved());

        assertTrue(ops.get("V_RPT").isExternal(), "OTHER != APP");
        assertEquals("VIEW", ops.get("V_RPT").getTableType(), "a view must stay a view");
        assertFalse(ops.get("V_RPT").isMetadataUnresolved());
    }
}
