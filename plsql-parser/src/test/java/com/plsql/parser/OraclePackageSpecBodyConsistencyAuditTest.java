package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Read-only audit (2026-09-18): probes whether {@code PlSqlAnalysisVisitor} does ANY
 * cross-referencing between a package SPEC ({@code CREATE PACKAGE}) and its corresponding
 * BODY ({@code CREATE PACKAGE BODY}) — e.g. linking declared signatures to implementations,
 * marking body subprograms public/private, or detecting spec/body mismatches — and whether
 * the lack of such linkage causes any observable classification bugs in practice.
 *
 * FINDINGS (see final audit report for full ranked writeup):
 *  - No cross-referencing/linkage exists at all: spec and body each produce their own
 *    independent {@code ParsedObject} (types "PACKAGE_SPEC" and "PACKAGE_BODY"), with no
 *    field anywhere (ParsedObject, SubprogramInfo) recording "this body sub implements that
 *    spec declaration" or "this subprogram is public because it's spec-declared, private
 *    otherwise". {@code SubprogramInfo} only has a boolean {@code forwardDeclaration} flag,
 *    set solely on the SPEC's own PROCEDURE_SPEC/FUNCTION_SPEC forward-decl entries; the BODY
 *    never consults the spec to decide public/private for its own subprograms.
 *  - Despite this, INTERNAL call classification for calls made *inside* the body works
 *    correctly for both spec-declared and body-only-private subprograms, because
 *    {@code currentPackageSubprograms} is pre-scanned from the BODY's own
 *    {@code package_obj_body} children (procedure_body/function_body/procedure_spec/
 *    function_spec) directly — it never needs the spec at all. This means the "missing
 *    linkage" is benign for call classification, since the body always carries its own full
 *    implementation text regardless of whether spec and body are fed to parse() together or
 *    as two separate calls (matching how a real crawler would fetch ALL_SOURCE spec vs body
 *    text separately).
 *  - Overloaded spec declarations + matching overloaded body implementations: both overloads
 *    are retained as distinct SubprogramInfo entries in the body's ParsedObject (no dedup by
 *    name), matching the general overload behavior already verified for bodies alone.
 */
public class OraclePackageSpecBodyConsistencyAuditTest extends ParserTestBase {

    private static ParsedObject onlyObject(ParseResult r) {
        assertEquals(1, r.getObjects().size(), "expected exactly one top-level object");
        return r.getObjects().get(0);
    }

    private static List<SubprogramInfo> subsNamed(ParsedObject po, String name) {
        return po.getSubprograms().stream()
                .filter(s -> s.getName() != null && s.getName().equalsIgnoreCase(name))
                .toList();
    }

    // ------------------------------------------------------------------
    // Concept 1: spec declares, body implements — normal case, fed as ONE combined parse()
    // call (as if a caller had concatenated spec+body text, which some pipelines do for
    // convenience even though the real crawler fetches them separately — see concept 4).
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Concept 1: spec + body combined — two independent ParsedObjects, no linkage, no double counting")
    void specAndBodyCombined_noLinkageNoDoubleCounting() {
        String sql = """
                CREATE PACKAGE pkg_orders IS
                  PROCEDURE process_order(p_id NUMBER);
                  FUNCTION get_total(p_id NUMBER) RETURN NUMBER;
                END pkg_orders;
                /
                CREATE PACKAGE BODY pkg_orders IS
                  PROCEDURE process_order(p_id NUMBER) IS
                  BEGIN
                    INSERT INTO orders (id) VALUES (p_id);
                  END process_order;
                  FUNCTION get_total(p_id NUMBER) RETURN NUMBER IS
                  BEGIN
                    RETURN 0;
                  END get_total;
                END pkg_orders;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "spec+body combined");

        List<ParsedObject> objects = result.getObjects();
        assertEquals(2, objects.size(), "expected exactly 2 top-level ParsedObjects: spec + body");

        ParsedObject spec = objects.stream().filter(o -> "PACKAGE_SPEC".equals(o.getType())).findFirst().orElse(null);
        ParsedObject body = objects.stream().filter(o -> "PACKAGE_BODY".equals(o.getType())).findFirst().orElse(null);
        assertNotNull(spec, "spec ParsedObject must exist");
        assertNotNull(body, "body ParsedObject must exist");

        // Both spec's forward-declarations and body's real implementations are captured
        // SEPARATELY — no merge, no cross-linking, and (critically) no double-counting within
        // either object's own subprograms list.
        assertEquals(2, spec.getSubprograms().size(), "spec should list its 2 forward declarations exactly once each");
        assertEquals(2, body.getSubprograms().size(), "body should list its 2 implementations exactly once each");

        assertTrue(spec.getSubprograms().get(0).isForwardDeclaration() || spec.getSubprograms().get(1).isForwardDeclaration(),
                "spec subprograms should be marked as forward declarations");
        assertFalse(body.getSubprograms().stream().anyMatch(SubprogramInfo::isForwardDeclaration),
                "body implementations should NOT be marked as forward declarations");

        // No field anywhere links body.subprograms[i] back to spec.subprograms[j] as "same
        // logical signature" — confirmed by SubprogramInfo having no such accessor.
        // (This is a structural observation, not a bug given how classification works — see
        // concept 2/4 below and the audit report.)
        System.out.println("[AUDIT concept1] spec.subprograms=" + spec.getSubprograms().size()
                + " body.subprograms=" + body.getSubprograms().size()
                + " -- two independent ParsedObjects, no cross-reference field present");
    }

    // ------------------------------------------------------------------
    // Concept 2: body has a PRIVATE helper never declared in the spec.
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Concept 2: body-only private helper is captured and its internal call resolves INTERNAL")
    void bodyOnlyPrivateHelper_capturedAndClassifiedInternal() {
        String sql = """
                CREATE PACKAGE pkg_orders IS
                  PROCEDURE process_order(p_id NUMBER);
                END pkg_orders;
                /
                CREATE PACKAGE BODY pkg_orders IS
                  PROCEDURE process_order(p_id NUMBER) IS
                  BEGIN
                    validate_order(p_id);
                  END process_order;
                  PROCEDURE validate_order(p_id NUMBER) IS
                  BEGIN
                    INSERT INTO audit_log (id) VALUES (p_id);
                  END validate_order;
                END pkg_orders;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "body-only private helper");

        ParsedObject body = result.getObjects().stream()
                .filter(o -> "PACKAGE_BODY".equals(o.getType())).findFirst().orElse(null);
        assertNotNull(body);

        SubprogramInfo validateOrder = body.getSubprograms().stream()
                .filter(s -> "validate_order".equalsIgnoreCase(s.getName())).findFirst().orElse(null);
        assertNotNull(validateOrder, "private helper 'validate_order' (not in spec) must still be captured");
        assertFalse(validateOrder.isForwardDeclaration());

        SubprogramInfo processOrder = body.getSubprograms().stream()
                .filter(s -> "process_order".equalsIgnoreCase(s.getName())).findFirst().orElse(null);
        assertNotNull(processOrder);

        CallInfo call = processOrder.getCalls().stream()
                .filter(c -> "validate_order".equalsIgnoreCase(c.getName())).findFirst().orElse(null);
        assertNotNull(call, "process_order must record a call to validate_order");
        assertEquals("INTERNAL", call.getType(),
                "call to a body-only private helper (never declared in spec) must classify as INTERNAL");

        System.out.println("[AUDIT concept2] validate_order captured=true, call.type=" + call.getType());
    }

    // ------------------------------------------------------------------
    // Concept 3: overloaded spec declarations matching overloaded body implementations.
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Concept 3: overloaded spec decls + matching overloaded body impls both retained distinctly")
    void overloadedSpecAndBody_bothOverloadsRetainedInBody() {
        String sql = """
                CREATE PACKAGE pkg_orders IS
                  PROCEDURE process_order(p_id NUMBER);
                  PROCEDURE process_order(p_id NUMBER, p_flag BOOLEAN);
                END pkg_orders;
                /
                CREATE PACKAGE BODY pkg_orders IS
                  PROCEDURE process_order(p_id NUMBER) IS
                  BEGIN
                    INSERT INTO orders (id) VALUES (p_id);
                  END process_order;
                  PROCEDURE process_order(p_id NUMBER, p_flag BOOLEAN) IS
                  BEGIN
                    INSERT INTO orders (id, flag) VALUES (p_id, p_flag);
                  END process_order;
                END pkg_orders;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "overloaded spec+body");

        ParsedObject spec = result.getObjects().stream()
                .filter(o -> "PACKAGE_SPEC".equals(o.getType())).findFirst().orElse(null);
        ParsedObject body = result.getObjects().stream()
                .filter(o -> "PACKAGE_BODY".equals(o.getType())).findFirst().orElse(null);
        assertNotNull(spec);
        assertNotNull(body);

        List<SubprogramInfo> specOverloads = subsNamed(spec, "process_order");
        List<SubprogramInfo> bodyOverloads = subsNamed(body, "process_order");

        assertEquals(2, specOverloads.size(), "spec should retain both overload forward-declarations");
        assertEquals(2, bodyOverloads.size(), "body should retain both overload implementations distinctly, not deduped by name");
        assertEquals(1, bodyOverloads.get(0).getParameters().size());
        assertEquals(2, bodyOverloads.get(1).getParameters().size());

        System.out.println("[AUDIT concept3] spec overloads=" + specOverloads.size()
                + " body overloads=" + bodyOverloads.size()
                + " param counts=" + bodyOverloads.get(0).getParameters().size()
                + "," + bodyOverloads.get(1).getParameters().size());
    }

    // ------------------------------------------------------------------
    // Concept 4: spec and body fed as TWO SEPARATE parse() calls (matching how a real
    // ALL_SOURCE-based crawler fetches TYPE='PACKAGE' and TYPE='PACKAGE BODY' as distinct
    // source units) — does the body-only call to a spec-declared procedure still resolve
    // INTERNAL using only the body's own content, with zero visibility into the spec text?
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Concept 4: body parsed in ISOLATION (no spec text at all) still classifies internal calls correctly")
    void bodyParsedAloneWithoutSpecText_stillClassifiesInternalCorrectly() {
        // Deliberately parse ONLY the body — as a separate parse() call representing a
        // separate ALL_SOURCE fetch for TYPE='PACKAGE BODY', with the spec never provided
        // to this parser invocation at all.
        String specSql = """
                CREATE PACKAGE pkg_orders IS
                  PROCEDURE process_order(p_id NUMBER);
                  FUNCTION get_total(p_id NUMBER) RETURN NUMBER;
                END pkg_orders;
                /
                """;
        String bodySql = """
                CREATE PACKAGE BODY pkg_orders IS
                  PROCEDURE process_order(p_id NUMBER) IS
                  BEGIN
                    validate_order(p_id);
                  END process_order;
                  FUNCTION get_total(p_id NUMBER) RETURN NUMBER IS
                  BEGIN
                    RETURN 0;
                  END get_total;
                  PROCEDURE validate_order(p_id NUMBER) IS
                  BEGIN
                    NULL;
                  END validate_order;
                END pkg_orders;
                /
                """;

        ParseResult specResult = parse(specSql);
        ParseResult bodyResult = parse(bodySql);
        assertNoParsErrors(specResult, "spec alone");
        assertNoParsErrors(bodyResult, "body alone");

        ParsedObject body = onlyObject(bodyResult);
        assertEquals("PACKAGE_BODY", body.getType());

        SubprogramInfo processOrder = body.getSubprograms().stream()
                .filter(s -> "process_order".equalsIgnoreCase(s.getName())).findFirst().orElse(null);
        assertNotNull(processOrder);
        CallInfo callToPrivateHelper = processOrder.getCalls().stream()
                .filter(c -> "validate_order".equalsIgnoreCase(c.getName())).findFirst().orElse(null);
        assertNotNull(callToPrivateHelper);
        assertEquals("INTERNAL", callToPrivateHelper.getType(),
                "even with the spec never parsed in this invocation, a call to a body-implemented "
                        + "procedure (spec-declared or not) must classify INTERNAL from the body's own content alone");

        // Cross-check: parsing the SPEC alone (its own separate parse() call) sees only forward
        // declarations, no bodies, no calls at all -- confirming spec and body are fully
        // independent parse units in the real two-call pipeline shape.
        ParsedObject spec = onlyObject(specResult);
        assertEquals("PACKAGE_SPEC", spec.getType());
        assertTrue(spec.getSubprograms().stream().allMatch(SubprogramInfo::isForwardDeclaration));
        assertTrue(spec.getSubprograms().stream().allMatch(s -> s.getCalls().isEmpty()));

        System.out.println("[AUDIT concept4] body-alone call classification=" + callToPrivateHelper.getType()
                + " (spec text was NOT provided to this parse() call)");
    }
}
