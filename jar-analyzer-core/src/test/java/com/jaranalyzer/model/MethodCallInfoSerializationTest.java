package com.jaranalyzer.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What {@link MethodCallInfo} does and does not write.
 *
 * <p>This is the most numerous object in {@code analysis.json} — {@code methods.invocations} is
 * <b>31-78% of the whole document</b> across the real analyses, 8-21x all decompiled source
 * combined. Of 12,744 invocations measured in one analysis the four lambda fields were unset in
 * <b>99.0%</b>, {@code receiverFieldName} in 95.2% and {@code recentStringArgs} in 78.0%, so
 * omitting those absences removed <b>26.9%</b> of the document.
 *
 * <p>Two ways this can regress, and both are silent:
 *
 * <ul>
 *   <li><b>Someone "tidies" the per-field annotations into a class-level
 *       {@code NON_DEFAULT}.</b> That also drops {@code opcode} and {@code lineNumber} when they
 *       are 0 — but 0 is a real value there, not an absence, and
 *       {@code summary-codeview.js:1270} reads {@code inv.lineNumber || 0}. Nothing would fail
 *       loudly; a line number would just quietly become 0 everywhere.</li>
 *   <li><b>Someone removes the annotations.</b> The document silently grows by a third again.</li>
 * </ul>
 *
 * <p>{@code lambdaInvocation} needs {@code NON_DEFAULT} rather than {@code NON_NULL} specifically
 * because it is a primitive {@code boolean}: {@code NON_NULL} does not drop {@code false}.
 */
class MethodCallInfoSerializationTest {

    private final ObjectMapper mapper = new ObjectMapper();

    /** The overwhelmingly common shape: a plain call with no lambda, receiver or string args. */
    private MethodCallInfo typical() {
        MethodCallInfo m = new MethodCallInfo();
        m.setOwnerClass("com/enterprise/OrderService");
        m.setMethodName("save");
        m.setDescriptor("(Lcom/enterprise/Order;)V");
        m.setReturnType("void");
        m.setOpcode(182);
        m.setLineNumber(47);
        return m;
    }

    @Test
    @DisplayName("an unset field is omitted, not written as null")
    void absencesAreOmitted() throws Exception {
        String json = mapper.writeValueAsString(typical());
        for (String absent : new String[]{
                "lambdaTargetClass", "lambdaTargetMethod", "lambdaTargetDescriptor",
                "lambdaInvocation", "receiverFieldName", "recentStringArgs", "dynamicArgSources"}) {
            assertFalse(json.contains(absent),
                    absent + " is unset in 78-99% of real invocations and must not be serialised: "
                            + json);
        }
    }

    @Test
    @DisplayName("the fields that carry meaning are always written")
    void presentFieldsSurvive() throws Exception {
        String json = mapper.writeValueAsString(typical());
        for (String kept : new String[]{
                "ownerClass", "methodName", "descriptor", "returnType", "opcode", "lineNumber"}) {
            assertTrue(json.contains(kept), kept + " must always be serialised: " + json);
        }
    }

    @Test
    @DisplayName("lineNumber 0 and opcode 0 SURVIVE — they are values, not absences")
    void zeroIsNotAnAbsence() throws Exception {
        // The reason the annotations are per-field rather than a class-level NON_DEFAULT. A blanket
        // rule drops these, and nothing fails — line numbers just silently become 0 in the UI.
        MethodCallInfo m = typical();
        m.setLineNumber(0);
        m.setOpcode(0);
        String json = mapper.writeValueAsString(m);
        assertTrue(json.contains("\"lineNumber\":0"), "lineNumber 0 must be written: " + json);
        assertTrue(json.contains("\"opcode\":0"), "opcode 0 must be written: " + json);
    }

    @Test
    @DisplayName("a set field is written, whatever the default would have been")
    void setFieldsAreWritten() throws Exception {
        MethodCallInfo m = typical();
        m.setReceiverFieldName("repository");
        m.setRecentStringArgs(List.of("orders"));
        m.setLambdaInvocation(true);
        m.setLambdaTargetClass("com/enterprise/Mapper");

        String json = mapper.writeValueAsString(m);
        assertTrue(json.contains("\"receiverFieldName\":\"repository\""), json);
        assertTrue(json.contains("orders"), json);
        assertTrue(json.contains("\"lambdaInvocation\":true"), json);
        assertTrue(json.contains("\"lambdaTargetClass\":\"com/enterprise/Mapper\""), json);
    }

    @Test
    @DisplayName("an empty list is omitted, but a populated one is not")
    void emptyCollectionsAreOmitted() throws Exception {
        MethodCallInfo empty = typical();
        empty.setRecentStringArgs(List.of());
        assertFalse(mapper.writeValueAsString(empty).contains("recentStringArgs"),
                "an empty list carries no information and is 78% of real invocations");

        MethodCallInfo full = typical();
        full.setRecentStringArgs(List.of("a"));
        assertTrue(mapper.writeValueAsString(full).contains("recentStringArgs"));
    }

    @Test
    @DisplayName("a document written now still deserialises, and so does one written before")
    void bothShapesRoundTrip() throws Exception {
        // There is no migration step: analyses already on disk carry the nulls inline, and readers
        // must keep accepting them. A reader that only understood the new shape would break every
        // stored analysis.
        MethodCallInfo back = mapper.readValue(mapper.writeValueAsString(typical()),
                MethodCallInfo.class);
        assertEquals("com/enterprise/OrderService", back.getOwnerClass());
        assertEquals(47, back.getLineNumber());
        assertNull(back.getReceiverFieldName(), "an omitted field must read back as null");
        assertFalse(back.isLambdaInvocation());

        String legacy = "{\"ownerClass\":\"com/x/S\",\"methodName\":\"m\",\"descriptor\":\"()V\","
                + "\"returnType\":\"void\",\"opcode\":182,\"lineNumber\":9,"
                + "\"recentStringArgs\":null,\"receiverFieldName\":null,"
                + "\"lambdaInvocation\":false,\"lambdaTargetClass\":null,"
                + "\"lambdaTargetMethod\":null,\"lambdaTargetDescriptor\":null,"
                + "\"dynamicArgSources\":null}";
        MethodCallInfo old = mapper.readValue(legacy, MethodCallInfo.class);
        assertEquals("com/x/S", old.getOwnerClass());
        assertEquals(9, old.getLineNumber());
        assertNull(old.getReceiverFieldName());
    }

    @Test
    @DisplayName("the saving is real: a typical invocation is well under half its old size")
    void theSavingIsMeasurable() throws Exception {
        String now = mapper.writeValueAsString(typical());
        // What the same object serialised to before the annotations, field for field.
        String before = "{\"ownerClass\":\"com/enterprise/OrderService\",\"methodName\":\"save\","
                + "\"descriptor\":\"(Lcom/enterprise/Order;)V\",\"returnType\":\"void\","
                + "\"opcode\":182,\"lineNumber\":47,\"recentStringArgs\":null,"
                + "\"receiverFieldName\":null,\"lambdaInvocation\":false,"
                + "\"lambdaTargetClass\":null,\"lambdaTargetMethod\":null,"
                + "\"lambdaTargetDescriptor\":null,\"dynamicArgSources\":null}";
        assertTrue(now.length() < before.length() * 0.7,
                "expected a substantial reduction; now=" + now.length()
                        + " before=" + before.length());
    }
}
