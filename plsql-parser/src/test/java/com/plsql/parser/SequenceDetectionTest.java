package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Sequence Detection: NEXTVAL and CURRVAL in various PL/SQL patterns")
public class SequenceDetectionTest extends ParserTestBase {

    // ---------------------------------------------------------------
    // Helper: collect all sequence names from the dependency summary
    // ---------------------------------------------------------------
    private Set<String> getSequences(ParseResult result) {
        Set<String> seqs = new java.util.LinkedHashSet<>();
        for (ParsedObject obj : result.getObjects()) {
            seqs.addAll(obj.getDependencies().getSequences());
        }
        return seqs;
    }

    // ---------------------------------------------------------------
    // Helper: collect all CallInfo objects with SEQUENCE type
    // ---------------------------------------------------------------
    private List<CallInfo> getSequenceCalls(SubprogramInfo sub) {
        return sub.getCalls().stream()
                .filter(c -> "NEXTVAL".equals(c.getName()) || "CURRVAL".equals(c.getName()))
                .collect(Collectors.toList());
    }

    @Test
    @DisplayName("Simple assignment: v := seq.NEXTVAL")
    void testSimpleNextvalAssignment() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE INSERT_REC IS
                    v_id NUMBER;
                  BEGIN
                    v_id := MY_SEQ.NEXTVAL;
                  END INSERT_REC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Simple NEXTVAL assignment");

        Set<String> seqs = getSequences(result);
        assertTrue(seqs.contains("MY_SEQ"), "Should detect MY_SEQ from NEXTVAL usage");

        SubprogramInfo sub = findSub(result, "INSERT_REC");
        assertNotNull(sub);
        List<CallInfo> seqCalls = getSequenceCalls(sub);
        assertEquals(1, seqCalls.size(), "Should have exactly 1 sequence call");
        assertEquals("NEXTVAL", seqCalls.get(0).getName());
        assertEquals("MY_SEQ", seqCalls.get(0).getPackageName());
    }

    @Test
    @DisplayName("Schema-qualified: schema.seq.NEXTVAL")
    void testSchemaQualifiedNextval() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE INSERT_REC IS
                    v_id NUMBER;
                  BEGIN
                    v_id := CUSTOMER.MY_SEQ.NEXTVAL;
                  END INSERT_REC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Schema-qualified NEXTVAL");

        Set<String> seqs = getSequences(result);
        assertTrue(seqs.contains("MY_SEQ"), "Should detect MY_SEQ from schema.seq.NEXTVAL");

        SubprogramInfo sub = findSub(result, "INSERT_REC");
        assertNotNull(sub);
        List<CallInfo> seqCalls = getSequenceCalls(sub);
        assertEquals(1, seqCalls.size());
        assertEquals("NEXTVAL", seqCalls.get(0).getName());
        assertEquals("MY_SEQ", seqCalls.get(0).getPackageName());
        assertEquals("CUSTOMER", seqCalls.get(0).getSchema());
    }

    @Test
    @DisplayName("CURRVAL usage: seq.CURRVAL")
    void testCurrvalUsage() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE USE_CURR IS
                    v_id NUMBER;
                  BEGIN
                    v_id := MY_SEQ.CURRVAL;
                  END USE_CURR;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CURRVAL usage");

        Set<String> seqs = getSequences(result);
        assertTrue(seqs.contains("MY_SEQ"), "Should detect MY_SEQ from CURRVAL usage");

        SubprogramInfo sub = findSub(result, "USE_CURR");
        assertNotNull(sub);
        List<CallInfo> seqCalls = getSequenceCalls(sub);
        assertEquals(1, seqCalls.size());
        assertEquals("CURRVAL", seqCalls.get(0).getName());
    }

    @Test
    @DisplayName("Sequence in INSERT VALUES clause")
    void testSequenceInInsertValues() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE INSERT_REC IS
                  BEGIN
                    INSERT INTO my_table (id, name)
                    VALUES (MY_SEQ.NEXTVAL, 'test');
                  END INSERT_REC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Sequence in INSERT VALUES");

        Set<String> seqs = getSequences(result);
        assertTrue(seqs.contains("MY_SEQ"), "Should detect sequence in INSERT VALUES");
    }

    @Test
    @DisplayName("Sequence in SELECT INTO from DUAL")
    void testSequenceInSelectIntoDual() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE GET_ID IS
                    v_id NUMBER;
                  BEGIN
                    SELECT MY_SEQ.NEXTVAL INTO v_id FROM DUAL;
                  END GET_ID;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Sequence in SELECT INTO");

        Set<String> seqs = getSequences(result);
        assertTrue(seqs.contains("MY_SEQ"), "Should detect sequence in SELECT INTO FROM DUAL");
    }

    @Test
    @DisplayName("Multiple different sequences in one subprogram")
    void testMultipleSequences() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE MULTI_SEQ IS
                    v_a NUMBER;
                    v_b NUMBER;
                    v_c NUMBER;
                  BEGIN
                    v_a := SEQ_ONE.NEXTVAL;
                    v_b := SEQ_TWO.NEXTVAL;
                    v_c := SEQ_ONE.CURRVAL;
                  END MULTI_SEQ;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Multiple sequences");

        Set<String> seqs = getSequences(result);
        assertTrue(seqs.contains("SEQ_ONE"), "Should detect SEQ_ONE");
        assertTrue(seqs.contains("SEQ_TWO"), "Should detect SEQ_TWO");
        assertEquals(2, seqs.size(), "Should have exactly 2 distinct sequences");

        SubprogramInfo sub = findSub(result, "MULTI_SEQ");
        assertNotNull(sub);
        List<CallInfo> seqCalls = getSequenceCalls(sub);
        assertEquals(3, seqCalls.size(), "Should have 3 sequence references (2 NEXTVAL + 1 CURRVAL)");
    }

    @Test
    @DisplayName("Sequence in INSERT subquery: INSERT INTO ... SELECT seq.NEXTVAL ...")
    void testSequenceInInsertSubquery() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE BULK_INSERT IS
                  BEGIN
                    INSERT INTO target_table (id, val)
                    SELECT MY_SEQ.NEXTVAL, source_val
                    FROM source_table;
                  END BULK_INSERT;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Sequence in INSERT subquery");

        Set<String> seqs = getSequences(result);
        assertTrue(seqs.contains("MY_SEQ"), "Should detect sequence in INSERT SELECT");
    }

    @Test
    @DisplayName("Sequence inside LPAD expression: LPAD(seq.NEXTVAL, 9, 0)")
    void testSequenceInsideLpad() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE ASSIGN_REF IS
                    v_ref VARCHAR2(20);
                  BEGIN
                    v_ref := LPAD(MY_SEQ.NEXTVAL, 9, '0');
                  END ASSIGN_REF;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Sequence inside LPAD");

        Set<String> seqs = getSequences(result);
        assertTrue(seqs.contains("MY_SEQ"), "Should detect sequence inside LPAD expression");
    }

    @Test
    @DisplayName("Sequence in UPDATE SET clause")
    void testSequenceInUpdateSet() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE UPD_REC IS
                  BEGIN
                    UPDATE my_table
                       SET batch_id = MY_SEQ.NEXTVAL
                     WHERE status = 'NEW';
                  END UPD_REC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Sequence in UPDATE SET");

        Set<String> seqs = getSequences(result);
        assertTrue(seqs.contains("MY_SEQ"), "Should detect sequence in UPDATE SET clause");
    }

    @Test
    @DisplayName("Sequence in NVL wrapper: NVL(val, seq.NEXTVAL)")
    void testSequenceInNvlWrapper() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE INSERT_REC IS
                  BEGIN
                    INSERT INTO my_table (id, name)
                    VALUES (NVL(NULL, MY_SEQ.NEXTVAL), 'test');
                  END INSERT_REC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Sequence in NVL wrapper");

        Set<String> seqs = getSequences(result);
        assertTrue(seqs.contains("MY_SEQ"), "Should detect sequence inside NVL expression");
    }

    @Test
    @DisplayName("Sequence call type is classified as SEQUENCE")
    void testSequenceCallTypeClassification() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_id NUMBER;
                  BEGIN
                    v_id := MY_SEQ.NEXTVAL;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Sequence call type");

        SubprogramInfo sub = findSub(result, "PROC");
        assertNotNull(sub);
        List<CallInfo> seqCalls = getSequenceCalls(sub);
        assertFalse(seqCalls.isEmpty());
        assertEquals("SEQUENCE", seqCalls.get(0).getType(),
                "Sequence references should be classified as SEQUENCE type");
    }

    @Test
    @DisplayName("Standalone procedure with sequence")
    void testStandaloneProcedureWithSequence() {
        String sql = """
                CREATE OR REPLACE PROCEDURE MY_PROC IS
                  v_id NUMBER;
                BEGIN
                  v_id := MY_SEQ.NEXTVAL;
                END MY_PROC;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Standalone procedure");

        Set<String> seqs = getSequences(result);
        assertTrue(seqs.contains("MY_SEQ"), "Should detect sequence in standalone procedure");
    }

    @Test
    @DisplayName("Standalone function with schema-qualified sequence in SELECT")
    void testStandaloneFunctionSchemaSequence() {
        String sql = """
                CREATE OR REPLACE FUNCTION GET_NEXT_ID RETURN NUMBER IS
                  v_id NUMBER;
                BEGIN
                  SELECT CUSTOMER.MY_SEQ.NEXTVAL INTO v_id FROM DUAL;
                  RETURN v_id;
                END GET_NEXT_ID;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Standalone function schema sequence");

        Set<String> seqs = getSequences(result);
        assertTrue(seqs.contains("MY_SEQ"),
                "Should detect schema.seq.NEXTVAL in standalone function");
    }

    @Test
    @DisplayName("Sequence concatenated in string expression: prefix || LPAD(seq.NEXTVAL, 9, 0)")
    void testSequenceInStringConcatenation() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE GEN_REF IS
                    v_ref VARCHAR2(30);
                  BEGIN
                    v_ref := 'PREFIX' || LPAD(MY_SEQ.NEXTVAL, 9, '0');
                  END GEN_REF;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Sequence in string concatenation");

        Set<String> seqs = getSequences(result);
        assertTrue(seqs.contains("MY_SEQ"),
                "Should detect sequence in string concatenation expression");
    }

    @Test
    @DisplayName("Sequence as SELECT column alias: SELECT seq.NEXTVAL AS id")
    void testSequenceAsSelectColumnAlias() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE INS_WITH_ALIAS IS
                  BEGIN
                    INSERT INTO target_table (id, val)
                    SELECT MY_SEQ.NEXTVAL AS new_id, col1
                    FROM source_table;
                  END INS_WITH_ALIAS;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Sequence as SELECT alias");

        Set<String> seqs = getSequences(result);
        assertTrue(seqs.contains("MY_SEQ"), "Should detect sequence used as SELECT column alias");
    }
}
