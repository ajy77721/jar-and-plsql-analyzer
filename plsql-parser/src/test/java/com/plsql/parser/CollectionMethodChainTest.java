package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for chained collection method calls in PL/SQL.
 *
 * Root cause: the collection_method_call grammar rule only supported
 * single-level collection access (variable_name('('expression')')?) before
 * the terminal method (.DELETE, .EXTEND, .TRIM, etc.). Real PL/SQL code
 * uses deeply chained member/index access like:
 *     workbook.sheets(s).rows(idx).delete
 *
 * Fix: replaced "variable_name ('(' expression ')')?" with "general_element"
 * in the collection_method_call rule, which supports arbitrary-depth chaining.
 */
@DisplayName("Collection Method Chaining Grammar Fix")
public class CollectionMethodChainTest extends ParserTestBase {

    // =========================================================================
    // Category 1: Chained .DELETE on nested collection access
    // =========================================================================

    @Nested
    @DisplayName("Chained .DELETE calls")
    class ChainedDelete {

        @Test
        @DisplayName("obj.member(idx1).member(idx2).delete parses without errors")
        void testDeepChainedDelete() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE CLEAR_DATA IS
                        TYPE row_rec IS RECORD (val VARCHAR2(100));
                        TYPE row_tab IS TABLE OF row_rec INDEX BY PLS_INTEGER;
                        TYPE sheet_rec IS RECORD (rows row_tab);
                        TYPE sheet_tab IS TABLE OF sheet_rec INDEX BY PLS_INTEGER;
                        TYPE wb_rec IS RECORD (sheets sheet_tab);
                        v_wb wb_rec;
                        s PLS_INTEGER;
                        r PLS_INTEGER;
                      BEGIN
                        v_wb.sheets(s).rows(r).delete;
                      END CLEAR_DATA;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "obj.member(idx).member(idx).delete");
        }

        @Test
        @DisplayName("obj.member(idx).member.delete (no index on last member) parses without errors")
        void testChainedDeleteNoTrailingIndex() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE CLEAR_ALL IS
                        TYPE sheet_rec IS RECORD (rows dbms_sql.varchar2_table);
                        TYPE sheet_tab IS TABLE OF sheet_rec INDEX BY PLS_INTEGER;
                        TYPE wb_rec IS RECORD (sheets sheet_tab);
                        v_wb wb_rec;
                        s PLS_INTEGER;
                      BEGIN
                        v_wb.sheets(s).rows.delete;
                      END CLEAR_ALL;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "obj.member(idx).member.delete (no trailing index)");
        }

        @Test
        @DisplayName("simple_collection.delete still works (regression)")
        void testSimpleCollectionDelete() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        TYPE t IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                        v_tab t;
                      BEGIN
                        v_tab.delete;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "simple collection.delete");
        }

        @Test
        @DisplayName("collection(idx).delete still works (regression)")
        void testIndexedCollectionDelete() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        TYPE t IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                        v_tab t;
                        i PLS_INTEGER := 5;
                      BEGIN
                        v_tab(i).delete;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "collection(idx).delete");
        }

        @Test
        @DisplayName("pkg.collection.delete still works (regression)")
        void testPackageQualifiedDelete() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                      BEGIN
                        my_pkg.my_tab.delete;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "pkg.collection.delete");
        }

        @Test
        @DisplayName("collection.delete(idx) — DELETE with argument")
        void testDeleteWithArgument() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        TYPE t IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                        v_tab t;
                      BEGIN
                        v_tab.delete(3);
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "collection.delete(idx)");
        }

        @Test
        @DisplayName("collection.delete(low, high) — DELETE with range arguments")
        void testDeleteWithRange() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        TYPE t IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                        v_tab t;
                      BEGIN
                        v_tab.delete(1, 10);
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "collection.delete(low, high)");
        }

        @Test
        @DisplayName("Three-level chained DELETE: a.b(x).c(y).d(z).delete")
        void testThreeLevelChainedDelete() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_data test_type;
                        i PLS_INTEGER;
                        j PLS_INTEGER;
                        k PLS_INTEGER;
                      BEGIN
                        v_data.level1(i).level2(j).level3(k).delete;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "three-level chained .delete");
        }
    }

    // =========================================================================
    // Category 2: Chained .EXTEND calls
    // =========================================================================

    @Nested
    @DisplayName("Chained .EXTEND calls")
    class ChainedExtend {

        @Test
        @DisplayName("obj.member(idx).collection.extend parses without errors")
        void testChainedExtend() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_data test_type;
                        s PLS_INTEGER := 1;
                      BEGIN
                        v_data.sheets(s).rows.extend;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "obj.member(idx).collection.extend");
        }

        @Test
        @DisplayName("obj.member(idx).collection.extend(n) with argument")
        void testChainedExtendWithArg() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_data test_type;
                        s PLS_INTEGER := 1;
                      BEGIN
                        v_data.sheets(s).rows.extend(10);
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "obj.member(idx).collection.extend(n)");
        }
    }

    // =========================================================================
    // Category 3: Chained .TRIM calls
    // =========================================================================

    @Nested
    @DisplayName("Chained .TRIM calls")
    class ChainedTrim {

        @Test
        @DisplayName("obj.member(idx).collection.trim parses without errors")
        void testChainedTrim() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_data test_type;
                        s PLS_INTEGER := 1;
                      BEGIN
                        v_data.sheets(s).rows.trim;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "obj.member(idx).collection.trim");
        }

        @Test
        @DisplayName("obj.member(idx).collection.trim(n) with argument")
        void testChainedTrimWithArg() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_data test_type;
                        s PLS_INTEGER := 1;
                      BEGIN
                        v_data.sheets(s).rows.trim(5);
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "obj.member(idx).collection.trim(n)");
        }
    }

    // =========================================================================
    // Category 4: Chained read-only collection methods (COUNT, EXISTS, etc.)
    // =========================================================================

    @Nested
    @DisplayName("Chained read-only collection methods")
    class ChainedReadOnly {

        @Test
        @DisplayName("obj.member(idx).collection.count in expression")
        void testChainedCount() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_data test_type;
                        s PLS_INTEGER := 1;
                        v_cnt PLS_INTEGER;
                      BEGIN
                        v_cnt := v_data.sheets(s).rows.count;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "obj.member(idx).collection.count");
        }

        @Test
        @DisplayName("obj.member(idx).collection.first / .last")
        void testChainedFirstLast() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_data test_type;
                        s PLS_INTEGER := 1;
                        v_first PLS_INTEGER;
                        v_last PLS_INTEGER;
                      BEGIN
                        v_first := v_data.sheets(s).rows.first;
                        v_last := v_data.sheets(s).rows.last;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "obj.member(idx).collection.first/last");
        }

        @Test
        @DisplayName("obj.member(idx).collection.exists(n)")
        void testChainedExists() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_data test_type;
                        s PLS_INTEGER := 1;
                      BEGIN
                        IF v_data.sheets(s).rows.exists(1) THEN
                          NULL;
                        END IF;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "obj.member(idx).collection.exists(n)");
        }
    }

    // =========================================================================
    // Category 5: Realistic PG_EXCEL_UTILS-style patterns
    // =========================================================================

    @Nested
    @DisplayName("Realistic multi-procedure package with chained collection methods")
    class RealisticPatterns {

        @Test
        @DisplayName("Excel-utils-style package with chained .delete in multiple procedures")
        void testExcelUtilsStylePackage() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY EXCEL_UTILS AS

                      PROCEDURE CLEAR_WORKBOOK(p_wb IN OUT workbook_type) IS
                        s PLS_INTEGER;
                        t_row_ind PLS_INTEGER;
                      BEGIN
                        s := p_wb.sheets.first;
                        WHILE s IS NOT NULL LOOP
                          t_row_ind := p_wb.sheets(s).rows.first;
                          WHILE t_row_ind IS NOT NULL LOOP
                            p_wb.sheets(s).rows(t_row_ind).delete;
                            t_row_ind := p_wb.sheets(s).rows.next(t_row_ind);
                          END LOOP;
                          p_wb.sheets(s).rows.delete;
                          s := p_wb.sheets.next(s);
                        END LOOP;
                      END CLEAR_WORKBOOK;

                      PROCEDURE NEW_SHEET(p_wb IN OUT workbook_type, p_name IN VARCHAR2) IS
                        s PLS_INTEGER;
                      BEGIN
                        s := p_wb.sheets.count + 1;
                        p_wb.sheets(s).rows.delete;
                      END NEW_SHEET;

                      PROCEDURE SET_ROW(p_wb IN OUT workbook_type, p_sheet IN PLS_INTEGER,
                                        p_row IN PLS_INTEGER, p_data IN VARCHAR2) IS
                      BEGIN
                        IF NOT p_wb.sheets.exists(p_sheet) THEN
                          RETURN;
                        END IF;
                        IF p_wb.sheets(p_sheet).rows.exists(p_row) THEN
                          p_wb.sheets(p_sheet).rows(p_row).delete;
                        END IF;
                      END SET_ROW;

                    END EXCEL_UTILS;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "Excel-utils-style package with chained .delete");
        }

        @Test
        @DisplayName("Mixed collection operations: delete, extend, trim, count in same block")
        void testMixedCollectionOps() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE PROCESS_DATA IS
                        v_data complex_type;
                        i PLS_INTEGER;
                        n PLS_INTEGER;
                      BEGIN
                        -- extend nested collection
                        v_data.groups(1).items.extend(10);
                        -- get count
                        n := v_data.groups(1).items.count;
                        -- check existence
                        IF v_data.groups(1).items.exists(5) THEN
                          -- trim last element
                          v_data.groups(1).items.trim(1);
                        END IF;
                        -- iterate and delete
                        i := v_data.groups(1).items.first;
                        WHILE i IS NOT NULL LOOP
                          v_data.groups(1).items(i).delete;
                          i := v_data.groups(1).items.next(i);
                        END LOOP;
                        -- delete all
                        v_data.groups(1).items.delete;
                      END PROCESS_DATA;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "Mixed collection operations in same block");
        }
    }
}
