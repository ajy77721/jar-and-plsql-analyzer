package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlDynamicSqlInfo;
import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * READ-ONLY AUDIT (2026-09-17) — dynamic/string-cursor (dimension 1) + secondary dimensions
 * for mysql-parser. MySQL has no REF CURSOR; PREPARE/EXECUTE is the closest real-world
 * equivalent for a runtime-built result set. No hard assertions on the still-open gap
 * (Case 2) — this test exists to print evidence, per the invoking audit's method.
 */
public class MySqlDynamicCursorAuditTest {

    private static List<String> tableNames(MySqlRoutineInfo r) {
        return r.getTableOps().stream().map(MySqlTableOpInfo::getTableName).toList();
    }

    // Case 1: PREPARE stmt FROM fully-literal SQL string
    @Test
    @DisplayName("1: PREPARE FROM literal SQL string, then EXECUTE ... USING")
    void prepareFromLiteral() {
        String sql = """
                CREATE PROCEDURE proc_prep_literal(IN p_status VARCHAR(20))
                BEGIN
                  SET @sql = 'SELECT * FROM orders WHERE status = ?';
                  PREPARE stmt FROM 'SELECT * FROM orders WHERE status = ?';
                  EXECUTE stmt USING p_status;
                  DEALLOCATE PREPARE stmt;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_prep_literal", null);
        System.out.println("[1] tableOps: " + tableNames(r));
        System.out.println("[1] dynamicSql: " + r.getDynamicSql().stream()
                .map(d -> d.getKind() + " stmt=" + d.getStmtName() + " src=" + d.getSqlSource()).toList());
        System.out.println("[1] errors: " + r.getErrors());
    }

    // Case 2: PREPARE FROM a @variable built via CONCAT with a runtime table name
    @Test
    @DisplayName("2: SET @sql = CONCAT(...); PREPARE FROM @sql (variable-interrupted, dynamic table name)")
    void prepareFromConcatVariable() {
        String sql = """
                CREATE PROCEDURE proc_prep_dynamic(IN v_table_name VARCHAR(64), IN p_status VARCHAR(20))
                BEGIN
                  SET @sql = CONCAT('SELECT * FROM ', v_table_name, ' WHERE status = ?');
                  PREPARE stmt FROM @sql;
                  EXECUTE stmt USING p_status;
                  DEALLOCATE PREPARE stmt;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_prep_dynamic", null);
        System.out.println("[2] tableOps: " + tableNames(r));
        System.out.println("[2] dynamicSql: " + r.getDynamicSql().stream()
                .map(d -> d.getKind() + " stmt=" + d.getStmtName() + " src=" + d.getSqlSource()
                        + " unresolvedVar=" + d.getUnresolvedVarName()
                        + " crossProcHint=" + d.getCrossProcSourceHint()).toList());
        System.out.println("[2] errors: " + r.getErrors());
        // Bug 8 (MySQL portion): no table op is captured (the table name truly isn't known at
        // parse time), but the PREPARE must now carry a structured unresolvedVarName flag pointing
        // at the runtime variable that interrupted the CONCAT, so this case is distinguishable from
        // "nothing dynamic happened here".
        org.junit.jupiter.api.Assertions.assertTrue(tableNames(r).isEmpty(),
                "table name is genuinely unresolvable at parse time: " + tableNames(r));
        MySqlDynamicSqlInfo prepare = r.getDynamicSql().stream()
                .filter(d -> d.getKind() == MySqlDynamicSqlInfo.Kind.PREPARE)
                .findFirst().orElse(null);
        org.junit.jupiter.api.Assertions.assertNotNull(prepare, "expected a PREPARE dynamic-sql entry");
        org.junit.jupiter.api.Assertions.assertEquals("v_table_name", prepare.getUnresolvedVarName(),
                "PREPARE FROM @sql must flag the variable that interrupted the CONCAT as unresolved");
    }

    // Case 3: control — plain DECLARE cursor CURSOR FOR SELECT + WHILE/FETCH loop, unaffected
    @Test
    @DisplayName("3: control — plain static cursor with WHILE/FETCH loop")
    void plainCursorControl() {
        String sql = """
                CREATE PROCEDURE proc_plain_cursor()
                BEGIN
                  DECLARE done INT DEFAULT FALSE;
                  DECLARE v_id INT;
                  DECLARE cur1 CURSOR FOR SELECT id FROM orders WHERE status = 'OPEN';
                  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
                  OPEN cur1;
                  read_loop: LOOP
                    FETCH cur1 INTO v_id;
                    IF done THEN
                      LEAVE read_loop;
                    END IF;
                  END LOOP;
                  CLOSE cur1;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_plain_cursor", null);
        System.out.println("[3] tableOps: " + tableNames(r));
        System.out.println("[3] errors: " + r.getErrors());
    }

    // Case 4: N/A — MySQL cursors take no parameters (not valid MySQL syntax), not tested.
}
