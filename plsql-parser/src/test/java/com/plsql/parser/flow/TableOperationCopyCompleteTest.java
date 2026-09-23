package com.plsql.parser.flow;

import com.plsql.parser.model.TableOperationInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Every field of {@link TableOperationInfo} must be carried by the crawler's copy.
 *
 * <p>{@code DependencyCrawler.resolveTableSchemas} rebuilds each operation to resolve its schema,
 * field by field. Anything not listed there is silently dropped — the operation still appears, with
 * that one attribute blank, so nothing looks broken.
 *
 * <p>This has now happened three times:
 *
 * <ol>
 *   <li>{@code columnUsages} — the enricher filled it, the copy dropped it, and {@code tables.json}
 *       came out with zero column rows while the logs reported "100% parsed".</li>
 *   <li>the same method copied 6 of 11 fields when the column work landed.</li>
 *   <li>{@code columnsParsed} — added to tell "no columns" apart from "could not parse", and
 *       dropped here, so 1,736 statements reported "never attempted" when they had been attempted
 *       and had failed.</li>
 * </ol>
 *
 * <p>The comment in that method already says "copy EVERY field, not a chosen few". A comment cannot
 * fail a build, which is why this test exists: it reflects over the model, so a field added
 * tomorrow is covered without anyone remembering to extend a list.
 */
class TableOperationCopyCompleteTest {

    /**
     * Fields the copy legitimately does not carry verbatim.
     *
     * <p>{@code schema} is resolved rather than copied — that is the method's whole purpose — and
     * {@code tableName} may be rewritten when a schema prefix is split out of it.
     */
    private static final List<String> RESOLVED_NOT_COPIED = List.of("schema", "tableName");

    @Test
    @DisplayName("resolveTableSchemas copies every field the model declares")
    void everyFieldIsCopied() throws Exception {
        String src = java.nio.file.Files.readString(java.nio.file.Path.of(
                "src/main/java/com/plsql/parser/flow/DependencyCrawler.java"));

        int at = src.indexOf("TableOperationInfo rt = new TableOperationInfo();");
        if (at < 0) fail("the copy block moved — this test can no longer see it");
        // The copy runs to the end of the enclosing loop; a generous window is enough because we
        // only look for setter names, and a setter outside the block would be a different bug.
        String block = src.substring(at, Math.min(src.length(), at + 4000));

        List<String> missing = new ArrayList<>();
        for (Field f : TableOperationInfo.class.getDeclaredFields()) {
            if (f.isSynthetic() || java.lang.reflect.Modifier.isStatic(f.getModifiers())) continue;
            String name = f.getName();
            if (RESOLVED_NOT_COPIED.contains(name)) continue;
            String setter = "rt.set" + Character.toUpperCase(name.charAt(0)) + name.substring(1) + "(";
            if (!block.contains(setter)) missing.add(name);
        }

        if (!missing.isEmpty()) {
            fail("These TableOperationInfo fields are declared but NOT copied in "
                    + "DependencyCrawler.resolveTableSchemas, so they are silently lost — the "
                    + "operation still appears with that attribute blank:\n  " + String.join("\n  ", missing)
                    + "\n\nAdd rt.set<Field>(op.get<Field>()) for each, or add it to "
                    + "RESOLVED_NOT_COPIED with a reason if it is deliberately re-derived.");
        }
    }
}
