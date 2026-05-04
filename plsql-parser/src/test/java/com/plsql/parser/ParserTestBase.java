package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;

public abstract class ParserTestBase {

    protected static PlSqlParserEngine engine;

    @BeforeAll
    static void setup() {
        engine = new PlSqlParserEngine();
    }

    protected ParseResult parse(String sql) {
        return engine.parseContent(sql, "test.sql");
    }

    protected SubprogramInfo findSub(ParseResult result, String name) {
        for (ParsedObject obj : result.getObjects()) {
            for (SubprogramInfo sub : obj.getSubprograms()) {
                if (sub.getName() != null && sub.getName().equalsIgnoreCase(name)) {
                    return sub;
                }
            }
        }
        return null;
    }

    protected void assertNoParsErrors(ParseResult result, String context) {
        List<String> errors = result.getErrors();
        if (errors != null && !errors.isEmpty()) {
            throw new AssertionError(context + " — got errors: " + errors);
        }
    }
}
