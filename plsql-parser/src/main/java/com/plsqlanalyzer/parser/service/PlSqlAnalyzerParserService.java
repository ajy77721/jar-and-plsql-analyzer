package com.plsqlanalyzer.parser.service;

import com.plsqlanalyzer.parser.grammar.PlSqlAnalyzerLexer;
import com.plsqlanalyzer.parser.grammar.PlSqlAnalyzerParser;
import com.plsqlanalyzer.parser.model.PlsqlUnit;
import com.plsqlanalyzer.parser.model.PlsqlUnitType;
import com.plsqlanalyzer.parser.visitor.PlSqlAnalyzerVisitorImpl;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Entry point for parsing PL/SQL source code using the custom PlSqlAnalyzer grammar.
 * Creates lexer/parser from source text, runs the visitor, returns PlsqlUnit.
 */
public class PlSqlAnalyzerParserService {

    private static final Logger log = LoggerFactory.getLogger(PlSqlAnalyzerParserService.class);

    /**
     * Parse PL/SQL source code and extract structural information.
     *
     * @param source     the PL/SQL source text
     * @param schemaName the schema this source belongs to (e.g., OPUS_CORE)
     * @param objectName the object name (e.g., PKG_CUSTOMER)
     * @return parsed PlsqlUnit with procedures, calls, table references
     */
    public PlsqlUnit parse(String source, String schemaName, String objectName) {
        if (source == null || source.isBlank()) {
            PlsqlUnit empty = new PlsqlUnit();
            empty.setName(objectName);
            empty.setSchemaName(schemaName);
            empty.getParseErrors().add("Empty source");
            return empty;
        }

        List<String> errors = new ArrayList<>();

        try {
            CharStream input = CharStreams.fromString(source);
            PlSqlAnalyzerLexer lexer = new PlSqlAnalyzerLexer(input);
            lexer.removeErrorListeners();
            lexer.addErrorListener(new CollectingErrorListener(errors));

            CommonTokenStream tokens = new CommonTokenStream(lexer);
            PlSqlAnalyzerParser parser = new PlSqlAnalyzerParser(tokens);
            parser.removeErrorListeners();
            parser.addErrorListener(new CollectingErrorListener(errors));

            // Use SLL prediction mode first for speed, fall back to LL on failure
            parser.getInterpreter().setPredictionMode(
                    org.antlr.v4.runtime.atn.PredictionMode.SLL);

            ParseTree tree;
            try {
                tree = parser.compilationUnit();
            } catch (Exception e) {
                errors.clear();
                tokens.seek(0);
                parser.reset();
                parser.getInterpreter().setPredictionMode(
                        org.antlr.v4.runtime.atn.PredictionMode.LL);
                tree = parser.compilationUnit();
            }

            PlSqlAnalyzerVisitorImpl visitor = new PlSqlAnalyzerVisitorImpl();
            visitor.visit(tree);

            PlsqlUnit unit = visitor.getUnit();
            // Set schema/name if not already set by the visitor (e.g., anonymous blocks)
            if (unit.getName() == null || unit.getName().isEmpty()) {
                unit.setName(objectName);
            }
            if (unit.getSchemaName() == null || unit.getSchemaName().isEmpty()) {
                unit.setSchemaName(schemaName);
            }
            unit.setSourceFile(schemaName.toUpperCase() + "." + objectName.toUpperCase());

            // Only include significant errors (not every token mismatch in loose rules)
            if (!errors.isEmpty()) {
                // Limit to first 20 errors to avoid noise
                unit.getParseErrors().addAll(errors.subList(0, Math.min(errors.size(), 20)));
                log.debug("Parse completed with {} error(s) for {}.{}", errors.size(), schemaName, objectName);
            }

            return unit;

        } catch (Exception e) {
            log.error("Failed to parse {}.{}: {}", schemaName, objectName, e.getMessage(), e);
            PlsqlUnit errorUnit = new PlsqlUnit();
            errorUnit.setName(objectName);
            errorUnit.setSchemaName(schemaName);
            errorUnit.getParseErrors().add("Parse failed: " + e.getMessage());
            errorUnit.getParseErrors().addAll(errors);
            return errorUnit;
        }
    }

    /**
     * Parse source fetched from ALL_SOURCE (array of lines).
     * ALL_SOURCE does NOT include "CREATE OR REPLACE" — it starts with "PACKAGE BODY ...",
     * "PROCEDURE ...", etc. We prepend it so the grammar can match.
     */
    public PlsqlUnit parseLines(List<String> sourceLines, String schemaName, String objectName) {
        String source = String.join("\n", sourceLines);

        // Detect Oracle wrapped/encrypted packages — cannot be parsed
        if (isWrappedSource(source)) {
            PlsqlUnit wrapped = new PlsqlUnit();
            wrapped.setName(objectName);
            wrapped.setSchemaName(schemaName);
            wrapped.setUnitType(detectUnitType(source));
            wrapped.getParseErrors().add("Wrapped/encrypted source — cannot parse");
            return wrapped;
        }

        source = ensureCreatePrefix(source);
        return parse(source, schemaName, objectName);
    }

    private boolean isWrappedSource(String source) {
        if (source == null) return false;
        String firstLine = source.lines().findFirst().orElse("").trim().toUpperCase();
        return firstLine.contains(" WRAPPED");
    }

    private PlsqlUnitType detectUnitType(String source) {
        if (source == null) return PlsqlUnitType.PACKAGE_BODY;
        String upper = source.stripLeading().toUpperCase();
        if (upper.startsWith("PACKAGE BODY ")) return PlsqlUnitType.PACKAGE_BODY;
        if (upper.startsWith("PACKAGE ")) return PlsqlUnitType.PACKAGE_SPEC;
        if (upper.startsWith("PROCEDURE ")) return PlsqlUnitType.PROCEDURE;
        if (upper.startsWith("FUNCTION ")) return PlsqlUnitType.FUNCTION;
        if (upper.startsWith("TRIGGER ")) return PlsqlUnitType.TRIGGER;
        return PlsqlUnitType.PACKAGE_BODY;
    }

    /**
     * ALL_SOURCE returns source without "CREATE OR REPLACE" prefix.
     * The grammar requires it, so prepend if missing.
     * Also handles Oracle 12c+ EDITIONABLE/NONEDITIONABLE prefixes.
     */
    private String ensureCreatePrefix(String source) {
        if (source == null || source.isBlank()) return source;
        String trimmed = source.stripLeading();
        String upper = trimmed.toUpperCase();
        if (upper.startsWith("CREATE")) return source;

        // Strip EDITIONABLE / NONEDITIONABLE prefix if present (Oracle 12c+)
        String afterEdition = trimmed;
        String upperAfter = upper;
        if (upper.startsWith("EDITIONABLE ") || upper.startsWith("NONEDITIONABLE ")) {
            int space = trimmed.indexOf(' ');
            afterEdition = trimmed.substring(space).stripLeading();
            upperAfter = afterEdition.toUpperCase();
        }

        // Detect what kind of object this is and prepend CREATE OR REPLACE
        if (upperAfter.startsWith("PACKAGE BODY ")
                || upperAfter.startsWith("PACKAGE ")
                || upperAfter.startsWith("PROCEDURE ")
                || upperAfter.startsWith("FUNCTION ")
                || upperAfter.startsWith("TRIGGER ")) {
            return "CREATE OR REPLACE " + trimmed;
        }
        return source;
    }

    /**
     * Error listener that collects errors into a list instead of printing to stderr.
     */
    private static class CollectingErrorListener extends BaseErrorListener {
        private final List<String> errors;

        CollectingErrorListener(List<String> errors) {
            this.errors = errors;
        }

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                                int line, int charPositionInLine, String msg,
                                RecognitionException e) {
            errors.add("line " + line + ":" + charPositionInLine + " " + msg);
        }
    }
}
