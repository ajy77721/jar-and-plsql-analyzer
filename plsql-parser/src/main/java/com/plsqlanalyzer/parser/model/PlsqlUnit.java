package com.plsqlanalyzer.parser.model;

import java.util.ArrayList;
import java.util.List;

public class PlsqlUnit {
    private String name;
    private String schemaName;
    private PlsqlUnitType unitType;
    private String sourceFile;
    private int startLine;
    private int endLine;
    private List<PlsqlProcedure> procedures = new ArrayList<>();
    private List<String> parseErrors = new ArrayList<>();

    public PlsqlUnit() {}

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getSchemaName() { return schemaName; }
    public void setSchemaName(String schemaName) { this.schemaName = schemaName; }

    public PlsqlUnitType getUnitType() { return unitType; }
    public void setUnitType(PlsqlUnitType unitType) { this.unitType = unitType; }

    public String getSourceFile() { return sourceFile; }
    public void setSourceFile(String sourceFile) { this.sourceFile = sourceFile; }

    public int getStartLine() { return startLine; }
    public void setStartLine(int startLine) { this.startLine = startLine; }

    public int getEndLine() { return endLine; }
    public void setEndLine(int endLine) { this.endLine = endLine; }

    public List<PlsqlProcedure> getProcedures() { return procedures; }
    public void setProcedures(List<PlsqlProcedure> procedures) { this.procedures = procedures; }

    public List<String> getParseErrors() { return parseErrors; }
    public void setParseErrors(List<String> parseErrors) { this.parseErrors = parseErrors; }
}
