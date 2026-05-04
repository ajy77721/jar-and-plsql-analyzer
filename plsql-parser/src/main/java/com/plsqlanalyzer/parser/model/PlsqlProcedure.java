package com.plsqlanalyzer.parser.model;

import java.util.ArrayList;
import java.util.List;

public class PlsqlProcedure {
    private String name;
    private PlsqlUnitType type;
    private int startLine;
    private int endLine;
    private List<ProcedureCall> calls = new ArrayList<>();
    private List<SqlAnalysisResult> sqlStatements = new ArrayList<>();
    private List<TableReference> tableReferences = new ArrayList<>();
    private List<ProcedureParameter> parameters = new ArrayList<>();
    private List<VariableInfo> variables = new ArrayList<>();
    private List<StatementInfo> statements = new ArrayList<>();
    private List<SequenceReference> sequenceReferences = new ArrayList<>();
    private List<JoinInfo> joinInfos = new ArrayList<>();
    private List<CursorInfo> cursorInfos = new ArrayList<>();

    public PlsqlProcedure() {}

    public PlsqlProcedure(String name, PlsqlUnitType type, int startLine, int endLine) {
        this.name = name;
        this.type = type;
        this.startLine = startLine;
        this.endLine = endLine;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public PlsqlUnitType getType() { return type; }
    public void setType(PlsqlUnitType type) { this.type = type; }

    public int getStartLine() { return startLine; }
    public void setStartLine(int startLine) { this.startLine = startLine; }

    public int getEndLine() { return endLine; }
    public void setEndLine(int endLine) { this.endLine = endLine; }

    public List<ProcedureCall> getCalls() { return calls; }
    public void setCalls(List<ProcedureCall> calls) { this.calls = calls; }

    public List<SqlAnalysisResult> getSqlStatements() { return sqlStatements; }
    public void setSqlStatements(List<SqlAnalysisResult> sqlStatements) { this.sqlStatements = sqlStatements; }

    public List<TableReference> getTableReferences() { return tableReferences; }
    public void setTableReferences(List<TableReference> tableReferences) { this.tableReferences = tableReferences; }

    public List<ProcedureParameter> getParameters() { return parameters; }
    public void setParameters(List<ProcedureParameter> parameters) { this.parameters = parameters; }

    public List<VariableInfo> getVariables() { return variables; }
    public void setVariables(List<VariableInfo> variables) { this.variables = variables; }

    public List<StatementInfo> getStatements() { return statements; }
    public void setStatements(List<StatementInfo> statements) { this.statements = statements; }

    public List<SequenceReference> getSequenceReferences() { return sequenceReferences; }
    public void setSequenceReferences(List<SequenceReference> sequenceReferences) { this.sequenceReferences = sequenceReferences; }

    public List<JoinInfo> getJoinInfos() { return joinInfos; }
    public void setJoinInfos(List<JoinInfo> joinInfos) { this.joinInfos = joinInfos; }

    public List<CursorInfo> getCursorInfos() { return cursorInfos; }
    public void setCursorInfos(List<CursorInfo> cursorInfos) { this.cursorInfos = cursorInfos; }
}
