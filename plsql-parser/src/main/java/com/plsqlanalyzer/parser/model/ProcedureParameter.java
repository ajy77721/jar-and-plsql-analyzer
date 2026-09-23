package com.plsqlanalyzer.parser.model;

public class ProcedureParameter {
    private String name;
    private String mode;      // IN, OUT, IN OUT
    private String dataType;
    private boolean noCopy;
    private int lineNumber;

    public ProcedureParameter() {}

    public ProcedureParameter(String name, String mode, String dataType, boolean noCopy, int lineNumber) {
        this.name = name;
        this.mode = mode;
        this.dataType = dataType;
        this.noCopy = noCopy;
        this.lineNumber = lineNumber;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getMode() { return mode; }
    public void setMode(String mode) { this.mode = mode; }

    public String getDataType() { return dataType; }
    public void setDataType(String dataType) { this.dataType = dataType; }

    public boolean isNoCopy() { return noCopy; }
    public void setNoCopy(boolean noCopy) { this.noCopy = noCopy; }

    public int getLineNumber() { return lineNumber; }
    public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }
}
