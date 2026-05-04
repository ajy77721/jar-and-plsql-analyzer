package com.plsqlanalyzer.analyzer.model;

import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.parser.model.PlsqlUnit;
import com.plsqlanalyzer.parser.service.OracleDictionaryService.TableMetadata;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AnalysisResult {
    private CallGraph callGraph;
    private Map<String, TableOperationSummary> tableOperations;
    private Map<String, SequenceOperationSummary> sequenceOperations = new HashMap<>();
    private Map<String, JoinOperationSummary> joinOperations = new HashMap<>();
    private Map<String, CursorOperationSummary> cursorOperations = new HashMap<>();
    private Map<String, TableMetadata> tableMetadata = new HashMap<>(); // TABLE_NAME -> pre-cached metadata
    private List<PlsqlUnit> units;
    private Map<String, List<String>> sourceMap = new HashMap<>(); // OWNER.NAME -> source lines
    private LocalDateTime timestamp;
    private int version;
    private int fileCount;
    private int procedureCount;
    private int errorCount;
    private String name;            // unique analysis name e.g. "OPUS_CORE_PKG_CUSTOMER_20260416_143022"
    private String entrySchema;     // schema of the analyzed entry point
    private String entryObjectName; // object name of the entry point
    private String entryObjectType; // PACKAGE, PROCEDURE, FUNCTION
    private String entryProcedure;  // specific procedure within package (optional)
    private String analysisMode;    // STATIC or CLAUDE_ENRICHED
    private int claudeIteration;    // how many times full Claude verification ran
    private String claudeEnrichedAt; // ISO timestamp of last Claude merge

    public AnalysisResult() {
        this.timestamp = LocalDateTime.now();
    }

    public int getVersion() { return version; }
    public void setVersion(int version) { this.version = version; }

    public CallGraph getCallGraph() { return callGraph; }
    public void setCallGraph(CallGraph callGraph) { this.callGraph = callGraph; }

    public Map<String, TableOperationSummary> getTableOperations() { return tableOperations; }
    public void setTableOperations(Map<String, TableOperationSummary> tableOperations) { this.tableOperations = tableOperations; }

    public List<PlsqlUnit> getUnits() { return units; }
    public void setUnits(List<PlsqlUnit> units) { this.units = units; }

    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }

    public int getFileCount() { return fileCount; }
    public void setFileCount(int fileCount) { this.fileCount = fileCount; }

    public int getProcedureCount() { return procedureCount; }
    public void setProcedureCount(int procedureCount) { this.procedureCount = procedureCount; }

    public int getErrorCount() { return errorCount; }
    public void setErrorCount(int errorCount) { this.errorCount = errorCount; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Map<String, List<String>> getSourceMap() { return sourceMap; }
    public void setSourceMap(Map<String, List<String>> sourceMap) { this.sourceMap = sourceMap; }

    public String getEntrySchema() { return entrySchema; }
    public void setEntrySchema(String entrySchema) { this.entrySchema = entrySchema; }

    public String getEntryObjectName() { return entryObjectName; }
    public void setEntryObjectName(String entryObjectName) { this.entryObjectName = entryObjectName; }

    public String getEntryObjectType() { return entryObjectType; }
    public void setEntryObjectType(String entryObjectType) { this.entryObjectType = entryObjectType; }

    public String getEntryProcedure() { return entryProcedure; }
    public void setEntryProcedure(String entryProcedure) { this.entryProcedure = entryProcedure; }

    public Map<String, SequenceOperationSummary> getSequenceOperations() { return sequenceOperations; }
    public void setSequenceOperations(Map<String, SequenceOperationSummary> sequenceOperations) { this.sequenceOperations = sequenceOperations; }

    public Map<String, JoinOperationSummary> getJoinOperations() { return joinOperations; }
    public void setJoinOperations(Map<String, JoinOperationSummary> joinOperations) { this.joinOperations = joinOperations; }

    public Map<String, CursorOperationSummary> getCursorOperations() { return cursorOperations; }
    public void setCursorOperations(Map<String, CursorOperationSummary> cursorOperations) { this.cursorOperations = cursorOperations; }

    public Map<String, TableMetadata> getTableMetadata() { return tableMetadata; }
    public void setTableMetadata(Map<String, TableMetadata> tableMetadata) { this.tableMetadata = tableMetadata; }

    public String getAnalysisMode() { return analysisMode; }
    public void setAnalysisMode(String analysisMode) { this.analysisMode = analysisMode; }

    public int getClaudeIteration() { return claudeIteration; }
    public void setClaudeIteration(int claudeIteration) { this.claudeIteration = claudeIteration; }

    public String getClaudeEnrichedAt() { return claudeEnrichedAt; }
    public void setClaudeEnrichedAt(String claudeEnrichedAt) { this.claudeEnrichedAt = claudeEnrichedAt; }
}
