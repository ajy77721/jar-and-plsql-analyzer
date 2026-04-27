package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.*;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class SchemaTableInfo {
    private String schema;
    private String tableName;
    private String objectType;
    private Set<String> operations = new LinkedHashSet<>();
    private List<Integer> lines = new ArrayList<>();
    private Set<String> referencedBy = new LinkedHashSet<>();

    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public String getObjectType() { return objectType; }
    public void setObjectType(String objectType) { this.objectType = objectType; }
    public Set<String> getOperations() { return operations; }
    public void setOperations(Set<String> operations) { this.operations = operations; }
    public List<Integer> getLines() { return lines; }
    public void setLines(List<Integer> lines) { this.lines = lines; }
    public Set<String> getReferencedBy() { return referencedBy; }
    public void setReferencedBy(Set<String> referencedBy) { this.referencedBy = referencedBy; }
}
