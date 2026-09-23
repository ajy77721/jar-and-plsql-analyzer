package com.plsqlanalyzer.config;

public class SchemaMapping {
    private String schema;
    private String description;
    private String dbLink;

    public SchemaMapping() {}

    public SchemaMapping(String schema, String description, String dbLink) {
        this.schema = schema;
        this.description = description;
        this.dbLink = dbLink;
    }

    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getDbLink() { return dbLink; }
    public void setDbLink(String dbLink) { this.dbLink = dbLink; }
}
