package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.*;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class DependencySummary {
    private Set<String> tables = new LinkedHashSet<>();
    private Set<String> views = new LinkedHashSet<>();
    private Set<String> materializedViews = new LinkedHashSet<>();
    private Set<String> packages = new LinkedHashSet<>();
    private Set<String> sequences = new LinkedHashSet<>();
    private Set<String> dbLinks = new LinkedHashSet<>();
    private Set<String> types = new LinkedHashSet<>();

    public Set<String> getTables() { return tables; }
    public void setTables(Set<String> tables) { this.tables = tables; }
    public Set<String> getViews() { return views; }
    public void setViews(Set<String> views) { this.views = views; }
    public Set<String> getMaterializedViews() { return materializedViews; }
    public void setMaterializedViews(Set<String> materializedViews) { this.materializedViews = materializedViews; }
    public Set<String> getPackages() { return packages; }
    public void setPackages(Set<String> packages) { this.packages = packages; }
    public Set<String> getSequences() { return sequences; }
    public void setSequences(Set<String> sequences) { this.sequences = sequences; }
    public Set<String> getDbLinks() { return dbLinks; }
    public void setDbLinks(Set<String> dbLinks) { this.dbLinks = dbLinks; }
    public Set<String> getTypes() { return types; }
    public void setTypes(Set<String> types) { this.types = types; }
}
