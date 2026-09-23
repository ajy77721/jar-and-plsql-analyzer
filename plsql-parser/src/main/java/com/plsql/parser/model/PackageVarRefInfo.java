package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A single reference to a package-qualified global variable (PKG.VAR),
 * carrying the source line number and whether the reference is a
 * read, a write, or of undeterminable access type.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class PackageVarRefInfo {
    public enum AccessType { READ, WRITE, UNKNOWN }

    private String packageName;
    private String variableName;
    private int line;
    private AccessType accessType = AccessType.UNKNOWN;

    public PackageVarRefInfo() {
    }

    public PackageVarRefInfo(String packageName, String variableName, int line, AccessType accessType) {
        this.packageName = packageName;
        this.variableName = variableName;
        this.line = line;
        this.accessType = accessType != null ? accessType : AccessType.UNKNOWN;
    }

    public String getPackageName() { return packageName; }
    public void setPackageName(String packageName) { this.packageName = packageName; }
    public String getVariableName() { return variableName; }
    public void setVariableName(String variableName) { this.variableName = variableName; }
    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }
    public AccessType getAccessType() { return accessType; }
    public void setAccessType(AccessType accessType) { this.accessType = accessType; }

    /**
     * Convenience: "PKG.VAR" as previously stored in the bare string list.
     *
     * <p>Derived, with no matching setter. It must stay READ_ONLY: without this, Jackson emits
     * {@code "ref"} on write and then rejects it on read with "Unrecognized field", which broke
     * {@code DependencyCrawler}'s node merge for every Oracle run that had any package-variable
     * reference — the crawl itself succeeded but merge aborted, so zero api/ artifacts were
     * written and the job still reported COMPLETE.
     */
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    public String getRef() {
        return (packageName != null ? packageName : "") + "." + (variableName != null ? variableName : "");
    }
}
