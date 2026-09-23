package com.jaranalyzer.model;

import java.util.ArrayList;
import java.util.List;

public class FieldInfo {
    private String name;
    private String type;
    private List<AnnotationInfo> annotations = new ArrayList<>();
    private int accessFlags;
    private String constantValue;

    /** The MongoDB field name from @Field, @BsonProperty, or @JsonProperty.
     *  null if no mapping annotation present (uses Java field name as-is). */
    private String mongoFieldName;

    /** True if this field is annotated with @Id or @BsonId (maps to _id). */
    private boolean idField;

    /** The JPA column name from @Column annotation (null if not present). */
    private String jpaColumnName;

    public FieldInfo() {}

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public List<AnnotationInfo> getAnnotations() { return annotations; }
    public void setAnnotations(List<AnnotationInfo> annotations) { this.annotations = annotations; }
    public int getAccessFlags() { return accessFlags; }
    public void setAccessFlags(int accessFlags) { this.accessFlags = accessFlags; }
    public String getConstantValue() { return constantValue; }
    public void setConstantValue(String constantValue) { this.constantValue = constantValue; }
    public String getMongoFieldName() { return mongoFieldName; }
    public void setMongoFieldName(String mongoFieldName) { this.mongoFieldName = mongoFieldName; }
    public boolean isIdField() { return idField; }
    public void setIdField(boolean idField) { this.idField = idField; }

    public String getJpaColumnName() { return jpaColumnName; }
    public void setJpaColumnName(String jpaColumnName) { this.jpaColumnName = jpaColumnName; }

    /** For collection-typed fields (List&lt;T&gt;, Set&lt;T&gt;, Map&lt;K,V&gt;): FQN of the element/value type T.
     *  null for non-collection fields. Extracted from the bytecode field signature attribute. */
    private String genericElementType;
    public String getGenericElementType() { return genericElementType; }
    public void setGenericElementType(String v) { this.genericElementType = v; }

    /** Returns the effective MongoDB field name: explicit mapping > "_id" for @Id > Java field name. */
    public String effectiveMongoName() {
        if (mongoFieldName != null) return mongoFieldName;
        if (idField) return "_id";
        return name;
    }

    /** Returns the effective JPA column name: explicit @Column > Java field name. */
    public String effectiveJpaColumnName() {
        if (jpaColumnName != null) return jpaColumnName;
        return name;
    }
}
