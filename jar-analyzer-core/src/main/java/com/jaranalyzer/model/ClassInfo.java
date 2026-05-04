package com.jaranalyzer.model;

import java.util.ArrayList;
import java.util.List;

public class ClassInfo {
    private String fullyQualifiedName;
    private String packageName;
    private String simpleName;
    private String superClass;
    private List<String> interfaces = new ArrayList<>();
    private List<AnnotationInfo> annotations = new ArrayList<>();
    private List<FieldInfo> fields = new ArrayList<>();
    private List<MethodInfo> methods = new ArrayList<>();
    private String stereotype;
    private int accessFlags;
    private boolean isInterface;
    private boolean isAbstract;
    private boolean isEnum;
    private String sourceJar;

    /** For repository interfaces: the FQN of the entity type parameter (e.g. from MongoRepository&lt;Entity, ID&gt;). */
    private String repositoryEntityType;

    public ClassInfo() {}

    public String getFullyQualifiedName() { return fullyQualifiedName; }
    public void setFullyQualifiedName(String fullyQualifiedName) { this.fullyQualifiedName = fullyQualifiedName; }
    public String getPackageName() { return packageName; }
    public void setPackageName(String packageName) { this.packageName = packageName; }
    public String getSimpleName() { return simpleName; }
    public void setSimpleName(String simpleName) { this.simpleName = simpleName; }
    public String getSuperClass() { return superClass; }
    public void setSuperClass(String superClass) { this.superClass = superClass; }
    public List<String> getInterfaces() { return interfaces; }
    public void setInterfaces(List<String> interfaces) { this.interfaces = interfaces; }
    public List<AnnotationInfo> getAnnotations() { return annotations; }
    public void setAnnotations(List<AnnotationInfo> annotations) { this.annotations = annotations; }
    public List<FieldInfo> getFields() { return fields; }
    public void setFields(List<FieldInfo> fields) { this.fields = fields; }
    public List<MethodInfo> getMethods() { return methods; }
    public void setMethods(List<MethodInfo> methods) { this.methods = methods; }
    public String getStereotype() { return stereotype; }
    public void setStereotype(String stereotype) { this.stereotype = stereotype; }
    public int getAccessFlags() { return accessFlags; }
    public void setAccessFlags(int accessFlags) { this.accessFlags = accessFlags; }
    public boolean getIsInterface() { return isInterface; }
    public void setIsInterface(boolean isInterface) { this.isInterface = isInterface; }
    public boolean getIsAbstract() { return isAbstract; }
    public void setIsAbstract(boolean isAbstract) { this.isAbstract = isAbstract; }
    public boolean getIsEnum() { return isEnum; }
    public void setIsEnum(boolean isEnum) { this.isEnum = isEnum; }
    public String getSourceJar() { return sourceJar; }
    public void setSourceJar(String sourceJar) { this.sourceJar = sourceJar; }
    public String getRepositoryEntityType() { return repositoryEntityType; }
    public void setRepositoryEntityType(String repositoryEntityType) { this.repositoryEntityType = repositoryEntityType; }
}
