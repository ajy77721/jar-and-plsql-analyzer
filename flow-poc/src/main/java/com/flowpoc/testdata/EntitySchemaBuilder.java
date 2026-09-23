/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  com.fasterxml.jackson.databind.JsonNode
 *  com.fasterxml.jackson.databind.ObjectMapper
 */
package com.flowpoc.testdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class EntitySchemaBuilder {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private EntitySchemaBuilder() {
    }

    public static Map<String, Map<String, Object>> buildCollectionSamples(InputStream analysisJson) {
        if (analysisJson == null) {
            return Collections.emptyMap();
        }
        try {
            JsonNode root = MAPPER.readTree(analysisJson);
            JsonNode classes = root.path("classes");
            if (!classes.isArray()) {
                return Collections.emptyMap();
            }
            LinkedHashMap<String, String> fqnToCollection = new LinkedHashMap<String, String>();
            LinkedHashMap<String, List<FieldInfo>> fqnToFields = new LinkedHashMap<String, List<FieldInfo>>();
            for (JsonNode cls : classes) {
                String simple;
                String fqn = cls.path("fullyQualifiedName").asText("");
                String stereotype = cls.path("stereotype").asText("");
                List<FieldInfo> fields = EntitySchemaBuilder.parseFields(cls.path("fields"));
                if (!fields.isEmpty()) {
                    fqnToFields.put(fqn, fields);
                }
                if (!"ENTITY".equalsIgnoreCase(stereotype)) continue;
                for (JsonNode ann : cls.path("annotations")) {
                    String annName = ann.path("name").asText("");
                    if (!"Document".equals(annName) && !"Collection".equals(annName)) continue;
                    String coll = ann.path("attributes").path("collection").asText("").trim();
                    if (coll.isBlank()) break;
                    fqnToCollection.put(fqn, coll);
                    break;
                }
                if (fqnToCollection.containsKey(fqn) || (simple = cls.path("simpleName").asText("")).isBlank()) continue;
                fqnToCollection.put(fqn, EntitySchemaBuilder.toCollectionName(simple));
            }
            for (JsonNode cls : classes) {
                String entityFqn;
                String stereotype = cls.path("stereotype").asText("");
                if (!"REPOSITORY".equalsIgnoreCase(stereotype) || (entityFqn = cls.path("repositoryEntityType").asText("")).isBlank() || fqnToCollection.containsKey(entityFqn)) continue;
                String simple = entityFqn.contains(".") ? entityFqn.substring(entityFqn.lastIndexOf(46) + 1) : entityFqn;
                fqnToCollection.put(entityFqn, EntitySchemaBuilder.toCollectionName(simple));
            }
            LinkedHashMap<String, Map<String, Object>> result = new LinkedHashMap<String, Map<String, Object>>();
            for (Map.Entry e : fqnToCollection.entrySet()) {
                String fqn = (String)e.getKey();
                String coll = (String)e.getValue();
                List fields = (List)fqnToFields.get(fqn);
                if (fields == null || fields.isEmpty()) continue;
                result.put(coll, EntitySchemaBuilder.buildSample(fields));
            }
            return result;
        }
        catch (Exception ignored) {
            return Collections.emptyMap();
        }
    }

    private static List<FieldInfo> parseFields(JsonNode fieldsNode) {
        ArrayList<FieldInfo> list = new ArrayList<FieldInfo>();
        if (!fieldsNode.isArray()) {
            return list;
        }
        for (JsonNode f : fieldsNode) {
            String javaName = f.path("name").asText("");
            String javaType = f.path("type").asText("String");
            String mongoName = f.path("mongoFieldName").asText("");
            boolean isId = f.path("idField").asBoolean(false);
            if (javaName.isBlank()) continue;
            String dbField = !mongoName.isBlank() ? mongoName : (isId ? "_id" : javaName);
            list.add(new FieldInfo(javaName, dbField, javaType, isId));
        }
        return list;
    }

    private static Map<String, Object> buildSample(List<FieldInfo> fields) {
        LinkedHashMap<String, Object> doc = new LinkedHashMap<String, Object>();
        for (FieldInfo f : fields) {
            doc.put(f.dbField, EntitySchemaBuilder.syntheticValue(f));
        }
        return doc;
    }

    private static Object syntheticValue(FieldInfo f) {
        String name = f.javaName.toLowerCase();
        String type = f.javaType;
        if (f.isId) {
            return "507f1f77bcf86cd799439011";
        }
        return switch (type) {
            case "boolean", "Boolean" -> true;
            case "int", "Integer" -> 1;
            case "long", "Long" -> 1L;
            case "double", "Double", "float", "Float" -> 1.0;
            case "BigDecimal" -> "1.00";
            case "LocalDate" -> "2024-01-15";
            case "LocalDateTime", "Date", "ZonedDateTime", "Instant" -> "2024-01-15T10:30:00";
            case "List", "ArrayList", "Set" -> Collections.emptyList();
            case "Map", "HashMap", "LinkedHashMap" -> Collections.emptyMap();
            default -> EntitySchemaBuilder.stringValue(name);
        };
    }

    private static String stringValue(String name) {
        if (name.contains("email")) {
            return "user@example.com";
        }
        if (name.contains("status")) {
            return "ACTIVE";
        }
        if (name.contains("phone") || name.contains("mobile")) {
            return "+1-555-0100";
        }
        if (name.contains("url") || name.contains("link")) {
            return "https://example.com";
        }
        if (name.contains("code")) {
            return "CODE001";
        }
        if (name.contains("name") && name.contains("first")) {
            return "John";
        }
        if (name.contains("name") && name.contains("last")) {
            return "Doe";
        }
        if (name.contains("name")) {
            return "Sample Name";
        }
        if (name.contains("description") || name.contains("notes") || name.contains("bio")) {
            return "Sample description";
        }
        if (name.contains("summary")) {
            return "Sample summary";
        }
        if (name.contains("location") || name.contains("address")) {
            return "123 Main St";
        }
        if (name.contains("type")) {
            return "TYPE_A";
        }
        if (name.contains("action")) {
            return "CREATE";
        }
        if (name.contains("role")) {
            return "USER";
        }
        if (name.contains("id") || name.endsWith("id")) {
            return "507f1f77bcf86cd799439011";
        }
        return "sample_value";
    }

    private static String toCollectionName(String simpleClassName) {
        String snake = simpleClassName.replaceAll("([A-Z])", "_$1").toLowerCase().replaceAll("^_", "");
        if (snake.endsWith("y")) {
            return snake.substring(0, snake.length() - 1) + "ies";
        }
        if (!snake.endsWith("s")) {
            return snake + "s";
        }
        return snake;
    }

    static final class FieldInfo {
        final String javaName;
        final String dbField;
        final String javaType;
        final boolean isId;

        FieldInfo(String javaName, String dbField, String javaType, boolean isId) {
            this.javaName = javaName;
            this.dbField = dbField;
            this.javaType = javaType;
            this.isId = isId;
        }
    }
}

