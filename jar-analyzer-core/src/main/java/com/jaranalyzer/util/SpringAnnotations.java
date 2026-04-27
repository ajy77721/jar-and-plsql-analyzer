package com.jaranalyzer.util;

import java.util.HashMap;
import java.util.Map;

public final class SpringAnnotations {

    private SpringAnnotations() {}

    public static final Map<String, String> ANNOTATION_MAP = new HashMap<>();
    public static final Map<String, String> HTTP_METHODS = new HashMap<>();
    static {
        HTTP_METHODS.put("GetMapping", "GET");
        HTTP_METHODS.put("PostMapping", "POST");
        HTTP_METHODS.put("PutMapping", "PUT");
        HTTP_METHODS.put("DeleteMapping", "DELETE");
        HTTP_METHODS.put("PatchMapping", "PATCH");
        HTTP_METHODS.put("HeadMapping", "HEAD");
        HTTP_METHODS.put("OptionsMapping", "OPTIONS");
        HTTP_METHODS.put("TraceMapping", "TRACE");
        // Spring 6.1+ HTTP Interface (declarative clients)
        HTTP_METHODS.put("GetExchange", "GET");
        HTTP_METHODS.put("PostExchange", "POST");
        HTTP_METHODS.put("PutExchange", "PUT");
        HTTP_METHODS.put("DeleteExchange", "DELETE");
        HTTP_METHODS.put("PatchExchange", "PATCH");
        HTTP_METHODS.put("HttpExchange", "ALL");
    }

    static {
        // Stereotypes
        ANNOTATION_MAP.put("Lorg/springframework/web/bind/annotation/RestController;", "RestController");
        ANNOTATION_MAP.put("Lorg/springframework/stereotype/Controller;", "Controller");
        ANNOTATION_MAP.put("Lorg/springframework/stereotype/Service;", "Service");
        ANNOTATION_MAP.put("Lorg/springframework/stereotype/Repository;", "Repository");
        ANNOTATION_MAP.put("Lorg/springframework/stereotype/Component;", "Component");
        ANNOTATION_MAP.put("Lorg/springframework/context/annotation/Configuration;", "Configuration");

        // JPA
        ANNOTATION_MAP.put("Ljavax/persistence/Entity;", "Entity");
        ANNOTATION_MAP.put("Ljakarta/persistence/Entity;", "Entity");
        ANNOTATION_MAP.put("Ljavax/persistence/Table;", "Table");
        ANNOTATION_MAP.put("Ljakarta/persistence/Table;", "Table");
        ANNOTATION_MAP.put("Ljavax/persistence/Column;", "Column");
        ANNOTATION_MAP.put("Ljakarta/persistence/Column;", "Column");
        ANNOTATION_MAP.put("Ljavax/persistence/JoinTable;", "JoinTable");
        ANNOTATION_MAP.put("Ljakarta/persistence/JoinTable;", "JoinTable");
        ANNOTATION_MAP.put("Ljavax/persistence/JoinColumn;", "JoinColumn");
        ANNOTATION_MAP.put("Ljakarta/persistence/JoinColumn;", "JoinColumn");
        ANNOTATION_MAP.put("Ljavax/persistence/MappedSuperclass;", "MappedSuperclass");
        ANNOTATION_MAP.put("Ljakarta/persistence/MappedSuperclass;", "MappedSuperclass");
        ANNOTATION_MAP.put("Ljavax/persistence/Embeddable;", "Embeddable");
        ANNOTATION_MAP.put("Ljakarta/persistence/Embeddable;", "Embeddable");
        ANNOTATION_MAP.put("Ljavax/persistence/Id;", "Id");
        ANNOTATION_MAP.put("Ljakarta/persistence/Id;", "Id");
        ANNOTATION_MAP.put("Ljavax/persistence/NamedQuery;", "NamedQuery");
        ANNOTATION_MAP.put("Ljakarta/persistence/NamedQuery;", "NamedQuery");
        ANNOTATION_MAP.put("Ljavax/persistence/NamedNativeQuery;", "NamedNativeQuery");
        ANNOTATION_MAP.put("Ljakarta/persistence/NamedNativeQuery;", "NamedNativeQuery");
        ANNOTATION_MAP.put("Lorg/springframework/data/jpa/repository/Query;", "Query");

        // MongoDB
        ANNOTATION_MAP.put("Lorg/springframework/data/mongodb/core/mapping/Document;", "Document");

        // Request mappings
        ANNOTATION_MAP.put("Lorg/springframework/web/bind/annotation/RequestMapping;", "RequestMapping");
        ANNOTATION_MAP.put("Lorg/springframework/web/bind/annotation/GetMapping;", "GetMapping");
        ANNOTATION_MAP.put("Lorg/springframework/web/bind/annotation/PostMapping;", "PostMapping");
        ANNOTATION_MAP.put("Lorg/springframework/web/bind/annotation/PutMapping;", "PutMapping");
        ANNOTATION_MAP.put("Lorg/springframework/web/bind/annotation/DeleteMapping;", "DeleteMapping");
        ANNOTATION_MAP.put("Lorg/springframework/web/bind/annotation/PatchMapping;", "PatchMapping");
        ANNOTATION_MAP.put("Lorg/springframework/web/bind/annotation/HeadMapping;", "HeadMapping");
        ANNOTATION_MAP.put("Lorg/springframework/web/bind/annotation/OptionsMapping;", "OptionsMapping");
        ANNOTATION_MAP.put("Lorg/springframework/web/bind/annotation/TraceMapping;", "TraceMapping");

        // Spring 6.1+ HTTP Interface (declarative clients)
        ANNOTATION_MAP.put("Lorg/springframework/web/service/annotation/GetExchange;", "GetExchange");
        ANNOTATION_MAP.put("Lorg/springframework/web/service/annotation/PostExchange;", "PostExchange");
        ANNOTATION_MAP.put("Lorg/springframework/web/service/annotation/PutExchange;", "PutExchange");
        ANNOTATION_MAP.put("Lorg/springframework/web/service/annotation/DeleteExchange;", "DeleteExchange");
        ANNOTATION_MAP.put("Lorg/springframework/web/service/annotation/PatchExchange;", "PatchExchange");
        ANNOTATION_MAP.put("Lorg/springframework/web/service/annotation/HttpExchange;", "HttpExchange");

        // DI
        ANNOTATION_MAP.put("Lorg/springframework/beans/factory/annotation/Autowired;", "Autowired");
        ANNOTATION_MAP.put("Ljakarta/inject/Inject;", "Inject");
        ANNOTATION_MAP.put("Ljavax/inject/Inject;", "Inject");
        ANNOTATION_MAP.put("Lorg/springframework/beans/factory/annotation/Qualifier;", "Qualifier");
        ANNOTATION_MAP.put("Ljakarta/inject/Named;", "Named");
        ANNOTATION_MAP.put("Ljavax/inject/Named;", "Named");
        ANNOTATION_MAP.put("Ljakarta/annotation/Resource;", "Resource");
        ANNOTATION_MAP.put("Ljavax/annotation/Resource;", "Resource");
        ANNOTATION_MAP.put("Lorg/springframework/context/annotation/Primary;", "Primary");
        ANNOTATION_MAP.put("Lorg/springframework/context/annotation/Bean;", "Bean");

        // Parameter bindings
        ANNOTATION_MAP.put("Lorg/springframework/web/bind/annotation/RequestBody;", "RequestBody");
        ANNOTATION_MAP.put("Lorg/springframework/web/bind/annotation/PathVariable;", "PathVariable");
        ANNOTATION_MAP.put("Lorg/springframework/web/bind/annotation/RequestParam;", "RequestParam");
        ANNOTATION_MAP.put("Lorg/springframework/web/bind/annotation/RequestHeader;", "RequestHeader");

        // Scheduling
        ANNOTATION_MAP.put("Lorg/springframework/scheduling/annotation/Scheduled;", "Scheduled");
        ANNOTATION_MAP.put("Lorg/springframework/scheduling/annotation/Schedules;", "Schedules");
        ANNOTATION_MAP.put("Lorg/springframework/scheduling/annotation/EnableScheduling;", "EnableScheduling");

        // Messaging / event-driven entry points
        ANNOTATION_MAP.put("Lorg/springframework/amqp/rabbit/annotation/RabbitListener;", "RabbitListener");
        ANNOTATION_MAP.put("Lorg/springframework/amqp/rabbit/annotation/RabbitListeners;", "RabbitListeners");
        ANNOTATION_MAP.put("Lorg/springframework/kafka/annotation/KafkaListener;", "KafkaListener");
        ANNOTATION_MAP.put("Lorg/springframework/kafka/annotation/KafkaListeners;", "KafkaListeners");
        ANNOTATION_MAP.put("Lorg/springframework/messaging/handler/annotation/MessageMapping;", "MessageMapping");

        // HTTP declarative clients (FeignClient)
        ANNOTATION_MAP.put("Lorg/springframework/cloud/openfeign/FeignClient;", "FeignClient");
        ANNOTATION_MAP.put("Lfeign/RequestLine;", "RequestLine");

        // MongoDB aggregation
        ANNOTATION_MAP.put("Lorg/springframework/data/mongodb/repository/Aggregation;", "Aggregation");

        // JPA named stored procedure queries
        ANNOTATION_MAP.put("Ljavax/persistence/NamedStoredProcedureQuery;", "NamedStoredProcedureQuery");
        ANNOTATION_MAP.put("Ljakarta/persistence/NamedStoredProcedureQuery;", "NamedStoredProcedureQuery");
        ANNOTATION_MAP.put("Ljavax/persistence/NamedStoredProcedureQueries;", "NamedStoredProcedureQueries");
        ANNOTATION_MAP.put("Ljakarta/persistence/NamedStoredProcedureQueries;", "NamedStoredProcedureQueries");

        // Other
        ANNOTATION_MAP.put("Lorg/springframework/transaction/annotation/Transactional;", "Transactional");
        ANNOTATION_MAP.put("Lorg/springframework/web/bind/annotation/ResponseBody;", "ResponseBody");
        ANNOTATION_MAP.put("Lorg/springframework/web/bind/annotation/CrossOrigin;", "CrossOrigin");
        ANNOTATION_MAP.put("Lorg/springframework/web/bind/annotation/ResponseStatus;", "ResponseStatus");
        ANNOTATION_MAP.put("Lorg/springframework/web/bind/annotation/ExceptionHandler;", "ExceptionHandler");
        ANNOTATION_MAP.put("Lorg/springframework/scheduling/annotation/Async;", "Async");
        ANNOTATION_MAP.put("Lorg/springframework/cache/annotation/Cacheable;", "Cacheable");
        ANNOTATION_MAP.put("Lorg/springframework/cache/annotation/CacheEvict;", "CacheEvict");
        ANNOTATION_MAP.put("Lorg/springframework/cache/annotation/CachePut;", "CachePut");
        ANNOTATION_MAP.put("Lorg/springframework/cache/annotation/Caching;", "Caching");
        ANNOTATION_MAP.put("Lorg/springframework/validation/annotation/Validated;", "Validated");
        ANNOTATION_MAP.put("Ljakarta/validation/Valid;", "Valid");
        ANNOTATION_MAP.put("Ljavax/validation/Valid;", "Valid");
    }

    public static String resolve(String descriptor) {
        return ANNOTATION_MAP.get(descriptor);
    }

    public static String httpMethodFor(String annotationName) {
        return HTTP_METHODS.get(annotationName);
    }
}
