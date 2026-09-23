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
        // JAX-RS (annotation name IS the HTTP verb)
        HTTP_METHODS.put("GET", "GET");
        HTTP_METHODS.put("POST", "POST");
        HTTP_METHODS.put("PUT", "PUT");
        HTTP_METHODS.put("DELETE", "DELETE");
        HTTP_METHODS.put("PATCH", "PATCH");
        HTTP_METHODS.put("HEAD", "HEAD");
        HTTP_METHODS.put("OPTIONS", "OPTIONS");
        // GraphQL (Spring for GraphQL + Netflix DGS)
        HTTP_METHODS.put("QueryMapping", "QUERY");
        HTTP_METHODS.put("MutationMapping", "MUTATION");
        HTTP_METHODS.put("SubscriptionMapping", "SUBSCRIPTION");
        HTTP_METHODS.put("SchemaMapping", "QUERY");
        HTTP_METHODS.put("DgsQuery", "QUERY");
        HTTP_METHODS.put("DgsMutation", "MUTATION");
        HTTP_METHODS.put("DgsSubscription", "SUBSCRIPTION");
        // Event / messaging entry points
        HTTP_METHODS.put("EventListener", "EVENT");
        HTTP_METHODS.put("TransactionalEventListener", "EVENT");
        HTTP_METHODS.put("JmsListener", "JMS");
        HTTP_METHODS.put("SqsListener", "SQS");
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
        ANNOTATION_MAP.put("Lorg/springframework/transaction/event/TransactionalEventListener;", "TransactionalEventListener");
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

        // Event-driven entry points
        ANNOTATION_MAP.put("Lorg/springframework/context/event/EventListener;", "EventListener");
        ANNOTATION_MAP.put("Lorg/springframework/jms/annotation/JmsListener;", "JmsListener");
        ANNOTATION_MAP.put("Lio/awspring/cloud/sqs/annotation/SqsListener;", "SqsListener");
        ANNOTATION_MAP.put("Lcom/amazonaws/services/sqs/AmazonSQS;", "SqsListener");

        // AOP / interception. These are resolved here so the advice names are canonical rather
        // than whatever simple name the annotation's own package happens to yield — an @Around
        // from a non-AspectJ package would otherwise be indistinguishable from AspectJ's.
        // AdviceDetector matches on exactly these names.
        ANNOTATION_MAP.put("Lorg/aspectj/lang/annotation/Aspect;", "Aspect");
        ANNOTATION_MAP.put("Lorg/aspectj/lang/annotation/Around;", "Around");
        ANNOTATION_MAP.put("Lorg/aspectj/lang/annotation/Before;", "Before");
        ANNOTATION_MAP.put("Lorg/aspectj/lang/annotation/After;", "After");
        ANNOTATION_MAP.put("Lorg/aspectj/lang/annotation/AfterReturning;", "AfterReturning");
        ANNOTATION_MAP.put("Lorg/aspectj/lang/annotation/AfterThrowing;", "AfterThrowing");
        ANNOTATION_MAP.put("Lorg/aspectj/lang/annotation/Pointcut;", "Pointcut");
        ANNOTATION_MAP.put("Lorg/aspectj/lang/annotation/DeclareParents;", "DeclareParents");
        ANNOTATION_MAP.put("Lorg/springframework/context/annotation/EnableAspectJAutoProxy;",
                "EnableAspectJAutoProxy");
        ANNOTATION_MAP.put("Lorg/springframework/core/annotation/Order;", "Order");

        // Security
        ANNOTATION_MAP.put("Lorg/springframework/security/access/prepost/PreAuthorize;", "PreAuthorize");
        ANNOTATION_MAP.put("Lorg/springframework/security/access/prepost/PostAuthorize;", "PostAuthorize");
        ANNOTATION_MAP.put("Lorg/springframework/security/access/annotation/Secured;", "Secured");
        ANNOTATION_MAP.put("Ljavax/annotation/security/RolesAllowed;", "RolesAllowed");
        ANNOTATION_MAP.put("Ljakarta/annotation/security/RolesAllowed;", "RolesAllowed");
        ANNOTATION_MAP.put("Ljavax/annotation/security/DenyAll;", "DenyAll");
        ANNOTATION_MAP.put("Ljakarta/annotation/security/DenyAll;", "DenyAll");
        ANNOTATION_MAP.put("Ljavax/annotation/security/PermitAll;", "PermitAll");
        ANNOTATION_MAP.put("Ljakarta/annotation/security/PermitAll;", "PermitAll");

        // GraphQL (Spring for GraphQL)
        ANNOTATION_MAP.put("Lorg/springframework/graphql/data/method/annotation/QueryMapping;", "QueryMapping");
        ANNOTATION_MAP.put("Lorg/springframework/graphql/data/method/annotation/MutationMapping;", "MutationMapping");
        ANNOTATION_MAP.put("Lorg/springframework/graphql/data/method/annotation/SubscriptionMapping;", "SubscriptionMapping");
        ANNOTATION_MAP.put("Lorg/springframework/graphql/data/method/annotation/SchemaMapping;", "SchemaMapping");
        // GraphQL DGS (Netflix)
        ANNOTATION_MAP.put("Lcom/netflix/graphql/dgs/DgsQuery;", "QueryMapping");
        ANNOTATION_MAP.put("Lcom/netflix/graphql/dgs/DgsMutation;", "MutationMapping");
        ANNOTATION_MAP.put("Lcom/netflix/graphql/dgs/DgsSubscription;", "SubscriptionMapping");
        ANNOTATION_MAP.put("Lcom/netflix/graphql/dgs/DgsComponent;", "DgsComponent");

        // JAX-RS (javax.ws.rs — Java EE / pre-Jakarta)
        ANNOTATION_MAP.put("Ljavax/ws/rs/GET;", "GET");
        ANNOTATION_MAP.put("Ljavax/ws/rs/POST;", "POST");
        ANNOTATION_MAP.put("Ljavax/ws/rs/PUT;", "PUT");
        ANNOTATION_MAP.put("Ljavax/ws/rs/DELETE;", "DELETE");
        ANNOTATION_MAP.put("Ljavax/ws/rs/PATCH;", "PATCH");
        ANNOTATION_MAP.put("Ljavax/ws/rs/HEAD;", "HEAD");
        ANNOTATION_MAP.put("Ljavax/ws/rs/OPTIONS;", "OPTIONS");
        ANNOTATION_MAP.put("Ljavax/ws/rs/Path;", "Path");
        ANNOTATION_MAP.put("Ljavax/ws/rs/Produces;", "Produces");
        ANNOTATION_MAP.put("Ljavax/ws/rs/Consumes;", "Consumes");
        // JAX-RS (jakarta.ws.rs — Jakarta EE 9+)
        ANNOTATION_MAP.put("Ljakarta/ws/rs/GET;", "GET");
        ANNOTATION_MAP.put("Ljakarta/ws/rs/POST;", "POST");
        ANNOTATION_MAP.put("Ljakarta/ws/rs/PUT;", "PUT");
        ANNOTATION_MAP.put("Ljakarta/ws/rs/DELETE;", "DELETE");
        ANNOTATION_MAP.put("Ljakarta/ws/rs/PATCH;", "PATCH");
        ANNOTATION_MAP.put("Ljakarta/ws/rs/HEAD;", "HEAD");
        ANNOTATION_MAP.put("Ljakarta/ws/rs/OPTIONS;", "OPTIONS");
        ANNOTATION_MAP.put("Ljakarta/ws/rs/Path;", "Path");
        ANNOTATION_MAP.put("Ljakarta/ws/rs/Produces;", "Produces");
        ANNOTATION_MAP.put("Ljakarta/ws/rs/Consumes;", "Consumes");
    }

    public static String resolve(String descriptor) {
        return ANNOTATION_MAP.get(descriptor);
    }

    public static String httpMethodFor(String annotationName) {
        return HTTP_METHODS.get(annotationName);
    }
}
