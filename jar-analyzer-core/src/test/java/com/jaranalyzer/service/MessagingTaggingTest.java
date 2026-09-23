package com.jaranalyzer.service;

import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.model.AnnotationInfo;
import com.jaranalyzer.model.ClassInfo;
import com.jaranalyzer.model.MethodInfo;
import com.jaranalyzer.model.ParameterInfo;
import com.jaranalyzer.service.CallGraphIndex.EventListenerRef;
import com.jaranalyzer.service.CallGraphIndex.IndexedClass;
import com.jaranalyzer.service.CallGraphIndex.IndexedMethod;
import com.jaranalyzer.service.CallGraphIndex.InvocationRef;
import com.jaranalyzer.service.CallGraphIndex.ResolutionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Category F — Messaging Tests (15 tests)
 *
 * Covers:
 *  F1-F2  : @KafkaListener → synthetic endpoint / topic extraction
 *  F3     : @RabbitListener → synthetic endpoint
 *  F4-F5  : ApplicationEventPublisher.publishEvent → @EventListener methods
 *  F6     : @TransactionalEventListener → callType=ASYNC_EVENT
 *  F7     : KafkaTemplate.send → MESSAGING_PRODUCER leaf
 *  F8-F15 : Additional messaging coverage (gaps marked @Disabled)
 */
@DisplayName("Category F — Messaging tagging")
class MessagingTaggingTest {

    // ── Reflection helpers ────────────────────────────────────────────────────

    private ResolutionContext getContext(CallTreeBuilder builder) throws Exception {
        Field f = CallTreeBuilder.class.getDeclaredField("ctx");
        f.setAccessible(true);
        return (ResolutionContext) f.get(builder);
    }

    private CallNode invokeCreateMessagingProducerLeaf(CallTreeBuilder builder,
                                                        InvocationRef call,
                                                        IndexedMethod callerMethod) throws Exception {
        Method m = CallTreeBuilder.class.getDeclaredMethod(
                "createMessagingProducerLeaf", InvocationRef.class, IndexedMethod.class);
        m.setAccessible(true);
        return (CallNode) m.invoke(builder, call, callerMethod);
    }

    // ── Stub builders ─────────────────────────────────────────────────────────

    private static ClassInfo classInfo(String fqn, String simpleName, String stereotype) {
        ClassInfo ci = new ClassInfo();
        ci.setFullyQualifiedName(fqn);
        ci.setSimpleName(simpleName);
        ci.setStereotype(stereotype);
        return ci;
    }

    private static AnnotationInfo annotation(String name, Map<String, Object> attrs) {
        AnnotationInfo ai = new AnnotationInfo();
        ai.setName(name);
        ai.getAttributes().putAll(attrs);
        return ai;
    }

    private static AnnotationInfo annotation(String name) {
        return annotation(name, Map.of());
    }

    private static IndexedClass minimalClass(String fqn, String simpleName, String stereotype) {
        ClassInfo ci = classInfo(fqn, simpleName, stereotype);
        return new IndexedClass(ci, Pattern.compile("(?!x)x"));
    }

    private static IndexedMethod minimalMethod(String name, String returnType,
                                                List<InvocationRef> invocations) {
        MethodInfo mi = new MethodInfo();
        mi.setName(name);
        mi.setDescriptor("()V");
        mi.setReturnType(returnType);
        IndexedMethod im = new IndexedMethod(mi);
        im.invocations.clear();
        im.invocations.addAll(invocations);
        return im;
    }

    private static IndexedMethod minimalMethod(String name, List<InvocationRef> invocations) {
        return minimalMethod(name, "void", invocations);
    }

    private static InvocationRef invRef(String ownerClass, String methodName,
                                         List<String> stringArgs) {
        return new InvocationRef(ownerClass, methodName, "()V", "void",
                stringArgs, 182, null);
    }

    // ── CallTreeBuilder factory ───────────────────────────────────────────────

    private CallTreeBuilder builder;
    private ResolutionContext ctx;

    @BeforeEach
    void setUp() throws Exception {
        DomainConfigLoader loader = new DomainConfigLoader();
        MongoMethodDetector mongoDetector = new MongoMethodDetector(loader);
        AggregationDetector aggregationDetector = new AggregationDetector(loader);
        JpaMethodDetector jpaDetector = new JpaMethodDetector(loader);
        MyBatisMethodDetector myBatisDetector = new MyBatisMethodDetector();
        RedisMethodDetector redisDetector = new RedisMethodDetector();
        CassandraMethodDetector cassandraDetector = new CassandraMethodDetector();
        ElasticsearchMethodDetector esDetector = new ElasticsearchMethodDetector();
        // CollectionResolver gained a CollectionLifecycleDetector collaborator with the
        // lifecycle-workflow feature; it is unused by the messaging-tagging paths under test.
        CollectionLifecycleDetector lifecycleDetector = new CollectionLifecycleDetector(loader);
        CollectionResolver resolver = new CollectionResolver(loader, mongoDetector,
                aggregationDetector, jpaDetector, myBatisDetector,
                redisDetector, cassandraDetector, esDetector, lifecycleDetector);
        builder = new CallTreeBuilder(loader, resolver, 30, 30, 2000, 100000);
        ctx = builder.createContext();
    }

    // ── F1: @KafkaListener → synthetic endpoint ───────────────────────────────

    @Test
    @DisplayName("F1 @KafkaListener(topics='orders') → synthetic KAFKA endpoint registered")
    void kafkaListener_singleTopic_registeredAsSyntheticEndpoint() {
        ClassInfo ci = classInfo("com.example.OrderConsumer", "OrderConsumer", "COMPONENT");

        AnnotationInfo kafkaAnn = annotation("KafkaListener",
                Map.of("topics", "orders"));
        MethodInfo mi = new MethodInfo();
        mi.setName("onOrder");
        mi.setDescriptor("(Ljava/lang/Object;)V");
        mi.setReturnType("void");
        mi.getAnnotations().add(kafkaAnn);
        ci.getMethods().add(mi);

        builder.indexClass(ci);

        boolean found = ctx.endpoints.stream()
                .anyMatch(ep -> "KAFKA".equals(ep.httpMethod)
                        && ep.controllerFqn.equals("com.example.OrderConsumer"));
        assertTrue(found, "@KafkaListener method must register a synthetic KAFKA endpoint");
    }

    @Test
    @DisplayName("F1b @KafkaListener topic name captured as synthetic path")
    void kafkaListener_topicNameCapturedAsPath() {
        ClassInfo ci = classInfo("com.example.OrderConsumer", "OrderConsumer", "COMPONENT");

        AnnotationInfo kafkaAnn = annotation("KafkaListener",
                Map.of("topics", "orders"));
        MethodInfo mi = new MethodInfo();
        mi.setName("onOrder");
        mi.setDescriptor("(Ljava/lang/Object;)V");
        mi.setReturnType("void");
        mi.getAnnotations().add(kafkaAnn);
        ci.getMethods().add(mi);

        builder.indexClass(ci);

        boolean found = ctx.endpoints.stream()
                .anyMatch(ep -> "KAFKA".equals(ep.httpMethod) && "orders".equals(ep.path));
        assertTrue(found, "Topic name 'orders' must be captured as the synthetic endpoint path");
    }

    // ── F2: @KafkaListener with multiple topics ────────────────────────────────

    @Test
    @DisplayName("F2 @KafkaListener(topics={orders,events}) → first topic captured as path")
    void kafkaListener_multipleTopics_firstTopicCaptured() {
        ClassInfo ci = classInfo("com.example.MultiConsumer", "MultiConsumer", "COMPONENT");

        AnnotationInfo kafkaAnn = annotation("KafkaListener",
                Map.of("topics", List.of("orders", "events")));
        MethodInfo mi = new MethodInfo();
        mi.setName("onMessage");
        mi.setDescriptor("(Ljava/lang/Object;)V");
        mi.setReturnType("void");
        mi.getAnnotations().add(kafkaAnn);
        ci.getMethods().add(mi);

        builder.indexClass(ci);

        boolean found = ctx.endpoints.stream()
                .anyMatch(ep -> "KAFKA".equals(ep.httpMethod)
                        && ep.controllerFqn.equals("com.example.MultiConsumer"));
        assertTrue(found,
                "@KafkaListener with multiple topics must still register at least one synthetic endpoint");
    }

    // ── F3: @RabbitListener ────────────────────────────────────────────────────

    @Test
    @DisplayName("F3 @RabbitListener(queues='payment-queue') → synthetic AMQP endpoint")
    void rabbitListener_registeredAsSyntheticEndpoint() {
        ClassInfo ci = classInfo("com.example.PaymentConsumer", "PaymentConsumer", "COMPONENT");

        AnnotationInfo rabbitAnn = annotation("RabbitListener",
                Map.of("queues", "payment-queue"));
        MethodInfo mi = new MethodInfo();
        mi.setName("onPayment");
        mi.setDescriptor("(Ljava/lang/Object;)V");
        mi.setReturnType("void");
        mi.getAnnotations().add(rabbitAnn);
        ci.getMethods().add(mi);

        builder.indexClass(ci);

        boolean found = ctx.endpoints.stream()
                .anyMatch(ep -> "AMQP".equals(ep.httpMethod)
                        && ep.controllerFqn.equals("com.example.PaymentConsumer")
                        && "payment-queue".equals(ep.path));
        assertTrue(found, "@RabbitListener must register a synthetic AMQP endpoint with queue name as path");
    }

    // ── F4: publishEvent → @EventListener resolution ─────────────────────────

    @Test
    @DisplayName("F4 publishEvent(new OrderPlaced()) → connects to @EventListener(OrderPlaced) methods")
    void publishEvent_connectsToEventListenerMethods() throws Exception {
        // Register an @EventListener for OrderPlaced
        ClassInfo listenerCi = classInfo("com.example.OrderEventHandler",
                "OrderEventHandler", "COMPONENT");
        AnnotationInfo evtAnn = annotation("EventListener",
                Map.of("value", "com.example.event.OrderPlaced"));
        MethodInfo listenerMi = new MethodInfo();
        listenerMi.setName("handleOrderPlaced");
        listenerMi.setDescriptor("(Lcom/example/event/OrderPlaced;)V");
        listenerMi.setReturnType("void");
        listenerMi.getAnnotations().add(evtAnn);
        listenerCi.getMethods().add(listenerMi);

        builder.indexClass(listenerCi);

        // The eventListenerMap must contain the handler
        boolean hasListener = ctx.eventListenerMap.containsKey("com.example.event.OrderPlaced")
                && !ctx.eventListenerMap.get("com.example.event.OrderPlaced").isEmpty();
        assertTrue(hasListener,
                "After indexing @EventListener(OrderPlaced), eventListenerMap must contain the handler");
    }

    // ── F5: @EventListener method → callType detection ────────────────────────

    @Test
    @DisplayName("F5 @EventListener(OrderPlaced.class) method → registered in eventListenerMap")
    void eventListenerAnnotation_methodRegisteredInMap() {
        ClassInfo listenerCi = classInfo("com.example.NotificationService",
                "NotificationService", "SERVICE");

        // Simulate @EventListener with the event type in the value attribute
        AnnotationInfo evtAnn = annotation("EventListener",
                Map.of("value", "com.example.event.OrderPlaced"));

        MethodInfo mi = new MethodInfo();
        mi.setName("sendNotification");
        mi.setDescriptor("(Lcom/example/event/OrderPlaced;)V");
        mi.setReturnType("void");
        mi.getAnnotations().add(evtAnn);
        listenerCi.getMethods().add(mi);

        builder.indexClass(listenerCi);

        List<EventListenerRef> listeners = ctx.eventListenerMap
                .getOrDefault("com.example.event.OrderPlaced", List.of());
        assertFalse(listeners.isEmpty(),
                "@EventListener method must appear in eventListenerMap under the event type key");
        assertTrue(listeners.stream().anyMatch(ref -> "sendNotification".equals(ref.method().name)),
                "The registered listener must match the annotated method name");
    }

    // ── F6: @TransactionalEventListener ───────────────────────────────────────

    @Test
    @DisplayName("F6 @TransactionalEventListener → registered with isTransactional=true")
    void transactionalEventListener_registeredAsTransactional() {
        ClassInfo ci = classInfo("com.example.TxEventHandler", "TxEventHandler", "COMPONENT");

        AnnotationInfo txEvtAnn = annotation("TransactionalEventListener",
                Map.of("value", "com.example.event.PaymentCompleted"));

        MethodInfo mi = new MethodInfo();
        mi.setName("onPaymentCompleted");
        mi.setDescriptor("(Lcom/example/event/PaymentCompleted;)V");
        mi.setReturnType("void");
        mi.getAnnotations().add(txEvtAnn);
        ci.getMethods().add(mi);

        builder.indexClass(ci);

        List<EventListenerRef> listeners = ctx.eventListenerMap
                .getOrDefault("com.example.event.PaymentCompleted", List.of());
        assertFalse(listeners.isEmpty(), "@TransactionalEventListener must be registered in eventListenerMap");
        assertTrue(listeners.stream().anyMatch(EventListenerRef::isTransactional),
                "@TransactionalEventListener must have isTransactional=true");
    }

    // ── F7: KafkaTemplate.send → MESSAGING_PRODUCER leaf ─────────────────────

    @Disabled("CallTreeBuilder.createMessagingProducerLeaf no longer exists — the messaging-producer-leaf capability was removed from production. Reached by reflection, so the compiler could not flag it and it failed only at runtime. Re-enable if that detection is rebuilt; deleting would lose the specification of what it did.")

    @Test
    @DisplayName("F7 KafkaTemplate.send(topic, value) → MESSAGING_PRODUCER leaf node, operationType=WRITE")
    void kafkaTemplateSend_producesMessagingProducerLeaf() throws Exception {
        InvocationRef call = invRef(
                "org.springframework.kafka.core.KafkaTemplate",
                "send",
                List.of("order-events")
        );
        IndexedMethod caller = minimalMethod("publishOrder", List.of(call));
        CallNode node = invokeCreateMessagingProducerLeaf(builder, call, caller);

        assertEquals("MESSAGING_PRODUCER", node.getCallType(),
                "KafkaTemplate.send must produce MESSAGING_PRODUCER callType");
        assertEquals("WRITE", node.getOperationType(),
                "Sending a message is a WRITE operation");
    }

    // ── F8: RabbitTemplate.convertAndSend → MESSAGING_PRODUCER ───────────────

    @Disabled("CallTreeBuilder.createMessagingProducerLeaf no longer exists — the messaging-producer-leaf capability was removed from production. Reached by reflection, so the compiler could not flag it and it failed only at runtime. Re-enable if that detection is rebuilt; deleting would lose the specification of what it did.")

    @Test
    @DisplayName("F8 RabbitTemplate.convertAndSend → MESSAGING_PRODUCER leaf")
    void rabbitTemplateConvertAndSend_producesMessagingProducerLeaf() throws Exception {
        InvocationRef call = invRef(
                "org.springframework.amqp.rabbit.core.RabbitTemplate",
                "convertAndSend",
                List.of("payment-queue")
        );
        IndexedMethod caller = minimalMethod("sendPaymentEvent", List.of(call));
        CallNode node = invokeCreateMessagingProducerLeaf(builder, call, caller);

        assertEquals("MESSAGING_PRODUCER", node.getCallType());
        assertEquals("WRITE", node.getOperationType());
    }

    // ── F9: Topic/queue name captured in stringLiterals ───────────────────────

    @Disabled("CallTreeBuilder.createMessagingProducerLeaf no longer exists — the messaging-producer-leaf capability was removed from production. Reached by reflection, so the compiler could not flag it and it failed only at runtime. Re-enable if that detection is rebuilt; deleting would lose the specification of what it did.")

    @Test
    @DisplayName("F9 MESSAGING_PRODUCER leaf captures topic name from stringArgs in stringLiterals")
    void messagingProducer_topicCapturedInStringLiterals() throws Exception {
        String topic = "order-events";
        InvocationRef call = invRef(
                "org.springframework.kafka.core.KafkaTemplate",
                "send",
                List.of(topic)
        );
        IndexedMethod caller = minimalMethod("publishOrder", List.of(call));
        CallNode node = invokeCreateMessagingProducerLeaf(builder, call, caller);

        assertNotNull(node.getStringLiterals(), "stringLiterals must not be null");
        assertTrue(node.getStringLiterals().contains(topic),
                "Topic name must be captured in stringLiterals");
    }

    // ── F10: MESSAGING_PRODUCER has crossModule=true ──────────────────────────

    @Disabled("CallTreeBuilder.createMessagingProducerLeaf no longer exists — the messaging-producer-leaf capability was removed from production. Reached by reflection, so the compiler could not flag it and it failed only at runtime. Re-enable if that detection is rebuilt; deleting would lose the specification of what it did.")

    @Test
    @DisplayName("F10 MESSAGING_PRODUCER leaf has crossModule=true")
    void messagingProducer_hasCrossModuleTrue() throws Exception {
        InvocationRef call = invRef(
                "org.springframework.kafka.core.KafkaTemplate",
                "send",
                List.of("events")
        );
        IndexedMethod caller = minimalMethod("publish", List.of(call));
        CallNode node = invokeCreateMessagingProducerLeaf(builder, call, caller);

        assertTrue(node.isCrossModule(), "MESSAGING_PRODUCER leaf must have crossModule=true");
    }

    // ── F11: Broker type tag in annotations ───────────────────────────────────

    @Disabled("CallTreeBuilder.createMessagingProducerLeaf no longer exists — the messaging-producer-leaf capability was removed from production. Reached by reflection, so the compiler could not flag it and it failed only at runtime. Re-enable if that detection is rebuilt; deleting would lose the specification of what it did.")

    @Test
    @DisplayName("F11 KafkaTemplate leaf annotated with @KafkaSend broker tag")
    void kafkaProducer_annotatedWithKafkaSendTag() throws Exception {
        InvocationRef call = invRef(
                "org.springframework.kafka.core.KafkaTemplate",
                "send",
                List.of("my-topic")
        );
        IndexedMethod caller = minimalMethod("doPublish", List.of(call));
        CallNode node = invokeCreateMessagingProducerLeaf(builder, call, caller);

        assertTrue(node.getAnnotations() != null
                        && node.getAnnotations().stream().anyMatch(a -> a.contains("Kafka")),
                "KafkaTemplate leaf must carry a Kafka-related broker annotation tag");
    }

    // ── F12: RabbitTemplate receive → operationType=READ ─────────────────────

    @Disabled("CallTreeBuilder.createMessagingProducerLeaf no longer exists — the messaging-producer-leaf capability was removed from production. Reached by reflection, so the compiler could not flag it and it failed only at runtime. Re-enable if that detection is rebuilt; deleting would lose the specification of what it did.")

    @Test
    @DisplayName("F12 RabbitTemplate.receive() → MESSAGING_PRODUCER leaf with operationType=READ")
    void rabbitTemplateReceive_producesReadMessagingLeaf() throws Exception {
        InvocationRef call = invRef(
                "org.springframework.amqp.rabbit.core.RabbitTemplate",
                "receive",
                List.of("queue-name")
        );
        IndexedMethod caller = minimalMethod("consumeMessage", List.of(call));
        CallNode node = invokeCreateMessagingProducerLeaf(builder, call, caller);

        assertEquals("MESSAGING_PRODUCER", node.getCallType());
        assertEquals("READ", node.getOperationType(),
                "receive operation is a READ (consuming) from the queue");
    }

    // ── F13: @EventListener inferred from first parameter type ───────────────

    @Test
    @DisplayName("F13 @EventListener without explicit type → inferred from first parameter type")
    void eventListener_withoutExplicitType_inferredFromParameter() {
        ClassInfo ci = classInfo("com.example.AuditHandler", "AuditHandler", "COMPONENT");

        // @EventListener with no value attribute — type inferred from parameter
        AnnotationInfo evtAnn = annotation("EventListener");

        ParameterInfo param = new ParameterInfo();
        param.setName("event");
        param.setType("com.example.event.AuditEvent");

        MethodInfo mi = new MethodInfo();
        mi.setName("onAudit");
        mi.setDescriptor("(Lcom/example/event/AuditEvent;)V");
        mi.setReturnType("void");
        mi.getAnnotations().add(evtAnn);
        mi.getParameters().add(param);
        ci.getMethods().add(mi);

        builder.indexClass(ci);

        List<EventListenerRef> listeners = ctx.eventListenerMap
                .getOrDefault("com.example.event.AuditEvent", List.of());
        assertFalse(listeners.isEmpty(),
                "@EventListener without explicit type must infer from first parameter type");
    }

    // ── F14: StreamBridge.send → MESSAGING_PRODUCER ───────────────────────────

    @Disabled("CallTreeBuilder.createMessagingProducerLeaf no longer exists — the messaging-producer-leaf capability was removed from production. Reached by reflection, so the compiler could not flag it and it failed only at runtime. Re-enable if that detection is rebuilt; deleting would lose the specification of what it did.")

    @Test
    @DisplayName("F14 StreamBridge.send → MESSAGING_PRODUCER leaf")
    void streamBridgeSend_producesMessagingProducerLeaf() throws Exception {
        InvocationRef call = invRef(
                "org.springframework.cloud.stream.function.StreamBridge",
                "send",
                List.of("output-channel")
        );
        IndexedMethod caller = minimalMethod("publishViaStream", List.of(call));
        CallNode node = invokeCreateMessagingProducerLeaf(builder, call, caller);

        assertEquals("MESSAGING_PRODUCER", node.getCallType(),
                "StreamBridge.send must be MESSAGING_PRODUCER");
    }

    // ── F15: Kafka producer with topic from caller string literals ────────────

    @Disabled("CallTreeBuilder.createMessagingProducerLeaf no longer exists — the messaging-producer-leaf capability was removed from production. Reached by reflection, so the compiler could not flag it and it failed only at runtime. Re-enable if that detection is rebuilt; deleting would lose the specification of what it did.")

    @Test
    @DisplayName("F15 KafkaTemplate.sendDefault — topic from caller stringLiterals as fallback")
    void kafkaTemplate_topicFromCallerStringLiterals_fallback() throws Exception {
        String topic = "fallback-topic";
        InvocationRef call = invRef(
                "org.springframework.kafka.core.KafkaTemplate",
                "sendDefault",
                List.of() // no direct string args
        );
        // Build method with string literals via reflection
        MethodInfo mi = new MethodInfo();
        mi.setName("publish");
        mi.setDescriptor("()V");
        mi.setReturnType("void");
        // Add string literal to the method's string literals list
        mi.getStringLiterals().add(topic);

        IndexedMethod caller = new IndexedMethod(mi);
        caller.invocations.add(call);

        CallNode node = invokeCreateMessagingProducerLeaf(builder, call, caller);

        assertEquals("MESSAGING_PRODUCER", node.getCallType());
        // Topic should be captured from caller's string literals as fallback
        assertTrue(
                node.getStringLiterals() != null && node.getStringLiterals().contains(topic),
                "Topic from caller stringLiterals must be captured as fallback in the messaging node"
        );
    }
}
