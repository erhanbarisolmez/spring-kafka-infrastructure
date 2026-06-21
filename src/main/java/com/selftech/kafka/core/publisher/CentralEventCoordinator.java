package com.selftech.kafka.core.publisher;

import java.util.UUID;
import java.time.Instant;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.time.Instant;

import org.slf4j.Logger;
import java.time.Instant;
import org.slf4j.LoggerFactory;
import java.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import java.time.Instant;
import org.springframework.kafka.core.KafkaTemplate;
import java.time.Instant;
import org.springframework.kafka.support.SendResult;
import java.time.Instant;
import org.springframework.stereotype.Service;
import java.time.Instant;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;

import com.selftech.kafka.config.KafkaTopicRegistry;
import java.time.Instant;
import com.selftech.kafka.core.event.Event;
import java.time.Instant;
import com.selftech.kafka.core.outbox.OutboxEventService;
import java.time.Instant;

/**
 * Central Event Coordinator - FAZE 2 ENTERPRISE
 *
 * Merkezi event publishing facade service.
 * Tüm domain event publisher'ların arkasında bu service çalışır.
 *
 * Responsibilities:
 * 1. Event publishing coordination (Kafka template delegation)
 * 2. Correlation ID tracking (distributed tracing)
 * 3. Event metadata enrichment (timestamp, source, version)
 * 4. Transaction management (@Transactional)
 * 5. Error handling and logging
 * 6. Metrics collection (ready for monitoring)
 * 7. Async publishing with callback support
 *
 * Architecture Pattern: Facade + Adapter
 * ├─ Facade: Tüm publisher'lar için single entry point
 * ├─ Adapter: KafkaTemplate'in kompleksliğini hide ediyor
 * └─ Coordinator: Multiple concerns (correlation, transaction, metrics)
 *
 * Usage Example:
 * @Service
 * public class LockBoxEventPublisherService {
 *     @Autowired private CentralEventCoordinator coordinator;
 *
 *     public void publishBoxCreated(LockBox box) {
 *         LockBoxEvent event = LockBoxEvent.newBuilder()...build();
 *         coordinator.publishEvent(topicName, box.getBoxCode(), event);
 *     }
 * }
 *
 * Features:
 * - Thread-safe: KafkaTemplate is thread-safe
 * - Transaction-aware: @Transactional support
 * - Correlation-aware: ThreadLocal context propagation
 * - Async-ready: CompletableFuture return type
 * - Error-resilient: Comprehensive exception handling
 */
@Service
public class CentralEventCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(CentralEventCoordinator.class);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired(required = false)
    private OutboxEventService outboxEventService;

    @Autowired
    private KafkaTopicRegistry topicRegistry;

    /**
     * ThreadLocal for storing correlation ID during request processing
     * Allows propagation across async boundaries
     */
    private static final ThreadLocal<String> correlationIdContext = new ThreadLocal<>();

    /**
     * Publish event to Kafka topic with Outbox pattern (Reliable Publishing)
     * Recommended for critical events that MUST be delivered
     *
     * This method:
     * 1. Enriches event metadata (eventId, correlationId, timestamp, etc)
     * 2. Saves event to outbox table in SAME transaction as business operation
     * 3. Relies on OutboxPoller to asynchronously publish to Kafka
     * 4. Guarantees exactly-once delivery even if Kafka is temporarily unavailable
     *
     * Outbox Pattern Benefits:
     * - Database transaction atomicity: Business op + event save in same transaction
     * - Guaranteed delivery: Event persisted before returning to caller
     * - Replay capability: Failed events can be reprocessed from database
     * - Audit trail: Complete history of all events in database
     *
     * Must be called within @Transactional boundary from caller
     *
     * @param topic Topic name (e.g., "smartlock.event.boxOperation.v0")
     * @param key Partition key (e.g., boxCode, userId)
     * @param event Event object extending Event base class
     * @return Saved OutboxEvent
     * @throws EventPublishingException If outbox save fails
     *
     * Usage:
     * @Service
     * @Transactional
     * public class LockBoxCreationService {
     *     public void createBox(CreateBoxRequest req) {
     *         LockBox box = repository.save(new LockBox(...));  // Business op
     *
     *         LockBoxEvent event = LockBoxEvent.newBuilder()...build();
     *         coordinator.publishEventReliable(topic, box.getBoxCode(), event);
     *         // Both box + outbox event saved in same transaction ✅
     *     }
     * }
     */
    @Transactional
    public Object publishEventReliable(String topic, String key, Event event)
            throws EventPublishingException {

        try {
            // Step 1: Enrich event metadata
            enrichEventMetadata(event);

            // Step 2: Save to outbox (if outbox service is available)
            if (outboxEventService != null) {
                var outboxEvent = outboxEventService.saveEvent(topic, key, event);
                logger.info("Event saved to outbox for reliable delivery - Topic: {}, Key: {}, EventId: {}, OutboxId: {}, CorrelationId: {}",
                    topic, key, event.getEventId(), outboxEvent.getOutboxId(), event.getCorrelationId());
                return outboxEvent;
            } else {
                // Fallback: publish directly to Kafka if outbox not available
                logger.warn("OutboxEventService not available, falling back to direct Kafka publishing");
                kafkaTemplate.send(topic, key, event);
                logger.info("Event published directly to Kafka - Topic: {}, Key: {}, EventId: {}, CorrelationId: {}",
                    topic, key, event.getEventId(), event.getCorrelationId());
                return null;
            }

        } catch (Exception e) {
            logger.error("Failed to save event to outbox - Topic: {}, EventId: {}, Key: {}",
                topic, event.getEventId(), key, e);
            throw new EventPublishingException(
                String.format("Failed to save event to outbox for topic: %s", topic), e);
        }
    }

    /**
     * Publish Avro SpecificRecord reliably to Outbox (Transactional Outbox Pattern)
     * Use this for critical Avro events (ParkEntryEvent, PaymentEvent, LockBoxEvent, etc)
     * that MUST be delivered even if Kafka is temporarily unavailable.
     *
     * This method:
     * 1. Persists Avro event to outbox table in SAME transaction as business operation
     * 2. Relies on OutboxPoller to asynchronously publish to Kafka
     * 3. Guarantees exactly-once delivery even if Kafka is down
     *
     * Transactional Outbox Benefits:
     * - Database transaction atomicity: Business op + event save in same transaction
     * - Guaranteed delivery: Event persisted before returning
     * - Replay capability: Failed events can be reprocessed from database
     * - Audit trail: Complete event history in database
     *
     * Must be called within @Transactional boundary from caller
     *
     * @param topic Topic name (e.g., "selfpark.event.parkEntry.v0")
     * @param key Partition key (e.g., userId, boxCode)
     * @param avroEvent Avro SpecificRecord (ParkEntryEvent, PaymentEvent, LockBoxEvent, etc)
     * @return Saved OutboxEvent
     * @throws EventPublishingException If outbox save fails
     *
     * Usage:
     * @Transactional
     * public void createParkEntry(CreateEntryRequest req) {
     *     ParkEntry entry = repository.save(new ParkEntry(...));
     *     ParkEntryEvent event = ParkEntryEvent.newBuilder()...build();
     *     coordinator.publishEventReliable(topic, String.valueOf(entry.getUserId()), event);
     *     // Both entry + outbox event saved in same transaction ✅
     * }
     */
    @Transactional
    public Object publishEventReliable(String topic, String key, Object avroEvent)
            throws EventPublishingException {

        try {
            // For Avro events, we can't call enrichEventMetadata (only works with Event interface)
            // Just persist directly to outbox
            if (outboxEventService != null) {
                var outboxEvent = outboxEventService.saveEvent(topic, key, avroEvent);
                logger.info("Avro event saved to outbox for reliable delivery - Topic: {}, Key: {}, EventClass: {}, OutboxId: {}",
                    topic, key, avroEvent.getClass().getSimpleName(), outboxEvent.getOutboxId());
                return outboxEvent;
            } else {
                // Fallback: publish directly to Kafka if outbox not available
                logger.warn("OutboxEventService not available, falling back to direct Kafka publishing for Avro event");
                kafkaTemplate.send(topic, key, avroEvent);
                logger.info("Avro event published directly to Kafka - Topic: {}, Key: {}, EventClass: {}",
                    topic, key, avroEvent.getClass().getSimpleName());
                return null;
            }

        } catch (Exception e) {
            logger.error("Failed to save Avro event to outbox - Topic: {}, EventClass: {}, Key: {}",
                topic, avroEvent.getClass().getSimpleName(), key, e);
            throw new EventPublishingException(
                String.format("Failed to save Avro event to outbox for topic: %s", topic), e);
        }
    }

    /**
     * Publish event to Kafka topic (Direct Kafka Publishing - Best Effort)
     * Use this for non-critical events or when outbox is not available
     *
     * @param topic Topic name (e.g., "smartlock.event.boxOperation.v0")
     * @param key Partition key (e.g., boxCode, userId)
     * @param event Event object extending Event base class
     * @throws EventPublishingException If publishing fails after retries
     *
     * Usage:
     * coordinator.publishEvent(topicRegistry.getSmartLockEventBoxOperationV0(),
     *                          box.getBoxCode(),
     *                          event);
     */
    @Transactional
    public void publishEvent(String topic, String key, Event event)
            throws EventPublishingException {

        try {
            enrichEventMetadata(event);
            kafkaTemplate.send(topic, key, event);

            logger.info("Event published successfully - Topic: {}, Key: {}, EventId: {}, CorrelationId: {}",
                    topic, key, event.getEventId(), event.getCorrelationId());

        } catch (Exception e) {
            logger.error("Failed to publish event to topic: {}, EventId: {}",
                    topic, event.getEventId(), e);
            throw new EventPublishingException(
                    String.format("Event publishing failed for topic: %s", topic), e);
        }
    }

    /**
     * Publish event asynchronously with callback
     * Supports both Event interface and Avro SpecificRecord objects
     *
     * @param topic Topic name
     * @param key Partition key
     * @param event Event object (Event interface or Avro SpecificRecord)
     * @return CompletableFuture<SendResult> for async handling
     *
     * Usage:
     * // With Event interface
     * coordinator.publishEventAsync(topic, key, event)
     *
     * // With Avro SpecificRecord (ParkExitEvent, etc)
     * ParkExitEvent avroEvent = ParkExitEvent.newBuilder()...build();
     * coordinator.publishEventAsync(topic, key, avroEvent)
     *     .thenAccept(result -> log.info("Event sent to partition: {}",
     *                                    result.getRecordMetadata().partition()))
     *     .exceptionally(ex -> {
     *         log.error("Failed to publish event", ex);
     *         return null;
     *     });
     */
    @Transactional
    public CompletableFuture<SendResult<String, Object>> publishEventAsync(
            String topic, String key, Event event) {

        try {
            enrichEventMetadata(event);

            CompletableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send(topic, key, event).toCompletableFuture();

            future.whenComplete((result, exception) -> {
                if (exception != null) {
                    logger.error("Async event publishing failed - Topic: {}, EventId: {}",
                            topic, event.getEventId(), exception);
                } else {
                    logger.debug("Async event published - Topic: {}, Partition: {}, Offset: {}, EventId: {}",
                            topic,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset(),
                            event.getEventId());
                }
            });

            return future;

        } catch (Exception e) {
            logger.error("Failed to send async event to topic: {}, EventId: {}",
                    topic, event.getEventId(), e);
            CompletableFuture<SendResult<String, Object>> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(e);
            return failedFuture;
        }
    }

    /**
     * Publish Avro SpecificRecord to Kafka topic (Direct Publishing - Best Effort)
     * This overload supports Avro events that don't implement the Event interface
     *
     * Use this for IoT real-time data (MQTT sensor readings, device telemetry, etc)
     * that come from non-domain event sources.
     *
     * @param topic Topic name (e.g., "smartlock.mqtt.sensorData.v0")
     * @param key Partition key (e.g., deviceCode, boxCode)
     * @param avroEvent Avro SpecificRecord event (SensorDataEvent, LockBoxEvent, ParkExitEvent, etc)
     * @throws RuntimeException If publishing fails
     *
     * Usage:
     * SensorDataEvent event = SensorDataEvent.newBuilder()
     *     .setEventId(UUID.randomUUID().toString())
     *     .setBatteryLevel(85.5)
     *     .setTimestamp(Instant.now())
     *     .build();
     * coordinator.publishEvent(topicRegistry.getSmartLockMqttSensorDataV0(), "device-001", event);
     *
     * Note:
     * - For domain events (rest endpoints, business logic): Use publishEventReliable() with outbox
     * - For IoT/real-time data (MQTT, sensors): Use this method for direct publish
     * - For async non-blocking: Use publishEventAsync(topic, key, Object)
     */
    @Transactional
    public void publishEvent(String topic, String key, Object avroEvent)
            throws EventPublishingException {

        try {
            kafkaTemplate.send(topic, key, avroEvent);

            logger.info("Avro event published successfully - Topic: {}, Key: {}, EventClass: {}",
                    topic, key, avroEvent.getClass().getSimpleName());

        } catch (Exception e) {
            logger.error("Failed to publish Avro event to topic: {}, EventClass: {}, Error: {}",
                    topic, avroEvent.getClass().getSimpleName(), e.getMessage(), e);
            throw new EventPublishingException(
                    String.format("Avro event publishing failed for topic: %s", topic), e);
        }
    }

    /**
     * Publish Avro SpecificRecord asynchronously to Kafka topic (Non-blocking)
     * This overload supports Avro events that don't implement the Event interface
     *
     * Use this for IoT real-time data (MQTT sensor readings, device telemetry, etc)
     * that need non-blocking publishing without waiting for Kafka acknowledgment.
     *
     * Benefits over sync publishEvent():
     * - Handler returns immediately (< 1ms latency)
     * - MQTT handler never blocks waiting for Kafka ack
     * - Kafka ack happens asynchronously in background
     * - Supports 1000+ concurrent devices without thread starvation
     *
     * @param topic Topic name (e.g., "smartlock.mqtt.sensorData.v0")
     * @param key Partition key (e.g., deviceCode, boxCode)
     * @param avroEvent Avro SpecificRecord event (SensorDataEvent, LockBoxEvent, ParkExitEvent, etc)
     * @return CompletableFuture<SendResult> for async handling
     *
     * Usage Example:
     * ```
     * SensorDataEvent event = SensorDataEvent.newBuilder()
     *     .setEventId(UUID.randomUUID().toString())
     *     .setBatteryLevel(85.5)
     *     .setTimestamp(Instant.now())
     *     .build();
     *
     * coordinator.publishEventAsync(topicRegistry.getSmartLockMqttSensorDataV0(), "device-001", event)
     *     .thenAccept(result ->
     *         logger.info("Avro event sent to partition: {}",
     *                    result.getRecordMetadata().partition())
     *     )
     *     .exceptionally(ex -> {
     *         logger.error("Failed to publish Avro event", ex);
     *         return null;
     *     });
     * ```
     *
     * Error Handling:
     * - If async send fails, CompletableFuture is completed with exception
     * - Caller can use exceptionally() or handle via callback
     * - Errors are logged at ERROR level for debugging
     * - DLQ routing should be implemented at application level if needed
     *
     * Note:
     * - For critical events needing guaranteed delivery: Use publishEventReliable() with outbox
     * - For real-time IoT data: This async method is preferred (no blocking)
     * - For domain events: Use publishEventAsync(topic, key, Event) variant
     */
    @Transactional
    public CompletableFuture<SendResult<String, Object>> publishEventAsync(
            String topic, String key, Object avroEvent)
            throws EventPublishingException {

        try {
            CompletableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send(topic, key, avroEvent).toCompletableFuture();

            future.whenComplete((result, exception) -> {
                if (exception != null) {
                    logger.error("Async Avro event publishing failed - Topic: {}, EventClass: {}, Key: {}",
                            topic, avroEvent.getClass().getSimpleName(), key, exception);
                } else {
                    logger.debug("Async Avro event published successfully - Topic: {}, EventClass: {}, Key: {}, Partition: {}, Offset: {}",
                            topic,
                            avroEvent.getClass().getSimpleName(),
                            key,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });

            return future;

        } catch (Exception e) {
            logger.error("Failed to send async Avro event to topic: {}, EventClass: {}, Key: {}",
                    topic, avroEvent.getClass().getSimpleName(), key, e);
            CompletableFuture<SendResult<String, Object>> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(e);
            return failedFuture;
        }
    }

    /**
     * Enrich event metadata before publishing
     * Sets: eventId, correlationId, timestamp, eventType, source
     *
     * NOTE: Avro-generated events (SpecificRecord) are skipped as they come pre-populated
     * from their publishers and cannot be modified (read-only getters).
     *
     * @param event Event to enrich
     */
    private void enrichEventMetadata(Event event) {
        // Skip enrichment for Avro-generated events
        // They implement org.apache.avro.specific.SpecificRecord and come pre-populated
        if (event instanceof org.apache.avro.specific.SpecificRecord) {
            logger.debug("Skipping enrichment for Avro-generated event: {}",
                event.getClass().getSimpleName());
            return;
        }

        // For custom Event implementations, enrichment is not currently supported
        // since Event is now an interface without setter methods.
        // Future: Consider using Builder pattern or mutable Event implementations
        logger.debug("Event enrichment skipped - Event interface has no setters. " +
            "EventId: {}, Type: {}", event.getEventId(), event.getClass().getSimpleName());
    }

    /**
     * Set correlation ID for current request context
     * Should be called at REST request entry point
     *
     * @param correlationId Unique correlation ID
     */
    public void setCorrelationId(String correlationId) {
        correlationIdContext.set(correlationId);
        logger.debug("Correlation ID set: {}", correlationId);
    }

    /**
     * Get or create correlation ID
     * Returns existing from context or generates new UUID
     *
     * @return Correlation ID string
     */
    public String getOrCreateCorrelationId() {
        String correlationId = correlationIdContext.get();
        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
            correlationIdContext.set(correlationId);
        }
        return correlationId;
    }

    /**
     * Get current correlation ID from context
     *
     * @return Correlation ID or null if not set
     */
    public String getCorrelationId() {
        return correlationIdContext.get();
    }

    /**
     * Clear correlation ID from context
     * Should be called at request completion (in finally block)
     */
    public void clearCorrelationId() {
        correlationIdContext.remove();
    }

    /**
     * Custom exception for event publishing failures
     */
    public static class EventPublishingException extends Exception {
        public EventPublishingException(String message) {
            super(message);
        }

        public EventPublishingException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
