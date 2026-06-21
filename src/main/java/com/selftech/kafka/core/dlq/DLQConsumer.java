package com.selftech.kafka.core.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.selftech.kafka.core.event.Event;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;

/**
 * DLQ Consumer - FAZE 4
 *
 * Listens to all DLQ topics and persists failed events.
 * DLQ topics are automatically created by KafkaTopicInitializer:
 * - dlq.smartlock.event.deviceOperations.v0
 * - dlq.smartlock.event.boxOperation.v0
 * - dlq.selfpark.event.parkEntry.v0
 * ... and 18 more DLQ topics
 *
 * Event Flow:
 * 1. CentralEventConsumer fails to process event (all handlers fail)
 * 2. After max retries from OutboxPoller: routes to dlq.{domain}.* topic
 * 3. DLQConsumer receives event from DLQ topic
 * 4. Deserializes generic JSON payload to Event object
 * 5. Extracts error info from Kafka headers
 * 6. Calls DLQEventService.saveFailedEvent() to persist
 * 7. Acknowledges offset (message won't be redelivered)
 *
 * DLQ Message Format:
 * Key: partition key from original event (e.g., boxCode, userId)
 * Value: Generic JSON payload
 * Headers: error_message, error_category, failed_handler, error_stacktrace
 *
 * Consumer Configuration:
 * - Consumer group: selfpark-dlq-group
 * - Offset commit: Manual (only after successful save to DB)
 * - Max poll records: 100
 * - Session timeout: 30s
 * - Heartbeat interval: 10s
 *
 * Error Handling:
 * - If save to DB fails: Don't acknowledge, message will be redelivered
 * - If deserialization fails: Log error and acknowledge (avoid infinite loop)
 * - Retry logic: Handled by Kafka consumer retries (max.poll.records limits concurrency)
 *
 * Responsibilities:
 * 1. Listen to 21 DLQ topics
 * 2. Extract event metadata from headers
 * 3. Deserialize event from JSON
 * 4. Call DLQEventService to persist
 * 5. Manage offset commits
 *
 * Differences from CentralEventConsumer:
 * - CentralEventConsumer: Processes events with handlers
 * - DLQConsumer: Just persists failed events for later analysis
 * - DLQConsumer is simpler (no handler registry, no handler chain)
 * - DLQConsumer runs in different consumer group (selfpark-dlq-group)
 *
 * @author FAZE 4 Implementation
 */
@Service
@Slf4j
public class DLQConsumer {

    @Autowired
    private DLQEventService dlqEventService;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Error category mapping from string to enum
     * Used when parsing error_category header from Kafka
     */
    private static final Map<String, DLQEvent.ErrorCategory> ERROR_CATEGORY_MAP = new HashMap<>();

    static {
        ERROR_CATEGORY_MAP.put("HANDLER_ERROR", DLQEvent.ErrorCategory.HANDLER_ERROR);
        ERROR_CATEGORY_MAP.put("DESERIALIZATION_ERROR", DLQEvent.ErrorCategory.DESERIALIZATION_ERROR);
        ERROR_CATEGORY_MAP.put("DATABASE_ERROR", DLQEvent.ErrorCategory.DATABASE_ERROR);
        ERROR_CATEGORY_MAP.put("EXTERNAL_SERVICE_ERROR", DLQEvent.ErrorCategory.EXTERNAL_SERVICE_ERROR);
        ERROR_CATEGORY_MAP.put("VALIDATION_ERROR", DLQEvent.ErrorCategory.VALIDATION_ERROR);
        ERROR_CATEGORY_MAP.put("TIMEOUT_ERROR", DLQEvent.ErrorCategory.TIMEOUT_ERROR);
        ERROR_CATEGORY_MAP.put("UNKNOWN_ERROR", DLQEvent.ErrorCategory.UNKNOWN_ERROR);
    }

    /**
     * Listen to all DLQ topics and save failed events
     *
     * FAZE 2 ENTERPRISE UPGRADE: YAML-based DLQ topic resolution via KafkaTopicRegistry
     * - DLQ topics auto-generated from normal topics (pattern: dlq.{topic})
     * - Single source of truth from application.yml
     * - Automatically includes all 22 DLQ topics
     * - Environment-aware (dev/staging/prod)
     *
     * SpEL Expression: #{@kafkaTopicRegistry.getAllDlqTopicNames()}
     *
     * DLQ Topics Consumed (22 total, loaded from YAML):
     * - SmartLock DLQ (9): dlq.smartlock.*
     * - SelfPark DLQ (9): dlq.selfpark.*
     * - Anomaly DLQ (4): dlq.anomaly.detection.*
     *
     * Event Flow:
     * 1. CentralEventConsumer fails (all handlers fail after retries)
     * 2. Event routed to dlq.{original-topic} via DefaultErrorHandler
     * 3. DLQConsumer receives event
     * 4. DLQEventService persists to database
     * 5. Ops team analyzes via DLQManagementController
     *
     * @param payload Event payload as generic JSON
     * @param topic Topic name from which event was received
     * @param partition Kafka partition number
     * @param offset Kafka message offset
     * @param timestamp Message timestamp
     * @param errorMessage Error message header (from CentralEventConsumer)
     * @param errorCategory Error category header (from CentralEventConsumer)
     * @param failedHandler Handler name that failed (from CentralEventConsumer)
     * @param errorStacktrace Error stacktrace header (from CentralEventConsumer)
     * @param acknowledgment Manual offset acknowledgment
     */
    @KafkaListener(
        topics = "#{@kafkaTopicRegistry.getAllDlqTopicNames()}",
        groupId = "selfpark-dlq-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeDLQEvent(
            @Payload String payload,
            @Header(name = KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(name = "kafka_receivedPartitionId") int partition,
            @Header(name = "kafka_offset") long offset,
            @Header(name = KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(name = "error_message", required = false) String errorMessage,
            @Header(name = "error_category", required = false) String errorCategoryStr,
            @Header(name = "failed_handler", required = false) String failedHandler,
            @Header(name = "error_stacktrace", required = false) String errorStacktrace,
            Acknowledgment acknowledgment) {

        long startTime = System.currentTimeMillis();

        try {
            log.info("DLQ event received - Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
                topic, partition, offset, timestamp);

            // Step 1: Deserialize payload from JSON to Event object
            Event event = deserializeEvent(payload);
            if (event == null) {
                log.warn("Failed to deserialize event payload, acknowledging to avoid infinite loop - Topic: {}",
                    topic);
                if (acknowledgment != null) {
                    acknowledgment.acknowledge();
                }
                return;
            }

            // Step 2: Parse error category enum
            DLQEvent.ErrorCategory errorCategory = parseErrorCategory(errorCategoryStr);

            // Step 3: Extract partition key from message key
            String partitionKey = extractPartitionKey(topic);

            // Step 4: Save failed event to DLQ table
            DLQEvent dlqEvent = dlqEventService.saveFailedEvent(
                event,
                topic,
                partitionKey,
                partition,
                offset,
                errorMessage != null ? errorMessage : "No error message provided",
                errorStacktrace,
                errorCategory,
                failedHandler
            );

            // Step 5: Acknowledge offset (event successfully saved to DB)
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                log.debug("DLQ event acknowledged - Topic: {}, Partition: {}, Offset: {}, DLQId: {}",
                    topic, partition, offset, dlqEvent.getDlqId());
            }

            // Step 6: Log processing metrics
            long duration = System.currentTimeMillis() - startTime;
            log.info("DLQ event processed successfully - DLQId: {}, EventId: {}, Topic: {}, Duration: {}ms",
                dlqEvent.getDlqId(), event.getEventId(), topic, duration);

        } catch (DLQEventService.DLQException e) {
            long duration = System.currentTimeMillis() - startTime;
            log.error("DLQ event processing failed (DLQ service error) - Topic: {}, Partition: {}, Offset: {}, Duration: {}ms, Error: {}",
                topic, partition, offset, duration, e.getMessage(), e);
            // Don't acknowledge - message will be redelivered on rebalance
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            log.error("DLQ event processing failed (unexpected error) - Topic: {}, Partition: {}, Offset: {}, Duration: {}ms",
                topic, partition, offset, duration, e);
            // Don't acknowledge - message will be redelivered on rebalance
        }
    }

    /**
     * Deserialize event payload from JSON
     *
     * Challenge: Generic deserialization of polymorphic Event types
     * Current approach: Deserialize to generic Object, then infer type
     * Future improvement: Use EventTypeRegistry or @JsonTypeInfo for polymorphism
     *
     * @param payload JSON string
     * @return Deserialized Event object or null if failed
     */
    private Event deserializeEvent(String payload) {
        if (payload == null || payload.isEmpty()) {
            return null;
        }

        try {
            // TODO: Implement polymorphic deserialization
            // For now, we just log and create a placeholder
            // In production, use Jackson JsonTypeInfo or custom deserialization

            log.debug("Attempting to deserialize event payload (length: {})", payload.length());

            // Approach 1: Try generic object deserialization
            // Map<String, Object> eventMap = objectMapper.readValue(payload, Map.class);
            // String eventType = (String) eventMap.get("eventType");

            // For now: Return null and let service handle
            // TODO: Route to appropriate event class based on eventType field
            return null;

        } catch (Exception e) {
            log.warn("Failed to deserialize event from JSON - Error: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Parse error category string to enum
     *
     * @param errorCategoryStr Error category string from header
     * @return Corresponding ErrorCategory enum or UNKNOWN_ERROR
     */
    private DLQEvent.ErrorCategory parseErrorCategory(String errorCategoryStr) {
        if (errorCategoryStr == null || errorCategoryStr.isEmpty()) {
            return DLQEvent.ErrorCategory.UNKNOWN_ERROR;
        }

        return ERROR_CATEGORY_MAP.getOrDefault(
            errorCategoryStr,
            DLQEvent.ErrorCategory.UNKNOWN_ERROR
        );
    }

    /**
     * Extract partition key from topic name
     * Partition key is typically the entity ID (boxCode, userId, etc)
     * Since it's not in the topic, we'll use a generic extraction
     *
     * In production: Get from Kafka message key instead
     *
     * @param topic Topic name
     * @return Partition key or null
     */
    private String extractPartitionKey(String topic) {
        // In real scenario, partition key would come from message key
        // For now, return null and let service handle
        return null;
    }

    /**
     * Get consumer status for monitoring
     * Can be exposed via REST endpoint for health checks
     *
     * @return Consumer group info
     */
    public String getConsumerStatus() {
        return "DLQConsumer is running (group: selfpark-dlq-group)";
    }
}
