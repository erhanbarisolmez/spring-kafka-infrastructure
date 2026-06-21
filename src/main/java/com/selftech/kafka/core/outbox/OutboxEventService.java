package com.selftech.kafka.core.outbox;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.selftech.kafka.core.event.Event;
import com.selftech.kafka.core.serialization.EventSerializationService;

/**
 * Outbox Event Service - FAZE 2 ENTERPRISE
 *
 * Business logic for reliable event publishing using Outbox pattern.
 * Acts as a facade for OutboxEventRepository operations.
 *
 * Responsibilities:
 * 1. Save events to outbox (called from domain services in @Transactional context)
 * 2. Retrieve unpublished events for polling
 * 3. Mark events as published after Kafka delivery
 * 4. Record failed publish attempts
 * 5. Manage event lifecycle and cleanup
 * 6. Idempotency checks (prevent duplicate entries)
 *
 * Architecture:
 * Domain Service → OutboxEventService → OutboxEventRepository → DB
 *                                     ↓
 *                        OutboxPoller (polls every 1-5s)
 *                                     ↓
 *                        CentralEventCoordinator → Kafka
 *
 * Transaction Strategy:
 * - saveEvent(): Must be called within @Transactional from caller
 *   (Same transaction as business entity save)
 * - markAsPublished(): Separate transaction (from poller)
 * - recordFailedAttempt(): Separate transaction (from poller)
 * - cleanup(): Separate scheduled transaction
 *
 * Monitoring:
 * - Log unpublished event count (target: ~0, warn if > 1000)
 * - Log failed event count (target: 0, alert if > 10)
 * - Track poller latency (P99 < 1s, P95 < 500ms)
 *
 * Configuration:
 * - Batch size for polling: 100 events per poller run
 * - Max retry attempts: 5 (then moved to DLQ)
 * - Cleanup TTL: 7 days (published events older than this deleted)
 * - Polling interval: 1-5 seconds (configurable)
 */
@Service
@Slf4j
public class OutboxEventService {

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private EventSerializationService eventSerializationService;

    // Configuration constants (should be externalized to application.yml)
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final long CLEANUP_TTL_DAYS = 7;
    private static final int BATCH_SIZE = 100;

    /**
     * Save event to outbox for reliable publishing
     * Must be called within @Transactional boundary from caller
     *
     * Usage:
     * @Service
     * @Transactional
     * public class LockBoxCreationService {
     *     public void createBox(CreateBoxRequest req) {
     *         LockBox box = repository.save(new LockBox(...));
     *         LockBoxEvent event = LockBoxEvent.newBuilder()...build();
     *         outboxService.saveEvent(topicName, box.getBoxCode(), event);
     *         // Both box + outbox saved in same transaction
     *     }
     * }
     *
     * @param topicName Kafka topic where event should be published
     * @param partitionKey Partition key for Kafka (e.g., boxCode, userId)
     * @param event Domain event to save
     * @return Saved OutboxEvent
     */
    public OutboxEvent saveEvent(String topicName, String partitionKey, Event event) {
        try {
            // Idempotency check: prevent duplicate outbox entries
            if (event.getEventId() != null && outboxEventRepository.existsByEventId(event.getEventId())) {
                log.warn("Outbox event already exists for eventId: {}, skipping duplicate", event.getEventId());
                return outboxEventRepository.findByEventId(event.getEventId());
            }

            // Serialize event to pretty JSON for readable storage
            String eventPayload = eventSerializationService.serializeAvroToJson((SpecificRecord) event);

            // Create outbox record
            OutboxEvent outboxEvent = OutboxEvent.builder()
                .outboxId(UUID.randomUUID().toString())
                .eventId(event.getEventId())
                .correlationId(event.getCorrelationId())
                .topicName(topicName)
                .partitionKey(partitionKey)
                .eventPayload(eventPayload)
                .eventType(event.getEventType())
                .eventSource(event.getSource())
                .published(false)
                .publishAttempts(0)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

            OutboxEvent saved = outboxEventRepository.save(outboxEvent);
            log.info("Event saved to outbox - OutboxId: {}, EventId: {}, Topic: {}, CorrelationId: {}",
                saved.getOutboxId(), saved.getEventId(), topicName, event.getCorrelationId());

            return saved;

        } catch (Exception e) {
            log.error("Failed to save event to outbox - Topic: {}, PartitionKey: {}, EventId: {}",
                topicName, partitionKey, event.getEventId(), e);
            throw new OutboxEventException("Failed to save event to outbox", e);
        }
    }

    /**
     * Save Avro SpecificRecord to outbox for reliable publishing
     * Use this for Avro events (ParkEntryEvent, PaymentEvent, LockBoxEvent, etc)
     * that don't implement the Event interface
     *
     * Must be called within @Transactional boundary from caller
     *
     * Phase 4 Enhancement: Support for Avro SpecificRecord objects
     * Allows storing Avro events in outbox without Event interface wrapper
     *
     * @param topicName Kafka topic where event should be published
     * @param partitionKey Partition key for Kafka (e.g., boxCode, userId)
     * @param avroEvent Avro SpecificRecord to save (ParkEntryEvent, PaymentEvent, etc)
     * @return Saved OutboxEvent
     */
    public OutboxEvent saveEvent(String topicName, String partitionKey, Object avroEvent) {
        try {
            // For Avro objects without Event interface, generate eventId and correlationId
            String eventId = UUID.randomUUID().toString();
            String correlationId = UUID.randomUUID().toString();
            String eventType = avroEvent.getClass().getSimpleName();

            // Serialize event to pretty JSON for readable storage
            String eventPayload = eventSerializationService.serializeAvroToJson((SpecificRecord) avroEvent);

            // Create outbox record
            OutboxEvent outboxEvent = OutboxEvent.builder()
                .outboxId(UUID.randomUUID().toString())
                .eventId(eventId)
                .correlationId(correlationId)
                .topicName(topicName)
                .partitionKey(partitionKey)
                .eventPayload(eventPayload)
                .eventType(eventType)
                .eventSource("AvroSpecificRecord")
                .published(false)
                .publishAttempts(0)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

            OutboxEvent saved = outboxEventRepository.save(outboxEvent);
            log.info("Avro event saved to outbox - OutboxId: {}, EventId: {}, Topic: {}, EventType: {}",
                saved.getOutboxId(), saved.getEventId(), topicName, eventType);

            return saved;

        } catch (Exception e) {
            log.error("Failed to save Avro event to outbox - Topic: {}, PartitionKey: {}, EventType: {}",
                topicName, partitionKey, avroEvent.getClass().getSimpleName(), e);
            throw new OutboxEventException("Failed to save Avro event to outbox", e);
        }
    }

    /**
     * Retrieve unpublished events for publishing
     * Called by OutboxPoller service periodically
     *
     * @return List of unpublished OutboxEvent records (paginated, max BATCH_SIZE)
     */
    @Transactional(readOnly = true)
    public List<OutboxEvent> getUnpublishedEvents() {
        try {
            Pageable pageable = PageRequest.of(0, BATCH_SIZE);
            List<OutboxEvent> events = outboxEventRepository.findUnpublishedEvents(pageable);

            if (!events.isEmpty()) {
                log.debug("Retrieved {} unpublished events for publishing", events.size());
            }

            return events;

        } catch (Exception e) {
            log.error("Failed to retrieve unpublished events from outbox", e);
            throw new OutboxEventException("Failed to retrieve unpublished events", e);
        }
    }

    /**
     * Retrieve unpublished events for a specific topic
     * Useful for topic-specific publishing
     *
     * @param topicName Topic name
     * @return List of unpublished events for the topic
     */
    @Transactional(readOnly = true)
    public List<OutboxEvent> getUnpublishedEventsByTopic(String topicName) {
        try {
            Pageable pageable = PageRequest.of(0, BATCH_SIZE);
            return outboxEventRepository.findUnpublishedEventsByTopic(topicName, pageable);

        } catch (Exception e) {
            log.error("Failed to retrieve unpublished events for topic: {}", topicName, e);
            throw new OutboxEventException("Failed to retrieve events for topic: " + topicName, e);
        }
    }

    /**
     * Mark events as successfully published to Kafka
     * Called by OutboxPoller after successful Kafka delivery
     *
     * @param outboxIds List of outbox IDs that were successfully published
     * @return Number of events updated
     */
    @Transactional
    public int markAsPublished(List<String> outboxIds) {
        if (outboxIds == null || outboxIds.isEmpty()) {
            return 0;
        }

        try {
            int updated = outboxEventRepository.markAsPublished(outboxIds, Instant.now());
            log.info("Marked {} events as published", updated);
            return updated;

        } catch (Exception e) {
            log.error("Failed to mark events as published - OutboxIds: {}", outboxIds, e);
            throw new OutboxEventException("Failed to mark events as published", e);
        }
    }

    /**
     * Record a failed publish attempt
     * Increments retry count and stores error message
     *
     * @param outboxId Outbox event ID
     * @param errorMessage Error message from exception
     */
    @Transactional
    public void recordFailedAttempt(String outboxId, String errorMessage) {
        try {
            int updated = outboxEventRepository.incrementPublishAttempts(outboxId, errorMessage, Instant.now());

            if (updated > 0) {
                log.warn("Recorded failed publish attempt for outboxId: {}, Error: {}", outboxId, errorMessage);
            }

        } catch (Exception e) {
            log.error("Failed to record publish attempt for outboxId: {}", outboxId, e);
            throw new OutboxEventException("Failed to record publish attempt", e);
        }
    }

    /**
     * Get failed events that exceeded max retry attempts
     * These events should be routed to DLQ for manual intervention
     *
     * @return List of failed OutboxEvent records
     */
    @Transactional(readOnly = true)
    public List<OutboxEvent> getFailedEvents() {
        try {
            Pageable pageable = PageRequest.of(0, BATCH_SIZE);
            List<OutboxEvent> failedEvents = outboxEventRepository.findFailedEvents(MAX_RETRY_ATTEMPTS, pageable);

            if (!failedEvents.isEmpty()) {
                log.warn("Found {} failed events exceeding max retry attempts", failedEvents.size());
            }

            return failedEvents;

        } catch (Exception e) {
            log.error("Failed to retrieve failed events from outbox", e);
            throw new OutboxEventException("Failed to retrieve failed events", e);
        }
    }

    /**
     * Get count of unpublished events (for monitoring)
     *
     * @return Number of events waiting to be published
     */
    @Transactional(readOnly = true)
    public long getUnpublishedEventCount() {
        try {
            return outboxEventRepository.countUnpublishedEvents();

        } catch (Exception e) {
            log.error("Failed to count unpublished events", e);
            return -1;
        }
    }

    /**
     * Get count of unpublished events for a topic (for monitoring)
     *
     * @param topicName Topic name
     * @return Number of unpublished events for the topic
     */
    @Transactional(readOnly = true)
    public long getUnpublishedEventCountByTopic(String topicName) {
        try {
            return outboxEventRepository.countUnpublishedEventsByTopic(topicName);

        } catch (Exception e) {
            log.error("Failed to count unpublished events for topic: {}", topicName, e);
            return -1;
        }
    }

    /**
     * Clean up old published events (TTL management)
     * Called by scheduled task to manage database size
     *
     * Deletes events that:
     * - Are marked as published (published=true)
     * - Are older than CLEANUP_TTL_DAYS
     *
     * @return Number of events deleted
     */
    @Transactional
    public int cleanupOldPublishedEvents() {
        try {
            Instant cutoffTime = Instant.now().minus(CLEANUP_TTL_DAYS, ChronoUnit.DAYS);
            int deleted = outboxEventRepository.deleteOldPublishedEvents(cutoffTime);

            if (deleted > 0) {
                log.info("Cleaned up {} old published events (older than {} days)", deleted, CLEANUP_TTL_DAYS);
            }

            return deleted;

        } catch (Exception e) {
            log.error("Failed to cleanup old published events", e);
            throw new OutboxEventException("Failed to cleanup old published events", e);
        }
    }

    /**
     * Get health status of outbox
     * Returns metrics for monitoring
     *
     * @return OutboxHealthStatus with counts and metrics
     */
    @Transactional(readOnly = true)
    public OutboxHealthStatus getHealthStatus() {
        try {
            long unpublishedCount = getUnpublishedEventCount();
            List<OutboxEvent> failedEvents = getFailedEvents();
            long failedCount = failedEvents.isEmpty() ? 0 : failedEvents.size();

            OutboxHealthStatus status = OutboxHealthStatus.builder()
                .unpublishedEventCount(unpublishedCount)
                .failedEventCount(failedCount)
                .maxRetryAttempts(MAX_RETRY_ATTEMPTS)
                .cleanupTtlDays(CLEANUP_TTL_DAYS)
                .batchSize(BATCH_SIZE)
                .status(unpublishedCount < 1000 && failedCount == 0 ? "HEALTHY" : "WARN")
                .timestamp(Instant.now())
                .build();

            return status;

        } catch (Exception e) {
            log.error("Failed to get outbox health status", e);
            return OutboxHealthStatus.builder()
                .status("ERROR")
                .timestamp(Instant.now())
                .build();
        }
    }

    /**
     * Custom exception for outbox operations
     */
    public static class OutboxEventException extends RuntimeException {
        public OutboxEventException(String message) {
            super(message);
        }

        public OutboxEventException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
