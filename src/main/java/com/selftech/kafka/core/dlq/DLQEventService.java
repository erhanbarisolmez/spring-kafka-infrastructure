package com.selftech.kafka.core.dlq;

import com.selftech.kafka.core.event.Event;
import com.selftech.kafka.core.serialization.EventSerializationService;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * DLQ Event Service - FAZE 4
 *
 * Business logic layer for Dead Letter Queue event management.
 * Handles saving failed events, retrieving for analysis, status transitions,
 * and metrics collection.
 *
 * Responsibilities:
 * 1. Save failed events to DLQ table (from CentralEventConsumer)
 * 2. Retrieve events for dashboard/analysis by status, topic, error type
 * 3. Update event status transitions (RECEIVED → ANALYZED → FIXED → REPROCESSED)
 * 4. Track reprocessing attempts and check max retry limits
 * 5. Provide metrics and monitoring data
 * 6. Manage event archival and cleanup
 *
 * Service Boundaries:
 * - Input: Failed events from CentralEventConsumer (after max retries)
 * - Output: DLQEventRepository persistence + DLQReprocessingService invocations
 * - Does NOT handle actual reprocessing (delegated to DLQReprocessingService)
 * - Does NOT publish events back to Kafka (handled by reprocessing service)
 *
 * Transaction Scope:
 * - All methods are @Transactional to ensure ACID properties
 * - Saves are immediate (not batched like OutboxPoller)
 * - Status updates are synchronous
 *
 * Constants:
 * - MAX_REPROCESS_ATTEMPTS: 5 (after 5 attempts, manual review required)
 * - DEFAULT_BATCH_SIZE: 100 (for batch operations)
 * - ARCHIVE_TTL_DAYS: 90 (keep archived events for 3 months)
 *
 * @author FAZE 4 Implementation
 */
@Service
@Slf4j
public class DLQEventService {

    @Autowired
    private DLQEventRepository dlqEventRepository;

    @Autowired
    private EventSerializationService eventSerializationService;

    // ====== Configuration Constants ======
    private static final int MAX_REPROCESS_ATTEMPTS = 5;
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int ARCHIVE_TTL_DAYS = 90;

    // ====== DLQ Event Persistence ======

    /**
     * Save a failed event to DLQ
     *
     * Called by CentralEventConsumer when:
     * 1. Event processing fails for all handlers
     * 2. Event has been retried from OutboxPoller multiple times
     * 3. Decision made to route to DLQ for manual inspection
     *
     * Flow:
     * 1. Check if event already in DLQ (idempotency)
     * 2. Serialize event to JSON
     * 3. Create DLQEvent with metadata and error info
     * 4. Persist to database
     * 5. Log for monitoring
     *
     * @param event Failed event object
     * @param topic Original topic name
     * @param partitionKey Partition key used
     * @param kafkaPartition Kafka partition number
     * @param kafkaOffset Kafka offset
     * @param errorMessage Error message from handler
     * @param errorStacktrace Full stack trace (truncated if too long)
     * @param errorCategory Categorized error type
     * @param failedHandler Handler class name that failed
     * @return Saved DLQEvent
     * @throws DLQException If save fails
     */
    @Transactional
    public DLQEvent saveFailedEvent(
            Event event,
            String topic,
            String partitionKey,
            Integer kafkaPartition,
            Long kafkaOffset,
            String errorMessage,
            String errorStacktrace,
            DLQEvent.ErrorCategory errorCategory,
            String failedHandler) throws DLQException {

        try {
            // Step 1: Check idempotency (avoid duplicate DLQ entries)
            String eventId = event.getEventId();
            if (dlqEventRepository.existsByEventId(eventId)) {
                log.warn("Event already exists in DLQ - EventId: {}, Topic: {}, CorrelationId: {}",
                    eventId, topic, event.getCorrelationId());
                return dlqEventRepository.findByEventId(eventId).orElse(null);
            }

            // Step 2: Serialize event to Base64(Avro binary)
            String eventPayload;
            try {
                eventPayload = eventSerializationService.serializeAvroToBase64((SpecificRecord) event);
            } catch (EventSerializationService.EventSerializationException e) {
                log.warn("Failed to serialize event to Avro, using toString() instead - EventId: {}", eventId);
                eventPayload = event.toString();
            }

            // Truncate stacktrace if too long (database limits)
            String truncatedStacktrace = errorStacktrace;
            if (errorStacktrace != null && errorStacktrace.length() > 10000) {
                truncatedStacktrace = errorStacktrace.substring(0, 10000) + "... [truncated]";
            }

            // Step 3: Create DLQEvent entity
            DLQEvent dlqEvent = new DLQEvent();
            dlqEvent.setEventId(eventId);
            dlqEvent.setCorrelationId(event.getCorrelationId());
            dlqEvent.setDomain(extractDomainFromTopic(topic));
            dlqEvent.setEventType(event.getEventType());
            dlqEvent.setTopicName(topic);
            dlqEvent.setPartitionKey(partitionKey);
            dlqEvent.setKafkaPartition(kafkaPartition);
            dlqEvent.setKafkaOffset(kafkaOffset);
            dlqEvent.setEventPayload(eventPayload);
            dlqEvent.setErrorMessage(errorMessage);
            dlqEvent.setErrorStacktrace(truncatedStacktrace);
            dlqEvent.setErrorCategory(errorCategory != null ? errorCategory : DLQEvent.ErrorCategory.UNKNOWN_ERROR);
            dlqEvent.setFailedHandler(failedHandler);
            dlqEvent.setReprocessAttempts(0);
            dlqEvent.setStatus(DLQEvent.DLQStatus.RECEIVED);
            dlqEvent.setLastModifiedBy("SYSTEM");

            // Step 4: Persist to database
            DLQEvent saved = dlqEventRepository.save(dlqEvent);

            log.info("Failed event saved to DLQ - DLQId: {}, EventId: {}, Topic: {}, ErrorCategory: {}, CorrelationId: {}",
                saved.getDlqId(), eventId, topic, errorCategory, event.getCorrelationId());

            return saved;

        } catch (Exception e) {
            log.error("Failed to save event to DLQ - EventId: {}, Topic: {}, Error: {}",
                event.getEventId(), topic, e.getMessage(), e);
            throw new DLQException("Failed to save event to DLQ: " + e.getMessage(), e);
        }
    }

    /**
     * Extract domain name from topic name
     * Examples:
     * - "smartlock.event.boxOperation.v0" → "smartlock"
     * - "selfpark.event.parkEntry.v0" → "selfpark"
     * - "dlq.smartlock.event.boxOperation.v0" → "smartlock"
     *
     * @param topicName Topic name
     * @return Domain name or "unknown"
     */
    private String extractDomainFromTopic(String topicName) {
        if (topicName == null || topicName.isEmpty()) {
            return "unknown";
        }

        String[] parts = topicName.split("\\.");
        if (parts.length > 0) {
            String potential = parts[0];
            if ("dlq".equals(potential) && parts.length > 1) {
                return parts[1];  // dlq.smartlock.* → smartlock
            }
            return potential;  // smartlock.* → smartlock
        }
        return "unknown";
    }

    // ====== Event Retrieval ======

    /**
     * Get DLQ events by status (paginated)
     * Used by DLQ dashboard
     *
     * @param status Event status
     * @param pageable Pagination params
     * @return Page of events
     */
    @Transactional(readOnly = true)
    public Page<DLQEvent> getEventsByStatus(DLQEvent.DLQStatus status, Pageable pageable) {
        log.debug("Fetching DLQ events with status: {}, Page: {}, Size: {}",
            status, pageable.getPageNumber(), pageable.getPageSize());
        return dlqEventRepository.findByStatusOrderByCreatedAtDesc(status, pageable);
    }

    /**
     * Get events requiring action (RECEIVED or ANALYZED)
     * Priority list for operations team
     *
     * @param pageable Pagination params
     * @return Page of events
     */
    @Transactional(readOnly = true)
    public Page<DLQEvent> getEventsRequiringAction(Pageable pageable) {
        log.debug("Fetching events requiring action, Page: {}, Size: {}",
            pageable.getPageNumber(), pageable.getPageSize());
        return dlqEventRepository.findEventsRequiringAction(pageable);
    }

    /**
     * Get events ready for reprocessing (FIXED or IGNORED)
     * Used by automatic reprocessing scheduler
     *
     * @param pageable Pagination params
     * @return Page of events
     */
    @Transactional(readOnly = true)
    public Page<DLQEvent> getEventsReadyForReprocessing(Pageable pageable) {
        log.debug("Fetching events ready for reprocessing, Page: {}, Size: {}",
            pageable.getPageNumber(), pageable.getPageSize());
        return dlqEventRepository.findEventsReadyForReprocessing(pageable);
    }

    /**
     * Get events by error category (for pattern analysis)
     *
     * @param errorCategory Error category
     * @param pageable Pagination params
     * @return Page of events
     */
    @Transactional(readOnly = true)
    public Page<DLQEvent> getEventsByErrorCategory(DLQEvent.ErrorCategory errorCategory, Pageable pageable) {
        log.debug("Fetching DLQ events with error category: {}", errorCategory);
        return dlqEventRepository.findByErrorCategoryOrderByCreatedAtDesc(errorCategory, pageable);
    }

    /**
     * Get events by topic (for domain-specific analysis)
     *
     * @param topicName Topic name
     * @param pageable Pagination params
     * @return Page of events
     */
    @Transactional(readOnly = true)
    public Page<DLQEvent> getEventsByTopic(String topicName, Pageable pageable) {
        log.debug("Fetching DLQ events for topic: {}", topicName);
        return dlqEventRepository.findByTopicNameOrderByCreatedAtDesc(topicName, pageable);
    }

    /**
     * Get events by domain (smartlock, selfpark, etc)
     *
     * @param domain Domain name
     * @param pageable Pagination params
     * @return Page of events
     */
    @Transactional(readOnly = true)
    public Page<DLQEvent> getEventsByDomain(String domain, Pageable pageable) {
        log.debug("Fetching DLQ events for domain: {}", domain);
        return dlqEventRepository.findByDomainOrderByCreatedAtDesc(domain, pageable);
    }

    /**
     * Get events by failed handler
     *
     * @param handlerName Handler class name
     * @param pageable Pagination params
     * @return Page of events
     */
    @Transactional(readOnly = true)
    public Page<DLQEvent> getEventsByFailedHandler(String handlerName, Pageable pageable) {
        log.debug("Fetching DLQ events for handler: {}", handlerName);
        return dlqEventRepository.findByFailedHandlerOrderByCreatedAtDesc(handlerName, pageable);
    }

    /**
     * Get single DLQ event by ID
     *
     * @param dlqId DLQ event ID
     * @return Optional containing event if found
     */
    @Transactional(readOnly = true)
    public Optional<DLQEvent> getEventById(String dlqId) {
        return dlqEventRepository.findById(dlqId);
    }

    /**
     * Get events created within time range
     *
     * @param startTime Range start
     * @param endTime Range end
     * @param pageable Pagination params
     * @return Page of events
     */
    @Transactional(readOnly = true)
    public Page<DLQEvent> getEventsByTimeRange(Instant startTime, Instant endTime, Pageable pageable) {
        log.debug("Fetching DLQ events between {} and {}", startTime, endTime);
        return dlqEventRepository.findByCreatedAtBetween(startTime, endTime, pageable);
    }

    // ====== Status Transitions ======

    /**
     * Mark event as analyzed (operations team identified root cause)
     *
     * @param dlqId DLQ event ID
     * @param analysisNotes Root cause analysis notes
     * @param modifiedBy Username/service making the change
     * @throws DLQException If event not found
     */
    @Transactional
    public void markAsAnalyzed(String dlqId, String analysisNotes, String modifiedBy) throws DLQException {
        DLQEvent event = dlqEventRepository.findById(dlqId)
            .orElseThrow(() -> new DLQException("DLQ event not found: " + dlqId));

        event.markAsAnalyzed(analysisNotes, modifiedBy);
        dlqEventRepository.save(event);

        log.info("DLQ event marked as analyzed - DLQId: {}, EventId: {}, ModifiedBy: {}",
            dlqId, event.getEventId(), modifiedBy);
    }

    /**
     * Mark event as fixed (ready for reprocessing)
     *
     * @param dlqId DLQ event ID
     * @param modifiedBy Username/service making the change
     * @throws DLQException If event not found
     */
    @Transactional
    public void markAsFixed(String dlqId, String modifiedBy) throws DLQException {
        DLQEvent event = dlqEventRepository.findById(dlqId)
            .orElseThrow(() -> new DLQException("DLQ event not found: " + dlqId));

        event.markAsFixed(modifiedBy);
        dlqEventRepository.save(event);

        log.info("DLQ event marked as fixed - DLQId: {}, EventId: {}, ModifiedBy: {}",
            dlqId, event.getEventId(), modifiedBy);
    }

    /**
     * Mark event as ignored (not a real error, operations decision)
     *
     * @param dlqId DLQ event ID
     * @param reason Why this event is being ignored
     * @param modifiedBy Username/service making the change
     * @throws DLQException If event not found
     */
    @Transactional
    public void markAsIgnored(String dlqId, String reason, String modifiedBy) throws DLQException {
        DLQEvent event = dlqEventRepository.findById(dlqId)
            .orElseThrow(() -> new DLQException("DLQ event not found: " + dlqId));

        event.markAsIgnored(reason, modifiedBy);
        dlqEventRepository.save(event);

        log.info("DLQ event marked as ignored - DLQId: {}, EventId: {}, Reason: {}, ModifiedBy: {}",
            dlqId, event.getEventId(), reason, modifiedBy);
    }

    /**
     * Record reprocessing attempt
     * Called by DLQReprocessingService before attempting reprocessing
     *
     * @param dlqId DLQ event ID
     * @param modifiedBy Username/service attempting reprocessing
     * @throws DLQException If event not found or has exceeded max attempts
     */
    @Transactional
    public void recordReprocessAttempt(String dlqId, String modifiedBy) throws DLQException {
        DLQEvent event = dlqEventRepository.findById(dlqId)
            .orElseThrow(() -> new DLQException("DLQ event not found: " + dlqId));

        if (event.hasExceededMaxReprocessAttempts(MAX_REPROCESS_ATTEMPTS)) {
            throw new DLQException(
                String.format("Event has exceeded max reprocess attempts (%d): %s",
                    MAX_REPROCESS_ATTEMPTS, dlqId));
        }

        event.recordReprocessAttempt(modifiedBy);
        dlqEventRepository.save(event);

        log.info("Reprocess attempt recorded - DLQId: {}, EventId: {}, Attempt: {}, ModifiedBy: {}",
            dlqId, event.getEventId(), event.getReprocessAttempts(), modifiedBy);
    }

    /**
     * Mark event as successfully reprocessed
     * Called by DLQReprocessingService after successful handler execution
     *
     * @param dlqId DLQ event ID
     * @param modifiedBy Username/service completing reprocessing
     * @throws DLQException If event not found
     */
    @Transactional
    public void markAsReprocessed(String dlqId, String modifiedBy) throws DLQException {
        DLQEvent event = dlqEventRepository.findById(dlqId)
            .orElseThrow(() -> new DLQException("DLQ event not found: " + dlqId));

        event.markAsReprocessed(modifiedBy);
        dlqEventRepository.save(event);

        log.info("DLQ event marked as reprocessed - DLQId: {}, EventId: {}, ModifiedBy: {}",
            dlqId, event.getEventId(), modifiedBy);
    }

    /**
     * Mark event as archived (for archival/export)
     *
     * @param dlqId DLQ event ID
     * @param modifiedBy Username/service archiving
     * @throws DLQException If event not found
     */
    @Transactional
    public void markAsArchived(String dlqId, String modifiedBy) throws DLQException {
        DLQEvent event = dlqEventRepository.findById(dlqId)
            .orElseThrow(() -> new DLQException("DLQ event not found: " + dlqId));

        event.markAsArchived(modifiedBy);
        dlqEventRepository.save(event);

        log.info("DLQ event marked as archived - DLQId: {}, EventId: {}, ModifiedBy: {}",
            dlqId, event.getEventId(), modifiedBy);
    }

    // ====== Monitoring and Metrics ======

    /**
     * Get DLQ health status summary
     * Used for monitoring dashboard
     *
     * @return DLQ health metrics
     */
    @Transactional(readOnly = true)
    public DLQHealthStatus getHealthStatus() {
        DLQHealthStatus status = new DLQHealthStatus();

        status.setTotalEvents(dlqEventRepository.count());
        status.setReceivedEvents(dlqEventRepository.countByStatus(DLQEvent.DLQStatus.RECEIVED));
        status.setAnalyzedEvents(dlqEventRepository.countByStatus(DLQEvent.DLQStatus.ANALYZED));
        status.setFixedEvents(dlqEventRepository.countByStatus(DLQEvent.DLQStatus.FIXED));
        status.setReprocessingEvents(dlqEventRepository.countByStatus(DLQEvent.DLQStatus.REPROCESSING));
        status.setReprocessedEvents(dlqEventRepository.countByStatus(DLQEvent.DLQStatus.REPROCESSED));
        status.setArchivedEvents(dlqEventRepository.countByStatus(DLQEvent.DLQStatus.ARCHIVED));
        status.setIgnoredEvents(dlqEventRepository.countByStatus(DLQEvent.DLQStatus.IGNORED));
        status.setEventsRequiringAction(dlqEventRepository.countEventsRequiringAction());

        status.setMaxReprocessAttempts(MAX_REPROCESS_ATTEMPTS);
        status.setTimestamp(Instant.now());

        return status;
    }

    /**
     * Get count of events by error category
     *
     * @param errorCategory Error category
     * @return Count of events
     */
    @Transactional(readOnly = true)
    public long getCountByErrorCategory(DLQEvent.ErrorCategory errorCategory) {
        return dlqEventRepository.countByErrorCategory(errorCategory);
    }

    /**
     * Get count of events by topic
     *
     * @param topicName Topic name
     * @return Count of events
     */
    @Transactional(readOnly = true)
    public long getCountByTopic(String topicName) {
        return dlqEventRepository.countByTopicName(topicName);
    }

    /**
     * Get count of events by domain
     *
     * @param domain Domain name
     * @return Count of events
     */
    @Transactional(readOnly = true)
    public long getCountByDomain(String domain) {
        return dlqEventRepository.countByDomain(domain);
    }

    // ====== Cleanup and Archival ======

    /**
     * Archive old DLQ events (keep for TTL period, then archive)
     * Called by scheduled task
     *
     * @param beforeInstant Archive events updated before this time
     * @return Number of events archived
     */
    @Transactional
    public int archiveOldEvents(Instant beforeInstant) {
        log.info("Archiving DLQ events older than {}", beforeInstant);
        // TODO: Implement export to archive storage (S3, etc)
        // For now, just mark as archived
        Page<DLQEvent> events = dlqEventRepository.findArchivedEventsBefore(beforeInstant,
            org.springframework.data.domain.PageRequest.of(0, 1000));

        List<String> dlqIds = events.getContent().stream()
            .map(DLQEvent::getDlqId)
            .toList();

        if (!dlqIds.isEmpty()) {
            return dlqEventRepository.updateStatusBatch(
                dlqIds,
                DLQEvent.DLQStatus.ARCHIVED,
                "SYSTEM",
                Instant.now()
            );
        }
        return 0;
    }

    /**
     * Clean up archived events older than TTL
     * Called by scheduled task
     *
     * @param beforeInstant Delete archived events before this time
     * @return Number of events deleted
     */
    @Transactional
    public int cleanupArchivedEvents(Instant beforeInstant) {
        log.info("Cleaning up archived DLQ events older than {}", beforeInstant);
        return dlqEventRepository.deleteArchivedEventsBefore(beforeInstant);
    }

    /**
     * Custom exception for DLQ operations
     */
    public static class DLQException extends Exception {
        public DLQException(String message) {
            super(message);
        }

        public DLQException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
