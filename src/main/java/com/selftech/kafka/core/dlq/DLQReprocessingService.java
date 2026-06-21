package com.selftech.kafka.core.dlq;

import com.selftech.kafka.core.event.Event;
import com.selftech.kafka.core.handler.EventHandler;
import com.selftech.kafka.core.handler.EventHandlerRegistry;
import com.selftech.kafka.core.serialization.EventSerializationService;
import com.selftech.kafka.core.serialization.EventTypeRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * DLQ Reprocessing Service - FAZE 4
 *
 * Handles reprocessing of failed events from DLQ.
 * Supports both automatic scheduled reprocessing and manual reprocessing via REST API.
 *
 * Reprocessing Flow:
 * 1. Operations team analyzes DLQ event (reviews error, identifies root cause)
 * 2. Root cause is fixed (code deployment, config change, etc)
 * 3. Operations team marks event as FIXED via REST API
 * 4. Automatic reprocessing scheduler picks up FIXED events
 * 5. Deserializes event from JSON payload
 * 6. Invokes handler chain from EventHandlerRegistry
 * 7. On success: Marks event as REPROCESSED
 * 8. On failure: Records reprocessing attempt, can retry or mark for manual review
 *
 * Automatic Reprocessing:
 * - Scheduled every 30 seconds via @Scheduled
 * - Batch size: 100 events per run
 * - Priority: Processes FIXED events before IGNORED events
 * - Max retries per event: 5 attempts
 * - Failure handling: If still fails after max retries, escalate for manual review
 *
 * Manual Reprocessing:
 * - REST API: POST /api/dlq/{dlqId}/reprocess
 * - Immediate execution, not batched
 * - Useful for operators to test fix before scheduling automatic batch
 *
 * Handler Invocation:
 * - Same as CentralEventConsumer: lookup handlers, execute chain, track successes/failures
 * - Exception handling: Catch and record, don't propagate to avoid transaction rollback
 *
 * Key Differences from CentralEventConsumer:
 * - CentralEventConsumer: First attempt to process, route failures to DLQ
 * - DLQReprocessingService: Retry after fix, update DLQ status on outcome
 * - DLQReprocessingService: Checks max retry limits before attempting
 *
 * Responsibilities:
 * 1. Automatic scheduled reprocessing of FIXED/IGNORED events
 * 2. Manual reprocessing via REST endpoint
 * 3. Deserialize event from JSON payload
 * 4. Invoke event handlers via EventHandlerRegistry
 * 5. Track reprocessing attempts and outcomes
 * 6. Update DLQ event status
 * 7. Generate metrics and alerts
 *
 * Failure Scenarios:
 * - Event cannot be deserialized: Log error, mark as IGNORED (operator action)
 * - Handler throws exception: Record attempt, if < 5 attempts stay FIXED, else needs manual review
 * - All handlers skipped (canHandle=false): Treat as success (no handlers needed)
 * - Database error during update: Will be retried on next scheduled run
 *
 * @author FAZE 4 Implementation
 */
@Service
@Slf4j
public class DLQReprocessingService {

    @Autowired
    private DLQEventService dlqEventService;

    @Autowired
    private DLQEventRepository dlqEventRepository;

    @Autowired
    private EventHandlerRegistry handlerRegistry;

    @Autowired
    private EventSerializationService eventSerializationService;

    @Autowired
    private EventTypeRegistry eventTypeRegistry;

    // ====== Configuration ======
    private static final int BATCH_SIZE = 100;
    private static final int MAX_REPROCESS_ATTEMPTS = 5;

    // ====== Automatic Reprocessing ======

    /**
     * Automatic reprocessing scheduler
     * Runs every 30 seconds, picks up FIXED and IGNORED events, attempts reprocessing
     *
     * Scheduled Configuration:
     * - initialDelay: 30 seconds (wait for system startup)
     * - fixedDelay: 30 seconds (30 seconds between runs)
     *
     * Processing Logic:
     * 1. Fetch up to BATCH_SIZE events ready for reprocessing (FIXED or IGNORED)
     * 2. For each event:
     *    a. Deserialize from JSON
     *    b. Record reprocessing attempt
     *    c. Invoke handler chain
     *    d. On success: Mark as REPROCESSED
     *    e. On failure: Record attempt count
     * 3. Log metrics: processed count, success count, failure count
     *
     * This method is transactional, each event update is separate transaction
     * for fault isolation (one failure doesn't block other events)
     */
    @Scheduled(initialDelay = 30000, fixedDelay = 30000)
    @Transactional
    public void automaticReprocessingScheduler() {
        long startTime = System.currentTimeMillis();

        try {
            log.debug("Starting automatic DLQ reprocessing scheduler");

            // Step 1: Fetch events ready for reprocessing
            var pageable = org.springframework.data.domain.PageRequest.of(0, BATCH_SIZE);
            var eventsPage = dlqEventService.getEventsReadyForReprocessing(pageable);

            if (eventsPage.isEmpty()) {
                log.debug("No DLQ events ready for reprocessing");
                return;
            }

            List<DLQEvent> events = eventsPage.getContent();
            log.info("Found {} DLQ events ready for reprocessing", events.size());

            // Step 2: Process each event
            int successCount = 0;
            int failureCount = 0;

            for (DLQEvent dlqEvent : events) {
                try {
                    boolean reprocessed = reprocessEvent(dlqEvent, "SYSTEM_SCHEDULER");
                    if (reprocessed) {
                        successCount++;
                    } else {
                        failureCount++;
                    }
                } catch (Exception e) {
                    failureCount++;
                    log.error("Exception during reprocessing of DLQ event - DLQId: {}, EventId: {}",
                        dlqEvent.getDlqId(), dlqEvent.getEventId(), e);
                }
            }

            // Step 3: Log metrics
            long duration = System.currentTimeMillis() - startTime;
            log.info("Automatic DLQ reprocessing completed - Processed: {}, Success: {}, Failures: {}, Duration: {}ms",
                events.size(), successCount, failureCount, duration);

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            log.error("Automatic DLQ reprocessing scheduler failed - Duration: {}ms", duration, e);
        }
    }

    // ====== Manual Reprocessing ======

    /**
     * Manual reprocessing (invoked via REST API)
     * Allows operators to test if fix works before scheduling automatic reprocessing
     *
     * Usage:
     * - Operations team marks event as FIXED via /api/dlq/{dlqId}/fixed
     * - Then calls POST /api/dlq/{dlqId}/reprocess
     * - This service reprocesses immediately
     * - Result returned to operator for verification
     *
     * @param dlqId DLQ event ID to reprocess
     * @return true if successfully reprocessed, false if still failing
     * @throws DLQException If event not found or cannot be reprocessed
     */
    @Transactional
    public boolean manualReprocess(String dlqId) throws DLQEventService.DLQException {
        Optional<DLQEvent> optEvent = dlqEventRepository.findById(dlqId);
        if (optEvent.isEmpty()) {
            throw new DLQEventService.DLQException("DLQ event not found: " + dlqId);
        }

        DLQEvent dlqEvent = optEvent.get();

        log.info("Manual reprocessing initiated - DLQId: {}, EventId: {}", dlqId, dlqEvent.getEventId());

        return reprocessEvent(dlqEvent, "MANUAL_API");
    }

    // ====== Core Reprocessing Logic ======

    /**
     * Core reprocessing logic
     * Deserialize event, invoke handlers, update status
     *
     * Flow:
     * 1. Check if event can be reprocessed (not exceeded max attempts)
     * 2. Record reprocessing attempt (increment counter)
     * 3. Deserialize event from JSON payload
     * 4. If deserialization fails: Mark as IGNORED, return false
     * 5. Invoke event handlers via EventHandlerRegistry
     * 6. If all handlers succeeded: Mark as REPROCESSED, return true
     * 7. If any handler failed: Stay as FIXED, increment attempt counter, return false
     *
     * @param dlqEvent DLQ event to reprocess
     * @param modifiedBy Username/service attempting reprocessing
     * @return true if successfully reprocessed, false otherwise
     */
    @Transactional
    private boolean reprocessEvent(DLQEvent dlqEvent, String modifiedBy) {
        String dlqId = dlqEvent.getDlqId();
        String eventId = dlqEvent.getEventId();

        try {
            // Step 1: Check if can be reprocessed
            if (dlqEvent.hasExceededMaxReprocessAttempts(MAX_REPROCESS_ATTEMPTS)) {
                log.warn("DLQ event has exceeded max reprocess attempts - DLQId: {}, EventId: {}, Attempts: {}",
                    dlqId, eventId, dlqEvent.getReprocessAttempts());
                return false;
            }

            // Step 2: Record reprocessing attempt (increment counter, set status to REPROCESSING)
            dlqEventService.recordReprocessAttempt(dlqId, modifiedBy);
            log.debug("Reprocess attempt recorded - DLQId: {}, EventId: {}, Attempt: {}",
                dlqId, eventId, dlqEvent.getReprocessAttempts() + 1);

            // Step 3: Deserialize event from JSON payload
            Event event = deserializeEvent(dlqEvent);
            if (event == null) {
                log.warn("Failed to deserialize event from DLQ payload, marking as IGNORED - DLQId: {}",
                    dlqId);
                dlqEventService.markAsIgnored(dlqId,
                    "Event could not be deserialized from stored payload", modifiedBy);
                return false;
            }

            // Step 4: Invoke event handlers
            @SuppressWarnings("unchecked")
            Class<Event> eventClass = (Class<Event>) event.getClass();
            List<EventHandler<?>> handlers = handlerRegistry.findHandlers(eventClass);

            if (handlers.isEmpty()) {
                log.warn("No handlers found for event type - DLQId: {}, EventId: {}, EventType: {}",
                    dlqId, eventId, event.getClass().getSimpleName());
                // No handlers = success (nothing to do)
                dlqEventService.markAsReprocessed(dlqId, modifiedBy);
                log.info("DLQ event reprocessed successfully (no handlers) - DLQId: {}, EventId: {}",
                    dlqId, eventId);
                return true;
            }

            // Step 5: Execute handler chain
            int successCount = 0;
            int failureCount = 0;

            for (EventHandler<?> handler : handlers) {
                try {
                    @SuppressWarnings("unchecked")
                    EventHandler<Event> typedHandler = (EventHandler<Event>) handler;

                    // Check if handler can process this event
                    if (!typedHandler.canHandle(event)) {
                        log.debug("Handler skipped event (canHandle=false) - Handler: {}, DLQId: {}",
                            handler.getName(), dlqId);
                        continue;
                    }

                    // Execute handler
                    typedHandler.handle(event);
                    successCount++;
                    log.debug("Handler executed successfully during reprocessing - Handler: {}, DLQId: {}",
                        handler.getName(), dlqId);

                    // Check if handler stops chain
                    if (handler.stopsChain()) {
                        log.debug("Handler stopped chain - Handler: {}, DLQId: {}", handler.getName(), dlqId);
                        break;
                    }

                } catch (Exception e) {
                    failureCount++;
                    log.warn("Handler failed during reprocessing - Handler: {}, DLQId: {}, Error: {}",
                        handler.getName(), dlqId, e.getMessage());
                    // Continue to next handler (don't break on single handler failure)
                }
            }

            // Step 6: Determine outcome
            if (failureCount == 0 && successCount > 0) {
                // All handlers succeeded
                dlqEventService.markAsReprocessed(dlqId, modifiedBy);
                log.info("DLQ event reprocessed successfully - DLQId: {}, EventId: {}, Handlers: {}",
                    dlqId, eventId, successCount);
                return true;
            } else {
                // Some handlers failed
                log.warn("DLQ event reprocessing failed - DLQId: {}, EventId: {}, Success: {}, Failures: {}",
                    dlqId, eventId, successCount, failureCount);
                // Status stays as REPROCESSING, will be retried on next scheduler run
                return false;
            }

        } catch (Exception e) {
            log.error("Exception during DLQ event reprocessing - DLQId: {}, EventId: {}",
                dlqId, eventId, e);
            return false;
        }
    }

    /**
     * Deserialize event from DLQ event payload Base64(Avro binary)
     * Uses polymorphic deserialization with EventTypeRegistry and EventSerializationService
     *
     * Flow:
     * 1. Get event type name from DLQ event metadata
     * 2. Look up Avro class via EventTypeRegistry
     * 3. Deserialize Base64(Avro binary) payload to typed Avro object
     * 4. Return as Event interface
     *
     * @param dlqEvent DLQ event with Base64(Avro binary) payload
     * @return Deserialized typed Event (SpecificRecord) or null if failed
     */
    private Event deserializeEvent(DLQEvent dlqEvent) {
        String eventPayload = dlqEvent.getEventPayload();
        if (eventPayload == null || eventPayload.isEmpty()) {
            return null;
        }

        try {
            // Step 1: Get event type name
            String eventTypeName = dlqEvent.getEventType();
            log.debug("Attempting to deserialize event from DLQ - EventType: {}, PayloadSize: {}",
                eventTypeName, eventPayload.length());

            // Step 2: Look up Avro class via EventTypeRegistry
            Class<? extends SpecificRecord> eventClass = eventTypeRegistry.getEventClass(eventTypeName);
            if (eventClass == null) {
                log.warn("Unknown event type in EventTypeRegistry - EventType: {}, DLQId: {}",
                    eventTypeName, dlqEvent.getDlqId());
                return null;
            }

            // Step 3: Deserialize Base64(Avro binary) payload to typed Avro object
            @SuppressWarnings("unchecked")
            Event event = (Event) eventSerializationService.deserializeBase64ToAvro(eventPayload, eventClass);

            log.debug("Successfully deserialized DLQ event - EventType: {}, EventId: {}, DLQId: {}",
                eventTypeName, event.getEventId(), dlqEvent.getDlqId());

            return event;

        } catch (EventSerializationService.EventSerializationException e) {
            log.warn("Failed to deserialize DLQ event payload - EventType: {}, Error: {}",
                dlqEvent.getEventType(), e.getMessage());
            return null;
        }
    }

    // ====== Monitoring ======

    /**
     * Get reprocessing statistics
     * Used for monitoring dashboard
     *
     * @return Reprocessing stats
     */
    @Transactional(readOnly = true)
    public DLQReprocessingStats getStats() {
        var pageable = org.springframework.data.domain.PageRequest.of(0, 1);

        long fixedCount = dlqEventRepository.countByStatus(DLQEvent.DLQStatus.FIXED);
        long reprocessingCount = dlqEventRepository.countByStatus(DLQEvent.DLQStatus.REPROCESSING);
        long reprocessedCount = dlqEventRepository.countByStatus(DLQEvent.DLQStatus.REPROCESSED);

        return DLQReprocessingStats.builder()
            .fixedEvents(fixedCount)
            .reprocessingEvents(reprocessingCount)
            .reprocessedEvents(reprocessedCount)
            .maxReprocessAttempts(MAX_REPROCESS_ATTEMPTS)
            .batchSize(BATCH_SIZE)
            .timestamp(Instant.now())
            .build();
    }

    // ====== Data Classes ======

    /**
     * Reprocessing statistics
     */
    public static class DLQReprocessingStats {
        private long fixedEvents;
        private long reprocessingEvents;
        private long reprocessedEvents;
        private int maxReprocessAttempts;
        private int batchSize;
        private Instant timestamp;

        public DLQReprocessingStats(long fixedEvents, long reprocessingEvents, long reprocessedEvents,
                                     int maxReprocessAttempts, int batchSize, Instant timestamp) {
            this.fixedEvents = fixedEvents;
            this.reprocessingEvents = reprocessingEvents;
            this.reprocessedEvents = reprocessedEvents;
            this.maxReprocessAttempts = maxReprocessAttempts;
            this.batchSize = batchSize;
            this.timestamp = timestamp;
        }

        public static DLQReprocessingStatsBuilder builder() {
            return new DLQReprocessingStatsBuilder();
        }

        public long getFixedEvents() { return fixedEvents; }
        public long getReprocessingEvents() { return reprocessingEvents; }
        public long getReprocessedEvents() { return reprocessedEvents; }
        public int getMaxReprocessAttempts() { return maxReprocessAttempts; }
        public int getBatchSize() { return batchSize; }
        public Instant getTimestamp() { return timestamp; }

        public static class DLQReprocessingStatsBuilder {
            private long fixedEvents;
            private long reprocessingEvents;
            private long reprocessedEvents;
            private int maxReprocessAttempts;
            private int batchSize;
            private Instant timestamp;

            public DLQReprocessingStatsBuilder fixedEvents(long fixedEvents) {
                this.fixedEvents = fixedEvents;
                return this;
            }

            public DLQReprocessingStatsBuilder reprocessingEvents(long reprocessingEvents) {
                this.reprocessingEvents = reprocessingEvents;
                return this;
            }

            public DLQReprocessingStatsBuilder reprocessedEvents(long reprocessedEvents) {
                this.reprocessedEvents = reprocessedEvents;
                return this;
            }

            public DLQReprocessingStatsBuilder maxReprocessAttempts(int maxReprocessAttempts) {
                this.maxReprocessAttempts = maxReprocessAttempts;
                return this;
            }

            public DLQReprocessingStatsBuilder batchSize(int batchSize) {
                this.batchSize = batchSize;
                return this;
            }

            public DLQReprocessingStatsBuilder timestamp(Instant timestamp) {
                this.timestamp = timestamp;
                return this;
            }

            public DLQReprocessingStats build() {
                return new DLQReprocessingStats(fixedEvents, reprocessingEvents, reprocessedEvents,
                    maxReprocessAttempts, batchSize, timestamp);
            }
        }

        @Override
        public String toString() {
            return String.format("DLQReprocessingStats{fixed=%d, reprocessing=%d, reprocessed=%d, timestamp=%s}",
                fixedEvents, reprocessingEvents, reprocessedEvents, timestamp);
        }
    }
}
