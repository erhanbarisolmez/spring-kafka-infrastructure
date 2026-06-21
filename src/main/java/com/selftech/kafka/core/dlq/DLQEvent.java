package com.selftech.kafka.core.dlq;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import jakarta.persistence.*;

/**
 * Dead Letter Queue Event Entity - FAZE 4
 *
 * Represents a failed event that couldn't be processed by handlers.
 * Stores complete event context for manual inspection and reprocessing.
 *
 * DLQ Events Flow:
 * 1. CentralEventConsumer fails to process event (after max retries from outbox)
 * 2. Event routed to dlq.{domain}.* topic
 * 3. DLQConsumer receives from DLQ topic
 * 4. Event persisted in dlq_events table
 * 5. Operations team can inspect, fix root cause, and reprocess via DLQReprocessingService
 *
 * Database Table: dlq_events
 * - Strategy: Partitioned by event_type for performance
 * - Retention: Keep indefinitely for audit trail (can be archived separately)
 * - Indexes: Quick lookup by status, eventId, topic, createdAt
 *
 * Key Differences from OutboxEvent:
 * - OutboxEvent: Events waiting to be published (temporary)
 * - DLQEvent: Events that failed after publishing (permanent audit trail)
 *
 * @author FAZE 4 Implementation
 */
@Entity
@Table(
    name = "dlq_events",
    indexes = {
        @Index(name = "idx_dlq_status_created", columnList = "status,created_at"),
        @Index(name = "idx_dlq_event_id", columnList = "event_id", unique = true),
        @Index(name = "idx_dlq_topic", columnList = "topic_name"),
        @Index(name = "idx_dlq_correlation_id", columnList = "correlation_id"),
        @Index(name = "idx_dlq_error_category", columnList = "error_category"),
        @Index(name = "idx_dlq_created_at", columnList = "created_at")
    }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DLQEvent {

    /**
     * DLQ Event Status enum
     * - RECEIVED: Initial status when event enters DLQ
     * - ANALYZED: Root cause analyzed by operators
     * - FIXED: Correction applied (code fix, config change, etc)
     * - REPROCESSING: Actively being reprocessed
     * - REPROCESSED: Successfully reprocessed
     * - ARCHIVED: Moved to long-term storage
     * - IGNORED: Deliberately ignored (not a real error)
     */
    public enum DLQStatus {
        RECEIVED,       // Initial state
        ANALYZED,       // Root cause identified
        FIXED,          // System corrected
        REPROCESSING,   // Retry in progress
        REPROCESSED,    // Successfully processed
        ARCHIVED,       // Moved to storage
        IGNORED         // Deliberately skipped
    }

    /**
     * Error Category enum for easier problem classification
     * - HANDLER_ERROR: Handler threw exception
     * - DESERIALIZATION_ERROR: Event couldn't be deserialized
     * - DATABASE_ERROR: Database operation failed
     * - EXTERNAL_SERVICE_ERROR: Third-party service unavailable
     * - VALIDATION_ERROR: Event data invalid
     * - TIMEOUT_ERROR: Processing exceeded timeout
     * - UNKNOWN_ERROR: Couldn't determine root cause
     */
    public enum ErrorCategory {
        HANDLER_ERROR,
        DESERIALIZATION_ERROR,
        DATABASE_ERROR,
        EXTERNAL_SERVICE_ERROR,
        VALIDATION_ERROR,
        TIMEOUT_ERROR,
        UNKNOWN_ERROR
    }

    /**
     * Unique DLQ event identifier
     */
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "dlq_id", columnDefinition = "uuid")
    private String dlqId;

    /**
     * Original event ID (from original event)
     * Used to correlate with original event in outbox
     */
    @Column(name = "event_id", nullable = false, length = 255)
    private String eventId;

    /**
     * Correlation ID for distributed tracing
     * Propagated through entire request flow
     */
    @Column(name = "correlation_id", length = 255)
    private String correlationId;

    /**
     * Domain that produced the event
     * Examples: "smartlock", "selfpark", "anomaly_detection"
     */
    @Column(name = "domain", length = 100)
    private String domain;

    /**
     * Event type/class name for routing
     * Examples: "LockBoxEvent", "ParkEntryEvent", "PaymentEvent"
     */
    @Column(name = "event_type", nullable = false, length = 255)
    private String eventType;

    /**
     * Topic from which event was routed to DLQ
     * Examples: "smartlock.event.boxOperation.v0", "dlq.smartlock.event.boxOperation.v0"
     */
    @Column(name = "topic_name", nullable = false, length = 255)
    private String topicName;

    /**
     * Partition key used when publishing original event
     */
    @Column(name = "partition_key", length = 255)
    private String partitionKey;

    /**
     * Kafka partition from which event originated
     */
    @Column(name = "kafka_partition")
    private Integer kafkaPartition;

    /**
     * Kafka offset from which event originated
     */
    @Column(name = "kafka_offset")
    private Long kafkaOffset;

    /**
     * Complete event payload as JSON
     * Stores original event data for reprocessing
     * Type: JSONB (PostgreSQL native JSON with indexing support)
     */
    @Column(name = "event_payload", columnDefinition = "jsonb", nullable = false)
    private String eventPayload;

    /**
     * Root cause error message
     * First error encountered when processing failed
     */
    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    /**
     * Full stack trace of exception
     * For debugging and root cause analysis
     */
    @Column(name = "error_stacktrace", columnDefinition = "TEXT")
    private String errorStacktrace;

    /**
     * Categorized error type
     * Helps operations team identify patterns
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "error_category", length = 50)
    private ErrorCategory errorCategory;

    /**
     * Handler that failed (if known)
     * Examples: "LockBoxEventHandler", "ParkEntryEventHandler"
     */
    @Column(name = "failed_handler", length = 255)
    private String failedHandler;

    /**
     * Number of times reprocessing was attempted
     * Incremented each time DLQReprocessingService.reprocess() is called
     */
    @Column(name = "reprocess_attempts")
    private Integer reprocessAttempts = 0;

    /**
     * Current status of DLQ event
     * Tracks progression: RECEIVED → ANALYZED → FIXED → REPROCESSED
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", length = 50, nullable = false)
    private DLQStatus status = DLQStatus.RECEIVED;

    /**
     * Notes from operations team during analysis
     * Example: "Database was locked, reprocessing after downtime"
     */
    @Column(name = "analysis_notes", columnDefinition = "TEXT")
    private String analysisNotes;

    /**
     * When this event was first received into DLQ
     */
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    /**
     * Last time status or notes were updated
     */
    @Column(name = "updated_at")
    private Instant updatedAt;

    /**
     * When event was successfully reprocessed (if applicable)
     * Set by DLQReprocessingService on successful reprocessing
     */
    @Column(name = "reprocessed_at")
    private Instant reprocessedAt;

    /**
     * Who (username/service) last modified this DLQ event
     * For audit trail and accountability
     */
    @Column(name = "last_modified_by", length = 255)
    private String lastModifiedBy;

    // ====== Lifecycle Methods ======

    @PrePersist
    protected void onCreate() {
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
        if (this.reprocessAttempts == null) {
            this.reprocessAttempts = 0;
        }
        if (this.status == null) {
            this.status = DLQStatus.RECEIVED;
        }
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = Instant.now();
    }

    // ====== Business Logic Methods ======

    /**
     * Mark this event as analyzed by operations team
     * @param notes Analysis notes about root cause
     * @param modifiedBy Username/service making the change
     */
    public void markAsAnalyzed(String notes, String modifiedBy) {
        this.status = DLQStatus.ANALYZED;
        this.analysisNotes = notes;
        this.lastModifiedBy = modifiedBy;
    }

    /**
     * Mark this event as fixed and ready for reprocessing
     * @param modifiedBy Username/service making the change
     */
    public void markAsFixed(String modifiedBy) {
        this.status = DLQStatus.FIXED;
        this.lastModifiedBy = modifiedBy;
    }

    /**
     * Record a reprocessing attempt
     * @param modifiedBy Username/service making the attempt
     */
    public void recordReprocessAttempt(String modifiedBy) {
        this.reprocessAttempts++;
        this.status = DLQStatus.REPROCESSING;
        this.lastModifiedBy = modifiedBy;
    }

    /**
     * Mark event as successfully reprocessed
     * @param modifiedBy Username/service completing the reprocessing
     */
    public void markAsReprocessed(String modifiedBy) {
        this.status = DLQStatus.REPROCESSED;
        this.reprocessedAt = Instant.now();
        this.lastModifiedBy = modifiedBy;
    }

    /**
     * Mark event as archived (moved to long-term storage)
     * @param modifiedBy Username/service archiving the event
     */
    public void markAsArchived(String modifiedBy) {
        this.status = DLQStatus.ARCHIVED;
        this.lastModifiedBy = modifiedBy;
    }

    /**
     * Mark event as ignored (not a real error)
     * @param reason Why this event is being ignored
     * @param modifiedBy Username/service making the decision
     */
    public void markAsIgnored(String reason, String modifiedBy) {
        this.status = DLQStatus.IGNORED;
        this.analysisNotes = reason;
        this.lastModifiedBy = modifiedBy;
    }

    /**
     * Check if event is ready for reprocessing
     * @return true if status is FIXED or IGNORED
     */
    public boolean isReadyForReprocessing() {
        return status == DLQStatus.FIXED || status == DLQStatus.IGNORED;
    }

    /**
     * Check if event has been successfully handled
     * @return true if status is REPROCESSED
     */
    public boolean isSuccessful() {
        return status == DLQStatus.REPROCESSED;
    }

    /**
     * Check if event requires action
     * @return true if status is RECEIVED or ANALYZED
     */
    public boolean requiresAction() {
        return status == DLQStatus.RECEIVED || status == DLQStatus.ANALYZED;
    }

    /**
     * Check if reprocessing has been attempted too many times
     * @param maxAttempts Maximum allowed reprocessing attempts
     * @return true if reprocess_attempts >= maxAttempts
     */
    public boolean hasExceededMaxReprocessAttempts(int maxAttempts) {
        return reprocessAttempts >= maxAttempts;
    }
}
