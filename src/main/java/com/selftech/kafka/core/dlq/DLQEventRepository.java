package com.selftech.kafka.core.dlq;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * DLQ Event Repository - FAZE 4
 *
 * Data access layer for DLQEvent persistence.
 * Provides optimized queries for DLQ management operations.
 *
 * Key Operations:
 * - Retrieve events by status for dashboard display
 * - Find events requiring action (RECEIVED, ANALYZED)
 * - Batch update status transitions
 * - Find failed reprocessing candidates
 * - Cleanup archived events for archival
 *
 * Indexing Strategy:
 * - (status, created_at): For finding events needing action
 * - (event_id): For idempotency checks
 * - (topic_name): For topic-specific analysis
 * - (correlation_id): For request tracing
 * - (error_category): For error pattern analysis
 * - (created_at): For time-based queries
 *
 * @author FAZE 4 Implementation
 */
@Repository
public interface DLQEventRepository extends JpaRepository<DLQEvent, String> {

    // ====== Retrieval Queries ======

    /**
     * Find all DLQ events with specific status (paginated)
     * Used by DLQ dashboard to display events by status
     *
     * @param status Event status
     * @param pageable Pagination params (page, size, sort)
     * @return Page of DLQ events
     */
    Page<DLQEvent> findByStatusOrderByCreatedAtDesc(DLQEvent.DLQStatus status, Pageable pageable);

    /**
     * Find all events that require action (RECEIVED or ANALYZED)
     * Priority list for operations team
     *
     * @param pageable Pagination params
     * @return Page of events needing action
     */
    @Query("SELECT d FROM DLQEvent d WHERE d.status IN ('RECEIVED', 'ANALYZED') ORDER BY d.createdAt ASC")
    Page<DLQEvent> findEventsRequiringAction(Pageable pageable);

    /**
     * Find all events that are ready for reprocessing (FIXED or IGNORED)
     * Used by automatic reprocessing scheduler
     *
     * @param pageable Pagination params
     * @return Page of events ready for reprocessing
     */
    @Query("SELECT d FROM DLQEvent d WHERE d.status IN ('FIXED', 'IGNORED') ORDER BY d.createdAt ASC")
    Page<DLQEvent> findEventsReadyForReprocessing(Pageable pageable);

    /**
     * Find failed events that have been reprocessed many times
     * Candidates for manual review or archival
     *
     * @param maxAttempts Threshold for reprocess attempts
     * @param pageable Pagination params
     * @return Page of events that exceeded reprocessing threshold
     */
    @Query("SELECT d FROM DLQEvent d WHERE d.reprocessAttempts >= :maxAttempts ORDER BY d.reprocessAttempts DESC, d.createdAt ASC")
    Page<DLQEvent> findFailedReprocessingEvents(@Param("maxAttempts") int maxAttempts, Pageable pageable);

    /**
     * Find DLQ events by error category (for pattern analysis)
     * Operations team uses this to identify systemic issues
     *
     * @param errorCategory Error category to search for
     * @param pageable Pagination params
     * @return Page of events with matching error category
     */
    Page<DLQEvent> findByErrorCategoryOrderByCreatedAtDesc(DLQEvent.ErrorCategory errorCategory, Pageable pageable);

    /**
     * Find DLQ events by topic name (for domain-specific analysis)
     *
     * @param topicName Topic name to search for
     * @param pageable Pagination params
     * @return Page of events from specific topic
     */
    Page<DLQEvent> findByTopicNameOrderByCreatedAtDesc(String topicName, Pageable pageable);

    /**
     * Find DLQ events by domain (smartlock, selfpark, etc)
     * For cross-topic domain analysis
     *
     * @param domain Domain name
     * @param pageable Pagination params
     * @return Page of events from specific domain
     */
    Page<DLQEvent> findByDomainOrderByCreatedAtDesc(String domain, Pageable pageable);

    /**
     * Find DLQ events by failed handler name
     * Helps identify problematic handlers
     *
     * @param handlerName Handler class name
     * @param pageable Pagination params
     * @return Page of events from specific handler
     */
    Page<DLQEvent> findByFailedHandlerOrderByCreatedAtDesc(String handlerName, Pageable pageable);

    /**
     * Find events created within time range
     * Used for time-based analysis and reporting
     *
     * @param startTime Start of time range
     * @param endTime End of time range
     * @param pageable Pagination params
     * @return Page of events in time range
     */
    @Query("SELECT d FROM DLQEvent d WHERE d.createdAt BETWEEN :startTime AND :endTime ORDER BY d.createdAt DESC")
    Page<DLQEvent> findByCreatedAtBetween(
        @Param("startTime") Instant startTime,
        @Param("endTime") Instant endTime,
        Pageable pageable
    );

    /**
     * Find event by original event ID (idempotency check)
     * Ensures same event isn't processed twice in DLQ
     *
     * @param eventId Original event ID
     * @return Optional containing event if found
     */
    Optional<DLQEvent> findByEventId(String eventId);

    /**
     * Check if event already exists in DLQ
     *
     * @param eventId Original event ID
     * @return true if event exists in DLQ
     */
    boolean existsByEventId(String eventId);

    // ====== Statistics and Monitoring ======

    /**
     * Count total DLQ events
     * Used for monitoring and dashboard
     *
     * @return Total number of DLQ events
     */
    long count();

    /**
     * Count events with specific status
     * Useful for dashboard metrics
     *
     * @param status Event status
     * @return Count of events with that status
     */
    long countByStatus(DLQEvent.DLQStatus status);

    /**
     * Count events requiring action (RECEIVED or ANALYZED)
     * High-level metric for operations team
     *
     * @return Count of events needing attention
     */
    @Query("SELECT COUNT(d) FROM DLQEvent d WHERE d.status IN ('RECEIVED', 'ANALYZED')")
    long countEventsRequiringAction();

    /**
     * Count events by error category
     * For identifying patterns
     *
     * @param errorCategory Error category
     * @return Count of events with that error category
     */
    long countByErrorCategory(DLQEvent.ErrorCategory errorCategory);

    /**
     * Count events by topic
     * For understanding which topics have issues
     *
     * @param topicName Topic name
     * @return Count of DLQ events from that topic
     */
    long countByTopicName(String topicName);

    /**
     * Count events by domain
     * For cross-domain analysis
     *
     * @param domain Domain name
     * @return Count of DLQ events from that domain
     */
    long countByDomain(String domain);

    /**
     * Find recently archived events (for audit trail export)
     *
     * @param limit Number of recent archived events to retrieve
     * @return List of recently archived events
     */
    @Query(value = "SELECT d FROM DLQEvent d WHERE d.status = 'ARCHIVED' ORDER BY d.updatedAt DESC LIMIT :limit")
    List<DLQEvent> findRecentlyArchivedEvents(@Param("limit") int limit);

    // ====== Update Queries ======

    /**
     * Batch update status for multiple DLQ events
     * Efficient status transition for bulk operations
     *
     * @param dlqIds List of DLQ event IDs
     * @param newStatus New status
     * @param modifiedBy Username/service making the change
     * @param updatedAt Timestamp of update
     * @return Number of rows updated
     */
    @Modifying
    @Transactional
    @Query("UPDATE DLQEvent d SET d.status = :newStatus, d.lastModifiedBy = :modifiedBy, d.updatedAt = :updatedAt WHERE d.dlqId IN :dlqIds")
    int updateStatusBatch(
        @Param("dlqIds") List<String> dlqIds,
        @Param("newStatus") DLQEvent.DLQStatus newStatus,
        @Param("modifiedBy") String modifiedBy,
        @Param("updatedAt") Instant updatedAt
    );

    /**
     * Increment reprocess attempts counter
     * Called when reprocessing is attempted
     *
     * @param dlqId DLQ event ID
     * @param timestamp When this update occurred
     */
    @Modifying
    @Transactional
    @Query("UPDATE DLQEvent d SET d.reprocessAttempts = d.reprocessAttempts + 1, d.updatedAt = :timestamp WHERE d.dlqId = :dlqId")
    void incrementReprocessAttempts(@Param("dlqId") String dlqId, @Param("timestamp") Instant timestamp);

    /**
     * Mark events as reprocessed in batch
     *
     * @param dlqIds List of DLQ event IDs
     * @param modifiedBy Username/service making the change
     * @param reprocessedAt Timestamp of successful reprocessing
     * @return Number of rows updated
     */
    @Modifying
    @Transactional
    @Query("UPDATE DLQEvent d SET d.status = 'REPROCESSED', d.reprocessedAt = :reprocessedAt, d.lastModifiedBy = :modifiedBy, d.updatedAt = :reprocessedAt WHERE d.dlqId IN :dlqIds")
    int markAsReprocessedBatch(
        @Param("dlqIds") List<String> dlqIds,
        @Param("reprocessedAt") Instant reprocessedAt,
        @Param("modifiedBy") String modifiedBy
    );

    /**
     * Update analysis notes and mark as analyzed
     *
     * @param dlqId DLQ event ID
     * @param notes Analysis notes
     * @param modifiedBy Username/service making the change
     * @return Number of rows updated
     */
    @Modifying
    @Transactional
    @Query("UPDATE DLQEvent d SET d.analysisNotes = :notes, d.status = 'ANALYZED', d.lastModifiedBy = :modifiedBy, d.updatedAt = CURRENT_TIMESTAMP WHERE d.dlqId = :dlqId")
    int updateAnalysisNotes(
        @Param("dlqId") String dlqId,
        @Param("notes") String notes,
        @Param("modifiedBy") String modifiedBy
    );

    // ====== Cleanup Queries ======

    /**
     * Find archived events older than specified date (for archival/deletion)
     * Part of TTL/retention policy
     *
     * @param beforeInstant Events archived before this instant
     * @param pageable Pagination params
     * @return Page of archived events ready for long-term storage
     */
    @Query("SELECT d FROM DLQEvent d WHERE d.status = 'ARCHIVED' AND d.updatedAt < :beforeInstant ORDER BY d.updatedAt ASC")
    Page<DLQEvent> findArchivedEventsBefore(@Param("beforeInstant") Instant beforeInstant, Pageable pageable);

    /**
     * Delete archived events older than specified date
     * Used for data retention policy
     *
     * @param beforeInstant Delete archived events before this instant
     * @return Number of rows deleted
     */
    @Modifying
    @Transactional
    @Query("DELETE FROM DLQEvent d WHERE d.status = 'ARCHIVED' AND d.updatedAt < :beforeInstant")
    int deleteArchivedEventsBefore(@Param("beforeInstant") Instant beforeInstant);

    /**
     * Find all events (for full export/backup)
     * Used for DLQ data export and archival
     *
     * @param pageable Pagination params
     * @return All DLQ events
     */
    @Query("SELECT d FROM DLQEvent d ORDER BY d.createdAt DESC")
    Page<DLQEvent> findAllEvents(Pageable pageable);
}
