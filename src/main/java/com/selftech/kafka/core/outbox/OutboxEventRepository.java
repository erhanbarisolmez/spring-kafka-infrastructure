package com.selftech.kafka.core.outbox;

import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * OutboxEvent Repository - FAZE 2 ENTERPRISE
 *
 * Data access layer for outbox event persistence and querying.
 * Optimized for polling and efficient batch processing.
 *
 * Responsibilities:
 * 1. Save outbox events (from domain services via OutboxEventService)
 * 2. Query unpublished events for poller
 * 3. Batch update published status
 * 4. Query failed events for DLQ processing
 * 5. Clean up old published events (TTL management)
 *
 * Performance Optimizations:
 * - Indexed columns: (created_at, published) for polling
 * - Batch operations for bulk updates
 * - Pagination for memory efficiency with large datasets
 * - Native queries for complex operations
 */
@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {

    /**
     * Find unpublished outbox events for processing
     * Ordered by creation time (FIFO)
     *
     * @param pageable Pagination for memory efficiency
     * @return List of unpublished events ready for publishing
     *
     * Usage:
     * Pageable page = PageRequest.of(0, 100);
     * List<OutboxEvent> events = repository.findUnpublishedEvents(page);
     */
    @Query("SELECT e FROM OutboxEvent e WHERE e.published = false ORDER BY e.createdAt ASC")
    List<OutboxEvent> findUnpublishedEvents(Pageable pageable);

    /**
     * Find events for a specific topic that haven't been published
     *
     * @param topicName Topic name (e.g., "smartlock.event.boxOperation.v0")
     * @param pageable Pagination
     * @return Unpublished events for the topic
     */
    @Query("SELECT e FROM OutboxEvent e WHERE e.published = false AND e.topicName = :topicName ORDER BY e.createdAt ASC")
    List<OutboxEvent> findUnpublishedEventsByTopic(
        @Param("topicName") String topicName,
        Pageable pageable
    );

    /**
     * Find events that failed to publish (exceeded retry attempts)
     * These should be routed to DLQ for manual intervention
     *
     * @param maxAttempts Maximum retry attempts allowed
     * @param pageable Pagination
     * @return Failed events that need DLQ processing
     */
    @Query("SELECT e FROM OutboxEvent e WHERE e.published = false AND e.publishAttempts >= :maxAttempts ORDER BY e.createdAt ASC")
    List<OutboxEvent> findFailedEvents(
        @Param("maxAttempts") Integer maxAttempts,
        Pageable pageable
    );

    /**
     * Find events older than specified time (for cleanup/TTL management)
     * Events that are published and older than TTL should be archived/deleted
     *
     * @param beforeInstant Cutoff time (e.g., Instant.now().minus(7, ChronoUnit.DAYS))
     * @param pageable Pagination
     * @return Old published events eligible for deletion
     */
    @Query("SELECT e FROM OutboxEvent e WHERE e.published = true AND e.publishedAt < :beforeInstant ORDER BY e.publishedAt ASC")
    List<OutboxEvent> findOldPublishedEvents(
        @Param("beforeInstant") Instant beforeInstant,
        Pageable pageable
    );

    /**
     * Count unpublished events (for monitoring)
     *
     * @return Number of events waiting to be published
     */
    @Query("SELECT COUNT(e) FROM OutboxEvent e WHERE e.published = false")
    long countUnpublishedEvents();

    /**
     * Count unpublished events for a specific topic
     *
     * @param topicName Topic name
     * @return Number of unpublished events for the topic
     */
    @Query("SELECT COUNT(e) FROM OutboxEvent e WHERE e.published = false AND e.topicName = :topicName")
    long countUnpublishedEventsByTopic(@Param("topicName") String topicName);

    /**
     * Mark events as published in batch
     * Called after successful Kafka publication
     *
     * @param outboxIds List of outbox IDs to mark as published
     * @return Number of rows updated
     */
    @Modifying
    @Transactional
    @Query("UPDATE OutboxEvent e SET e.published = true, e.publishedAt = :publishedAt WHERE e.outboxId IN :outboxIds")
    int markAsPublished(
        @Param("outboxIds") List<String> outboxIds,
        @Param("publishedAt") Instant publishedAt
    );

    /**
     * Increment publish attempts for failed events
     * Called when publishing to Kafka fails
     *
     * @param outboxId Outbox event ID
     * @param errorMessage Error message from exception
     * @return Number of rows updated
     */
    @Modifying
    @Transactional
    @Query("UPDATE OutboxEvent e SET e.publishAttempts = e.publishAttempts + 1, e.lastError = :errorMessage, e.updatedAt = :updatedAt WHERE e.outboxId = :outboxId")
    int incrementPublishAttempts(
        @Param("outboxId") String outboxId,
        @Param("errorMessage") String errorMessage,
        @Param("updatedAt") Instant updatedAt
    );

    /**
     * Delete old published events (cleanup)
     * Called by scheduled task to manage database size
     *
     * @param beforeInstant Cutoff time
     * @return Number of rows deleted
     */
    @Modifying
    @Transactional
    @Query("DELETE FROM OutboxEvent e WHERE e.published = true AND e.publishedAt < :beforeInstant")
    int deleteOldPublishedEvents(@Param("beforeInstant") Instant beforeInstant);

    /**
     * Check if event with specific eventId already exists
     * Used to prevent duplicate outbox entries for same event
     *
     * @param eventId Event ID
     * @return true if event exists
     */
    @Query("SELECT CASE WHEN COUNT(e) > 0 THEN true ELSE false END FROM OutboxEvent e WHERE e.eventId = :eventId")
    boolean existsByEventId(@Param("eventId") String eventId);

    /**
     * Find event by event ID (for idempotency checks)
     *
     * @param eventId Event ID
     * @return Existing outbox event or null
     */
    @Query("SELECT e FROM OutboxEvent e WHERE e.eventId = :eventId")
    OutboxEvent findByEventId(@Param("eventId") String eventId);
}
