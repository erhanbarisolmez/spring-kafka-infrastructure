package com.selftech.kafka.core.outbox;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * Outbox Event Entity - FAZE 2 ENTERPRISE
 *
 * Reliable event publishing pattern implementation.
 * Events are first persisted to database, then published to Kafka
 * asynchronously.
 *
 * Benefits:
 * 1. Database Transaction Atomicity: Event published + business operation in
 * same transaction
 * 2. Guaranteed Delivery: Even if Kafka is temporarily unavailable, events are
 * persisted
 * 3. Exactly-once Semantics: Outbox poller tracks published events, prevents
 * duplicates
 * 4. Audit Trail: Complete history of all published events in database
 * 5. Replay Capability: Failed events can be reprocessed from outbox
 *
 * Architecture:
 * ├─ Outbox: Events staged for publishing (published=false)
 * ├─ Published: Events successfully sent to Kafka (published=true)
 * └─ Poller: Async service that reads outbox and publishes to Kafka
 *
 * Database Table: outbox_events
 * - Indexed on (created_at, published) for efficient polling
 * - TTL: 7 days (configurable) after publication
 * - Partitioned by created_date for large scale
 *
 * Usage Pattern:
 * 1. Service creates business entity + saves to database
 * 2. Same transaction saves OutboxEvent via OutboxEventService
 * 3. Transaction commits (both business data + outbox event)
 * 4. OutboxPoller polls table every 1-5 seconds
 * 5. Publishes unpublished events to Kafka
 * 6. Marks event as published (published=true, published_at=now)
 * 7. Event stays in DB for audit trail (deleted after TTL)
 *
 * Example:
 * 
 * @Service
 * @Transactional
 *                public class LockBoxCreationService {
 *                public void createBox(CreateBoxRequest req) {
 *                LockBox box = repository.save(new LockBox(...));
 *
 *                LockBoxEvent event = LockBoxEvent.newBuilder()
 *                .boxCode(box.getBoxCode())
 *                .operation("CREATED")
 *                .build();
 *
 *                outboxEventService.saveEvent(
 *                topicConfig.getSmartLockEventBoxOperationV0(),
 *                box.getBoxCode(),
 *                event
 *                );
 *                // Both box + outbox event saved in same transaction
 *                }
 *                }
 */
@Entity
@Table(name = "outbox_events", indexes = {
    @Index(name = "idx_outbox_created_published", columnList = "created_at,published"),
    @Index(name = "idx_outbox_published", columnList = "published"),
    @Index(name = "idx_outbox_topic", columnList = "topic_name")
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class OutboxEvent implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Primary Key - Outbox Event ID (UUID)
   * Unique identifier for each outbox record
   */
  @Id
  @Column(name = "outbox_id", columnDefinition = "VARCHAR(36)")
  private String outboxId;

  /**
   * Event ID from domain event (UUID)
   * Links to the original domain event
   */
  @Column(name = "event_id", nullable = false, columnDefinition = "VARCHAR(36)")
  private String eventId;

  /**
   * Correlation ID for distributed tracing
   * Tracks related operations across services
   */
  @Column(name = "correlation_id", columnDefinition = "VARCHAR(36)")
  private String correlationId;

  /**
   * Kafka Topic Name
   * Where this event should be published
   * Example: "smartlock.event.boxOperation.v0"
   */
  @Column(name = "topic_name", nullable = false)
  private String topicName;

  /**
   * Partition Key
   * Used for partitioning in Kafka (e.g., boxCode, userId)
   */
  @Column(name = "partition_key", nullable = false)
  private String partitionKey;

  /**
   * Event Payload (JSON or serialized)
   * The actual event data to be published
   * Stored as JSONB for PostgreSQL native support
   */
  // @JdbcTypeCode(SqlTypes.JSON)
  @Column(name = "event_payload", nullable = false, columnDefinition = "text")
  private String eventPayload;

  /**
   * Event Type
   * Identifies the type of event (LockBoxCreated, PaymentInitiated, etc.)
   */
  @Column(name = "event_type")
  private String eventType;

  /**
   * Event Source
   * Which domain/service created this event (SMARTLOCK, SELFPARK, ANOMALY)
   */
  @Column(name = "event_source")
  private String eventSource;

  /**
   * Publication Status
   * false = pending (needs publishing to Kafka)
   * true = published (successfully sent to Kafka)
   * Default: false
   */
  @Column(name = "published", nullable = false)
  @Builder.Default
  private Boolean published = false;

  /**
   * When the event was published to Kafka
   * null if not yet published (published=false)
   */
  @Column(name = "published_at")
  private Instant publishedAt;

  /**
   * Number of publish attempts
   * Used for retry logic and debugging
   * Incremented each time poller tries to publish
   */
  @Column(name = "publish_attempts", nullable = false)
  @Builder.Default
  private Integer publishAttempts = 0;

  /**
   * Last error message if publishing failed
   * Helps with debugging failed publications
   */
  @Column(name = "last_error", columnDefinition = "TEXT")
  private String lastError;

  /**
   * When this outbox record was created
   * Used for polling and TTL management
   */
  @Column(name = "created_at", nullable = false, updatable = false)
  @Builder.Default
  private Instant createdAt = Instant.now();

  /**
   * When this outbox record was last updated
   * Tracks modification history
   */
  @Column(name = "updated_at")
  @Builder.Default
  private Instant updatedAt = Instant.now();

  /**
   * Pre-persist hook for auto-generating IDs and timestamps
   */
  @PrePersist
  protected void onCreate() {
    if (this.outboxId == null || this.outboxId.isBlank()) {
      this.outboxId = UUID.randomUUID().toString();
    }
    if (this.createdAt == null) {
      this.createdAt = Instant.now();
    }
    this.updatedAt = Instant.now();
  }

  /**
   * Pre-update hook for updating timestamp
   */
  @PreUpdate
  protected void onUpdate() {
    this.updatedAt = Instant.now();
  }

  /**
   * Check if event is ready to be published
   * 
   * @return true if not yet published and under max retry attempts
   */
  public boolean isReadyForPublishing() {
    return !published && publishAttempts < 5;
  }

  /**
   * Mark event as successfully published
   */
  public void markAsPublished() {
    this.published = true;
    this.publishedAt = Instant.now();
    this.lastError = null;
  }

  /**
   * Record a failed publish attempt
   * 
   * @param error Exception message
   */
  public void recordFailedAttempt(String error) {
    this.publishAttempts++;
    this.lastError = error;
    this.updatedAt = Instant.now();
  }

  /**
   * Check if event has exceeded max retry attempts
   * 
   * @return true if max retries exceeded
   */
  public boolean hasExceededMaxRetries() {
    return publishAttempts >= 5;
  }

  /**
   * Get human-readable description
   */
  @Override
  public String toString() {
    return String.format(
        "OutboxEvent{id=%s, eventId=%s, topic=%s, published=%s, attempts=%d, createdAt=%s}",
        outboxId, eventId, topicName, published, publishAttempts, createdAt);
  }
}
