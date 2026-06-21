package com.selftech.kafka.config;

import java.time.Duration;

/**
 * Topic Definition Record - Immutable Topic Metadata
 *
 * Type-safe representation of a Kafka topic configuration.
 *
 * Fields:
 * - name: Full topic name (e.g., "smartlock.event.payment.v1")
 * - category: Topic classification (EVENT, STREAM, METRIC, ANOMALY, ALERT)
 * - partitions: Number of partitions (parallelism)
 * - replicationFactor: Replication factor (durability)
 * - retention: Retention duration (data lifecycle)
 *
 * Usage:
 * TopicDefinition def = new TopicDefinition(
 *     "smartlock.event.payment.v1",
 *     TopicCategory.EVENT,
 *     3,
 *     (short) 3,
 *     Duration.ofDays(30)
 * );
 *
 * Benefits:
 * - Immutable (thread-safe)
 * - Type-safe (compile-time validation)
 * - Record syntax (concise, readable)
 * - Auto-generated equals/hashCode/toString
 */
public record TopicDefinition(
    String name,
    TopicCategory category,
    int partitions,
    short replicationFactor,
    Duration retention
) {
    /**
     * Validation constructor
     */
    public TopicDefinition {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be null or blank");
        }
        if (category == null) {
            throw new IllegalArgumentException("Topic category cannot be null");
        }
        if (partitions <= 0) {
            throw new IllegalArgumentException("Partitions must be positive: " + partitions);
        }
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("Replication factor must be positive: " + replicationFactor);
        }
        if (retention == null || retention.isNegative() || retention.isZero()) {
            throw new IllegalArgumentException("Retention must be positive duration");
        }
    }

    /**
     * Get retention in milliseconds (for Kafka config)
     */
    public long retentionMs() {
        return retention.toMillis();
    }

    /**
     * Get retention in days (for human-readable display)
     */
    public long retentionDays() {
        return retention.toDays();
    }
}
