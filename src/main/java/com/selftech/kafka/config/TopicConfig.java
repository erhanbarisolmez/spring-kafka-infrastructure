package com.selftech.kafka.config;

import lombok.Getter;
import lombok.Setter;

/**
 * Topic Config - YAML Binding Model
 *
 * Represents a single topic configuration loaded from application.yml.
 *
 * YAML Example:
 * app:
 *   kafka:
 *     topics:
 *       smartlock-payment:
 *         name: smartlock.event.payment.v1
 *         category: EVENT
 *         retention-days: 30
 *
 * Fields:
 * - name: Full topic name (e.g., "smartlock.event.payment.v1")
 * - category: Topic classification (EVENT, STREAM, METRIC, ANOMALY, ALERT)
 * - retentionDays: Retention period in days (e.g., 30)
 *
 * Usage:
 * Used by KafkaTopicRegistry for @ConfigurationProperties binding.
 */
@Getter
@Setter
public class TopicConfig {
    /**
     * Full topic name
     * Example: smartlock.event.payment.v1
     */
    private String name;

    /**
     * Topic category classification
     * Example: EVENT
     */
    private TopicCategory category;

    /**
     * Retention period in days
     * Example: 30
     */
    private int retentionDays;
}
