package com.selftech.kafka.config;

/**
 * Topic Category Enum - Enterprise Topic Classification
 *
 * Categorizes Kafka topics for better organization and management.
 *
 * Categories:
 * - EVENT: Domain events (payments, entries, exits, transactions)
 * - STREAM: Real-time data streams (MQTT sensor data)
 * - METRIC: Metrics and statistics (anomaly metrics, performance metrics)
 * - ANOMALY: Anomaly detection results (alerts, detections)
 * - ALERT: Critical alerts and notifications (system alerts, user alerts)
 *
 * Usage:
 * app:
 *   kafka:
 *     topics:
 *       smartlock-payment:
 *         category: EVENT
 *
 * Benefits:
 * - Clear topic organization
 * - Retention policy per category
 * - Monitoring and alerting per category
 * - Documentation and discoverability
 */
public enum TopicCategory {
    /**
     * Domain Events
     * Examples: payment events, park entry/exit, transactions
     * Typical retention: 30 days
     */
    EVENT,

    /**
     * Real-time Data Streams
     * Examples: MQTT sensor data, device telemetry
     * Typical retention: 7 days (raw data)
     */
    STREAM,

    /**
     * Metrics and Statistics
     * Examples: performance metrics, aggregated statistics
     * Typical retention: 90 days
     */
    METRIC,

    /**
     * Anomaly Detection Results
     * Examples: detected anomalies, anomaly scores
     * Typical retention: 90 days (compliance)
     */
    ANOMALY,

    /**
     * Critical Alerts and Notifications
     * Examples: system alerts, security alerts
     * Typical retention: 365 days (audit trail)
     */
    ALERT
}
