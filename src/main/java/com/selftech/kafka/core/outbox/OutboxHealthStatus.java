package com.selftech.kafka.core.outbox;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Outbox Health Status - FAZE 2 ENTERPRISE
 *
 * Monitoring data for outbox system health and metrics.
 * Used for observability and alerting.
 *
 * Status Levels:
 * - HEALTHY: Unpublished count < 1000, No failed events
 * - WARN: Unpublished count >= 1000 OR some failed events
 * - ERROR: Exception occurred during health check
 *
 * Usage:
 * OutboxHealthStatus status = outboxEventService.getHealthStatus();
 * if ("WARN".equals(status.getStatus())) {
 *     alerting.sendAlert("Outbox backlog detected");
 * }
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class OutboxHealthStatus {

    /**
     * Number of unpublished events pending Kafka delivery
     * Target: < 100
     * Warning threshold: >= 1000
     * Critical threshold: >= 10000
     */
    private Long unpublishedEventCount;

    /**
     * Number of events that exceeded max retry attempts
     * These events are stuck and need manual intervention
     * Target: 0
     * Warning: > 0
     * Critical: >= 10
     */
    private Long failedEventCount;

    /**
     * Maximum retry attempts before marking event as failed
     * Default: 5
     */
    private Integer maxRetryAttempts;

    /**
     * Cleanup TTL in days for published events
     * Events older than this are eligible for deletion
     * Default: 7 days
     */
    private Long cleanupTtlDays;

    /**
     * Batch size used by poller
     * Number of events retrieved per polling cycle
     * Default: 100
     */
    private Integer batchSize;

    /**
     * Overall health status
     * Values: HEALTHY, WARN, ERROR
     */
    private String status;

    /**
     * Timestamp when this status was generated
     */
    private Instant timestamp;

    /**
     * Check if outbox is healthy
     * @return true if status is HEALTHY
     */
    public boolean isHealthy() {
        return "HEALTHY".equals(status);
    }

    /**
     * Check if there are issues
     * @return true if status is WARN or ERROR
     */
    public boolean hasIssues() {
        return !isHealthy();
    }

    /**
     * Get human-readable description
     */
    @Override
    public String toString() {
        return String.format(
            "OutboxHealthStatus{status=%s, unpublished=%d, failed=%d, maxRetries=%d, timestamp=%s}",
            status, unpublishedEventCount, failedEventCount, maxRetryAttempts, timestamp
        );
    }
}
