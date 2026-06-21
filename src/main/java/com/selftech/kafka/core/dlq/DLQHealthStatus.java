package com.selftech.kafka.core.dlq;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DLQ Health Status - FAZE 4
 *
 * Monitoring and metrics data structure for DLQ health.
 * Provides snapshot of current DLQ state for dashboard and alerts.
 *
 * Usage:
 * - Exposed via REST endpoint: GET /api/dlq/health
 * - Used by monitoring dashboards (Grafana, etc)
 * - Triggers alerts when thresholds exceeded (e.g., receivedEvents > 100)
 *
 * Status Interpretation:
 * - HEALTHY: eventsRequiringAction == 0, all events being processed
 * - WARNING: eventsRequiringAction > 0 but < 100, minor issues
 * - CRITICAL: eventsRequiringAction >= 100 or processingStalled
 *
 * @author FAZE 4 Implementation
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DLQHealthStatus {

    /**
     * Total number of events in DLQ (all statuses)
     */
    private long totalEvents;

    /**
     * Events with RECEIVED status (just arrived, not yet analyzed)
     * Should be small number, operations team should analyze quickly
     */
    private long receivedEvents;

    /**
     * Events with ANALYZED status (root cause identified)
     * Waiting for fix to be applied
     */
    private long analyzedEvents;

    /**
     * Events with FIXED status (ready for reprocessing)
     * Waiting for automatic/manual reprocessing
     */
    private long fixedEvents;

    /**
     * Events currently being reprocessed
     */
    private long reprocessingEvents;

    /**
     * Events successfully reprocessed (resolved)
     */
    private long reprocessedEvents;

    /**
     * Events moved to archive storage (for long-term retention)
     */
    private long archivedEvents;

    /**
     * Events marked as ignored (not real errors)
     */
    private long ignoredEvents;

    /**
     * Count of events requiring action (RECEIVED or ANALYZED)
     * Key metric for monitoring - should be kept low
     * Alert threshold: > 100
     */
    private long eventsRequiringAction;

    /**
     * Maximum reprocessing attempts allowed per event
     */
    private int maxReprocessAttempts;

    /**
     * When this status was captured
     */
    private Instant timestamp;

    // ====== Computed Properties ======

    /**
     * Check if DLQ is in healthy state
     * HEALTHY: No events requiring action
     *
     * @return true if eventsRequiringAction == 0
     */
    public boolean isHealthy() {
        return eventsRequiringAction == 0;
    }

    /**
     * Check if DLQ requires attention
     * WARNING: Some events needing action but manageable
     *
     * @return true if eventsRequiringAction > 0 and < 100
     */
    public boolean requiresAttention() {
        return eventsRequiringAction > 0 && eventsRequiringAction < 100;
    }

    /**
     * Check if DLQ is in critical state
     * CRITICAL: Many events needing action
     *
     * @return true if eventsRequiringAction >= 100
     */
    public boolean isCritical() {
        return eventsRequiringAction >= 100;
    }

    /**
     * Get human-readable status
     *
     * @return Status string: "HEALTHY", "WARNING", or "CRITICAL"
     */
    public String getStatusString() {
        if (isHealthy()) {
            return "HEALTHY";
        } else if (requiresAttention()) {
            return "WARNING";
        } else {
            return "CRITICAL";
        }
    }

    /**
     * Get processing success rate
     * (reprocessedEvents + ignoredEvents) / totalEvents
     *
     * @return Success rate as percentage (0-100)
     */
    public double getSuccessRate() {
        if (totalEvents == 0) {
            return 100.0;  // No events = 100% success
        }
        long processed = reprocessedEvents + ignoredEvents;
        return (processed * 100.0) / totalEvents;
    }

    /**
     * Get pending processing rate
     * (fixedEvents + reprocessingEvents) / totalEvents
     *
     * @return Pending rate as percentage (0-100)
     */
    public double getPendingRate() {
        if (totalEvents == 0) {
            return 0.0;
        }
        long pending = fixedEvents + reprocessingEvents;
        return (pending * 100.0) / totalEvents;
    }

    /**
     * Get analysis completion rate
     * (analyzedEvents + fixedEvents + reprocessedEvents + ignoredEvents) / totalEvents
     *
     * @return Analysis rate as percentage (0-100)
     */
    public double getAnalysisCompletionRate() {
        if (totalEvents == 0) {
            return 100.0;  // No events = complete
        }
        long analyzed = analyzedEvents + fixedEvents + reprocessedEvents + ignoredEvents;
        return (analyzed * 100.0) / totalEvents;
    }

    /**
     * Get recommended action
     * Based on current state
     *
     * @return Recommendation string
     */
    public String getRecommendedAction() {
        if (isHealthy()) {
            return "Monitor DLQ health periodically";
        } else if (eventsRequiringAction < 10) {
            return "Review received/analyzed events";
        } else if (eventsRequiringAction < 50) {
            return "Expedite analysis and fixes";
        } else {
            return "URGENT: Immediate action required, DLQ backlog critical";
        }
    }

    @Override
    public String toString() {
        return String.format(
            "DLQHealthStatus{status=%s, total=%d, requiring_action=%d, " +
            "received=%d, analyzed=%d, fixed=%d, reprocessing=%d, " +
            "reprocessed=%d, ignored=%d, archived=%d, success_rate=%.1f%%, timestamp=%s}",
            getStatusString(), totalEvents, eventsRequiringAction,
            receivedEvents, analyzedEvents, fixedEvents, reprocessingEvents,
            reprocessedEvents, ignoredEvents, archivedEvents, getSuccessRate(), timestamp
        );
    }
}
