package com.selftech.kafka.connect.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;

/**
 * Reconciliation Result
 *
 * Tracks the outcome of a connector reconciliation cycle.
 * Used for logging and metrics reporting.
 *
 * Example Usage:
 * ReconciliationResult result = new ReconciliationResult();
 * result.addCreated("elasticsearch-sink-smartlock-anomaly");
 * result.addHealthy("elasticsearch-sink-smartlock-anomaly");
 * result.hasChanges() → true
 *
 * @see com.selftech.kafka.connect.reconciler.ConnectorReconciler
 */
@Getter
public class ReconciliationResult {

    private final List<String> created = new ArrayList<>();
    private final List<String> updated = new ArrayList<>();
    private final List<String> deleted = new ArrayList<>();
    private final List<String> healthy = new ArrayList<>();
    private final Map<String, String> unhealthy = new HashMap<>();
    private final Map<String, String> failed = new HashMap<>();

    /**
     * Add created connector
     */
    public void addCreated(String connectorName) {
        created.add(connectorName);
    }

    /**
     * Add updated connector
     */
    public void addUpdated(String connectorName) {
        updated.add(connectorName);
    }

    /**
     * Add deleted connector
     */
    public void addDeleted(String connectorName) {
        deleted.add(connectorName);
    }

    /**
     * Add healthy connector
     */
    public void addHealthy(String connectorName) {
        healthy.add(connectorName);
    }

    /**
     * Add unhealthy connector with reason
     */
    public void addUnhealthy(String connectorName, String reason) {
        unhealthy.put(connectorName, reason);
    }

    /**
     * Add failed operation with error message
     */
    public void addFailed(String connectorName, String errorMessage) {
        failed.put(connectorName, errorMessage);
    }

    /**
     * Check if any changes were made (created, updated, or deleted)
     */
    public boolean hasChanges() {
        return !created.isEmpty() || !updated.isEmpty() || !deleted.isEmpty();
    }

    /**
     * Get total number of connectors managed
     */
    public int getTotalManaged() {
        return healthy.size() + unhealthy.size();
    }

    /**
     * Check if reconciliation was successful (no failures, all healthy)
     */
    public boolean isSuccessful() {
        return failed.isEmpty() && unhealthy.isEmpty();
    }

    @Override
    public String toString() {
        return String.format(
            "ReconciliationResult{created=%d, updated=%d, deleted=%d, healthy=%d, unhealthy=%d, failed=%d}",
            created.size(), updated.size(), deleted.size(),
            healthy.size(), unhealthy.size(), failed.size()
        );
    }
}
