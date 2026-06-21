package com.selftech.kafka.connect.model;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Connector Status Response
 *
 * Represents the status response from Kafka Connect REST API.
 * Used to verify connector health after reconciliation.
 *
 * Example JSON Response:
 * {
 *   "name": "elasticsearch-sink-smartlock-anomaly-sensor",
 *   "connector": {
 *     "state": "RUNNING",
 *     "worker_id": "kafka-connect:8083"
 *   },
 *   "tasks": [
 *     {
 *       "id": 0,
 *       "state": "RUNNING",
 *       "worker_id": "kafka-connect:8083"
 *     }
 *   ]
 * }
 *
 * Possible States:
 * - RUNNING: Connector is running normally
 * - FAILED: Connector has failed
 * - PAUSED: Connector is paused
 * - UNASSIGNED: Connector is not assigned to a worker
 *
 * @param name Connector name
 * @param state Connector state (RUNNING, FAILED, PAUSED, UNASSIGNED)
 * @param connector Connector info map
 * @param tasks Task info list
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ConnectorStatus(
    @JsonProperty("name")
    String name,

    @JsonProperty("connector")
    Map<String, Object> connector,

    @JsonProperty("tasks")
    Object tasks
) {
    /**
     * Get connector state
     * @return State string (RUNNING, FAILED, PAUSED, UNASSIGNED)
     */
    public String state() {
        if (connector == null) {
            return "UNKNOWN";
        }
        return (String) connector.getOrDefault("state", "UNKNOWN");
    }

    /**
     * Check if connector is running
     * @return true if state is RUNNING
     */
    public boolean isRunning() {
        return "RUNNING".equals(state());
    }

    /**
     * Check if connector is healthy (running)
     * @return true if state is RUNNING
     */
    public boolean isHealthy() {
        return isRunning();
    }
}
