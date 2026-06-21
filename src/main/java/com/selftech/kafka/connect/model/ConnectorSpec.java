package com.selftech.kafka.connect.model;

import java.util.Map;

/**
 * Connector Specification Record
 *
 * Immutable representation of a Kafka Connect connector specification.
 * This is the JSON format sent to Kafka Connect REST API.
 *
 * Example JSON:
 * {
 *   "name": "elasticsearch-sink-smartlock-anomaly-sensor",
 *   "config": {
 *     "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
 *     "topics": "smartlock.anomaly.sensorData.v0",
 *     "connection.url": "http://elasticsearch:9200",
 *     "batch.size": "500",
 *     ...
 *   }
 * }
 *
 * @param name Connector name (unique identifier)
 * @param config Connector configuration map
 */
public record ConnectorSpec(
    String name,
    Map<String, Object> config
) {
    /**
     * Validation constructor
     */
    public ConnectorSpec {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Connector name cannot be null or blank");
        }
        if (config == null || config.isEmpty()) {
            throw new IllegalArgumentException("Connector config cannot be null or empty");
        }
    }
}
