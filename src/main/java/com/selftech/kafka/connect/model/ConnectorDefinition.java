package com.selftech.kafka.connect.model;

import java.util.Map;

import lombok.Getter;
import lombok.Setter;

/**
 * Connector Definition Model
 *
 * Represents a single Elasticsearch sink connector configuration from YAML.
 *
 * YAML Example:
 * app:
 *   kafka:
 *     connect:
 *       elasticsearch:
 *         definitions:
 *           smartlock-anomaly-sensor:
 *             enabled: true
 *             source-topic-ref: smartlock-anomaly-sensor-data
 *             index-name: anomalies-smartlock-sensor
 *             id-field: anomalyId
 *             tasks-max: 2
 *             flush-size: 500
 */
@Getter
@Setter
public class ConnectorDefinition {

    /**
     * Enable/disable this connector
     * Default: true
     */
    private boolean enabled = true;

    /**
     * Reference to topic key in KafkaTopicRegistry
     * Example: "smartlock-anomaly-sensor-data"
     *
     * This will be resolved to actual topic name:
     * "smartlock.anomaly.sensorData.v0"
     */
    private String sourceTopicRef;

    /**
     * Elasticsearch index name
     * Example: "anomalies-smartlock-sensor"
     */
    private String indexName;

    /**
     * Optional: Document ID field name
     * If specified, this field from the event will be used as Elasticsearch document ID
     * Example: "anomalyId"
     */
    private String idField;

    /**
     * Optional: Override default tasks-max
     * If null, uses default from ElasticsearchConnectorProperties.defaults.tasksMax
     */
    private Integer tasksMax;

    /**
     * Optional: Override default flush-size
     * If null, uses default from ElasticsearchConnectorProperties.defaults.flushSize
     */
    private Integer flushSize;

    /**
     * Optional: Custom Single Message Transforms (SMTs)
     * Example:
     * transforms:
     *   transforms: "insertTimestamp,route"
     *   transforms.insertTimestamp.type: "org.apache.kafka.connect.transforms.InsertField$Value"
     *   transforms.insertTimestamp.timestamp.field: "@timestamp"
     */
    private Map<String, String> transforms;

    /**
     * Validation: Ensure required fields are set
     */
    public void validate() {
        if (sourceTopicRef == null || sourceTopicRef.isBlank()) {
            throw new IllegalArgumentException("sourceTopicRef is required");
        }
        if (indexName == null || indexName.isBlank()) {
            throw new IllegalArgumentException("indexName is required");
        }
    }
}
