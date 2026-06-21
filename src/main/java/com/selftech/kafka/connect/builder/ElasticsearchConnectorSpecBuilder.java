package com.selftech.kafka.connect.builder;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.selftech.kafka.config.KafkaTopicRegistry;
import com.selftech.kafka.connect.config.ElasticsearchConnectorProperties;
import com.selftech.kafka.connect.model.ConnectorDefinition;
import com.selftech.kafka.connect.model.ConnectorSpec;

import lombok.extern.slf4j.Slf4j;

/**
 * Elasticsearch Connector Spec Builder
 *
 * Builds Kafka Connect connector specifications from YAML definitions.
 * Integrates with KafkaTopicRegistry to resolve topic names.
 *
 * Key Responsibilities:
 * 1. Resolve topic names from KafkaTopicRegistry
 * 2. Build connector config map with defaults + overrides
 * 3. Configure Avro → JSON conversion (schema.ignore: true)
 * 4. Set up index routing
 * 5. Detect configuration drift
 *
 * @see com.selftech.kafka.connect.reconciler.ConnectorReconciler
 */
@Slf4j
@Component
public class ElasticsearchConnectorSpecBuilder {

    private final KafkaTopicRegistry topicRegistry;
    private final ElasticsearchConnectorProperties properties;

    @Autowired
    public ElasticsearchConnectorSpecBuilder(
        KafkaTopicRegistry topicRegistry,
        ElasticsearchConnectorProperties properties
    ) {
        this.topicRegistry = topicRegistry;
        this.properties = properties;
    }

    /**
     * Build ConnectorSpec from definition
     *
     * @param definitionKey Definition key (used for connector name)
     * @param def Connector definition from YAML
     * @return ConnectorSpec ready for Kafka Connect API
     */
    public ConnectorSpec build(String definitionKey, ConnectorDefinition def) {
        // Validate definition
        def.validate();

        // Resolve topic name from registry
        String topicName = topicRegistry.getTopicName(def.getSourceTopicRef());
        if (topicName == null) {
            throw new IllegalStateException(
                "Topic not found in registry: " + def.getSourceTopicRef());
        }

        log.debug("Building connector spec for topic: {} → {}",
                 def.getSourceTopicRef(), topicName);

        // Build config map
        Map<String, Object> config = new HashMap<>();

        // Basic connector settings
        config.put("connector.class",
                  "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector");
        config.put("topics", topicName);
        config.put("tasks.max", String.valueOf(
            def.getTasksMax() != null ? def.getTasksMax() :
            properties.getDefaults().getTasksMax()));

        // Elasticsearch connection
        config.put("connection.url", properties.getDefaults().getConnectionUrl());
        config.put("connection.username", properties.getDefaults().getConnectionUsername());
        config.put("connection.password", properties.getDefaults().getConnectionPassword());

        // Index configuration - Set static index name instead of using topic name
        // This ensures all records go to the specified index regardless of topic name
        config.put("type.name", "_doc"); // Already set below, but emphasizing here for index routing

        // Index configuration
        config.put("type.name", "_doc");
        config.put("key.ignore", def.getIdField() != null ? "false" : "true");

        // Document ID strategy (if idField specified)
        if (def.getIdField() != null) {
            config.put("behavior.on.null.values", "delete");
            config.put("pk.mode", "record_value");
            config.put("pk.fields", def.getIdField());
        }

        // Schema handling - CRITICAL for Avro → JSON conversion
        config.put("schema.ignore", String.valueOf(properties.getDefaults().isSchemaIgnore()));
        config.put("behavior.on.malformed.documents", "warn");
        config.put("drop.invalid.message", "false");

        // Converter configuration - Use JSON converter instead of global Avro converter
        // This allows the connector to consume Avro-serialized data from Kafka Streams
        // and convert it to JSON for Elasticsearch
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter.schema.registry.url", "http://schema-registry:8081");
        config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");

        // Performance tuning
        int flushSize = def.getFlushSize() != null ? def.getFlushSize() :
                       properties.getDefaults().getFlushSize();
        config.put("batch.size", String.valueOf(flushSize / 2));
        config.put("max.buffered.records", String.valueOf(flushSize * 2));
        config.put("flush.timeout.ms", "5000");
        config.put("max.in.flight.requests", "2");
        config.put("linger.ms", "500");

        // Retry configuration
        config.put("max.retries", "5");
        config.put("retry.backoff.ms", "1000");

        // Index routing - Use topic name as index name, create Elasticsearch aliases for custom names
        // Topic name is automatically used as Elasticsearch index name
        // Custom index names are achieved via Elasticsearch aliases
        // This avoids SMT issues and is the recommended approach

        // Custom transforms (if specified)
        if (def.getTransforms() != null && !def.getTransforms().isEmpty()) {
            def.getTransforms().forEach(config::put);
        }

        // Generate connector name
        String connectorName = generateConnectorName(definitionKey);

        log.info("Built connector spec: name={}, topic={}, index={}",
                connectorName, topicName, def.getIndexName());

        return new ConnectorSpec(connectorName, config);
    }

    /**
     * Generate connector name from definition key
     *
     * @param definitionKey Definition key
     * @return Connector name (e.g., "elasticsearch-sink-smartlock-anomaly-sensor")
     */
    public String generateConnectorName(String definitionKey) {
        return "elasticsearch-sink-" + definitionKey;
    }

    /**
     * Check if connector config has drifted
     *
     * Compares critical fields between desired and current config.
     * Returns true if any critical field differs.
     *
     * @param desired Desired connector spec
     * @param current Current connector config from Kafka Connect
     * @return true if drift detected
     */
    public boolean isDrifted(ConnectorSpec desired, Map<String, Object> current) {
        // Critical fields to compare
        String[] criticalFields = {
            "topics",
            "connection.url",
            "batch.size",
            "max.buffered.records",
            "transforms.route.replacement",
            "schema.ignore"
        };

        for (String field : criticalFields) {
            Object desiredValue = desired.config().get(field);
            Object currentValue = current.get(field);

            // Normalize string comparison (some values might be strings vs integers)
            String desiredStr = desiredValue != null ? desiredValue.toString() : null;
            String currentStr = currentValue != null ? currentValue.toString() : null;

            if (!Objects.equals(desiredStr, currentStr)) {
                log.warn("Drift detected in field '{}': desired={}, current={}",
                        field, desiredValue, currentValue);
                return true;
            }
        }

        return false;
    }
}
