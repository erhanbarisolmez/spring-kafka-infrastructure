package com.selftech.kafka.connect.config;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.selftech.kafka.connect.model.ConnectorDefinition;

import lombok.Getter;
import lombok.Setter;

/**
 * Elasticsearch Connector Auto-Configuration Properties
 *
 * Binds to app.kafka.connect.elasticsearch.* properties in application.yml
 *
 * Configuration Example:
 * app:
 *   kafka:
 *     connect:
 *       elasticsearch:
 *         enabled: true
 *         defaults:
 *           tasks-max: 1
 *           flush-size: 1000
 *           schema-ignore: true
 *         auto-bind:
 *           categories:
 *             ANOMALY: elasticsearch
 *             ALERT: elasticsearch
 *         definitions:
 *           smartlock-anomaly-sensor:
 *             enabled: true
 *             source-topic-ref: smartlock-anomaly-sensor-data
 *             index-name: anomalies-smartlock-sensor
 *
 * Key Features:
 * 1. Auto-Bind by Category: All ANOMALY/ALERT topics → Elasticsearch automatically
 * 2. Explicit Definitions: Override auto-bind with custom connector configs
 * 3. Defaults: Shared settings applied to all connectors
 *
 * @see com.selftech.kafka.connect.reconciler.ConnectorReconciler
 */
@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "app.kafka.connect.elasticsearch")
public class ElasticsearchConnectorProperties {

    /**
     * Enable/disable Elasticsearch connector auto-configuration
     * Default: true
     * Environment Variable: ELASTICSEARCH_CONNECTOR_ENABLED
     */
    private boolean enabled = true;

    /**
     * Default settings applied to all connectors
     */
    private Defaults defaults = new Defaults();

    /**
     * Auto-bind topics by category to Elasticsearch
     * Example: ANOMALY → elasticsearch, ALERT → elasticsearch
     */
    private AutoBind autoBind = new AutoBind();

    /**
     * Explicit connector definitions (overrides auto-bind)
     * Key: Connector definition key (e.g., "smartlock-anomaly-sensor")
     * Value: ConnectorDefinition with custom settings
     */
    private Map<String, ConnectorDefinition> definitions = new HashMap<>();

    /**
     * Default Connector Settings
     */
    @Getter
    @Setter
    public static class Defaults {

        /**
         * Default number of tasks per connector
         * Default: 1
         */
        private int tasksMax = 1;

        /**
         * Default flush size (batch size for Elasticsearch)
         * Default: 1000 records
         */
        private int flushSize = 1000;

        /**
         * Ignore Avro schema (convert Avro → JSON)
         * CRITICAL: Must be true for Avro to JSON conversion
         * Default: true
         */
        private boolean schemaIgnore = true;

        /**
         * Elasticsearch connection URL
         * Default: http://elasticsearch:9200
         * Environment Variable: ELASTICSEARCH_URL
         */
        private String connectionUrl = "http://elasticsearch:9200";

        /**
         * Elasticsearch username
         * Default: elastic
         * Environment Variable: ELASTICSEARCH_USERNAME
         */
        private String connectionUsername = "elastic";

        /**
         * Elasticsearch password
         * CRITICAL: Use environment variable, never hardcode!
         * Default: changeme (for dev only)
         * Environment Variable: ELASTICSEARCH_PASSWORD
         */
        private String connectionPassword = "changeme";
    }

    /**
     * Auto-Bind Configuration
     * Maps TopicCategory → Target sink (e.g., ANOMALY → elasticsearch)
     */
    @Getter
    @Setter
    public static class AutoBind {

        /**
         * Category to target mapping
         * Key: TopicCategory name (ANOMALY, ALERT, etc.)
         * Value: Target sink ("elasticsearch")
         *
         * Example:
         * categories:
         *   ANOMALY: elasticsearch
         *   ALERT: elasticsearch
         *
         * Result: All ANOMALY and ALERT topics get Elasticsearch connectors automatically
         */
        private Map<String, String> categories = new HashMap<>();
    }
}
