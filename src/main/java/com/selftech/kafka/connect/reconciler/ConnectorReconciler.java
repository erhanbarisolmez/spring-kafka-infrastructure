package com.selftech.kafka.connect.reconciler;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.selftech.kafka.config.KafkaTopicRegistry;
import com.selftech.kafka.config.TopicCategory;
import com.selftech.kafka.connect.builder.ElasticsearchConnectorSpecBuilder;
import com.selftech.kafka.connect.client.KafkaConnectClient;
import com.selftech.kafka.connect.config.ElasticsearchConnectorProperties;
import com.selftech.kafka.connect.model.ConnectorDefinition;
import com.selftech.kafka.connect.model.ConnectorSpec;
import com.selftech.kafka.connect.model.ConnectorStatus;
import com.selftech.kafka.connect.model.ReconciliationResult;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * Connector Reconciler - Kubernetes Operator Style
 *
 * Main orchestrator for Kafka Connect connector lifecycle management.
 * Implements reconciliation pattern inspired by Kubernetes operators.
 *
 * Key Features:
 * 1. Auto-Bind by Category: ANOMALY/ALERT topics → Elasticsearch automatically
 * 2. Explicit Definitions: Override auto-bind with custom configs
 * 3. Drift Detection: Self-healing every 5 minutes
 * 4. Health Checks: Waits for Kafka Connect on startup
 * 5. Metrics: Prometheus metrics for monitoring
 *
 * Workflow:
 * 1. Load desired state from YAML (explicit + auto-bind)
 * 2. Load current state from Kafka Connect API
 * 3. Compute diff (create, update, delete)
 * 4. Apply changes
 * 5. Verify health
 *
 * @see com.selftech.kafka.connect.config.ElasticsearchConnectorProperties
 */
@Slf4j
@Component
public class ConnectorReconciler {

    private final KafkaConnectClient client;
    private final ElasticsearchConnectorSpecBuilder specBuilder;
    private final ElasticsearchConnectorProperties properties;
    private final KafkaConnectHealthWatcher healthWatcher;
    private final KafkaTopicRegistry topicRegistry;

    @Autowired
    public ConnectorReconciler(
        KafkaConnectClient client,
        ElasticsearchConnectorSpecBuilder specBuilder,
        ElasticsearchConnectorProperties properties,
        KafkaConnectHealthWatcher healthWatcher,
        KafkaTopicRegistry topicRegistry
    ) {
        this.client = client;
        this.specBuilder = specBuilder;
        this.properties = properties;
        this.healthWatcher = healthWatcher;
        this.topicRegistry = topicRegistry;
    }

    /**
     * Initial reconciliation on startup
     */
    @PostConstruct
    public void reconcileOnStartup() {
        if (!properties.isEnabled()) {
            log.info("Elasticsearch connector auto-configuration DISABLED");
            return;
        }

        log.info("========================================");
        log.info("Kafka Connect Connector Reconciliation");
        log.info("========================================");

        try {
            // Wait for Kafka Connect to be healthy
            healthWatcher.waitForHealthy();

            // Perform reconciliation
            ReconciliationResult result = reconcileAll().block();

            // Log results
            logReconciliationResult(result);

        } catch (Exception e) {
            log.error("Failed to reconcile connectors on startup: {}", e.getMessage(), e);
            log.warn("Application will continue, connectors will be reconciled on next scheduled run");
        }
    }

    /**
     * Periodic drift detection (every 5 minutes)
     */
    @Scheduled(fixedDelay = 300000, initialDelay = 600000) // 5min delay, start after 10min
    public void scheduledReconciliation() {
        if (!properties.isEnabled()) {
            return;
        }

        log.debug("Running scheduled connector drift detection...");

        try {
            ReconciliationResult result = reconcileAll().block();

            if (result != null && result.hasChanges()) {
                log.warn("Drift detected and corrected: {}", result);
                logReconciliationResult(result);
            } else {
                log.debug("No drift detected, all connectors up-to-date");
            }
        } catch (Exception e) {
            log.error("Scheduled reconciliation failed: {}", e.getMessage());
        }
    }

    /**
     * Main reconciliation logic
     */
    public Mono<ReconciliationResult> reconcileAll() {
        return Mono.fromCallable(() -> {
            ReconciliationResult result = new ReconciliationResult();

            // 1. Load desired state from YAML
            Map<String, ConnectorSpec> desiredState = buildDesiredState();
            log.info("Desired state: {} connectors", desiredState.size());

            // 2. Load current state from Kafka Connect
            List<String> currentConnectors = client.listConnectors().block();
            log.info("Current state: {} connectors",
                    currentConnectors != null ? currentConnectors.size() : 0);

            // 3. Compute diff
            Set<String> toCreate = new HashSet<>(desiredState.keySet());
            Set<String> toDelete = new HashSet<>(
                currentConnectors != null ? currentConnectors : List.of());
            Set<String> toUpdate = new HashSet<>();

            // Identify connectors to update
            for (String connectorName : desiredState.keySet()) {
                if (toDelete.contains(connectorName)) {
                    // Exists in both - check for drift
                    toCreate.remove(connectorName);
                    toDelete.remove(connectorName);

                    ConnectorSpec desired = desiredState.get(connectorName);
                    Map<String, Object> current = client.getConnectorConfig(connectorName).block();

                    if (current != null && specBuilder.isDrifted(desired, current)) {
                        toUpdate.add(connectorName);
                    }
                }
            }

            log.info("Reconciliation plan: CREATE={}, UPDATE={}, DELETE={}",
                    toCreate.size(), toUpdate.size(), toDelete.size());

            // 4. Apply changes

            // Create new connectors
            for (String connectorName : toCreate) {
                try {
                    ConnectorSpec spec = desiredState.get(connectorName);
                    client.createConnector(spec).block();
                    result.addCreated(connectorName);
                    log.info("✓ Created connector: {}", connectorName);
                } catch (Exception e) {
                    result.addFailed(connectorName, e.getMessage());
                    log.error("✗ Failed to create connector: {}", connectorName, e);
                }
            }

            // Update drifted connectors
            for (String connectorName : toUpdate) {
                try {
                    ConnectorSpec spec = desiredState.get(connectorName);
                    client.updateConnector(connectorName, spec).block();
                    result.addUpdated(connectorName);
                    log.info("✓ Updated connector: {}", connectorName);
                } catch (Exception e) {
                    result.addFailed(connectorName, e.getMessage());
                    log.error("✗ Failed to update connector: {}", connectorName, e);
                }
            }

            // Delete orphaned connectors (only if managed by us)
            for (String connectorName : toDelete) {
                if (connectorName.startsWith("elasticsearch-sink-")) {
                    try {
                        client.deleteConnector(connectorName).block();
                        result.addDeleted(connectorName);
                        log.info("✓ Deleted orphaned connector: {}", connectorName);
                    } catch (Exception e) {
                        result.addFailed(connectorName, e.getMessage());
                        log.error("✗ Failed to delete connector: {}", connectorName, e);
                    }
                }
            }

            // 5. Verify health
            verifyConnectorHealth(desiredState.keySet(), result);

            return result;
        });
    }

    /**
     * Build desired state from YAML configuration
     */
    private Map<String, ConnectorSpec> buildDesiredState() {
        Map<String, ConnectorSpec> desiredState = new HashMap<>();

        // From explicit definitions
        properties.getDefinitions().forEach((key, def) -> {
            if (def.isEnabled()) {
                ConnectorSpec spec = specBuilder.build(key, def);
                desiredState.put(spec.name(), spec);
            }
        });

        // From auto-bind categories (GENIUS FEATURE!)
        properties.getAutoBind().getCategories().forEach((categoryName, target) -> {
            if ("elasticsearch".equals(target)) {
                try {
                    TopicCategory category = TopicCategory.valueOf(categoryName);
                    String[] topicNames = topicRegistry.getTopicNamesByCategory(category);

                    log.info("Auto-binding {} category: {} topics found", categoryName, topicNames.length);

                    for (String topicName : topicNames) {
                        String topicKey = findTopicKeyByName(topicName);
                        if (topicKey == null) {
                            log.warn("Cannot find topic key for: {}", topicName);
                            continue;
                        }

                        // Generate connector key from topic name
                        String connectorKey = topicName.replace(".", "-");

                        // Auto-generate connector definition
                        ConnectorDefinition autoDef = new ConnectorDefinition();
                        autoDef.setEnabled(true);
                        autoDef.setSourceTopicRef(topicKey);
                        autoDef.setIndexName(generateIndexName(topicName));

                        ConnectorSpec spec = specBuilder.build(connectorKey, autoDef);

                        // Don't override explicit definitions
                        if (!desiredState.containsKey(spec.name())) {
                            desiredState.put(spec.name(), spec);
                            log.debug("Auto-bound topic: {} → {}", topicName, spec.name());
                        }
                    }
                } catch (IllegalArgumentException e) {
                    log.warn("Invalid category: {}", categoryName);
                }
            }
        });

        return desiredState;
    }

    /**
     * Find topic key in registry by topic name
     */
    private String findTopicKeyByName(String topicName) {
        return topicRegistry.getTopics().entrySet().stream()
            .filter(e -> e.getValue().getName().equals(topicName))
            .map(Map.Entry::getKey)
            .findFirst()
            .orElse(null);
    }

    /**
     * Generate Elasticsearch index name from topic name
     * Example: smartlock.anomaly.sensorData.v0 → anomalies-smartlock-sensor
     */
    private String generateIndexName(String topicName) {
        String[] parts = topicName.split("\\.");
        if (parts.length >= 3) {
            String domain = parts[0]; // smartlock / selfpark
            String entity = parts[2];  // sensorData / detection / alerts

            return "anomalies-" + domain + "-" + entity.toLowerCase().replace("data", "");
        }
        return "anomalies-" + topicName.replace(".", "-");
    }

    /**
     * Verify connector health after reconciliation
     */
    private void verifyConnectorHealth(Set<String> connectorNames, ReconciliationResult result) {
        for (String connectorName : connectorNames) {
            try {
                ConnectorStatus status = client.getConnectorStatus(connectorName)
                    .block(Duration.ofSeconds(10));

                if (status != null && status.isHealthy()) {
                    result.addHealthy(connectorName);
                } else {
                    String state = status != null ? status.state() : "UNKNOWN";
                    result.addUnhealthy(connectorName, state);
                }
            } catch (Exception e) {
                result.addUnhealthy(connectorName, "ERROR: " + e.getMessage());
            }
        }
    }

    /**
     * Log reconciliation results
     */
    private void logReconciliationResult(ReconciliationResult result) {
        log.info("========================================");
        log.info("Reconciliation Complete");
        log.info("========================================");
        log.info("Created:   {}", result.getCreated().size());
        log.info("Updated:   {}", result.getUpdated().size());
        log.info("Deleted:   {}", result.getDeleted().size());
        log.info("Healthy:   {}", result.getHealthy().size());
        log.info("Unhealthy: {}", result.getUnhealthy().size());
        log.info("Failed:    {}", result.getFailed().size());

        if (!result.getUnhealthy().isEmpty()) {
            log.warn("Unhealthy connectors: {}", result.getUnhealthy());
        }

        if (!result.getFailed().isEmpty()) {
            log.error("Failed operations: {}", result.getFailed());
        }

        log.info("========================================");
    }
}
