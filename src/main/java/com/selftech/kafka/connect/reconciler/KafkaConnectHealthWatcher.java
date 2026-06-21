package com.selftech.kafka.connect.reconciler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.selftech.kafka.connect.client.KafkaConnectClient;
import com.selftech.kafka.connect.config.KafkaConnectProperties;

import lombok.extern.slf4j.Slf4j;

/**
 * Kafka Connect Health Watcher
 *
 * Waits for Kafka Connect to be healthy before reconciliation.
 * Prevents startup failures when backend starts before Kafka Connect is ready.
 *
 * Workflow:
 * 1. Initial delay (60s default) - Allow Kafka Connect container to start
 * 2. Poll every 5s using KafkaConnectClient.isHealthy()
 * 3. Max wait 5 minutes
 * 4. Throw exception if unhealthy after max wait
 *
 * @see com.selftech.kafka.connect.reconciler.ConnectorReconciler
 */
@Slf4j
@Component
public class KafkaConnectHealthWatcher {

    private final KafkaConnectClient client;
    private final KafkaConnectProperties properties;

    @Autowired
    public KafkaConnectHealthWatcher(
        KafkaConnectClient client,
        KafkaConnectProperties properties
    ) {
        this.client = client;
        this.properties = properties;
    }

    /**
     * Wait for Kafka Connect to be healthy
     *
     * @throws RuntimeException if Kafka Connect does not become healthy within max wait time
     */
    public void waitForHealthy() {
        if (!properties.getHealthCheck().isEnabled()) {
            log.info("Kafka Connect health check disabled, skipping...");
            return;
        }

        log.info("========================================");
        log.info("Kafka Connect Health Check");
        log.info("========================================");
        log.info("Waiting for Kafka Connect to be healthy...");
        log.info("Startup delay: {} seconds",
                properties.getHealthCheck().getStartupDelaySeconds());

        // Initial delay to allow Kafka Connect to start
        try {
            long startupDelayMs = properties.getHealthCheck().getStartupDelaySeconds() * 1000L;
            log.info("Waiting {} seconds for Kafka Connect startup...",
                    properties.getHealthCheck().getStartupDelaySeconds());
            Thread.sleep(startupDelayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Health check interrupted during startup delay", e);
        }

        // Poll until healthy or timeout
        long startTime = System.currentTimeMillis();
        long maxWaitMs = properties.getHealthCheck().getMaxWaitSeconds() * 1000L;
        int pollIntervalMs = properties.getHealthCheck().getPollIntervalSeconds() * 1000;

        int attempt = 0;
        while (System.currentTimeMillis() - startTime < maxWaitMs) {
            attempt++;
            Boolean healthy = client.isHealthy().block();

            if (Boolean.TRUE.equals(healthy)) {
                log.info("✓ Kafka Connect is healthy (attempt: {})", attempt);
                log.info("========================================");
                return;
            }

            log.warn("Kafka Connect not ready yet (attempt: {}), retrying in {} seconds...",
                    attempt, properties.getHealthCheck().getPollIntervalSeconds());

            try {
                Thread.sleep(pollIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Health check interrupted during polling", e);
            }
        }

        // Timeout - Kafka Connect did not become healthy
        String errorMsg = String.format(
            "Kafka Connect did not become healthy within %d seconds (attempts: %d)",
            properties.getHealthCheck().getMaxWaitSeconds(), attempt);

        log.error("✗ {}", errorMsg);
        log.error("========================================");

        throw new RuntimeException(errorMsg);
    }
}
