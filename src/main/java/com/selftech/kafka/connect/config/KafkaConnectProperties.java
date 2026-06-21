package com.selftech.kafka.connect.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Getter;
import lombok.Setter;

/**
 * Kafka Connect Configuration Properties
 *
 * Binds to app.kafka.connect.* properties in application.yml
 *
 * Configuration Example:
 * app:
 *   kafka:
 *     connect:
 *       url: http://localhost:8086
 *       connection-timeout: 5000
 *       read-timeout: 30000
 *       max-retries: 3
 *
 * @see com.selftech.kafka.connect.client.KafkaConnectClient
 */
@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "app.kafka.connect")
public class KafkaConnectProperties {

    /**
     * Kafka Connect REST API endpoint
     * Default: http://localhost:8086
     */
    private String url = "http://localhost:8086";

    /**
     * Connection timeout in milliseconds
     * Default: 5000ms (5 seconds)
     */
    private int connectionTimeout = 5000;

    /**
     * Read timeout in milliseconds
     * Default: 30000ms (30 seconds)
     */
    private int readTimeout = 30000;

    /**
     * Maximum retry attempts for failed requests
     * Default: 3
     */
    private int maxRetries = 3;

    /**
     * Backoff time between retries in milliseconds
     * Default: 1000ms (1 second)
     */
    private int retryBackoffMs = 1000;

    /**
     * Health check configuration
     */
    private HealthCheck healthCheck = new HealthCheck();

    /**
     * Health Check Configuration
     */
    @Getter
    @Setter
    public static class HealthCheck {

        /**
         * Enable health checks before reconciliation
         * Default: true
         */
        private boolean enabled = true;

        /**
         * Initial delay before first health check (wait for Kafka Connect to start)
         * Default: 60 seconds
         */
        private int startupDelaySeconds = 60;

        /**
         * Maximum time to wait for Kafka Connect to become healthy
         * Default: 300 seconds (5 minutes)
         */
        private int maxWaitSeconds = 300;

        /**
         * Interval between health check polls
         * Default: 5 seconds
         */
        private int pollIntervalSeconds = 5;
    }
}
