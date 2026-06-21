package com.selftech.kafka.connect.client;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import com.selftech.kafka.connect.config.KafkaConnectProperties;
import com.selftech.kafka.connect.model.ConnectorSpec;
import com.selftech.kafka.connect.model.ConnectorStatus;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Kafka Connect REST API Client
 *
 * Reactive client for Kafka Connect REST API operations.
 * Uses WebClient for non-blocking HTTP calls.
 *
 * Key Features:
 * - Health checks
 * - Connector CRUD operations
 * - Automatic retry with exponential backoff
 * - Error handling (404 → empty, others → retry)
 *
 * API Reference: https://docs.confluent.io/platform/current/connect/references/restapi.html
 *
 * @see com.selftech.kafka.connect.reconciler.ConnectorReconciler
 */
@Slf4j
@Service
public class KafkaConnectClient {

    private final WebClient webClient;
    private final KafkaConnectProperties properties;

    @Autowired
    public KafkaConnectClient(
        @Qualifier("kafkaConnectWebClient") WebClient webClient,
        KafkaConnectProperties properties
    ) {
        this.webClient = webClient;
        this.properties = properties;
    }

    /**
     * Health check - Verify Kafka Connect is reachable
     *
     * Endpoint: GET /
     *
     * @return Mono<Boolean> - true if healthy, false otherwise
     */
    public Mono<Boolean> isHealthy() {
        return webClient.get()
            .uri("/")
            .retrieve()
            .toBodilessEntity()
            .map(response -> response.getStatusCode().is2xxSuccessful())
            .doOnSuccess(healthy -> {
                if (Boolean.TRUE.equals(healthy)) {
                    log.debug("Kafka Connect health check: OK");
                }
            })
            .onErrorResume(throwable -> {
                log.warn("Kafka Connect health check failed: {}", throwable.getMessage());
                return Mono.just(false);
            });
    }

    /**
     * List all connectors
     *
     * Endpoint: GET /connectors
     *
     * @return Mono<List<String>> - List of connector names
     */
    public Mono<List<String>> listConnectors() {
        return webClient.get()
            .uri("/connectors")
            .retrieve()
            .bodyToMono(new ParameterizedTypeReference<List<String>>() {})
            .retryWhen(retrySpec())
            .doOnSuccess(connectors -> log.debug("Listed {} connectors", connectors.size()))
            .onErrorResume(throwable -> {
                log.error("Failed to list connectors: {}", throwable.getMessage());
                return Mono.just(List.of());
            });
    }

    /**
     * Get connector configuration
     *
     * Endpoint: GET /connectors/{name}/config
     *
     * @param connectorName Connector name
     * @return Mono<Map<String, Object>> - Connector config, or empty if not found
     */
    public Mono<Map<String, Object>> getConnectorConfig(String connectorName) {
        return webClient.get()
            .uri("/connectors/{name}/config", connectorName)
            .retrieve()
            .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
            .retryWhen(retrySpec())
            .doOnSuccess(config -> log.debug("Retrieved config for connector: {}", connectorName))
            .onErrorResume(WebClientResponseException.NotFound.class, e -> {
                log.debug("Connector not found: {}", connectorName);
                return Mono.empty();
            });
    }

    /**
     * Create connector
     *
     * Endpoint: POST /connectors
     * Body: {"name": "...", "config": {...}}
     *
     * @param spec Connector specification
     * @return Mono<Map<String, Object>> - Created connector info
     */
    public Mono<Map<String, Object>> createConnector(ConnectorSpec spec) {
        return webClient.post()
            .uri("/connectors")
            .bodyValue(Map.of("name", spec.name(), "config", spec.config()))
            .retrieve()
            .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
            .retryWhen(retrySpec())
            .doOnSuccess(result -> log.info("Created connector: {}", spec.name()))
            .doOnError(throwable -> log.error("Failed to create connector {}: {}",
                                            spec.name(), throwable.getMessage()));
    }

    /**
     * Delete connector
     *
     * Endpoint: DELETE /connectors/{name}
     *
     * @param connectorName Connector name
     * @return Mono<Void>
     */
    public Mono<Void> deleteConnector(String connectorName) {
        return webClient.delete()
            .uri("/connectors/{name}", connectorName)
            .retrieve()
            .toBodilessEntity()
            .then()
            .retryWhen(retrySpec())
            .doOnSuccess(v -> log.info("Deleted connector: {}", connectorName))
            .doOnError(throwable -> log.error("Failed to delete connector {}: {}",
                                            connectorName, throwable.getMessage()))
            .onErrorResume(WebClientResponseException.NotFound.class, e -> {
                log.debug("Connector already deleted: {}", connectorName);
                return Mono.empty();
            });
    }

    /**
     * Update connector (implemented as DELETE + CREATE)
     *
     * Kafka Connect doesn't have a native UPDATE endpoint.
     * We implement update as:
     * 1. Delete existing connector
     * 2. Wait 2 seconds for cleanup
     * 3. Create new connector with updated config
     *
     * @param connectorName Connector name
     * @param spec New connector specification
     * @return Mono<Map<String, Object>> - Created connector info
     */
    public Mono<Map<String, Object>> updateConnector(String connectorName, ConnectorSpec spec) {
        log.info("Updating connector (DELETE + CREATE): {}", connectorName);

        return deleteConnector(connectorName)
            .delayElement(Duration.ofSeconds(2)) // Allow time for cleanup
            .then(createConnector(spec))
            .doOnSuccess(result -> log.info("Updated connector: {}", connectorName));
    }

    /**
     * Get connector status
     *
     * Endpoint: GET /connectors/{name}/status
     *
     * @param connectorName Connector name
     * @return Mono<ConnectorStatus> - Connector status
     */
    public Mono<ConnectorStatus> getConnectorStatus(String connectorName) {
        return webClient.get()
            .uri("/connectors/{name}/status", connectorName)
            .retrieve()
            .bodyToMono(ConnectorStatus.class)
            .retryWhen(retrySpec())
            .doOnSuccess(status -> log.debug("Connector {} status: {}",
                                           connectorName, status.state()))
            .onErrorResume(WebClientResponseException.NotFound.class, e -> {
                log.warn("Connector status not found: {}", connectorName);
                return Mono.empty();
            });
    }

    /**
     * Restart connector
     *
     * Endpoint: POST /connectors/{name}/restart
     *
     * @param connectorName Connector name
     * @return Mono<Void>
     */
    public Mono<Void> restartConnector(String connectorName) {
        return webClient.post()
            .uri("/connectors/{name}/restart", connectorName)
            .retrieve()
            .toBodilessEntity()
            .then()
            .retryWhen(retrySpec())
            .doOnSuccess(v -> log.info("Restarted connector: {}", connectorName))
            .doOnError(throwable -> log.error("Failed to restart connector {}: {}",
                                            connectorName, throwable.getMessage()));
    }

    /**
     * Pause connector
     *
     * Endpoint: PUT /connectors/{name}/pause
     *
     * @param connectorName Connector name
     * @return Mono<Void>
     */
    public Mono<Void> pauseConnector(String connectorName) {
        return webClient.put()
            .uri("/connectors/{name}/pause", connectorName)
            .retrieve()
            .toBodilessEntity()
            .then()
            .retryWhen(retrySpec())
            .doOnSuccess(v -> log.info("Paused connector: {}", connectorName))
            .doOnError(throwable -> log.error("Failed to pause connector {}: {}",
                                            connectorName, throwable.getMessage()));
    }

    /**
     * Resume connector
     *
     * Endpoint: PUT /connectors/{name}/resume
     *
     * @param connectorName Connector name
     * @return Mono<Void>
     */
    public Mono<Void> resumeConnector(String connectorName) {
        return webClient.put()
            .uri("/connectors/{name}/resume", connectorName)
            .retrieve()
            .toBodilessEntity()
            .then()
            .retryWhen(retrySpec())
            .doOnSuccess(v -> log.info("Resumed connector: {}", connectorName))
            .doOnError(throwable -> log.error("Failed to resume connector {}: {}",
                                            connectorName, throwable.getMessage()));
    }

    /**
     * Retry specification with exponential backoff
     *
     * Configuration:
     * - Max retries: From properties (default: 3)
     * - Initial backoff: From properties (default: 1000ms)
     * - Filter: Don't retry 404 errors
     *
     * @return Retry specification
     */
    private Retry retrySpec() {
        return Retry.backoff(properties.getMaxRetries(),
                           Duration.ofMillis(properties.getRetryBackoffMs()))
            .filter(throwable -> !(throwable instanceof WebClientResponseException.NotFound))
            .doBeforeRetry(signal ->
                log.warn("Retrying Kafka Connect request, attempt: {}, error: {}",
                        signal.totalRetries() + 1, signal.failure().getMessage()));
    }
}
