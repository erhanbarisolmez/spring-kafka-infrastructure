package com.selftech.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Kafka Topic Verifier & Creator - PHASE 7.7
 *
 * Ensures all topics from KafkaTopicRegistry are physically created in Kafka
 * before the application starts Kafka Streams or consumers.
 *
 * Problems Solved:
 * - Spring Kafka Admin creates topics asynchronously and unreliably
 * - Spring Kafka Admin doesn't recreate topics if they were deleted after initial creation
 * - @DependsOn("allTopics") only waits for bean initialization, not actual topic creation
 * - Kafka Streams starts before topics exist, causing UNKNOWN_TOPIC_OR_PARTITION errors
 *
 * Solution:
 * - @PostConstruct method that actively polls Kafka to verify topics exist
 * - If topics are missing, CREATES them immediately using AdminClient
 * - Handles both initial creation failures and recreating deleted topics
 * - Retry mechanism with exponential backoff
 * - Blocks application startup until all topics are created
 * - Provides detailed logging for troubleshooting
 *
 * Usage:
 * - KafkaStreamsConfig uses @DependsOn("kafkaTopicVerifier") to wait for verification
 */
@Slf4j
@Component("kafkaTopicVerifier")
@DependsOn("allTopics")  // Wait for Spring Kafka Admin to start creating topics
public class KafkaTopicVerifier {

    @Autowired
    private KafkaAdmin kafkaAdmin;

    @Autowired
    private KafkaTopicRegistry registry;

    private static final int MAX_RETRIES = 30;  // 30 retries (reduced since we create missing topics immediately)
    private static final long INITIAL_BACKOFF_MS = 500;  // Start with 500ms
    private static final long MAX_BACKOFF_MS = 2000;  // Max 2 seconds between retries
    private static final long TIMEOUT_MS = 60_000;  // 1 minute total timeout (topics created immediately)

    /**
     * Verify all topics exist - if missing, CREATE them immediately.
     * This method blocks until all topics are created or timeout occurs.
     */
    @PostConstruct
    public void verifyTopicsCreated() {
        log.info("=== Kafka Topic Verifier & Creator - Starting ===");

        // Build topic definitions map for easy lookup
        Map<String, TopicDefinition> topicDefMap = new HashMap<>();
        for (TopicDefinition def : registry.toDefinitions()) {
            topicDefMap.put(def.name(), def);
            topicDefMap.put(TopicNameResolver.dlq(def.name()), def); // DLQ uses same definition
        }

        // Get expected topic names from registry (normal + DLQ)
        List<String> expectedTopics = registry.toDefinitions().stream()
            .flatMap(def -> java.util.stream.Stream.of(
                def.name(),
                TopicNameResolver.dlq(def.name())
            ))
            .sorted()
            .toList();

        log.info("Expected topics count: {} (Normal: {}, DLQ: {})",
            expectedTopics.size(),
            registry.toDefinitions().size(),
            registry.toDefinitions().size());

        long startTime = System.currentTimeMillis();
        int attempt = 0;
        long backoffMs = INITIAL_BACKOFF_MS;

        while (attempt < MAX_RETRIES) {
            attempt++;

            try {
                // Query Kafka for existing topics
                Set<String> existingTopics = listTopicsFromKafka();

                // Find missing topics
                List<String> missingTopics = expectedTopics.stream()
                    .filter(topic -> !existingTopics.contains(topic))
                    .toList();

                if (missingTopics.isEmpty()) {
                    long elapsedMs = System.currentTimeMillis() - startTime;
                    log.info("✓ All {} topics verified in Kafka (took {}ms, {} attempts)",
                        expectedTopics.size(), elapsedMs, attempt);
                    log.info("=== Kafka Topic Verifier - Verification Complete ===");
                    return;  // Success!
                }

                // CREATE MISSING TOPICS IMMEDIATELY
                log.warn("Attempt {}/{}: {} topics missing - CREATING them now",
                    attempt, MAX_RETRIES, missingTopics.size());

                if (log.isDebugEnabled()) {
                    log.debug("Missing topics: {}", missingTopics);
                }

                // Create missing topics
                createMissingTopics(missingTopics, topicDefMap);

                // Check timeout
                long elapsedMs = System.currentTimeMillis() - startTime;
                if (elapsedMs > TIMEOUT_MS) {
                    String errorMsg = String.format(
                        "TIMEOUT: %d topics not created after %dms. Missing: %s",
                        missingTopics.size(), elapsedMs, missingTopics
                    );
                    log.error(errorMsg);
                    throw new IllegalStateException(errorMsg);
                }

                // Short backoff to allow topics to be created
                Thread.sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Topic verification interrupted", e);
            } catch (Exception e) {
                log.warn("Attempt {}/{}: Error checking/creating topics: {}",
                    attempt, MAX_RETRIES, e.getMessage());

                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Topic verification interrupted", ie);
                }
            }
        }

        // Max retries exceeded
        throw new IllegalStateException(
            String.format("Failed to verify/create topics after %d attempts", MAX_RETRIES)
        );
    }

    /**
     * Create missing topics using AdminClient
     *
     * @param missingTopicNames List of missing topic names
     * @param topicDefMap Map of topic names to their definitions
     */
    private void createMissingTopics(List<String> missingTopicNames, Map<String, TopicDefinition> topicDefMap) {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            List<NewTopic> newTopics = new ArrayList<>();

            for (String topicName : missingTopicNames) {
                TopicDefinition def = topicDefMap.get(topicName);
                if (def == null) {
                    log.error("No definition found for topic: {}", topicName);
                    continue;
                }

                // Determine if this is a DLQ topic
                boolean isDlq = topicName.startsWith("dlq.");

                // Create NewTopic with appropriate configuration
                Map<String, String> configs = new HashMap<>();
                if (isDlq) {
                    // DLQ topics: 7 days retention
                    configs.put("retention.ms", String.valueOf(java.time.Duration.ofDays(7).toMillis()));
                } else {
                    // Normal topics: use definition retention
                    configs.put("retention.ms", String.valueOf(def.retentionMs()));
                }
                configs.put("min.insync.replicas", "1");
                configs.put("compression.type", "snappy");
                configs.put("cleanup.policy", "delete");

                NewTopic newTopic = new NewTopic(topicName, def.partitions(), def.replicationFactor())
                    .configs(configs);

                newTopics.add(newTopic);

                log.info("Creating missing topic: {} [{}] - Retention: {}d, Partitions: {}, RF: {}",
                    topicName,
                    def.category(),
                    isDlq ? 7 : def.retentionDays(),
                    def.partitions(),
                    def.replicationFactor());
            }

            if (!newTopics.isEmpty()) {
                CreateTopicsResult result = adminClient.createTopics(newTopics);
                result.all().get(30, TimeUnit.SECONDS);
                log.info("✓ Successfully created {} missing topics", newTopics.size());
            }

        } catch (Exception e) {
            log.error("Failed to create missing topics: {}", e.getMessage(), e);
            // Don't throw - let retry mechanism handle it
        }
    }

    /**
     * List all topics from Kafka using AdminClient
     */
    private Set<String> listTopicsFromKafka() throws Exception {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            ListTopicsOptions options = new ListTopicsOptions()
                .listInternal(false)  // Exclude internal topics like __consumer_offsets
                .timeoutMs(5000);

            return adminClient.listTopics(options)
                .names()
                .get(10, TimeUnit.SECONDS);
        }
    }
}
