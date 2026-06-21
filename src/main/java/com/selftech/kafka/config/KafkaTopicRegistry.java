package com.selftech.kafka.config;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Kafka Topic Registry - Enterprise Topic Management
 *
 * Central registry for all Kafka topics loaded from application.yml.
 *
 * YAML Configuration:
 * app:
 *   kafka:
 *     default-partitions: 3
 *     default-replication-factor: 1
 *     topics:
 *       smartlock-payment:
 *         name: smartlock.event.payment.v1
 *         category: EVENT
 *         retention-days: 30
 *
 * Benefits:
 * - Single source of truth (YAML)
 * - No hardcoded strings in code
 * - Environment-aware (dev/staging/prod)
 * - Easy to add/modify topics
 * - Type-safe access via getters
 *
 * Usage:
 * @Autowired
 * private KafkaTopicRegistry registry;
 *
 * String topicName = registry.getTopicName("smartlock-payment");
 * Collection<TopicDefinition> allTopics = registry.toDefinitions();
 */
@Slf4j
@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaTopicRegistry {

    /**
     * Default partition count for all topics (can be overridden per topic)
     */
    private int defaultPartitions = 3;

    /**
     * Default replication factor for all topics (can be overridden per topic)
     */
    private short defaultReplicationFactor = 1;

    /**
     * Map of topic configurations loaded from YAML
     * Key: topic key (e.g., "smartlock-payment")
     * Value: TopicConfig object with name, category, retentionDays
     */
    private Map<String, TopicConfig> topics = new HashMap<>();

    /**
     * Convert all topic configs to TopicDefinition objects
     *
     * @return Collection of TopicDefinition (immutable, type-safe)
     */
    public Collection<TopicDefinition> toDefinitions() {
        return topics.values().stream()
            .map(cfg -> new TopicDefinition(
                cfg.getName(),
                cfg.getCategory(),
                defaultPartitions,
                defaultReplicationFactor,
                Duration.ofDays(cfg.getRetentionDays())
            ))
            .toList();
    }

    /**
     * Get topic name by key (String version - legacy support)
     *
     * @param key Topic key from YAML (e.g., "smartlock-payment")
     * @return Full topic name (e.g., "smartlock.event.payment.v1")
     * @deprecated Use {@link #getTopicName(TopicKey)} for type-safe access
     */
    @Deprecated
    public String getTopicName(String key) {
        TopicConfig config = topics.get(key);
        if (config == null) {
            log.warn("Topic key not found in registry: {}", key);
            return null;
        }
        return config.getName();
    }

    /**
     * Get topic name by TopicKey enum (Type-safe, recommended)
     *
     * Provides compile-time safety and IDE autocomplete support.
     *
     * @param topicKey TopicKey enum value (e.g., TopicKey.SELFPARK_PARK_ENTRY)
     * @return Full topic name (e.g., "selfpark.event.parkEntry.v0")
     * @throws IllegalArgumentException if topic key not found in YAML configuration
     */
    public String getTopicName(TopicKey topicKey) {
        String key = topicKey.getKey();
        TopicConfig config = topics.get(key);
        if (config == null) {
            String errorMsg = String.format(
                "Topic key '%s' (%s) not found in application.yml. " +
                "Please add it under app.kafka.topics configuration.",
                key, topicKey.name()
            );
            log.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }
        return config.getName();
    }

    /**
     * Get topic config by key (String version - legacy support)
     *
     * @param key Topic key from YAML
     * @return Optional of TopicConfig
     * @deprecated Use {@link #getTopicConfig(TopicKey)} for type-safe access
     */
    @Deprecated
    public Optional<TopicConfig> getTopicConfig(String key) {
        return Optional.ofNullable(topics.get(key));
    }

    /**
     * Get topic config by TopicKey enum (Type-safe, recommended)
     *
     * @param topicKey TopicKey enum value
     * @return Optional of TopicConfig
     */
    public Optional<TopicConfig> getTopicConfig(TopicKey topicKey) {
        return Optional.ofNullable(topics.get(topicKey.getKey()));
    }

    /**
     * Get all topic names (for @KafkaListener arrays)
     *
     * @return String array of all topic names
     */
    public String[] getAllTopicNames() {
        return topics.values().stream()
            .map(TopicConfig::getName)
            .toArray(String[]::new);
    }

    /**
     * Get consumable topic names for CentralEventConsumer
     *
     * Excludes STREAM and ANOMALY categories since they are processed by Kafka Streams.
     * Only returns EVENT, ALERT, and METRIC categories for traditional consumer processing.
     *
     * @return String array of topic names that should be consumed by CentralEventConsumer
     */
    public String[] getConsumableTopicNames() {
        return topics.values().stream()
            .filter(cfg -> cfg.getCategory() == TopicCategory.EVENT
                        || cfg.getCategory() == TopicCategory.ALERT
                        || cfg.getCategory() == TopicCategory.METRIC)
            .map(TopicConfig::getName)
            // Exclude topics that are processed by Kafka Streams only (anomaly detection)
            .filter(name -> !name.equals("selfpark.event.parkEntry.v0")
                        && !name.equals("selfpark.event.parkExit.v0"))
            .toArray(String[]::new);
    }

    /**
     * Get topic names by category
     *
     * @param category Topic category filter
     * @return String array of topic names matching category
     */
    public String[] getTopicNamesByCategory(TopicCategory category) {
        return topics.values().stream()
            .filter(cfg -> cfg.getCategory() == category)
            .map(TopicConfig::getName)
            .toArray(String[]::new);
    }

    /**
     * Get all DLQ topic names
     *
     * @return String array of DLQ topic names
     */
    public String[] getAllDlqTopicNames() {
        return topics.values().stream()
            .map(TopicConfig::getName)
            .map(TopicNameResolver::dlq)
            .toArray(String[]::new);
    }

    /**
     * Validation report for startup diagnostics
     *
     * @return Formatted validation report
     */
    public String getValidationReport() {
        StringBuilder report = new StringBuilder();
        report.append("=== Kafka Topic Registry Validation ===\n");
        report.append("Default Partitions: ").append(defaultPartitions).append("\n");
        report.append("Default Replication Factor: ").append(defaultReplicationFactor).append("\n");
        report.append("Total Topics: ").append(topics.size()).append("\n\n");

        report.append("--- Topics by Category ---\n");
        for (TopicCategory category : TopicCategory.values()) {
            long count = topics.values().stream()
                .filter(cfg -> cfg.getCategory() == category)
                .count();
            report.append("  ").append(category).append(": ").append(count).append("\n");
        }

        report.append("\n--- All Topics ---\n");
        topics.forEach((key, cfg) -> {
            report.append("  ").append(key)
                .append(" → ").append(cfg.getName())
                .append(" [").append(cfg.getCategory())
                .append(", ").append(cfg.getRetentionDays()).append("d]\n");
        });

        return report.toString();
    }
}
