package com.selftech.kafka.config;

import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import lombok.extern.slf4j.Slf4j;

/**
 * Kafka Topic Initializer - FAZE 2 ENTERPRISE UPGRADED
 *
 * Fully Automated Topic Management:
 * - Topics loaded from application.yml via KafkaTopicRegistry
 * - DLQ topics automatically generated via TopicNameResolver
 * - Zero manual @Bean methods per topic
 * - Convention-based, maintainable, scalable
 *
 * How it works:
 * 1. KafkaTopicRegistry loads topics from YAML
 * 2. toDefinitions() converts to TopicDefinition objects
 * 3. flatMap creates normal + DLQ topics automatically
 * 4. NewTopic beans configured with retention, RF, partitions
 *
 * Benefits:
 * - Single source of truth (YAML)
 * - No code changes for new topics
 * - Environment-aware (dev/staging/prod)
 * - DLQ auto-generation
 * - Type-safe, immutable configuration
 */
@Slf4j
@Configuration
public class KafkaTopicInitializer {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Autowired
    private KafkaTopicRegistry registry;

    /**
     * Kafka Admin Bean for topic management
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        java.util.Map<String, Object> configs = new java.util.HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    /**
     * FAZE 2: Fully Automated Topic Creation
     *
     * Creates all topics (normal + DLQ) from KafkaTopicRegistry.
     * Zero manual @Bean methods required.
     *
     * Workflow:
     * 1. Load topic definitions from registry (YAML)
     * 2. For each definition, create normal + DLQ topic
     * 3. Apply retention, RF, partitions from definition
     * 4. Log creation for visibility
     *
     * @return List of NewTopic beans (normal + DLQ)
     */
    @Bean
    public List<NewTopic> allTopics() {
        log.info("=== Kafka Topic Initializer - FAZE 2 Enterprise ===");
        log.info("Loading topics from KafkaTopicRegistry...");

        // Log registry validation report
        log.info("\n{}", registry.getValidationReport());

        // Create normal + DLQ topics automatically
        List<NewTopic> topics = registry.toDefinitions().stream()
            .flatMap(def -> Stream.of(
                createNormalTopic(def),
                createDlqTopic(def)
            ))
            .toList();

        log.info("Total topics created: {} (Normal: {}, DLQ: {})",
            topics.size(),
            registry.toDefinitions().size(),
            registry.toDefinitions().size());

        return topics;
    }

    /**
     * Create a normal topic from TopicDefinition
     *
     * @param def Topic definition
     * @return NewTopic with retention, RF, partitions
     */
    private NewTopic createNormalTopic(TopicDefinition def) {
        log.info("Creating topic: {} [{}] - Retention: {}d, Partitions: {}, RF: {}",
            def.name(),
            def.category(),
            def.retentionDays(),
            def.partitions(),
            def.replicationFactor());

        return new NewTopic(def.name(), def.partitions(), def.replicationFactor())
            .configs(buildTopicConfigs(def));
    }

    /**
     * Create a DLQ topic from TopicDefinition
     *
     * @param def Topic definition (DLQ inherits same config)
     * @return NewTopic for DLQ with "dlq." prefix
     */
    private NewTopic createDlqTopic(TopicDefinition def) {
        String dlqName = TopicNameResolver.dlq(def.name());

        log.debug("Creating DLQ topic: {} (for {})", dlqName, def.name());

        // DLQ uses same partitions/RF but shorter retention (7 days)
        return new NewTopic(dlqName, def.partitions(), def.replicationFactor())
            .configs(buildDlqConfigs());
    }

    /**
     * Build Kafka topic configuration map
     *
     * @param def Topic definition
     * @return Configuration map
     */
    private java.util.Map<String, String> buildTopicConfigs(TopicDefinition def) {
        java.util.Map<String, String> configs = new java.util.HashMap<>();
        configs.put("retention.ms", String.valueOf(def.retentionMs()));
        configs.put("min.insync.replicas", "1"); // Single broker setup
        configs.put("compression.type", "snappy");
        configs.put("cleanup.policy", "delete");
        return configs;
    }

    /**
     * Build DLQ topic configuration (shorter retention)
     *
     * @return Configuration map for DLQ topics
     */
    private java.util.Map<String, String> buildDlqConfigs() {
        java.util.Map<String, String> configs = new java.util.HashMap<>();
        configs.put("retention.ms", String.valueOf(java.time.Duration.ofDays(7).toMillis()));
        configs.put("min.insync.replicas", "1");
        configs.put("compression.type", "snappy");
        configs.put("cleanup.policy", "delete");
        return configs;
    }
}
