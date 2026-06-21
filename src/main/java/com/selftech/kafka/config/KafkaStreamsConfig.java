package com.selftech.kafka.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Streams configuration for real-time anomaly detection topology
 * Enables stream processing for device events, box operations, and parking activities
 */
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    /**
     * Configure Kafka Streams properties
     * Sets default serialization to String keys and Avro values via schema registry
     *
     * IMPORTANT: @DependsOn("kafkaTopicVerifier") ensures all Kafka topics are PHYSICALLY created
     * in Kafka (not just Spring beans) before Kafka Streams starts.
     * This prevents "UNKNOWN_TOPIC_OR_PARTITION" errors during startup.
     *
     * Flow: allTopics bean → Spring Kafka Admin creates topics async → kafkaTopicVerifier polls Kafka
     * → verifies all topics exist → KafkaStreamsConfig starts safely
     */
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    @DependsOn("kafkaTopicVerifier")
    public KafkaStreamsConfiguration kStreamsConfig(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>();

        // Application ID identifies the Kafka Streams application for state store coordination
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "selfpark-anomaly-detection");

        // Bootstrap servers from Spring Kafka properties
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
            kafkaProperties.getBootstrapServers());

        // Default key and value serdes - String for keys, ByteArray for values (allowing custom serdes per topology)
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());

        // Processing topology guarantees
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);

        // Commit interval for state stores (30 seconds)
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30000);

        // Session and retention configuration for windowed operations
        props.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, 86400000); // 1 day

        // Thread configuration
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);

        // Metrics configuration
        props.put(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, 30000);
        props.put(StreamsConfig.METRICS_NUM_SAMPLES_CONFIG, 3);

        return new KafkaStreamsConfiguration(props);
    }

    // NOTE: StreamsBuilder bean is automatically created by @EnableKafkaStreams annotation
    // We don't need to manually define it here as it would cause bean conflicts
}
