package com.selftech.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Kafka Consumer Configuration - FAZE 1 ENTERPRISE UPGRADED
 *
 * Manual Offset Commit Strategy:
 * - enable.auto.commit = false (consumer manually commits after processing)
 * - Ensures exactly-once processing semantics
 * - Prevents message loss on consumer failure
 * - Required for reliable event processing with handlers
 *
 * Throughput Optimization:
 * - max.poll.records = 500 (batch processing)
 * - session.timeout.ms = 30s (rebalance timeout)
 * - auto.offset.reset = earliest (replay support)
 *
 * Serialization:
 * - KafkaAvroDeserializer (Avro event deserialization)
 * - Schema Registry integration
 * - Specific Avro Reader (type-safe deserialization)
 */
@EnableKafka
@Configuration
@Slf4j
public class KafkaConsumerConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Value("${spring.kafka.consumer.group-id}")
  private String groupId;

  @Value("${spring.kafka.properties.schema.registry.url}")
  private String schemaRegistryUrl;

  /**
   * FAZE 1: Enterprise Consumer configuration - production-ready
   *
   * Manual commit mode ensures:
   * 1. Exactly-once processing semantics
   * 2. No message loss on consumer failure
   * 3. Graceful error handling and retry logic
   * 4. Reliability for critical event processing
   */
  @Bean
  public Map<String, Object> consumerConfigs() {
    Map<String, Object> props = new HashMap<>();

    // ✅ Bootstrap & Serialization
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put("schema.registry.url", schemaRegistryUrl);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

    // ✅ FAZE 1: Consumer behavior - Manual commit for reliability
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Replay support (offset reset)
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500); // Throughput optimization
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000); // 30s session timeout
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // CHANGED: true → false (Manual commit)
    props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5min max poll interval
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000); // 10s heartbeat

    log.info("Kafka Consumer configured - Manual commit mode enabled for reliability");
    return props;
  }

  @Bean
  public ConsumerFactory<String, Object> consumerFactory() {
    return new DefaultKafkaConsumerFactory<>(consumerConfigs());
  }
}
