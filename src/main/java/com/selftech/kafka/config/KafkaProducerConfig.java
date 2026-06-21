package com.selftech.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

@Configuration
public class KafkaProducerConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Value("${spring.kafka.properties.schema.registry.url}")
  private String schemaRegistryUrl;

  /**
   * Tüm producer'lar için ortak konfigürasyon - FAZE 0 UPGRADED
   * Avro serializer, production-ready reliability & performance
   */
  @Bean
  public Map<String, Object> producerConfigs() {
    Map<String, Object> props = new HashMap<>();

    // ✅ Bootstrap & Serialization
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put("schema.registry.url", schemaRegistryUrl);

    // ✅ FAZE 0: Reliability (Durability)
    props.put(ProducerConfig.ACKS_CONFIG, "all"); // Tüm replicas ack
    props.put(ProducerConfig.RETRIES_CONFIG, 5); // 5 retry (was 3)
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // Duplicate prevention

    // ✅ FAZE 0: Performance (Throughput - 100K+ msg/sec)
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // 40% size reduction
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768); // 32 KB batch
    props.put(ProducerConfig.LINGER_MS_CONFIG, 10); // 10ms wait for batch

    // ✅ FAZE 0: Timeout & Ordering
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000); // 30s request timeout
    props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000); // 120s delivery timeout
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); // Ordering guarantee

    // ✅ FAZE 0: Metrics
    props.put(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG, 3);
    props.put(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, 30000);

    return props;
  }
}
