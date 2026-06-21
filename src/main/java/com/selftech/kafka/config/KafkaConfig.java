package com.selftech.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * Kafka Orchestration Configuration - FAZE 0 UPGRADED
 *
 * Producer & Consumer bean'leri KafkaProducerConfig ve Kaf::kaConsumerConfig
 * içinde tanımlanmıştır
 * Bu sınıf admin client ve error handling'i yönetir
 */
@Configuration
public class KafkaConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Value("${spring.kafka.properties.schema.registry.url}")
  private String schemaRegistryUrl;

  @Bean
  public AdminClient kafkaAdminClient() {
    return AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
  }

  /**
   * Genel KafkaTemplate<String, Object> Bean'i
   * EventProducerService ve diğer generic producer'lar tarafından kullanılır
   */
  @Bean
  public KafkaTemplate<String, Object> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

  /**
   * Genel ProducerFactory<String, Object>
   */
  @Bean
  public ProducerFactory<String, Object> producerFactory() {
    return new DefaultKafkaProducerFactory<>(producerConfigs());
  }

  /**
   * Genel Producer Konfigürasyonu - FAZE 0 UPGRADED
   */
  private Map<String, Object> producerConfigs() {
    Map<String, Object> props = new HashMap<>();

    // ✅ Bootstrap & Serialization
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put("schema.registry.url", schemaRegistryUrl);

    // ✅ FAZE 0: Reliability (Durability)
    props.put(ProducerConfig.ACKS_CONFIG, "all"); // Tüm replicas ack
    props.put(ProducerConfig.RETRIES_CONFIG, 5); // 5 retry attempts
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

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
      ConsumerFactory<String, Object> consumerFactory,
      CommonErrorHandler commonErrorHandler) {
    ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.setCommonErrorHandler(commonErrorHandler);

    // ✅ CRITICAL: Set AckMode to MANUAL for manual offset commit
    // This allows Acknowledgment parameter in @KafkaListener methods
    // Without this, Spring cannot inject Acknowledgment object
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

    return factory;
  }

  @Bean
  public CommonErrorHandler defaultErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
    // ✅ DLQ'ya gönderen recoverer
    // Topic adının başına "dlq." ekleyerek hedef DLQ topic'ini belirler
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
        kafkaTemplate,
        (record, exception) -> new TopicPartition(
            "dlq." + record.topic(),
            record.partition()));

    // ✅ FAZE 0: Exponential backoff retry
    // Retry delays: 100ms, 300ms (3x), 900ms (3x), 1000ms (max)
    ExponentialBackOff backoff = new ExponentialBackOff();
    backoff.setInitialInterval(100); // 100ms ilk interval
    backoff.setMultiplier(3.0); // 3x multiplier
    backoff.setMaxInterval(1000); // 1000ms maximum
    backoff.setMaxElapsedTime(5000); // 5s total max time

    DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backoff);
    // ✅ FAZE 0: DefaultErrorHandler automatically retries with exponential backoff
    // After retries exhausted, publishes to DLQ via DeadLetterPublishingRecoverer
    return errorHandler;
  }
}
