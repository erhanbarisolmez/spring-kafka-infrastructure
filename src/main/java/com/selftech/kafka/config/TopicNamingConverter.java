package com.selftech.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Topic Naming Converter - FAZE 4.5+
 *
 * API endpoints'den gelen topic names'i Kafka topic format'ına dönüştürür.
 *
 * Conversion Rules:
 * Input: /selfpark/testbox/data
 * Output: selfpark.testbox.data.v0
 *
 * Workflow:
 * 1. Leading "/" sil
 * 2. "/" ile ayrılmış parçaları "." ile birleştir
 * 3. ".v0" suffix ekle
 *
 * Örnekler:
 * ✅ /smartlock/device/operations → smartlock.device.operations.v0
 * ✅ /selfpark/test/box/data → selfpark.test.box.data.v0
 * ✅ /anomaly/detection/alerts → anomaly.detection.alerts.v0
 *
 * Usage:
 * @Autowired
 * private TopicNamingConverter converter;
 *
 * String kafkaTopic = converter.convertToKafkaTopic("/selfpark/testbox/data");
 * // Result: "selfpark.testbox.data.v0"
 */
@Slf4j
@Component
public class TopicNamingConverter {

    private static final String VERSION_SUFFIX = ".v0";
    private static final String SLASH_SEPARATOR = "/";
    private static final String DOT_SEPARATOR = ".";

    /**
     * API endpoint format'tan Kafka topic format'ına dönüştür
     *
     * Examples:
     * /selfpark/testbox/data → selfpark.testbox.data.v0
     * /smartlock/device/operations → smartlock.device.operations.v0
     *
     * @param apiTopicFormat API format topic string (e.g., /selfpark/testbox/data)
     * @return Kafka topic format (e.g., selfpark.testbox.data.v0)
     */
    public String convertToKafkaTopic(String apiTopicFormat) {
        if (apiTopicFormat == null || apiTopicFormat.isEmpty()) {
            log.warn("Topic name is null or empty");
            return null;
        }

        try {
            // Step 1: Leading "/" karakterini sil
            String cleaned = apiTopicFormat.startsWith(SLASH_SEPARATOR)
                ? apiTopicFormat.substring(1)
                : apiTopicFormat;

            if (cleaned.isEmpty()) {
                log.warn("Topic name is empty after removing slash - Input: {}", apiTopicFormat);
                return null;
            }

            // Step 2: "/" karakterlerini "." ile değiştir
            String normalized = cleaned.replace(SLASH_SEPARATOR, DOT_SEPARATOR);

            // Step 3: ".v0" suffix ekle (eğer yoksa)
            String kafkaTopic = normalized.endsWith(VERSION_SUFFIX)
                ? normalized
                : normalized + VERSION_SUFFIX;

            log.debug("Converted topic name - Input: {}, Output: {}", apiTopicFormat, kafkaTopic);
            return kafkaTopic;

        } catch (Exception e) {
            log.error("Failed to convert topic name - Input: {}, Error: {}", apiTopicFormat, e.getMessage());
            throw new TopicNamingConversionException("Failed to convert topic name: " + apiTopicFormat, e);
        }
    }

    /**
     * Kafka topic format'tan API endpoint format'ına dönüştür (reverse operation)
     *
     * Examples:
     * selfpark.testbox.data.v0 → /selfpark/testbox/data
     * smartlock.device.operations.v0 → /smartlock/device/operations
     *
     * @param kafkaTopic Kafka topic format (e.g., selfpark.testbox.data.v0)
     * @return API format topic string (e.g., /selfpark/testbox/data)
     */
    public String convertToApiTopic(String kafkaTopic) {
        if (kafkaTopic == null || kafkaTopic.isEmpty()) {
            log.warn("Kafka topic is null or empty");
            return null;
        }

        try {
            // Step 1: ".v0" suffix'i sil (eğer varsa)
            String cleaned = kafkaTopic.endsWith(VERSION_SUFFIX)
                ? kafkaTopic.substring(0, kafkaTopic.length() - VERSION_SUFFIX.length())
                : kafkaTopic;

            if (cleaned.isEmpty()) {
                log.warn("Topic name is empty after removing version suffix - Input: {}", kafkaTopic);
                return null;
            }

            // Step 2: "." karakterlerini "/" ile değiştir
            String apiTopic = SLASH_SEPARATOR + cleaned.replace(DOT_SEPARATOR, SLASH_SEPARATOR);

            log.debug("Converted Kafka topic to API format - Input: {}, Output: {}", kafkaTopic, apiTopic);
            return apiTopic;

        } catch (Exception e) {
            log.error("Failed to convert Kafka topic to API format - Input: {}, Error: {}", kafkaTopic, e.getMessage());
            throw new TopicNamingConversionException("Failed to convert Kafka topic: " + kafkaTopic, e);
        }
    }

    /**
     * Topic naming validation
     *
     * Valid pattern: /[a-z0-9]+(/[a-z0-9]+)+
     * Valid example: /smartlock/device/operations
     * Invalid example: /invalid, /Test/Case, /test-case
     *
     * @param apiTopicFormat Topic format to validate
     * @return true if valid, false otherwise
     */
    public boolean isValidTopicFormat(String apiTopicFormat) {
        if (apiTopicFormat == null || apiTopicFormat.isEmpty()) {
            return false;
        }

        // Pattern: / başla, [a-z0-9]+ segment'ler, / ile ayrıl, en az 2 segment
        return apiTopicFormat.matches("^/[a-z0-9]+((/[a-z0-9]+)+)?$");
    }

    /**
     * Custom exception for topic naming conversion errors
     */
    public static class TopicNamingConversionException extends RuntimeException {
        public TopicNamingConversionException(String message) {
            super(message);
        }

        public TopicNamingConversionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
