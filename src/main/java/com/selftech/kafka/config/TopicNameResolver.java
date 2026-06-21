package com.selftech.kafka.config;

/**
 * Topic Name Resolver - DLQ and Retry Topic Naming
 *
 * Provides centralized naming conventions for DLQ and retry topics.
 *
 * Naming Conventions:
 * - DLQ: dlq.{original-topic}
 * - Retry: retry.{level}.{original-topic}
 *
 * Examples:
 * - dlq("smartlock.event.payment.v1") → "dlq.smartlock.event.payment.v1"
 * - retry("smartlock.event.payment.v1", 1) → "retry.1.smartlock.event.payment.v1"
 * - retry("smartlock.event.payment.v1", 2) → "retry.2.smartlock.event.payment.v1"
 *
 * Usage:
 * String dlqTopic = TopicNameResolver.dlq("smartlock.event.payment.v1");
 * String retryTopic = TopicNameResolver.retry("smartlock.event.payment.v1", 1);
 *
 * Benefits:
 * - Single source of truth for DLQ/Retry naming
 * - No hardcoded strings
 * - Convention-based (predictable, discoverable)
 * - Easy to change naming convention globally
 */
public final class TopicNameResolver {

    private static final String DLQ_PREFIX = "dlq.";
    private static final String RETRY_PREFIX = "retry.";

    // Utility class - private constructor
    private TopicNameResolver() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }

    /**
     * Generate DLQ topic name from normal topic
     *
     * @param topic Normal topic name
     * @return DLQ topic name with "dlq." prefix
     */
    public static String dlq(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be null or blank");
        }
        return DLQ_PREFIX + topic;
    }

    /**
     * Generate retry topic name with level
     *
     * Retry levels represent exponential backoff stages:
     * - Level 1: First retry (e.g., 100ms delay)
     * - Level 2: Second retry (e.g., 1s delay)
     * - Level 3: Third retry (e.g., 10s delay)
     *
     * @param topic Normal topic name
     * @param level Retry level (1, 2, 3, etc.)
     * @return Retry topic name with "retry.{level}." prefix
     */
    public static String retry(String topic, int level) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be null or blank");
        }
        if (level <= 0) {
            throw new IllegalArgumentException("Retry level must be positive: " + level);
        }
        return RETRY_PREFIX + level + "." + topic;
    }

    /**
     * Check if a topic is a DLQ topic
     *
     * @param topic Topic name to check
     * @return true if topic starts with "dlq."
     */
    public static boolean isDlq(String topic) {
        return topic != null && topic.startsWith(DLQ_PREFIX);
    }

    /**
     * Check if a topic is a retry topic
     *
     * @param topic Topic name to check
     * @return true if topic starts with "retry."
     */
    public static boolean isRetry(String topic) {
        return topic != null && topic.startsWith(RETRY_PREFIX);
    }

    /**
     * Extract original topic name from DLQ topic
     *
     * @param dlqTopic DLQ topic name
     * @return Original topic name (without "dlq." prefix)
     */
    public static String extractFromDlq(String dlqTopic) {
        if (!isDlq(dlqTopic)) {
            throw new IllegalArgumentException("Not a DLQ topic: " + dlqTopic);
        }
        return dlqTopic.substring(DLQ_PREFIX.length());
    }

    /**
     * Extract original topic name from retry topic
     *
     * @param retryTopic Retry topic name
     * @return Original topic name (without "retry.{level}." prefix)
     */
    public static String extractFromRetry(String retryTopic) {
        if (!isRetry(retryTopic)) {
            throw new IllegalArgumentException("Not a retry topic: " + retryTopic);
        }
        // Format: retry.{level}.{topic}
        int secondDotIndex = retryTopic.indexOf('.', RETRY_PREFIX.length());
        if (secondDotIndex == -1) {
            throw new IllegalArgumentException("Invalid retry topic format: " + retryTopic);
        }
        return retryTopic.substring(secondDotIndex + 1);
    }
}
