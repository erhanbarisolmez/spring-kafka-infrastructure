package com.selftech.kafka.config;

/**
 * Type-safe Topic Key Registry
 *
 * Centralized enum for all Kafka topic keys defined in application.yml.
 * Provides compile-time safety, IDE autocomplete, and prevents typos.
 *
 * Usage:
 * - Instead of: topicRegistry.getTopicName("selfpark-park-entry")
 * - Use:        topicRegistry.getTopicName(TopicKey.SELFPARK_PARK_ENTRY)
 *
 * Benefits:
 * - ✅ Type-safe: Compile-time checking
 * - ✅ IDE autocomplete: See all available topics
 * - ✅ Refactoring support: Rename safely
 * - ✅ No typos: Impossible to misspell
 * - ✅ Single source of truth: All topic keys in one place
 *
 * @see KafkaTopicRegistry
 */
public enum TopicKey {

    // ========================================
    // SmartLock Topics (9 total)
    // ========================================

    /**
     * MQTT sensor data stream from IoT devices
     * Topic: smartlock.mqtt.sensorData.v0
     */
    SMARTLOCK_MQTT_SENSOR_DATA("smartlock-mqtt-sensor-data"),

    /**
     * SmartLock device operations (lock/unlock, status changes)
     * Topic: smartlock.event.deviceOperations.v0
     */
    SMARTLOCK_DEVICE_OPERATIONS("smartlock-device-operations"),

    /**
     * SmartLock box operations (open/close, weight changes)
     * Topic: smartlock.event.boxOperation.v0
     */
    SMARTLOCK_BOX_OPERATION("smartlock-box-operation"),

    /**
     * Customer transactions (rentals, returns)
     * Topic: smartlock.event.customerTransactions.v0
     */
    SMARTLOCK_CUSTOMER_TRANSACTIONS("smartlock-customer-transactions"),

    /**
     * SMS events (sent, delivered, failed)
     * Topic: smartlock.event.smsEvents.v0
     */
    SMARTLOCK_SMS_EVENTS("smartlock-sms-events"),

    /**
     * Payment events (initiated, completed, failed)
     * Topic: smartlock.event.paymentEvents.v0
     */
    SMARTLOCK_PAYMENT_EVENTS("smartlock-payment-events"),

    /**
     * SmartLock anomaly alerts
     * Topic: smartlock.anomaly.alerts.v0
     */
    SMARTLOCK_ANOMALY_ALERTS("smartlock-anomaly-alerts"),

    /**
     * SmartLock anomaly detection events
     * Topic: smartlock.anomaly.detection.v0
     */
    SMARTLOCK_ANOMALY_DETECTION("smartlock-anomaly-detection"),

    /**
     * SmartLock sensor data anomalies
     * Topic: smartlock.anomaly.sensorData.v0
     */
    SMARTLOCK_ANOMALY_SENSOR_DATA("smartlock-anomaly-sensor-data"),

    // ========================================
    // SelfPark Topics (9 total)
    // ========================================

    /**
     * Authentication events (login, logout, token refresh)
     * Topic: selfpark.event.auth.v0
     */
    SELFPARK_AUTH("selfpark-auth"),

    /**
     * Payment events for parking
     * Topic: selfpark.event.payment.v0
     */
    SELFPARK_PAYMENT("selfpark-payment"),

    /**
     * Park exit events (vehicle leaving)
     * Topic: selfpark.event.parkExit.v0
     */
    SELFPARK_PARK_EXIT("selfpark-park-exit"),

    /**
     * Park entry events (vehicle entering)
     * Topic: selfpark.event.parkEntry.v0
     */
    SELFPARK_PARK_ENTRY("selfpark-park-entry"),

    /**
     * Balance change events (deposits, withdrawals)
     * Topic: selfpark.event.balance.v0
     */
    SELFPARK_BALANCE("selfpark-balance"),

    /**
     * Invoice generation events
     * Topic: selfpark.event.invoice.v0
     */
    SELFPARK_INVOICE("selfpark-invoice"),

    /**
     * Park information updates (capacity, pricing)
     * Topic: selfpark.event.parkInfo.v0
     */
    SELFPARK_PARK_INFO("selfpark-park-info"),

    /**
     * SelfPark anomaly alerts
     * Topic: selfpark.anomaly.alerts.v0
     */
    SELFPARK_ANOMALY_ALERTS("selfpark-anomaly-alerts"),

    /**
     * SelfPark anomaly detection events
     * Topic: selfpark.anomaly.detection.v0
     */
    SELFPARK_ANOMALY_DETECTION("selfpark-anomaly-detection"),

    // ========================================
    // Anomaly Detection Topics (4 total)
    // ========================================

    /**
     * Global anomaly detection alerts (all domains)
     * Topic: anomaly.detection.event.alerts.v0
     */
    ANOMALY_DETECTION_ALERTS("anomaly-detection-alerts"),

    /**
     * Anomaly detection metrics (precision, recall, F1)
     * Topic: anomaly.detection.event.metrics.v0
     */
    ANOMALY_DETECTION_METRICS("anomaly-detection-metrics"),

    /**
     * Batch processing anomaly results
     * Topic: anomaly.detection.event.batch.v0
     */
    ANOMALY_DETECTION_BATCH("anomaly-detection-batch"),

    /**
     * Generic anomaly detection events
     * Topic: anomaly.detection.v0
     */
    ANOMALY_DETECTION("anomaly-detection");

    // ========================================
    // Enum Implementation
    // ========================================

    /**
     * The YAML key used in application.yml (app.kafka.topics.{key})
     */
    private final String key;

    /**
     * Constructor
     *
     * @param key YAML topic key from application.yml
     */
    TopicKey(String key) {
        this.key = key;
    }

    /**
     * Get the YAML key for this topic
     *
     * @return Topic key string (e.g., "selfpark-park-entry")
     */
    public String getKey() {
        return key;
    }

    /**
     * Get user-friendly display name
     *
     * @return Enum name formatted (e.g., "SELFPARK_PARK_ENTRY")
     */
    @Override
    public String toString() {
        return name() + " [" + key + "]";
    }
}
