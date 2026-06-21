package com.selftech.kafka.converter;

import org.apache.avro.specific.SpecificRecord;

/**
 * Generic Event Converter Interface - FAZE 4.5
 *
 * Purpose:
 * Converts incoming JSON data from various sources (MQTT, API, IoT devices)
 * to Avro-compliant event objects for Kafka publishing.
 *
 * Architecture:
 * ├─ JSON Input (from MQTT, API, IoT devices)
 * │  └─ Timestamp: ISO8601 string, epoch long, epoch seconds, etc.
 * │  └─ Fields: Various formats, nullable, type mismatches
 * │
 * ├─ EventConverter (polymorphic)
 * │  ├─ Parse JSON
 * │  ├─ Normalize formats
 * │  ├─ Validate required fields
 * │  └─ Convert to Avro event
 * │
 * └─ Avro Event (ready for Kafka)
 *    └─ Timestamp: epoch milliseconds (long)
 *    └─ All fields: typed, validated
 *
 * Example:
 * ```
 * // JSON from MQTT: {"timestamp": "2025-11-03T14:21:31.759Z", ...}
 * String jsonData = "{...}";
 *
 * // Use factory to get appropriate converter
 * EventConverter converter = factory.getConverter("smartlock.mqtt.sensorData.v0");
 *
 * // Convert to Avro
 * SensorDataEvent event = (SensorDataEvent) converter.convert(jsonData);
 *
 * // Now ready for Kafka producer
 * eventProducer.sendEvent(topic, key, event);
 * ```
 *
 * @param <T> Target Avro event type (extends SpecificRecord)
 */
public interface EventConverter<T extends SpecificRecord> {

    /**
     * Convert JSON input to Avro event
     *
     * Responsibilities:
     * 1. Parse JSON string to object
     * 2. Extract fields with type normalization
     * 3. Handle timestamp format conversion (ISO8601 → epoch millis)
     * 4. Validate required fields
     * 5. Build Avro event using generated builder
     * 6. Handle conversion errors gracefully
     *
     * @param jsonInput JSON string from MQTT/API/IoT device
     * @return Avro event ready for Kafka publishing
     * @throws EventConversionException if conversion fails (validation, format, etc.)
     *
     * Example JSON input for SensorDataEvent:
     * {
     *   "batteryLevel": 95.5,
     *   "doorOpen": false,
     *   "weight": 45.2,
     *   "temperature": 22.5,
     *   "humidity": 65.0,
     *   "lockStatus": "LOCKED",
     *   "deviceStatus": "ACTIVE",
     *   "timestamp": "2025-11-03T14:21:31.759Z"
     * }
     */
    T convert(String jsonInput) throws EventConversionException;

    /**
     * Get target Avro event class
     *
     * Used by factory for validation and type checking
     *
     * @return Target Avro SpecificRecord class
     *
     * Example:
     * converter.getTargetEventClass() → SensorDataEvent.class
     */
    Class<T> getTargetEventClass();

    /**
     * Get topic patterns this converter handles
     *
     * Used by factory for topic-based converter selection
     *
     * @return Array of topic patterns (e.g., "smartlock.mqtt.*", "smartlock.event.boxOperation.*")
     *
     * Supported patterns:
     * - Exact match: "smartlock.mqtt.sensorData.v0"
     * - Prefix match: "smartlock.mqtt.*"
     * - Suffix match: "*.sensorData.v0"
     * - Wildcard: "*"
     */
    String[] getTopicPatterns();

    /**
     * Check if this converter can handle the given topic
     *
     * Used by factory for topic matching
     *
     * @param topic Topic name (e.g., "smartlock.mqtt.sensorData.v0")
     * @return true if this converter should handle this topic
     *
     * Example logic:
     * if ("smartlock.mqtt.sensorData.v0".matches(getTopicPatterns()[0])) {
     *     return true;
     * }
     */
    boolean canHandle(String topic);

    /**
     * Get converter name for logging
     *
     * @return Human-readable converter name (e.g., "SensorDataEventConverter")
     */
    String getConverterName();
}
