package com.selftech.kafka.converter.converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.selftech.kafka.converter.AbstractEventConverter;
import com.selftech.kafka.converter.EventConversionException;
import com.selftech.kafka.models.avro.SensorDataEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * SensorDataEvent Converter - FAZE 4.5
 *
 * Converts JSON sensor data from MQTT/IoT devices to Avro SensorDataEvent
 *
 * Purpose:
 * Normalize and convert JSON sensor readings from ESP32/IoT devices to Avro format.
 *
 * Handles:
 * 1. Timestamp format conversion (ISO8601 string → epoch milliseconds)
 * 2. Optional field mapping with null defaults
 * 3. Type validation and error handling
 * 4. Event ID generation (if not provided)
 *
 * Input JSON Example:
 * ```
 * {
 *   "batteryLevel": 95.5,
 *   "doorOpen": false,
 *   "weight": 45.2,
 *   "temperature": 22.5,
 *   "humidity": 65.0,
 *   "lockStatus": "LOCKED",
 *   "deviceStatus": "ACTIVE",
 *   "timestamp": "2025-11-03T14:21:31.759Z",
 *   "gpsCoordinates": "40.7128,-74.0060"
 * }
 * ```
 *
 * Output:
 * SensorDataEvent with all fields properly typed and timestamp as epoch millis
 *
 * Supported Topic Patterns:
 * - "smartlock.mqtt.sensorData.v0" (exact match)
 * - "smartlock.mqtt.*" (wildcard for any MQTT sensor data)
 * - "smartlock.*.data.v0" (broader pattern)
 *
 * Error Handling:
 * - Missing required fields: throws EventConversionException
 * - Invalid timestamp format: uses current time as fallback
 * - Invalid JSON: throws EventConversionException
 *
 * Timestamp Handling:
 * Automatically detects and converts:
 * - ISO8601 format: "2025-11-03T14:21:31.759Z" → epoch millis
 * - Epoch seconds: 1699010491 → epoch millis
 * - Epoch millis: 1699010491759 → unchanged
 * - If null or missing: uses System.currentTimeMillis()
 *
 * @author FAZE 4.5 Implementation
 */
@Component
@Slf4j
public class SensorDataEventConverter extends AbstractEventConverter<SensorDataEvent> {

    // Topic patterns this converter handles
    private static final String[] TOPIC_PATTERNS = {
        "smartlock.mqtt.sensorData.v0",
        "smartlock.mqtt.*",
        "smartlock.*.data.v0"
    };

    /**
     * Convert JSON sensor data to Avro SensorDataEvent
     *
     * @param jsonInput JSON string from IoT device
     * @return SensorDataEvent ready for Kafka publishing
     * @throws EventConversionException if conversion fails
     */
    @Override
    public SensorDataEvent convert(String jsonInput) throws EventConversionException {
        logConversionStart(jsonInput);

        try {
            // Step 1: Parse JSON
            JsonNode node = parseJson(jsonInput);

            // Step 2: Extract fields with type normalization
            // Note: All fields are optional except we generate eventId if not provided

            String eventId = extractString(node, "eventId", false, generateEventId());
            Double batteryLevel = extractDouble(node, "batteryLevel", false, null);
            Boolean doorOpen = extractBoolean(node, "doorOpen", false, null);
            Double weight = extractDouble(node, "weight", false, null);
            String lockStatus = extractString(node, "lockStatus", false, null);
            Double temperature = extractDouble(node, "temperature", false, null);
            Double humidity = extractDouble(node, "humidity", false, null);
            String gpsCoordinates = extractString(node, "gpsCoordinates", false, null);
            String deviceStatus = extractString(node, "deviceStatus", false, null);

            // Step 3: Handle timestamp conversion
            // Required: timestamp field must be present (but can use current time if missing)
            long timestamp;
            if (node.has("timestamp") && !node.get("timestamp").isNull()) {
                timestamp = normalizeTimestamp(node.get("timestamp"));
            } else {
                logger.warn("Timestamp field missing, using current time. EventId: {}", eventId);
                timestamp = System.currentTimeMillis();
            }

            // Step 4: Build Avro event using generated builder
            SensorDataEvent event = SensorDataEvent.newBuilder()
                .setEventId(eventId)
                .setBatteryLevel(batteryLevel)
                .setDoorOpen(doorOpen)
                .setWeight(weight)
                .setLockStatus(lockStatus)
                .setTemperature(temperature)
                .setHumidity(humidity)
                .setGpsCoordinates(gpsCoordinates)
                .setDeviceStatus(deviceStatus)
                .setTimestamp(java.time.Instant.ofEpochMilli(timestamp))
                .setCorrelationId(null)  // Will be set by CentralEventCoordinator if needed
                .setEventType("SensorDataReceived")
                .setSource("SMARTLOCK")
                .setSchemaVersion("1.0")
                .setMetadata(null)  // Will be enriched by MqttToKafkaBridgeService with deviceCode
                .build();

            logConversionSuccess(eventId);
            return event;

        } catch (EventConversionException e) {
            logConversionFailure("Event conversion exception", e);
            throw e;
        } catch (Exception e) {
            String errorMsg = "Unexpected error during conversion: " + e.getMessage();
            logConversionFailure(errorMsg, e);
            throw new EventConversionException(
                getTargetEventClass().getSimpleName(),
                jsonInput,
                errorMsg,
                e
            );
        }
    }

    /**
     * Get target Avro event class
     */
    @Override
    public Class<SensorDataEvent> getTargetEventClass() {
        return SensorDataEvent.class;
    }

    /**
     * Get topic patterns this converter handles
     */
    @Override
    public String[] getTopicPatterns() {
        return TOPIC_PATTERNS;
    }

    /**
     * Get converter name for logging
     */
    @Override
    public String getConverterName() {
        return "SensorDataEventConverter";
    }
}
